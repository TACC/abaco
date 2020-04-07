import requests
import json
import datetime
import time

from config import Config
from models import dict_to_camel, Actor, Execution, ExecutionsSummary, Nonce, Worker, get_permissions, \
    set_permission
from worker import shutdown_workers, shutdown_worker
from stores import actors_store, executions_store, logs_store, nonce_store, permissions_store
from prometheus_client import start_http_server, Summary, MetricsHandler, Counter, Gauge, generate_latest
from channels import ActorMsgChannel, CommandChannel, ExecutionResultsChannel
from agaveflask.logs import get_logger
logger = get_logger(__name__)

message_gauges = {}
worker_gaueges = {}
cmd_channel_gauges = {}
PROMETHEUS_URL = 'http://172.17.0.1:9090'
DEFAULT_SYNC_MAX_IDLE_TIME = 600 # defaults to 10*60 = 600 s = 10 min

MAX_WORKERS_PER_HOST = Config.get('spawner', 'max_workers_per_host')

command_gauge = Gauge(
    'message_count_for_command_channel',
    'Number of messages currently in this command channel',
    ['name'])

def create_gauges(actor_ids):
    logger.debug("METRICS: Made it to create_gauges; actor_ids: {}".format(actor_ids))
    inbox_lengths = {}
    for actor_id in actor_ids:
        logger.debug("top of for loop for actor_id: {}".format(actor_id))

        channel_name = None
        try:
            actor = actors_store[actor_id]
        except KeyError:
            logger.error("actor {} does not exist.".format(actor_id))
            continue

            # If the actor doesn't have a gauge, add one
        if actor_id not in message_gauges.keys():
            try:
                g = Gauge(
                    'message_count_for_actor_{}'.format(actor_id.decode("utf-8").replace('-', '_')),
                    'Number of messages for actor {}'.format(actor_id.decode("utf-8").replace('-', '_'))
                )
                message_gauges.update({actor_id: g})
                logger.debug('Created gauge {}'.format(g))
            except Exception as e:
                logger.error("got exception trying to create/instantiate the gauge; "
                             "actor {}; exception: {}".format(actor_id, e))
        else:
            # Otherwise, get this actor's existing gauge
            try:
                g = message_gauges[actor_id]
            except Exception as e:
                logger.info("got exception trying to instantiate an existing gauge; "
                            "actor: {}: exception:{}".format(actor_id, e))

            # Update this actor's command channel metric
            channel_name = actor.get("queue")

            queues_list = Config.get('spawner', 'host_queues').replace(' ', '')
            valid_queues = queues_list.split(',')

            if not channel_name or channel_name not in valid_queues:
                channel_name = 'default'

        # Update this actor's gauge to its current # of messages
        try:
            ch = ActorMsgChannel(actor_id=actor_id.decode("utf-8"))
        except Exception as e:
            logger.error("Exception connecting to ActorMsgChannel: {}".format(e))
            raise e
        result = {'messages': len(ch._queue._queue)}
        inbox_lengths[actor_id.decode("utf-8")] = len(ch._queue._queue)
        ch.close()
        g.set(result['messages'])
        logger.debug("METRICS: {} messages found for actor: {}.".format(result['messages'], actor_id))

        # add a worker gauge for this actor if one does not exist
        if actor_id not in worker_gaueges.keys():
            try:
                g = Gauge(
                    'worker_count_for_actor_{}'.format(actor_id.decode("utf-8").replace('-', '_')),
                    'Number of workers for actor {}'.format(actor_id.decode("utf-8").replace('-', '_'))
                )
                worker_gaueges.update({actor_id: g})
                logger.debug('Created worker gauge {}'.format(g))
            except Exception as e:
                logger.info("got exception trying to instantiate the Worker Gauge: {}".format(e))
        else:
            # Otherwise, get the worker gauge that already exists
            g = worker_gaueges[actor_id]

        # Update this actor's worker IDs
        workers = Worker.get_workers(actor_id)
        result = {'workers': len(workers)}
        g.set(result['workers'])

    if not channel_name:
        return
    ch = CommandChannel(name=channel_name)
    cmd_length = len(ch._queue._queue)
    command_gauge.labels(channel_name).set(cmd_length)
    logger.debug("METRICS COMMAND CHANNEL {} size: {}".format(channel_name, command_gauge))
    ch.close()

    # Return actor_ids so we don't have to query for them again later
    return actor_ids, inbox_lengths, cmd_length


def calc_change_rate(data, last_metric, actor_id):
    change_rate = 0
    try:
        previous_data = last_metric[actor_id]
        previous_message_count = int(previous_data[0]['value'][1])
        try:
            # what is data?
            current_message_count = int(data[0]['value'][1])
            change_rate = current_message_count - previous_message_count
        except:
            logger.debug("Could not calculate change rate.")
    except:
        logger.info("No previous data yet for new actor {}".format(actor_id))
    return change_rate


def allow_autoscaling(max_workers, num_workers, cmd_length):
    # first check if the number of messages on the command channel exceeds the limit:
    try:
        max_cmd_length = int(Config.get('spawner', 'max_cmd_length'))
    except:
        max_cmd_length = 10

    if cmd_length > max_cmd_length:
        return False
    if int(num_workers) >= int(max_workers):
        logger.debug('METRICS NO AUTOSCALE - criteria not met. {} '.format(num_workers))
        return False

    logger.debug('METRICS AUTOSCALE - criteria met. {} '.format(num_workers))
    return True

def scale_up(actor_id):
    tenant, aid = actor_id.decode('utf8').split('_')
    logger.debug('METRICS Attempting to create a new worker for {}'.format(actor_id))
    try:
        # create a worker & add to this actor
        actor = Actor.from_db(actors_store[actor_id])
        worker_id = Worker.request_worker(tenant=tenant, actor_id=actor_id)
        logger.info("New worker id: {}".format(worker_id))
        if actor.queue:
            channel_name = actor.queue
        else:
            channel_name = 'default'
        ch = CommandChannel(name=channel_name)
        ch.put_cmd(actor_id=actor.db_id,
                   worker_id=worker_id,
                   image=actor.image,
                   tenant=tenant,
                   stop_existing=False)
        ch.close()
        logger.debug('METRICS Added worker successfully for {}'.format(actor_id))
        return channel_name
    except Exception as e:
        logger.debug("METRICS - SOMETHING BROKE: {} - {} - {}".format(type(e), e, e.args))
        return None


def scale_down(actor_id, is_sync_actor=False):
    logger.debug(f"top of scale_down for actor_id: {actor_id}")
    workers = Worker.get_workers(actor_id)
    logger.debug('METRICS NUMBER OF WORKERS: {}'.format(len(workers)))
    try:
        while len(workers) > 0:
            logger.debug('METRICS made it STATUS check')
            check_ttl = False
            sync_max_idle_time = 0
            if len(workers) == 1 and is_sync_actor:
                logger.debug("only one worker, on sync actor. checking worker idle time..")
                try:
                    sync_max_idle_time = int(Config.get('workers', 'sync_max_idle_time'))
                except Exception as e:
                    logger.error(f"Got exception trying to read sync_max_idle_time from config; e:{e}")
                    sync_max_idle_time = DEFAULT_SYNC_MAX_IDLE_TIME
                check_ttl = True
            worker = workers.popitem()[1]
            if check_ttl:
                try:
                    last_execution = int(float(worker.get('last_execution_time', 0)))
                except Exception as e:
                    logger.error(f"metrics got exception trying to compute last_execution! e: {e}")
                    last_execution = 0
                # if worker has made zero executions, use the create_time
                if last_execution == 0:
                    last_execution = worker.get('create_time', 0)
                logger.debug("using last_execution: {}".format(last_execution))
                try:
                    last_execution = int(float(last_execution))
                except:
                    logger.error("Could not cast last_execution {} to int(float()".format(last_execution))
                    last_execution = 0
                if last_execution + sync_max_idle_time < time.time():
                    # shutdown worker
                    logger.info("OK to shut down this worker -- beyond ttl.")
                    # continue onto additional checks below
                else:
                    logger.info("Autoscaler not shuting down this worker - still time left.")
                    break

            logger.debug('METRICS SCALE DOWN current worker: {}'.format(worker['status']))
            # check status of the worker is ready
            if worker['status'] == 'READY':
                # scale down
                try:
                    shutdown_worker(actor_id, worker['id'], delete_actor_ch=False)
                    continue
                except Exception as e:
                    logger.debug('METRICS ERROR shutting down worker: {} - {} - {}'.format(type(e), e, e.args))
                logger.debug('METRICS shut down worker {}'.format(worker['id']))

    except IndexError:
        logger.debug('METRICS only one worker found for actor {}. '
                     'Will not scale down'.format(actor_id))
    except Exception as e:
        logger.debug("METRICS SCALE UP FAILED: {}".format(e))

