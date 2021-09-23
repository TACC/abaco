import requests
import json
import datetime
import time

from common.config import conf
from models import dict_to_camel, Actor, Execution, ExecutionsSummary, Nonce, Worker, get_permissions, \
    set_permission, site
from worker import shutdown_workers, shutdown_worker
from stores import actors_store, executions_store, logs_store, nonce_store, permissions_store
from prometheus_client import start_http_server, Summary, MetricsHandler, Counter, Gauge, generate_latest
from channels import ActorMsgChannel, CommandChannel, ExecutionResultsChannel
from common.logs import get_logger
logger = get_logger(__name__)

message_gauges = {}
worker_gaueges = {}
cmd_channel_gauges = {}
PROMETHEUS_URL = 'http://172.17.0.1:9090'
DEFAULT_SYNC_MAX_IDLE_TIME = 600 # defaults to 10*60 = 600 s = 10 min

MAX_WORKERS_PER_HOST = conf.spawner_max_workers_per_host

command_gauge = Gauge(
    'message_count_for_command_channel',
    'Number of messages currently in this command channel',
    ['name'])

#####################
# NOTE: This module has been deprecated and is no longer used by the Abaco runtime.
# ###################


def create_gauges(actor_ids):
    """
    Creates a Prometheus gauge for each actor id. The gauge is used to track the number of
    pending messages in the actor's queue.
    :param actor_ids: list of actors that should be processed. Does not include stateful actors or
    actors in a shutting down state.
    :return:
    """
    logger.debug(f"top of create_gauges; actor_ids: {actor_ids}")
    # dictionary mapping actor_ids to their message queue lengths
    inbox_lengths = {}
    for actor_id in actor_ids:
        logger.debug(f"top of for loop for actor_id: {actor_id}")
        # first, make sure the actor still exists in the actor store
        try:
            actor = actors_store[site()][actor_id]
        except KeyError:
            logger.info(f"actor {actor_id} does not exist in store; continuing to next actor.")
            continue

        # If the actor doesn't have a gauge, add one
        if actor_id not in message_gauges.keys():
            try:
                g = Gauge(
                    f"message_count_for_actor_{actor_id.replace('-', '_')}",
                    f"Number of messages for actor {actor_id.replace('-', '_')}"
                )
                message_gauges.update({actor_id: g})
                logger.debug(f'Created gauge {g}')
            except Exception as e:
                logger.error("got exception trying to create/instantiate the gauge; "
                             "actor {}; exception: {}".format(actor_id, e))
                g = None
        else:
            # Otherwise, get this actor's existing gauge
            try:
                g = message_gauges[actor_id]
            except Exception as e:
                logger.info("got exception trying to instantiate an existing gauge; "
                            "actor: {}: exception:{}".format(actor_id, e))
                g = None
        # Update this actor's gauge to its current # of messages
        try:
            ch = ActorMsgChannel(actor_id=actor_id)
            msg_length = len(ch._queue._queue)
        except Exception as e:
            logger.error(f"Exception connecting to ActorMsgChannel: {e}")
            raise e
        ch.close()
        result = {'messages': msg_length}
        # add the actor's current message queue length to the inbox_lengths in-memory variable
        inbox_lengths[actor_id] = msg_length
        # if we were able to create the gauge, set it to the current message:
        if g:
            try:
                g.set(result['messages'])
            except Exception as e:
                logger.error(f"Got exception trying to set the messages on the gauge for actor: {actor_id}; "
                             f"exception: {e}")
        logger.debug(f"METRICS: {result['messages']} messages found for actor: {actor_id}.")

        # add a worker gauge for this actor if one does not exist
        if actor_id not in worker_gaueges.keys():
            try:
                g = Gauge(
                    f"worker_count_for_actor_{actor_id.replace('-', '_')}",
                    f"Number of workers for actor {actor_id.replace('-', '_')}"
                )
                worker_gaueges.update({actor_id: g})
                logger.debug(f'Created worker gauge {g}')
            except Exception as e:
                logger.info(f"got exception trying to instantiate the Worker Gauge: {e}")
        else:
            # Otherwise, get the worker gauge that already exists
            g = worker_gaueges[actor_id]

        # Update this actor's worker IDs
        workers = Worker.get_workers(actor_id)
        result = {'workers': len(workers)}
        try:
            g.set(result['workers'])
        except Exception as e:
            logger.error(f"got exception trying to set the worker gauge for actor {actor_id}; exception: {e}")
        logger.debug(f"METRICS: {result['workers']} workers found for actor: {actor_id}.")

        # Update this actor's command channel metric
        # channel_name = actor.get("queue")
        #
        # queues_list = Config.get('spawner', 'host_queues').replace(' ', '')
        # valid_queues = queues_list.split(',')
        #
        # if not channel_name or channel_name not in valid_queues:
        #     channel_name = 'default'
        #
        # if not channel_name:
        #     # TODO -- this must be changed. there is no way returning no arguments will result in
        #     # anythng but an exception. The calling function is expecting 3 arguments...
        #     # if we really want to blow up right here we should just raise an appropriate exception.
        #     return

    # TODO -- this code needs to be fixed. What follows is only a partial fix; what I think we want to do
    # is set the length of all of the different command channels once at the end of this loop. What was
    # happening instead was that it was only setting one of the command channel's lengths -- whatever command
    # channel happened to belong to the last actor in the loop.
    channel_name = 'default'
    ch = CommandChannel(name=channel_name)
    cmd_length = len(ch._queue._queue)
    command_gauge.labels(channel_name).set(cmd_length)
    logger.debug(f"METRICS COMMAND CHANNEL {channel_name} size: {command_gauge}")
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
        logger.info(f"No previous data yet for new actor {actor_id}")
    return change_rate


def allow_autoscaling(max_workers, num_workers, cmd_length):
    """
    This function returns True if the conditions for a specific actor indicate it could be scaled up.
    Note that this does NOT mean the actor WILL be scaled up, as this function does not take into account
    the current number of messages queued for the actor.
    :param max_workers: The maximum number of workers allowed for a specific actor.
    :param num_workers: The current number of workers for a specific actor.
    :param cmd_length: The current lenght of the associated command channel for a specific actor.
    :return:
    """
    logger.debug(f"top of allow_autoscaling; max_workers: {max_workers}, num_workers: {num_workers}, "
                 f"cmd_length: {cmd_length}")
    # first check if the number of messages on the command channel exceeds the limit:
    try:
        max_cmd_length = int(conf.spawner_max_cmd_length)
    except Exception as e:
        logger.info(f"Autoscaler got exception trying to compute the max_cmd_lenght; using 10. E"
                    f"xception: {e}")
        max_cmd_length = 10

    if cmd_length > max_cmd_length:
        logger.info(f"Will NOT scale up: Current cmd_length ({cmd_length}) is greater than ({max_cmd_length}).")
        return False
    if int(num_workers) >= int(max_workers):
        logger.debug(f'Will NOT scale up: num_workers ({num_workers}) was >= max_workers ({max_workers})')
        return False

    logger.debug(f'Will scale up: num_workers ({num_workers}) was <= max_workers ({max_workers})')
    return True

def scale_up(actor_id):
    tenant, aid = actor_id.split('_')
    logger.debug(f'METRICS Attempting to create a new worker for {actor_id}')
    try:
        # create a worker & add to this actor
        actor = Actor.from_db(actors_store[site()][actor_id])
        worker_id = Worker.request_worker(tenant=tenant, actor_id=actor_id)
        logger.info(f"New worker id: {worker_id}")
        if actor.queue:
            channel_name = actor.queue
        else:
            channel_name = 'default'
        ch = CommandChannel(name=channel_name)
        ch.put_cmd(actor_id=actor.db_id,
                   worker_id=worker_id,
                   image=actor.image,
                   revision=actor.revision,
                   tenant=tenant,
                   site_id=site(),
                   stop_existing=False)
        ch.close()
        logger.debug(f'METRICS Added worker successfully for {actor_id}')
        return channel_name
    except Exception as e:
        logger.debug(f"METRICS - SOMETHING BROKE: {type(e)} - {e} - {e.args}")
        return None


def scale_down(actor_id, is_sync_actor=False):
    """
    This function determines whether an actor's worker pool should be scaled down and if so,
    initiates the scaling down.
    :param actor_id: the actor_id
    :param is_sync_actor: whether or not the actor has the SYNC hint.
    :return:
    """
    logger.debug(f"top of scale_down for actor_id: {actor_id}")
    # we retrieve the current workers again as we will need the entire worker objects (not just the number).
    workers = Worker.get_workers(actor_id)
    logger.debug(f'scale_down number of workers: {len(workers)}')
    try:
        # iterate through all the actor's workers and determine if they should be shut down.
        while len(workers) > 0:
            # whether to check the TTL for this worker; we only check TTL for SYNC actors; for non-sync,
            # workers are immediately shut down when the actor has no messages.
            check_ttl = False
            sync_max_idle_time = 0
            if len(workers) == 1 and is_sync_actor:
                logger.debug("only one worker, on sync actor. checking worker idle time..")
                sync_max_idle_time = conf.worker_sync_max_idle_time
                check_ttl = True
            worker = workers.pop()
            logger.debug(f"check_ttl: {check_ttl} for worker: {worker}")
            if check_ttl:
                last_execution = worker.get('last_execution_time', 0)
                if not last_execution == 0:
                    try:
                        last_execution = int(last_execution.timestamp())
                    except Exception as e:
                        logger.error(f"metrics got exception trying to compute last_execution! e: {e}")
                        last_execution = 0
                # if worker has made zero executions, use the create_time
                if last_execution == 0:
                    last_execution = worker.get('create_time', 0)
                    if not last_execution == 0:
                        try:
                            last_execution = int(last_execution.timestamp())
                        except:
                            logger.error(f"Could not cast last_execution {last_execution} to int(float()")
                            last_execution = 0
                logger.debug(f"using last_execution: {last_execution}")
                if last_execution + sync_max_idle_time < time.time():
                    # shutdown worker
                    logger.info("OK to shut down this worker -- beyond ttl.")
                    # continue onto additional checks below
                else:
                    logger.info("Autoscaler not shuting down this worker - still time left.")
                    continue

            logger.debug('based on TTL, worker could be scaled down.')
            # check status of the worker is ready
            if worker['status'] == 'READY':
                # scale down
                logger.debug('worker was in READY status; attempting shutdown.')
                try:
                    shutdown_worker(actor_id, worker['id'], delete_actor_ch=False)
                    logger.debug('sent worker shutdown message.')
                    continue
                except Exception as e:
                    logger.debug(f'METRICS ERROR shutting down worker: {type(e)} - {e} - {e.args}')
                logger.debug(f"METRICS shut down worker {worker['id']}")

    except IndexError:
        logger.debug('METRICS only one worker found for actor {}. '
                     'Will not scale down'.format(actor_id))
    except Exception as e:
        logger.debug(f"METRICS SCALE UP FAILED: {e}")

