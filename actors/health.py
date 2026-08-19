# Ensures that:
# 1. all worker containers in the database are still responsive; workers that have stopped
#    responding are shutdown and removed from the database.
# 2. Enforce ttl for idle workers.
#
# In the future, this module will also implement:
# 3. all actors with stateless=true have a number of workers proportional to the messages in the queue.

# Execute from a container on a schedule as follows:
# docker run -it --rm -v /var/run/docker.sock:/var/run/docker.sock abaco/core-v3 python3 -u /actors/health.py

import os
import shutil
import time
import datetime
import copy

from queues import ChannelTimeoutException
import rabbit_mgmt

from auth import get_tenants, get_tenant_verify
import codes
from tapisservice.config import conf
from tapisservice.logs import get_logger
from models import Actor, Execution, Worker, is_hashid, get_current_utc_time, site
from channels import ActorMsgChannel, CommandChannel, WorkerChannel
from stores import actors_store, executions_store, workers_store
from worker import shutdown_worker
from kubernetes import client, config


# Give permissions some files since we are running as Tapis user, not root.
# folder_permissions is made to require no sudo permissions to run.
os.system(f'sudo /usr/local/sbin/folder_permissions.sh /home/tapis/runtime_files')

# This isn't currently working because they require different code in some functions
if conf.container_backend == "docker":
    from docker_utils import rm_container, get_current_worker_containers, container_running, run_container
    # Another run of folder_permissions, for docker.sock
    os.system(f'sudo /usr/local/sbin/folder_permissions.sh /var/run/docker.sock')
elif conf.container_backend == "kubernetes":
    from kubernetes_utils import rm_container, get_current_worker_containers, container_running, run_container
    # k8 client creation
    config.load_incluster_config()
    k8 = client.CoreV1Api()

TAG = os.environ.get('TAG') or conf.version or ""
if not TAG[0] == ":":
    TAG = f":{TAG}"
AE_IMAGE = f"{os.environ.get('AE_IMAGE', 'abaco/core-v3')}{TAG}"

logger = get_logger(__name__)

# max executions allowed in a mongo document; if the total executions for a given actor exceeds this number,
# the health process will place
MAX_EXECUTIONS_PER_MONGO_DOC = 25000

def get_actor_ids():
    """Returns the list of actor ids currently registered."""
    return [aid for aid in actors_store[site()]]

def check_workers_store(ttl):
    logger.debug("Top of check_workers_store.")
    """Run through all workers in workers_store[site()] and ensure there is no data integrity issue."""
    for worker in workers_store[site()].items():
        try:
            check_worker_health(worker, ttl)
        except Exception as e:
            worker_id = worker.get('id', 'unknown') if isinstance(worker, dict) else 'unknown'
            logger.error(f"Exception in check_workers_store for worker {worker_id}: {e}")

def get_worker(wid):
    """
    Check to see if a string `wid` is the id of a worker in the worker store.
    If so, return it; if not, return None.
    """
    logger.debug(f"top of get_worker for wid: {wid}")
    worker = workers_store[site()].items({'id': wid})
    if worker:
        return worker[0]
    return None

def clean_up_socket_dirs():
    logger.debug("top of clean_up_socket_dirs")
    # Following gets the container path dir and cleans that, ignores host
    socket_dir = conf.worker_socket_paths.split(':')[1]
    logger.debug(f"processing socket_dir: {socket_dir}")
    for p in os.listdir(socket_dir):
        # check to see if p is a worker
        worker = get_worker(p)
        if not worker:
            path = os.path.join(socket_dir, p)
            logger.debug(f"Determined that {p} was not a worker; deleting directory: {path}.")
            shutil.rmtree(path)

def clean_up_fifo_dirs():
    logger.debug("top of clean_up_fifo_dirs")
    # Following gets the container path dir and cleans that, ignores host
    fifo_dir = conf.worker_fifo_paths.split(':')[1]
    logger.debug(f"processing fifo_dir: {fifo_dir}")
    for p in os.listdir(fifo_dir):
        # check to see if p is a worker
        logger.debug(f"Checking if fifodir {p} has an associated worker.")
        worker = get_worker(p)
        if not worker:
            path = os.path.join(fifo_dir, p)
            logger.debug(f"Determined that {p} was not a worker; deleting directory: {path}.")
            shutil.rmtree(path)

def clean_up_ipc_dirs():
    """Remove all directories created for worker sockets and fifos"""
    clean_up_socket_dirs()
    clean_up_fifo_dirs()

def check_worker_health(worker, ttl):
    """Check the specific health of a worker object in the db."""
    logger.debug("top of check_worker_health")
    actor_id = worker.get('actor_id', '')
    worker_id = worker.get('id', '')
    logger.info(f"Checking status of worker from db with worker_id: {worker_id}")
    ### This doesn't seem like it should be neccessary, commenting for now. This entire function wasn't
    ### running until May 17th, so the omission should be fine.
    # if not worker_id:
    #     logger.error(f"Corrupt data in the workers_store[site()]. Worker object without an id attribute. {worker}")
    #     try:
    #         workers_store[site()].pop_field([actor_id])
    #     except KeyError:
    #         # it's possible another health agent already removed the worker record.
    #         pass
    #     return None
    # make sure the actor id still exists, delete worker if not.
    try:
        actors_store[site()][actor_id]
    except KeyError:
        logger.error(f"Corrupt data in the workers_store[site()]. Worker object found but no corresponding actor. {worker}")
        try:
            hard_delete_worker(actor_id, worker_id, reason_str="Worker's actor_id field does not resolve to existing actor. Deleting worker.")
            # This used to be the code, I'm not really quite sure what it was doing.
            # todo - removing worker objects from db can be problematic if other aspects of the worker
            # are not cleaned up properly. this code should be reviewed.
            #workers_store[site()].pop_field([actor_id])
        except KeyError:
            # it's possible another health agent already removed the worker record.
            pass
        return None
    except Exception as e:
        logger.error(f"Error getting actor_id: {actor_id} tied to worker_id: {worker_id}. error: {e}")
        return None

def zero_out_workers_db():
    """
    Set all workers collections in the db to empty. Run this as part of a maintenance; steps:
      1) remove all docker containers
      2) run this function
    :return:
    """
    for worker in workers_store[site()].items(proj_inp=None):
        del workers_store[site()][worker['_id']]

def delete_worker_rabbit_queues(worker_id):
    """
    Delete a worker's rabbit queues (worker_<id> and spawner_worker_<id>) via the management API.
    A hard-deleted worker never consumes or deletes its own queues, so without this the durable
    queues live in the broker forever.
    """
    for queue_name in (f"worker_{worker_id}", f"spawner_worker_{worker_id}"):
        try:
            if rabbit_mgmt.delete_queue(site(), queue_name):
                logger.info(f"deleted rabbit queue {queue_name} for hard-deleted worker {worker_id}")
        except Exception as e:
            logger.error(f"Could not delete rabbit queue {queue_name}; e: {e}")


def hard_delete_worker(actor_id, worker_id, worker_container_id=None, reason_str=None):
    """
    Hard delete of worker from the db. Will also try to hard remove the worker container id, if one is passed,
    but does not stop for errors.
    :param actor_id: db_id of the actor.
    :param worker_id: id of the worker
    :param worker_container_id: Docker container id of the worker container (optional)
    :param reason_str: The reason the worker is being hard deleted (optional, for the logs only).
    :return: None
    """
    logger.error(f"Top of hard_delete_worker for actor_id: {actor_id}; "
                 f"worker_id: {worker_id}; "
                 f"worker_container_id: {worker_container_id}; "
                 f"reason: {reason_str}")

    # hard delete from worker db --
    try:
        Worker.delete_worker(actor_id, worker_id)
        logger.info(f"worker {worker_id} deleted from store")
    except Exception as e:
        logger.error(f"Got exception trying to delete worker: {worker_id}; exception: {e}")
        # Fallback: try direct DB delete by _id in case the record is too corrupt for normal deletion
        try:
            workers_store[site()]._db.delete_one({'_id': f'{actor_id}_{worker_id}'})
            logger.info(f"worker {worker_id} deleted via direct DB fallback")
        except Exception as e2:
            logger.error(f"Direct DB fallback also failed for worker {worker_id}: {e2}")

    # also try to delete container --
    if worker_container_id:
        try:
            rm_container(worker_container_id)
            logger.info(f"worker {worker_id} container deleted from docker")
        except Exception as e:
            logger.error(f"Got exception trying to delete worker container; worker: {worker_id}; "
                         f"container: {worker_container_id}; exception: {e}")

    # also delete the worker's rabbit queues; nothing else ever will --
    delete_worker_rabbit_queues(worker_id)


def check_workers(actor_id, ttl):
    """Check health of all workers for an actor."""
    logger.info(f"Checking health for actor: {actor_id}")
    try:
        workers = Worker.get_workers(actor_id)
    except Exception as e:
        logger.error(f"Got exception trying to retrieve workers: {e}")
        return None
    logger.debug(f"workers: {workers}")
    host_id = os.environ.get('SPAWNER_HOST_ID', conf.spawner_host_id)
    logger.debug(f"host_id: {host_id}")
    worker_containers = get_current_worker_containers()
    
    # kubernetes issue - pod data too large for logs
    if conf.container_backend == 'kubernetes':
        loggable_worker_containers = copy.copy(worker_containers)
        for container in loggable_worker_containers:
            container["pod"] = "Removed due to length (around 200 lines for pod data)"
        logger.info(f"Health: worker_containers for host_id {conf.spawner_host_id}: {loggable_worker_containers}")

    # set when rabbit is unreachable: skip the remaining channel-based health checks this
    # cycle, but keep running the TTL / ERROR / container checks for every worker.
    rabbit_unreachable = False
    for worker in workers:
        try:
            worker_id = worker['id']
            worker_status = worker.get('status')
        except (KeyError, TypeError) as e:
            # Corrupt worker record (missing 'id' or not a dict). Clean it up and move on.
            logger.error(f"Corrupt worker record for actor {actor_id}: {worker}. Exception: {e}. Attempting cleanup.")
            try:
                # worker['id'] just failed, so there is no worker id to hard-delete by;
                # remove the record by its _id.
                if isinstance(worker, dict) and worker.get('_id'):
                    workers_store[site()]._db.delete_one({'_id': worker['_id']})
                    logger.info(f"Deleted corrupt worker record by _id: {worker.get('_id')}")
            except Exception as cleanup_e:
                logger.error(f"Failed to clean up corrupt worker record: {cleanup_e}")
            continue
        # if the worker has only been requested, it will not have a host_id. it is possible
        # the worker will ultimately get scheduled on a different host; however, if there is
        # some issue and the worker is "stuck" in the early phases, we should remove it..
        if 'host_id' not in worker:
            # check for an old create time
            worker_create_t = worker.get('create_time')
            # in versions prior to 1.9, worker create_time was not set until after it was READY
            if not worker_create_t:
                hard_delete_worker(actor_id, worker_id, reason_str='Worker did not have a host_id or create_time field.')
            # if still no host after 5 minutes, delete it
            if worker_create_t <  get_current_utc_time() - datetime.timedelta(minutes=5):
                hard_delete_worker(actor_id, worker_id, reason_str='Worker did not have a host_id and had '
                                                                   'old create_time field.')
            continue

        # ignore workers on different hosts because this health agent cannot interact with the
        # docker daemon responsible for the worker container..
        if not host_id == worker['host_id']:
            continue

        # we need to delete any worker that is in SHUTDOWN REQUESTED or SHUTTING down for too long
        if worker_status == codes.SHUTDOWN_REQUESTED or worker_status == codes.SHUTTING_DOWN:
            # Get last health check time. If there is no health check time, use the create time.
            worker_last_health_check_time = worker.get('last_health_check_time') or worker.get('create_time')
            if not worker_last_health_check_time:
                hard_delete_worker(actor_id, worker_id, reason_str='Worker in SHUTDOWN and no health checks.')
                # worker is gone; without this continue, the WorkerChannel construction below would
                # re-declare the deleted worker's durable queue, orphaning it in rabbit forever.
                continue
            elif worker_last_health_check_time < (get_current_utc_time() - datetime.timedelta(minutes=5)):
                hard_delete_worker(actor_id, worker_id, reason_str='Worker in SHUTDOWN for too long.')
                continue

        # check if the worker has not responded to a health check recently; we use a relatively long period
        # (60 minutes) of idle health checks in case there is an issue with sending health checks through rabbitmq.
        # this needs to be watched closely though...
        worker_last_health_check_time = worker.get('last_health_check_time')
        if not worker_last_health_check_time or \
                (worker_last_health_check_time < get_current_utc_time() - datetime.timedelta(minutes=60)):
            hard_delete_worker(actor_id, worker_id, reason_str='Worker has not health checked for too long.')
            continue

        # first send worker a health check (skipped once rabbit proved unreachable this cycle)
        if rabbit_unreachable:
            logger.debug(f"skipping channel health check for worker {worker_id}; rabbit unreachable.")
        else:
            logger.info(f"sending worker {worker_id} a health check")
            try:
                ch = WorkerChannel(worker_id=worker_id)
                try:
                    logger.debug(f"Issuing status check to channel: {worker['ch_name']}")
                    ch.put('status')
                except (ChannelTimeoutException, Exception) as e:
                    logger.error(f"Got exception of type {type(e)} trying to send worker {worker_id} a "
                                 f"health check. e: {e}")
                finally:
                    try:
                        ch.close()
                    except Exception as e:
                        logger.error(f"Got an error trying to close the worker channel for dead worker. Exception: {e}")
            except (RuntimeError, ConnectionResetError, OSError) as e:
                # was `break`, which also skipped the TTL / ERROR / container checks below
                # for every remaining worker of this actor.
                logger.critical(f"RabbitMQ unreachable during health check for worker {worker_id}: {e}. "
                                f"Skipping remaining channel-based health checks this cycle.")
                rabbit_unreachable = True
            except Exception as e:
                logger.error(f"Could not create WorkerChannel for worker {worker_id}: {e}")

        # now check if the worker has been idle beyond the max worker_ttl configured for this abaco:
        if ttl < 0:
            # ttl < 0 means infinite life
            logger.info("Infinite ttl configured; leaving worker")
            continue
        # we don't shut down workers that are currently running:
        if not worker['status'] == codes.BUSY:
            last_execution = worker.get('last_execution_time', 0)
            # if worker has made zero executions, use the create_time
            if last_execution == 0:
                last_execution = worker.get('create_time', datetime.datetime.min)
            logger.debug(f"using last_execution: {last_execution}")
            try:
                assert type(last_execution) == datetime.datetime
            except:
                logger.error("Time received for TTL measurements is not of type datetime.")
                last_execution = datetime.datetime.min
            worker_end_time = last_execution + datetime.timedelta(seconds=ttl)
            now_time = datetime.datetime.utcnow()
            if worker_end_time < now_time:
                # shutdown worker
                logger.info("Shutting down worker beyond ttl.")
                shutdown_worker(actor_id, worker_id)
            else:
                logger.info(f"Worker is within ttl. Leaving worker. worker_end_time: {worker_end_time} < current_time: {now_time}")

        if worker['status'] == codes.ERROR:
            # shutdown worker
            logger.info("Shutting down worker in error status.")
            shutdown_worker(actor_id, worker_id)


        ### Ensure the worker container still exists on the correct host_id. Workers can be deleted after restarts or crashes.
        # Kubernetes container names have to be completely lowercase.
        if conf.container_backend == 'kubernetes':
            db_worker_id = worker_id.lower()
        else:
            db_worker_id = worker_id

        worker_container_found = False
        if worker['host_id'] == conf.spawner_host_id and worker['status'] == 'READY':
            try:
                for container in worker_containers:
                    if db_worker_id in container['worker_id']:
                        worker_container_found = True
                        break
                if not worker_container_found:
                    logger.warning(f"Worker container {db_worker_id} not found on host {conf.spawner_host_id} as expected. Deleting record.")
                    hard_delete_worker(actor_id, db_worker_id, reason_str='Worker container not found on proper host.')
            except Exception as e:
                logger.critical(f'Error when checking worker container existence. e: {e}')

def check_containers():
    # Delete hanging containers without db record. Should be ran only when if leave_containers=False
    logger.info(f"Top of check_containers in health. Looking to delete hanging containers.")
    worker_records = workers_store[site()].items()
    worker_ids_in_db = []
    for worker in worker_records:
        try:
            worker_ids_in_db.append(worker['id'].lower())
        except Exception as e:
            logger.info(f"worker did not have 'id' field: worker: {worker}")
    logger.info(f"check_containers(). List of all worker_ids found in db: {worker_ids_in_db}")

    worker_containers = get_current_worker_containers()
    # We check only by worker_id (not actors-worker-tenant-actor_id-worker_id) because it's easier + better to leave container then delete extras
    # for container in worker_containers:
    #     container_worker_id = container['worker_id']
    #     if not container_worker_id in worker_ids_in_db:
    #         # Couldn't find worker doc matching worker container
    #         if conf.container_backend == 'kubernetes':
    #             # reconstitute container name actors-worker-<tenant>-<actor-id>-<worker-id>
    #             container_id = f"actors-worker-{container['tenant_id']}-{container['actor_id']}-{container_worker_id}"
    #             logger.debug(f"Container {container_id} found, but no worker_id == {container_worker_id} found in db. Hanging container, deleting pod.")
    #         else: #docker
    #             # reconstitute container name worker_<tenant>_<actor-id>_<worker-id>
    #             container_id = f"worker_{container['tenant_id']}_{container['actor_id']}_{container_worker_id}"
    #             logger.debug(f"Container {container_id} found, but no worker_id == {container_worker_id} found in db. Hanging container, deleting container.")
    #         # rm_container is backend specific, but was already imported earlier.
    #         rm_container(container_id)

def get_host_queues():
    """
    Read host_queues string from config and parse to return a Python list.
    :return: list[str]
    """
    try:
        host_queues = conf.spawner_host_queues
        return host_queues
    except Exception as e:
        msg = f"Got unexpected exception attempting to parse the host_queues config. Exception: {e}"
        logger.error(e)
        raise e

def start_spawner(queue, idx='0'):
    """
    Start a spawner on this host listening to a queue, `queue`.
    :param queue: (str) - the queue the spawner should listen to.
    :param idx: (str) - the index to use as a suffix to the spawner container name.
    :return:
    """
    command = 'python3 -u /actors/spawner.py'
    name = f'healthg_{queue}_spawner_{idx}'

    try:
        environment = dict(os.environ)
    except Exception as e:
        environment = {}
        logger.error(f"Unable to convert environment to dict; exception: {e}")

    environment.update({'AE_IMAGE': AE_IMAGE.split(':')[0],
                        'queue': queue,
    })
    if not '_abaco_secret' in environment:
        msg = 'Error in health process trying to start spawner. Did not find an _abaco_secret. Aborting'
        logger.critical(msg)
        raise

    # check logging strategy to determine log file name:
    log_file = 'abaco.log'
    if conf.log_filing_strategy == 'split' and conf.get('spawner_log_file'):
        log_file = conf.get('spawner_log_file')

    try:
        run_container(AE_IMAGE,
                      command,
                      name=name,
                      environment=environment,
                      mounts=[],
                      log_file=log_file)
    except Exception as e:
        logger.critical(f"Could not restart spawner for queue {queue}. Exception: {e}")

def check_spawner(queue):
    """
    Check the health and existence of a spawner on this host for a particular queue.
    :param queue: (str) - the queue to check on.
    :return:
    """
    logger.debug(f"top of check_spawner for queue: {queue}")
    # spawner container names by convention should have the format <project>_<queue>_spawner_<count>; for example
    #   abaco_default_spawner_2.
    # so, we look for container names containing a string with that format:
    spawner_name_segment = f'{queue}_spawner'
    if not container_running(name=spawner_name_segment):
        logger.critical(f"No spawners running for queue {queue}! Launching new spawner..")
        start_spawner(queue)
    else:
        logger.debug(f"spawner for queue {queue} already running.")

def check_spawners():
    """
    Check health of spawners running on a given host.
    :return:
    """
    logger.debug("top of check_spawners")
    host_queues = get_host_queues()
    logger.debug(f"checking spawners for queues: {host_queues}")
    for queue in host_queues:
        check_spawner(queue)

def shutdown_all_workers():
    """
    Utility function for properly shutting down all existing workers.
    This function is useful when deploying a new version of the worker code.
    """
    # iterate over the workers_store[site()] directly, not the actors_store[site()], since there could be data integrity issue.
    logger.debug("Top of shutdown_all_workers.")
    actors_with_workers = set()
    for worker in workers_store[site()].items():
        actors_with_workers.add(worker['actor_id'])

    for actor_id in actors_with_workers:
        check_workers(actor_id, 0)

def check_orphan_worker_queues():
    """
    Sweep the broker for worker_*/spawner_worker_* queues whose worker no longer exists in the
    workers store and delete them. Catches every path that leaves a durable queue behind (worker
    crash, container kill, host restart) and cleans up queues orphaned before this fix shipped.
    A live worker is always in the store before its queues are declared (REQUESTED precedes queue
    creation), so anything unknown to the store is safe to remove.
    """
    try:
        queues = rabbit_mgmt.list_queues(site(), ("name", "consumers"))
    except Exception as e:
        logger.error(f"check_orphan_worker_queues could not list queues; e: {e}")
        return
    live_worker_ids = {w.get('id') for w in workers_store[site()].items(proj_inp={'id': 1})}
    deleted = 0
    for q in queues:
        name = q.get('name', '')
        if name.startswith('spawner_worker_'):
            worker_id = name[len('spawner_worker_'):]
        elif name.startswith('worker_'):
            worker_id = name[len('worker_'):]
        else:
            continue
        # only touch queues with no consumer whose worker is unknown to the store
        if q.get('consumers') or worker_id in live_worker_ids:
            continue
        try:
            if rabbit_mgmt.delete_queue(site(), name, if_unused=True):
                deleted += 1
                logger.info(f"deleted orphan queue {name}; worker {worker_id} not in workers store.")
        except Exception as e:
            logger.error(f"could not delete orphan queue {name}; e: {e}")
    if deleted:
        logger.info(f"check_orphan_worker_queues deleted {deleted} orphan queue(s).")



def get_queue_depth(queue_name):
    """Message count for a queue via the management API; None when unknowable (mgmt
    unreachable / queue absent). None means the reconciler must NOT act."""
    try:
        queue = rabbit_mgmt.get_queue(site(), queue_name)
        if queue is None:
            # no queue at all: for a live actor that is the broker-wipe case the reconciler
            # exists for (re-publishing re-declares it). The reconciler checks that the actor
            # still exists before acting, so a deleted actor's queue is never re-created.
            return 0
        return queue.get('messages')
    except Exception as e:
        logger.warning(f"could not read depth of {queue_name}: {e}")
        return None


def reconcile_lost_messages():
    """The recovery half of the outbox: find executions stuck in SUBMITTED whose actor
    queue is EMPTY -- meaning the message is gone from rabbit (broker wipe, crash after
    ingest) -- and re-publish them from the copy controllers stored on the execution
    record. Executions with no stored copy (binary/oversized) are marked ERROR so they
    surface instead of hanging in SUBMITTED forever.

    With message_outbox unset (the default) this is DETECT-ONLY: lost messages are
    marked ERROR so they surface, and nothing is ever re-published. Enabling
    message_outbox in config turns on the recovery half (verified live 2026-08-19).

    Conservative by design: acts only when depth == 0 (a non-empty queue makes "which
    message is missing" unanswerable), only past an age threshold (a just-consumed
    message can sit in SUBMITTED for a beat before RUNNING), and at most
    reconcile_max_requeues times per execution (default 1, claimed atomically).

    Interaction with dead-lettering: a poison message that the broker moved to abaco_dlq
    (x-delivery-limit, or a worker nack for an undeserializable body) also leaves its
    execution SUBMITTED with an empty queue, and from here it is indistinguishable from a
    lost message -- nothing marks the execution when the broker dead-letters. So every
    re-publish of such an execution replays the poison message (and, on quorum queues,
    up to rabbit_delivery_limit more worker crashes) before it is dead-lettered again.
    That is why the default is a single re-publish and the ERROR status_message names
    both possibilities instead of claiming the message was lost."""
    threshold_s = int(conf.get('reconcile_lost_messages_after_s') or 900)
    # strict: an explicit 0 means never re-publish (was `or 2`, which turned 0 into 2).
    # Default 1: an execution whose message was dead-lettered is replayed at most once.
    max_requeues = conf.get('reconcile_max_requeues')
    max_requeues = 1 if max_requeues is None else int(max_requeues)
    now = datetime.datetime.utcnow()
    try:
        # indexed on status (stores.py); only the fields the reconciler reads, not whole documents
        stuck = executions_store[site()].items(
            {'status': codes.SUBMITTED},
            {'_id': False, 'id': True, 'actor_id': True, 'message_received_time': True,
             'queued_message': True, 'reconcile_requeues': True})
    except Exception as e:
        logger.error(f"reconciler could not list SUBMITTED executions: {e}")
        return
    by_actor = {}
    for e in stuck:
        by_actor.setdefault(e.get('actor_id'), []).append(e)
    for actor_id, execs in by_actor.items():
        if not actor_id:
            continue
        stale = []
        for e in execs:
            t = e.get('message_received_time')
            try:
                if isinstance(t, str):
                    t = datetime.datetime.fromisoformat(t.replace('Z', '')).replace(tzinfo=None)
                if t is None or (now - t).total_seconds() < threshold_s:
                    continue
            except Exception:
                continue
            stale.append(e)
        if not stale:
            continue
        # A deleted actor's SUBMITTED executions must never be re-published: ActorMsgChannel
        # would re-create actor_msg_<id> for an actor that no longer exists.
        try:
            actors_store[site()][actor_id]
        except KeyError:
            for e in stale:
                _reconciler_mark_error(actor_id, e.get('id'),
                                       "the actor was deleted before this message was processed")
            continue
        except Exception as ex:
            logger.error(f"reconciler could not look up actor {actor_id}; skipping it this run: {ex}")
            continue
        depth = get_queue_depth(f'actor_msg_{actor_id}')
        if depth is None or depth > 0:
            continue
        for e in stale:
            ex_id = e.get('id')
            qm = e.get('queued_message')
            requeues = e.get('reconcile_requeues') or 0
            key = f"{actor_id}_{ex_id}"
            try:
                if qm and requeues < max_requeues:
                    # claim the attempt BEFORE publishing: the increment only succeeds if the
                    # execution is still SUBMITTED and nobody else (a concurrent health run)
                    # has counted this attempt, so an execution is published at most
                    # max_requeues times. A publish that then fails still uses up the attempt.
                    if not executions_store[site()].claim_increment(
                            key, 'reconcile_requeues', requeues, {'status': codes.SUBMITTED}):
                        logger.info(f"reconciler: execution {ex_id} (actor {actor_id}) changed or was "
                                    f"claimed by another run; not re-publishing")
                        continue
                    ch = ActorMsgChannel(actor_id=actor_id)
                    try:
                        ch.put_msg(message=qm['message'], d=qm.get('d') or {})
                    finally:
                        try:
                            ch.close()
                        except Exception as close_e:
                            logger.error(f"reconciler could not close the channel for actor {actor_id}: {close_e}")
                    logger.critical(f"RECONCILER: re-published lost message for execution {ex_id} "
                                    f"(actor {actor_id}, attempt {requeues + 1}/{max_requeues})")
                else:
                    # neutral wording: the reconciler cannot tell a lost message from one the
                    # broker dead-lettered to abaco_dlq (see docstring).
                    reason = (f"no message in the actor's queue after {threshold_s}s; it may have "
                              f"been dead-lettered to abaco_dlq or lost")
                    if qm and requeues:
                        reason += f"; re-published {requeues}x from the stored copy without completing"
                    elif qm:
                        reason += "; re-publishing disabled (reconcile_max_requeues is 0)"
                    elif conf.get('message_outbox'):
                        reason += "; no recoverable copy (binary or oversized)"
                    else:
                        reason += "; recovery disabled (message_outbox not set)"
                    _reconciler_mark_error(actor_id, ex_id, reason)
            except Exception as ex:
                logger.error(f"reconciler failed on execution {ex_id} (actor {actor_id}): {ex}")


def _reconciler_mark_error(actor_id, ex_id, reason):
    """Mark a stuck execution ERROR with a reason (status_message), only if it is STILL SUBMITTED. The
    reconciler decides from a snapshot taken at the start of the run; by the time it reaches an actor, a
    worker may have run and finished the execution, and an unconditional write turned COMPLETE into ERROR."""
    try:
        res = executions_store[site()].full_update(
            {'_id': f"{actor_id}_{ex_id}", 'status': codes.SUBMITTED},
            {'$set': {'status': codes.ERROR, 'status_message': reason}, '$unset': {'queued_message': ''}})
        if not res.modified_count:
            logger.info(f"reconciler: execution {ex_id} (actor {actor_id}) is no longer SUBMITTED; left alone")
            return
        logger.critical(f"RECONCILER: execution {ex_id} (actor {actor_id}) ERROR: {reason}")
    except Exception as ex:
        logger.error(f"reconciler could not mark execution {ex_id} (actor {actor_id}) ERROR: {ex}")


def main():
    logger.info(f"Running abaco health checks. Now: {time.time()}")
    if conf.container_backend == 'docker':
        try:
            clean_up_ipc_dirs()
        except Exception as e:
            logger.error(f"Got exception from clean_up_ipc_dirs: {e}")
        # TODO - turning off the check_spawners call in the health process for now as there seem to be some issues.
        # the way the check works currently is to look for a spawner with a specific name. However, that check does not
        # appear to be working currently.
        # check_spawners()

    ttl = conf.worker_worker_ttl
    ids = get_actor_ids()
    logger.info(f"Found {len(ids)} actor(s). Now checking status.")
    # Go through actors, check their workers
    for aid in ids:
        check_workers(aid, ttl)
    
    # Go through containers and check if they are still running
    # Works for both kubernetes and docker
    if not conf.worker_leave_containers:
        check_containers()

    # Check health of all workers, even if not connected to an actor.
    check_workers_store(ttl)

    # Re-publish (or surface) executions whose messages vanished from rabbit.
    try:
        reconcile_lost_messages()
    except Exception as e:
        logger.error(f"Got exception from reconcile_lost_messages: {e}")

    # Remove rabbit queues left behind by workers that no longer exist.
    check_orphan_worker_queues()

if __name__ == '__main__':
    main()
