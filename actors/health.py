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

import channelpy

from auth import get_tenants, get_tenant_verify
import codes
from tapisservice.config import conf
from tapisservice.logs import get_logger
from models import Actor, Worker, is_hashid, get_current_utc_time, site
from channels import CommandChannel, WorkerChannel
from stores import actors_store, executions_store, workers_store
from worker import shutdown_worker
from kubernetes import client, config


# Give permissions some files since we are running as Tapis user, not root.
# folder_permissions is made to require no sudo permissions to run.
os.system(f'sudo /home/tapis/actors/folder_permissions.sh /home/tapis/runtime_files')

# This isn't currently working because they require different code in some functions
if conf.container_backend == "docker":
    from docker_utils import rm_container, get_current_worker_containers, container_running, run_container
    # Another run of folder_permissions, for docker.sock
    os.system(f'sudo /home/tapis/actors/folder_permissions.sh /var/run/docker.sock')
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
        aid = worker['actor_id']
        check_worker_health(aid, worker, ttl)

def get_worker(wid):
    """
    Check to see if a string `wid` is the id of a worker in the worker store.
    If so, return it; if not, return None.
    """
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
        worker = get_worker(p)
        if not worker:
            path = os.path.join(fifo_dir, p)
            logger.debug(f"Determined that {p} was not a worker; deleting directory: {path}.")
            shutil.rmtree(path)

def clean_up_ipc_dirs():
    """Remove all directories created for worker sockets and fifos"""
    clean_up_socket_dirs()
    clean_up_fifo_dirs()

def check_worker_health(actor_id, worker, ttl):
    """Check the specific health of a worker object."""
    logger.debug("top of check_worker_health")
    worker_id = worker.get('id')
    logger.info(f"Checking status of worker from db with worker_id: {worker_id}")
    if not worker_id:
        logger.error(f"Corrupt data in the workers_store[site()]. Worker object without an id attribute. {worker}")
        try:
            workers_store[site()].pop_field([actor_id])
        except KeyError:
            # it's possible another health agent already removed the worker record.
            pass
        return None
    # make sure the actor id still exists:
    try:
        actors_store[site()][actor_id]
    except KeyError:
        logger.error(f"Corrupt data in the workers_store[site()]. Worker object found but no corresponding actor. {worker}")
        try:
            # todo - removing worker objects from db can be problematic if other aspects of the worker are not cleaned
            # up properly. this code should be reviewed.
            workers_store[site()].pop_field([actor_id])
        except KeyError:
            # it's possible another health agent already removed the worker record.
            pass
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

    # also try to delete container --
    if worker_container_id:
        try:
            rm_container(worker_container_id)
            logger.info(f"worker {worker_id} container deleted from docker")
        except Exception as e:
            logger.error(f"Got exception trying to delete worker container; worker: {worker_id}; "
                         f"container: {worker_container_id}; exception: {e}")


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

    for worker in workers:
        worker_id = worker['id']
        worker_status = worker.get('status')
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
            worker_last_health_check_time = worker.get('last_health_check_time')
            if not worker_last_health_check_time:
                worker_last_health_check_time = worker.get('create_time')
            if not worker_last_health_check_time:
                hard_delete_worker(actor_id, worker_id, reason_str='Worker in SHUTDOWN and no health checks.')
            elif worker_last_health_check_time < get_current_utc_time() - datetime.timedelta(minutes=5):
                hard_delete_worker(actor_id, worker_id, reason_str='Worker in SHUTDOWN for too long.')

        # check if the worker has not responded to a health check recently; we use a relatively long period
        # (60 minutes) of idle health checks in case there is an issue with sending health checks through rabbitmq.
        # this needs to be watched closely though...
        worker_last_health_check_time = worker.get('last_health_check_time')
        if not worker_last_health_check_time or \
                (worker_last_health_check_time < get_current_utc_time() - datetime.timedelta(minutes=60)):
            hard_delete_worker(actor_id, worker_id, reason_str='Worker has not health checked for too long.')

        # first send worker a health check
        logger.info(f"sending worker {worker_id} a health check")
        ch = WorkerChannel(worker_id=worker_id)
        try:
            logger.debug(f"Issuing status check to channel: {worker['ch_name']}")
            ch.put('status')
        except (channelpy.exceptions.ChannelTimeoutException, Exception) as e:
            logger.error(f"Got exception of type {type(e)} trying to send worker {worker_id} a "
                         f"health check. e: {e}")
        finally:
            try:
                ch.close()
            except Exception as e:
                logger.error(f"Got an error trying to close the worker channel for dead worker. Exception: {e}")

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
            if last_execution + datetime.timedelta(seconds=ttl) < datetime.datetime.utcnow():
                # shutdown worker
                logger.info("Shutting down worker beyond ttl.")
                shutdown_worker(actor_id, worker_id)
            else:
                logger.info("Still time left for this worker.")

        if worker['status'] == codes.ERROR:
            # shutdown worker
            logger.info("Shutting down worker in error status.")
            shutdown_worker(actor_id, worker_id)

        # Ensure the worker container still exists on the correct host_id. Workers can be deleted after restarts or crashes.
        worker_container_found = False
        if worker['host_id'] == conf.spawner_host_id and worker['status'] == 'READY':
            try:
                for container in worker_containers:
                    # Kubernete container names have to be completely lowercase.
                    if conf.container_backend == 'kubernetes':
                        db_worker_id = worker_id.lower()
                    else:
                        db_worker_id = worker_id
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
    worker_records = workers_store['tacc'].items()
    worker_ids_in_db = []
    for worker in worker_records:
        worker_ids_in_db.append(worker['id'].lower())
    logger.info(f"check_containers(). List of all worker_ids found in db: {worker_ids_in_db}")

    worker_containers = get_current_worker_containers()
    # We check only by worker_id (not actors-worker-tenant-actor_id-worker_id) because it's easier. + better to leave container, then delete
    for container in worker_containers:
        container_worker_id = container['worker_id']
        if not container_worker_id in worker_ids_in_db:
            # Couldn't find worker doc matching worker container
            # reconstitute container name actors-worker-<tenant>-<actor-id>-<worker-id>
            container_id = f"actors-worker-{container['tenant_id']}-{container['actor_id']}-{container_worker_id}"
            logger.debug(f"Container {container_id} found, but no worker_id == {container_worker_id} found in db. Hanging container, deleting pod.")
            rm_container(container_id)

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
    for aid in ids:
        check_workers(aid, ttl)
    if conf.container_backend == 'kubernetes' and not conf.worker_leave_containers:
        check_containers()
    tenants = get_tenants()

    # TODO - turning off the check_workers_store for now. unclear that removing worker objects
    # check_workers_store(ttl)

if __name__ == '__main__':
    main()