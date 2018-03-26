# Ensures that:
# 1. all worker containers in the database are still responsive; workers that have stopped
#    responding are shutdown and removed from the database.
# 2. Enforce ttl for idle workers.
#
# In the future, this module will also implement:
# 3. all actors with stateless=true have a number of workers proportional to the messages in the queue.

# Execute from a container on a schedule as follows:
# docker run -it --rm -v /var/run/docker.sock:/var/run/docker.sock abaco/core python3 -u /actors/health.py

import os
import shutil
import time

import channelpy

import codes
from config import Config
from docker_utils import rm_container, DockerError, container_running, run_container_with_docker
from models import Actor, Worker
from channels import CommandChannel, WorkerChannel
from stores import actors_store, workers_store
from worker import shutdown_worker

AE_IMAGE = os.environ.get('AE_IMAGE', 'abaco/core')

from agaveflask.logs import get_logger, get_log_file_strategy
logger = get_logger(__name__)


def get_actor_ids():
    """Returns the list of actor ids currently registered."""
    return [db_id for db_id, _ in actors_store.items()]

def check_workers_store(ttl):
    logger.debug("Top of check_workers_store.")
    """Run through all workers in workers_store and ensure there is no data integrity issue."""
    for actor_id, worker in workers_store.items():
        check_worker_health(worker, actor_id, ttl)

def get_worker(wid):
    """
    Check to see if a string `wid` is the id of a worker in the worker store.
    If so, return it; if not, return None.
    """
    for actor_id, value in workers_store.items():
        for worker_id, worker in value.items():
            if worker_id == wid:
                return worker
    return None

def clean_up_socket_dirs():
    logger.debug("top of clean_up_socket_dirs")
    socket_dir = os.path.join('/host/', Config.get('workers', 'socket_host_path_dir').strip('/'))
    logger.debug("processing socket_dir: {}".format(socket_dir))
    for p in os.listdir(socket_dir):
        # check to see if p is a worker
        worker = get_worker(p)
        if not worker:
            path = os.path.join(socket_dir, p)
            logger.debug("Determined that {} was not a worker; deleting directory: {}.".format(p, path))
            shutil.rmtree(path)

def clean_up_fifo_dirs():
    logger.debug("top of clean_up_fifo_dirs")
    fifo_dir = os.path.join('/host/', Config.get('workers', 'fifo_host_path_dir').strip('/'))
    logger.debug("processing fifo_dir: {}".format(fifo_dir))
    for p in os.listdir(fifo_dir):
        # check to see if p is a worker
        worker = get_worker(p)
        if not worker:
            path = os.path.join(fifo_dir, p)
            logger.debug("Determined that {} was not a worker; deleting directory: {}.".format(p, path))
            shutil.rmtree(path)

def clean_up_ipc_dirs():
    """Remove all directories created for worker sockets and fifos"""
    clean_up_socket_dirs()
    clean_up_fifo_dirs()

def check_worker_health(actor_id, worker):
    """Check the specific health of a worker object."""
    logger.debug("top of check_worker_health")
    worker_id = worker.get('id')
    logger.info("Checking status of worker from db with worker_id: {}".format())
    if not worker_id:
        logger.error("Corrupt data in the workers_store. Worker object without an id attribute. {}".format(worker))
        try:
            workers_store.pop(actor_id)
        except KeyError:
            # it's possible another health agent already removed the worker record.
            pass
        return None
    # make sure the actor id still exists:
    try:
        actors_store[actor_id]
    except KeyError:
        logger.error("Corrupt data in the workers_store. Worker object found but no corresponding actor. {}".format(worker))
        try:
            # todo - removing worker objects from db can be problematic if other aspects of the worker are not cleaned
            # up properly. this code should be reviewed.
            workers_store.pop(actor_id)
        except KeyError:
            # it's possible another health agent already removed the worker record.
            pass
        return None


def check_workers(actor_id, ttl):
    """Check health of all workers for an actor."""
    logger.info("Checking health for actor: {}".format(actor_id))
    try:
        workers = Worker.get_workers(actor_id)
    except Exception as e:
        logger.error("Got exception trying to retrieve workers: {}".format(e))
        return None
    logger.debug("workers: {}".format(workers))
    host_id = os.environ.get('SPAWNER_HOST_ID', Config.get('spawner', 'host_id'))
    logger.debug("host_id: {}".format(host_id))
    for _, worker in workers.items():
        # if the worker has only been requested, it will not have a host_id.
        if 'host_id' not in worker:
            # @todo- we will skip for now, but we need something more robust in case the worker is never claimed.
            continue
        # ignore workers on different hosts
        if not host_id == worker['host_id']:
            continue
        # first check if worker is responsive; if not, will need to manually kill
        logger.info("Checking health for worker: {}".format(worker))
        ch = WorkerChannel(worker_id=worker['id'])
        worker_id = worker.get('id')
        try:
            logger.debug("Issuing status check to channel: {}".format(worker['ch_name']))
            result = ch.put_sync('status', timeout=5)
        except channelpy.exceptions.ChannelTimeoutException:
            logger.info("Worker did not respond, removing container and deleting worker.")
            try:
                rm_container(worker['cid'])
            except DockerError:
                pass
            try:
                Worker.delete_worker(actor_id, worker_id)
                logger.info("worker {} deleted from store".format(worker_id))
            except Exception as e:
                logger.error("Got exception trying to delete worker: {}".format(e))
            # if the put_sync timed out and we removed the worker, we also need to delete the channel
            # otherwise the un-acked message will remain.
            try:
                ch.delete()
            except Exception as e:
                logger.error("Got exception: {} while trying to delete worker channel for worker: {}".format(e, worker_id))
        finally:
            try:
                ch.close()
            except Exception as e:
                logger.error("Got an error trying to close the worker channel for dead worker. Exception: {}".format(e))
        if not result == 'ok':
            logger.error("Worker responded unexpectedly: {}, deleting worker.".format(result))
            try:
                rm_container(worker['cid'])
                Worker.delete_worker(actor_id, worker_id)
            except Exception as e:
                logger.error("Got error removing/deleting worker: {}".format(e))
        else:
            # worker is healthy so update last health check:
            Worker.update_worker_health_time(actor_id, worker_id)
            logger.info("Worker ok.")
        # now check if the worker has been idle beyond the ttl:
        if ttl < 0:
            # ttl < 0 means infinite life
            logger.info("Infinite ttl configured; leaving worker")
            return
        # we don't shut down workers that are currently running:
        if not worker['status'] == codes.BUSY:
            last_execution = int(float(worker.get('last_execution_time', 0)))
            # if worker has made zero executions, use the create_time
            if last_execution == 0:
                last_execution = worker.get('create_time', 0)
            logger.debug("using last_execution: {}".format(last_execution))
            try:
                last_execution = int(float(last_execution))
            except:
                logger.error("Could not case last_execution {} to int(float()".format(last_execution))
                last_execution = 0
            if last_execution + ttl < time.time():
                # shutdown worker
                logger.info("Shutting down worker beyond ttl.")
                shutdown_worker(worker['id'])
            else:
                logger.info("Still time left for this worker.")
        elif worker['status'] == codes.ERROR:
            # shutdown worker
            logger.info("Shutting down worker in error status.")
            shutdown_worker(worker['id'])
        else:
            logger.debug("Worker not in READY status, will postpone.")


def manage_workers(actor_id):
    """Scale workers for an actor if based on message queue size and policy."""
    logger.info("Entering manage_workers for {}".format(actor_id))
    try:
        actor = Actor.from_db(actors_store[actor_id])
    except KeyError:
        logger.info("Did not find actor; returning.")
        return
    workers = Worker.get_workers(actor_id)
    #TODO - implement policy

def shutdown_all_workers():
    """
    Utility function for properly shutting down all existing workers.
    This function is useful when deploying a new version of the worker code.
    """
    # iterate over the workers_store directly, not the actors_store, since there could be data integrity issue.
    for actor_id, worker in workers_store.items():
        # call check_workers with ttl = 0 so that all will be shut down:
        check_workers(actor_id, 0)

def main():
    logger.info("Running abaco health checks. Now: {}".format(time.time()))
    try:
        clean_up_ipc_dirs()
    except Exception as e:
        logger.error("Got exception from clean_up_ipc_dirs: {}".format(e))
    try:
        ttl = Config.get('workers', 'worker_ttl')
    except Exception as e:
        logger.error("Could not get worker_ttl config. Exception: {}".format(e))
    if not container_running(name='spawner*'):
        logger.critical("No spawners running! Launching new spawner..")
        command = 'python3 -u /actors/spawner.py'
        # check logging strategy to determine log file name:
        if get_log_file_strategy() == 'split':
            log_file = 'spawner.log'
        else:
            log_file = 'service.log'
        try:
            run_container_with_docker(AE_IMAGE,
                                      command,
                                      name='abaco_spawner_0',
                                      environment={'AE_IMAGE': AE_IMAGE},
                                      mounts=[],
                                      log_file=log_file)
        except Exception as e:
            logger.critical("Could not restart spawner. Exception: {}".format(e))
    try:
        ttl = int(ttl)
    except Exception as e:
        logger.error("Invalid ttl config: {}. Setting to -1.".format(e))
        ttl = -1
    ids = get_actor_ids()
    logger.info("Found {} actor(s). Now checking status.".format(len(ids)))
    for id in ids:
        check_workers(id, ttl)
        # manage_workers(id)
    # TODO - turning off the check_workers_store for now. unclear that removing worker objects
    # check_workers_store(ttl)

if __name__ == '__main__':
    main()