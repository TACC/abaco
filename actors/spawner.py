import json
import os
import time

import rabbitpy

from channelpy.exceptions import ChannelTimeoutException
from pymongo.errors import OperationFailure

from codes import BUSY, ERROR, SPAWNER_SETUP, PULLING_IMAGE, CREATING_CONTAINER, UPDATING_STORE, READY, \
    REQUESTED, SHUTDOWN_REQUESTED, SHUTTING_DOWN
from tapisservice.config import conf
from tapisservice.logs import get_logger
from errors import WorkerException
from models import Actor, Worker, site
from stores import actors_store, workers_store
from channels import ActorMsgChannel, CommandChannel, WorkerChannel, SpawnerWorkerChannel
from health import get_worker


# Give permissions to Docker copied folders and files.
# Have to do this as we are running as Tapis user, not root.
# This script requires no permissions.
os.system(f'sudo /home/tapis/actors/folder_permissions.sh /home/tapis/runtime_files')
if conf.container_backend == 'docker':
    os.system(f'sudo /home/tapis/actors/folder_permissions.sh /var/run/docker.sock')
    import docker_utils
    from docker_utils import DockerError
if conf.container_backend == 'kubernetes':
    import kubernetes_utils

logger = get_logger(__name__)

MAX_WORKERS = conf.spawner_max_workers_per_host
logger.info(f"Spawner running with MAX_WORKERS = {MAX_WORKERS}")


class SpawnerException(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = message


class Spawner(object):

    def __init__(self):
        self.container_backend = conf.container_backend
        self.num_workers = conf.worker_init_count
        self.secret = os.environ.get('_abaco_secret')
        self.queue = os.environ.get('queue', 'default')
        self.cmd_ch = CommandChannel(name=self.queue)
        self.tot_workers = 0
        self.host_id = conf.spawner_host_id

    def run(self):
        while True:
            # check resource threshold before subscribing
            while True:
                if self.overloaded():
                    logger.critical(f"METRICS - SPAWNER FOR HOST {self.host_id} OVERLOADED!!!")
                    # self.update_status to OVERLOADED
                    time.sleep(5)
                else:
                    break
            cmd, msg_obj = self.cmd_ch.get_one()
            # directly ack the messages from the command channel; problems generated from starting workers are
            # handled downstream; e.g., by setting the actor in an ERROR state; command messages should not be re-queued
            msg_obj.ack()
            try:
                self.process(cmd)
            except Exception as e:
                logger.error(f"spawner got an exception trying to process cmd: {cmd}. "
                             f"Exception type: {type(e)}. Exception: {e}")

    def get_tot_workers(self):
        logger.debug("top of get_tot_workers")
        self.tot_workers = 0
        logger.debug(f'spawner host_id: {self.host_id}')
        for worker_info in workers_store[site()].items():
            try:
                if worker_info['host_id'] == self.host_id:
                    self.tot_workers += 1
            except KeyError:
                continue
        logger.debug(f"returning total workers: {self.tot_workers}")
        return self.tot_workers

    def overloaded(self):
        logger.debug("top of overloaded")
        self.get_tot_workers()
        logger.info(f"total workers for this host: {self.tot_workers}")
        if self.tot_workers >= MAX_WORKERS:
            return True

    def stop_workers(self, actor_id, worker_ids):
        """Stop existing workers; used when updating an actor's image."""
        logger.debug(f"Top of stop_workers() for actor: {actor_id}.")
        try:
            workers_list = workers_store[site()].items({'actor_id': actor_id})
        except KeyError:
            logger.debug(f"workers_store had no workers for actor: {actor_id}")
            workers_list = []

        # if there are existing workers, we need to close the actor message channel and
        # gracefully shutdown the existing worker processes.
        if len(workers_list) > 0:
            logger.info(f"Found {len(workers_list)} workers to stop.")
            # first, close the actor msg channel to prevent any new messages from being pulled
            # by the old workers.
            actor_ch = ActorMsgChannel(actor_id)
            actor_ch.close()
            logger.info(f"Actor channel closed for actor: {actor_id}")
            # now, send messages to workers for a graceful shutdown:
            for worker in workers_list:
                # don't stop the new workers:
                if worker['id'] not in worker_ids:
                    ch = WorkerChannel(worker_id=worker['id'])
                    # since this is an update, there are new workers being started, so
                    # don't delete the actor msg channel:
                    ch.put('stop-no-delete')
                    logger.info(f"Sent 'stop-no-delete' message to worker_id: {worker['id']}")
                    ch.close()
                else:
                    logger.debug("skipping worker {worker} as it it not in worker_ids.")
        else:
            logger.info("No workers to stop.")

    def process(self, cmd):
        """Main spawner method for processing a command from the CommandChannel."""
        logger.info(f"top of process; cmd: {cmd}")
        actor_id = cmd['actor_id']
        site_id = cmd['site_id']
        try:
            actor = Actor.from_db(actors_store[site()][actor_id])
        except Exception as e:
            msg = f"Exception in spawner trying to retrieve actor object from store. Aborting. Exception: {e}"
            logger.error(msg)
            return
        worker_id = cmd['worker_id']
        image = cmd['image']
        revision = cmd['revision']
        tenant = cmd['tenant']
        stop_existing = cmd.get('stop_existing', True)
        num_workers = 1
        logger.debug(f"spawner command params: actor_id: {actor_id} worker_id: {worker_id} image: {image}"
                     f"tenant: {tenant} site_id: {site_id} stop_existing: {stop_existing} num_workers: {num_workers}")
        # if the worker was sent a delete request before spawner received this message to create the worker,
        # the status will be SHUTDOWN_REQUESTED, not REQUESTED. in that case, we simply abort and remove the
        # worker from the collection.
        try:
            logger.debug("spawner checking worker's status for SHUTDOWN_REQUESTED")
            worker = Worker.get_worker(actor_id, worker_id, site_id)
            logger.debug(f"spawner got worker; worker: {worker}")
        except Exception as e:
            logger.error(f"spawner got exception trying to retrieve worker. "
                         f"actor_id: {actor_id}; worker_id: {worker_id}; e: {e}")
            return

        status = worker.get('status')
        if not status == REQUESTED:
            logger.debug(f"worker was NOT in REQUESTED status. status: {status}")
            if status == SHUTDOWN_REQUESTED or status == SHUTTING_DOWN or status == ERROR:
                logger.debug(f"worker status was {status}; spawner deleting worker and returning..")
                try:
                    Worker.delete_worker(actor_id, worker_id, site_id)
                    logger.debug(f"spawner called delete_worker because its status was: {status}. {actor_id}_{worker_id}")
                    return
                except Exception as e:
                    logger.error(f"spawner got exception trying to delete a worker in SHUTDOWN_REQUESTED status."
                                 f"actor_id: {actor_id}; worker_id: {worker_id}; e: {e}")
                    return
            else:
                logger.error(f"spawner found worker in unexpected status: {status}. Not processing command and returning.")
                return

        # before starting up a new worker, check to see if the actor is in ERROR state; it is possible the actor was put
        # into ERROR state after the autoscaler put the worker request message on the spawner command channel.
        if actor.status == ERROR:
            logger.debug(f"actor {actor_id} was in ERROR status while spawner was starting worker {worker_id} . "
                         f"Aborting creation of worker.")
            try:
                Worker.delete_worker(actor_id, worker_id, site_id)
                logger.debug("spawner deleted worker because actor was in ERROR status.")
            except Exception as e:
                logger.error(f"spawner got exception trying to delete a worker was in ERROR status."
                             f"actor_id: {actor_id}; worker_id: {worker_id}; e: {e}")
            # either way, return from processing this worker message:
            return

        # worker status was REQUESTED; moving on to SPAWNER_SETUP ----
        Worker.update_worker_status(actor_id, worker_id, SPAWNER_SETUP, site_id)
        logger.debug(f"spawner has updated worker status to SPAWNER_SETUP; worker_id: {worker_id}")
        api_server = actor.get('api_server', None)

        spawner_ch = SpawnerWorkerChannel(worker_id=worker_id)
        logger.debug(f"spawner attempting to start worker; worker_id: {worker_id}")
        try:
            worker = self.start_worker(
                image,
                revision,
                tenant,
                actor_id,
                worker_id,
                spawner_ch,
                api_server,
                site_id
            )
        except Exception as e:
            msg = f"Spawner got an exception from call to start_worker. Exception:{e}"
            logger.error(msg)
            self.error_out_actor(actor_id, worker_id, msg, site_id)
            return

        logger.debug(f"Returned from start_worker; Created new worker: {worker}")
        spawner_ch.close()
        logger.debug("Worker's spawner channel closed")

        if stop_existing:
            logger.info(f"Stopping existing workers: {worker_id}")
            # TODO - update status to stop_requested
            self.stop_workers(actor_id, [worker_id])

    def start_worker(self,
                     image,
                     revision,
                     tenant,
                     actor_id,
                     worker_id,
                     spawner_ch,
                     api_server,
                     site_id):

        # start an actor executor container and wait for a confirmation that image was pulled.
        attempts = 0
        Worker.update_worker_status(actor_id, worker_id, PULLING_IMAGE, site_id)
        if self.container_backend.lower() == "docker":
            try:
                logger.debug(f"spawner pulling image {image}...")
                docker_utils.pull_image(image)
            except DockerError as e:
                # return a message to the spawner that there was an error pulling image and abort.
                # this is not necessarily an error state: the user simply could have provided an
                # image name that does not exist in the registry. This is the first time we would
                # find that out.
                logger.info(f"spawner got a DockerError trying to pull image. Error: {e}.")
                raise e
            logger.info(f"Image {image} pulled successfully.")
            # Done pulling image
        # Run Worker Container
        while True:
            try:
                if self.container_backend.lower() == "docker":
                    Worker.update_worker_status(actor_id, worker_id, CREATING_CONTAINER, site_id)
                    logger.debug(f'spawner creating worker container - backend: {self.container_backend}')
                    worker_dict = docker_utils.run_worker(
                        image,
                        revision,
                        actor_id,
                        worker_id,
                        tenant,
                        api_server
                    )
                    logger.debug(f'finished run worker; worker dict: {worker_dict}')
                elif self.container_backend.lower() == "kubernetes":
                    Worker.update_worker_status(actor_id, worker_id, CREATING_CONTAINER, site_id)
                    logger.debug(f'spawner creating worker container - backend: {self.container_backend}')
                    worker_dict = kubernetes_utils.run_worker(
                        image,
                        revision,
                        actor_id,
                        worker_id,
                        tenant,
                        api_server
                    )
                    logger.debug(f'finished run k8 worker; worker dict: {worker_dict}')
            except Exception as e:
                logger.error(f"Spawner got a container exception from run_worker; backend: {self.container_backend}; Exception: {e}")
                if 'read timeout' in e.message:
                    logger.info(f"Exception was a read timeout; trying run_worker again; backend {self.container_backend}")
                    time.sleep(5)
                    attempts = attempts + 1
                    if attempts > 20:
                        msg = f"Spawner continued to get error for 20 attempts. Exception: {e}"
                        logger.critical(msg)
                        # todo - should we be calling kill_worker here? (it is called in the exception block of the else below)
                        raise SpawnerException(msg)
                    continue
                else:
                    logger.info("Exception was NOT a read timeout; quiting on this worker.")
                    # delete this worker from the workers store:
                    try:
                        self.kill_worker(actor_id, worker_id)
                    except WorkerException as e:
                        logger.info(f"Got WorkerException from delete_worker(); worker_id: {worker_id}; Exception: {e}")
                    raise SpawnerException(message=f"Unable to start worker; error: {e}")
            break

        logger.debug('finished loop')
        try:
            worker_dict['ch_name'] = WorkerChannel.get_name(worker_id)
        except Exception as e:
            logger.debug(e)

        # if the actor is not already in READY status, set actor status to READY before worker status has been
        # set to READY.
        # it is possible the actor status is already READY because this request is the autoscaler starting a new worker
        # for an existing actor. It is also possible that another worker put the actor into state ERROR during the
        # time this spawner was setting up the worker; we check for that here.
        try:
            actor = Actor.from_db(actors_store[site()][actor_id])
            if not actor.status == READY:
                # for now, we will allow a newly created, healthy worker to change an actor from status ERROR to READY,
                # but this policy is subject to review.
                if actor.status == ERROR:
                    logger.info(f"actor {actor_id} was in ERROR status; new worker {worker_id} overriding it to READY...")
                try:
                    Actor.set_status(actor_id, READY, status_message=" ", site_id=site_id)
                except KeyError:
                    # it is possible the actor was already deleted during worker start up; if
                    # so, the worker should have a stop message waiting for it. starting subscribe
                    # as usual should allow this process to work as expected.
                    pass
        except Exception as e:
            logger.error(e)
        # finalize worker with READY status
        worker = Worker(tenant=tenant, **worker_dict)
        logger.info(f"calling add_worker for worker: {worker}.")
        Worker.add_worker(actor_id, worker, site_id)

        spawner_ch.put('READY')  # step 4
        logger.info('sent message through channel')

    def error_out_actor(self, actor_id, worker_id, message, site_id=None):
        """In case of an error, put the actor in error state and kill all workers"""
        site_id = site_id or site()
        logger.debug(f"top of error_out_actor for worker: {actor_id}_{worker_id}")
        # it is possible the actor was deleted already -- only set the actor status to ERROR if
        # it still exists in the store
        actor = actors_store[site()].get(actor_id)
        if actor:
            Actor.set_status(actor_id, ERROR, status_message=message, site_id=site_id)
        # check to see how far the spawner got setting up the worker:
        try:
            worker = Worker.get_worker(actor_id, worker_id)
            worker_status = worker.get('status')
            logger.debug(f"got worker status for {actor_id}_{worker_id}; status: {worker_status}")
        except Exception as e:
            logger.debug(f"got exception in error_out_actor trying to determine worker status for {actor_id}_{worker_id}; "
                         f"e:{e};")
            # skip all worker processing is skipped.
            return

        if worker_status == UPDATING_STORE or worker_status == READY or worker_status == BUSY:
            logger.debug(f"worker status was: {worker_status}; trying to stop_worker")
            # for workers whose containers are running, we first try to stop workers using the "graceful" approach -
            try:
                self.stop_workers(actor_id, worker_ids=[])
                logger.info(f"Spawner just stopped worker {actor_id}_{worker_id} in error_out_actor")
                return
            except Exception as e:
                logger.error(f"spawner got exception trying to run stop_workers. Exception: {e}")
                logger.info("setting worker_status to ERROR so that kill_worker will run.")
                worker_status = ERROR

        # if the spawner was never able to start the worker container, we need to simply delete the worker record
        if worker_status == REQUESTED or worker_status == SPAWNER_SETUP or worker_status == PULLING_IMAGE or \
            worker_status == ERROR:
            logger.debug(f"worker status was: {worker_status}; trying to kill_worker")
            try:
                self.kill_worker(actor_id, worker_id, site_id)
                logger.info(f"Spawner just killed worker {actor_id}_{worker_id} in error_out_actor")
            except Exception as e:
                logger.info(f"Received Exception trying to kill worker: {worker_id}. Exception: {e}")
                logger.info("Spawner will continue on since this is exception processing.")

    def kill_worker(self, actor_id, worker_id, site_id=None):
        logger.debug(f"top of kill_worker: {actor_id}_{worker_id}")
        site_id = site_id or site()
        try:
            Worker.delete_worker(actor_id, worker_id, site_id)
            logger.debug(f"worker deleted; {actor_id}_{worker_id}")
        except WorkerException as e:
            logger.info(f"Got WorkerException from delete_worker().; worker_id: {worker_id}; Exception: {e}")
        except Exception as e:
            logger.error(f"Got an unexpected exception from delete_worker().; worker_id: {worker_id}; Exception: {e}")


def main():
    # todo - find something more elegant
    # Ensure Mongo can connect.
    msg = "Spawner started. Connecting to Mongo..."
    logger.debug(msg)
    print(msg)
    idy = 0
    while idy < 5:
        try:
            # Testing whatever function to see if Mongo connects
            len(workers_store[site()])
            msg = "Spawner successfully connected to Mongo"
            logger.debug(msg)
            print(msg)
            break
        except OperationFailure:
            msg = "Waiting for Mongo connection"
            logger.debug(msg)
            print(msg)
            time.sleep(3)
            idy +=1
    # Start spawner
    idx = 0
    while idx < 3:
        try:
            sp = Spawner()
            logger.info("spawner made connection to rabbit, entering main loop")
            logger.info(f"spawner using abaco_host_path={os.environ.get('abaco_host_path')}")
            sp.run()
        except (rabbitpy.exceptions.ConnectionException, RuntimeError):
            # rabbit seems to take a few seconds to come up
            time.sleep(5)
            idx += 1
    logger.critical("spawner could not connect to rabbitMQ. Shutting down!")

if __name__ == '__main__':
    main()
