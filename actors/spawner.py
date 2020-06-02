import json
import os
import time

import rabbitpy

from channelpy.exceptions import ChannelTimeoutException

from codes import BUSY, ERROR, SPAWNER_SETUP, PULLING_IMAGE, CREATING_CONTAINER, UPDATING_STORE, READY, \
    REQUESTED, SHUTDOWN_REQUESTED, SHUTTING_DOWN
from common.config import conf
from docker_utils import DockerError, run_worker, pull_image
from errors import WorkerException
from models import Actor, Worker
from stores import actors_store, workers_store
from channels import ActorMsgChannel, ClientsChannel, CommandChannel, WorkerChannel, SpawnerWorkerChannel
from health import get_worker

from agaveflask.logs import get_logger
logger = get_logger(__name__)

MAX_WORKERS = conf.spawner_max_workers_per_host
logger.info("Spawner running with MAX_WORKERS = {}".format(MAX_WORKERS))


class SpawnerException(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = message


class Spawner(object):

    def __init__(self):
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
                    logger.critical("METRICS - SPAWNER FOR HOST {} OVERLOADED!!!".format(self.host_id))
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
                logger.error("spawner got an exception trying to process cmd: {}. "
                             "Exception type: {}. Exception: {}".format(cmd, type(e), e))

    def get_tot_workers(self):
        logger.debug("top of get_tot_workers")
        self.tot_workers = 0
        logger.debug('spawner host_id: {}'.format(self.host_id))
        for worker_info in workers_store.items():
            try:
                if worker_info['host_id'] == self.host_id:
                    self.tot_workers += 1
            except KeyError:
                continue
        logger.debug("returning total workers: {}".format(self.tot_workers))
        return self.tot_workers

    def overloaded(self):
        logger.debug("top of overloaded")
        self.get_tot_workers()
        logger.info("total workers for this host: {}".format(self.tot_workers))
        if self.tot_workers >= MAX_WORKERS:
            return True

    def stop_workers(self, actor_id, worker_ids):
        """Stop existing workers; used when updating an actor's image."""
        logger.debug("Top of stop_workers() for actor: {}.".format(actor_id))
        try:
            workers_list = workers_store.items({'actor_id': actor_id})
        except KeyError:
            logger.debug("workers_store had no workers for actor: {}".format(actor_id))
            workers_list = []

        # if there are existing workers, we need to close the actor message channel and
        # gracefully shutdown the existing worker processes.
        if len(workers_list) > 0:
            logger.info(f"Found {len(workers_list)} workers to stop.")
            # first, close the actor msg channel to prevent any new messages from being pulled
            # by the old workers.
            actor_ch = ActorMsgChannel(actor_id)
            actor_ch.close()
            logger.info("Actor channel closed for actor: {}".format(actor_id))
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
        logger.info("top of process; cmd: {}".format(cmd))
        actor_id = cmd['actor_id']
        try:
            actor = Actor.from_db(actors_store[actor_id])
        except Exception as e:
            msg = f"Exception in spawner trying to retrieve actor object from store. Aborting. Exception: {e}"
            logger.error(msg)
            return
        worker_id = cmd['worker_id']
        image = cmd['image']
        tenant = cmd['tenant']
        stop_existing = cmd.get('stop_existing', True)
        num_workers = 1
        logger.debug("spawner command params: actor_id: {} worker_id: {} image: {} tenant: {} "
                    "stop_existing: {} num_workers: {}".format(actor_id, worker_id,
                                                               image, tenant, stop_existing, num_workers))
        # if the worker was sent a delete request before spawner received this message to create the worker,
        # the status will be SHUTDOWN_REQUESTED, not REQUESTED. in that case, we simply abort and remove the
        # worker from the collection.
        try:
            logger.debug("spawner checking worker's status for SHUTDOWN_REQUESTED")
            worker = Worker.get_worker(actor_id, worker_id)
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
                    Worker.delete_worker(actor_id, worker_id)
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
                Worker.delete_worker(actor_id, worker_id)
                logger.debug("spawner deleted worker because actor was in ERROR status.")
            except Exception as e:
                logger.error(f"spawner got exception trying to delete a worker was in ERROR status."
                             f"actor_id: {actor_id}; worker_id: {worker_id}; e: {e}")
            # either way, return from processing this worker message:
            return

        # worker status was REQUESTED; moving on to SPAWNER_SETUP ----
        Worker.update_worker_status(actor_id, worker_id, SPAWNER_SETUP)
        logger.debug("spawner has updated worker status to SPAWNER_SETUP; worker_id: {}".format(worker_id))
        client_id = None
        client_secret = None
        client_access_token = None
        client_refresh_token = None
        api_server = None
        client_secret = None

        # ---- Oauth client generation for the worker -------
        # check if tenant and instance configured for client generation
        tenant_auth_object = conf.get(f"{tenant}_auth_object") or {}
        generate_clients = tenant_auth_object.get("generate_clients") or conf.global_auth_object.get('generate_clients')
        logger.debug(f"final generate_clients: {generate_clients}")
        if generate_clients:
            logger.debug("client generation was configured to be available; now checking the actor's token attr.")
            # updated 1.3.0-- check whether the actor requires a token:
            if actor.token:
                if type(actor.token) == str and actor.token.lower() == 'false':
                    logger.debug("actor.token was a string set to false, so not generating a token")
                else:
                    logger.debug("spawner starting client generation")
                    client_id, \
                    client_access_token, \
                    client_refresh_token, \
                    api_server, \
                    client_secret = self.client_generation(actor_id, worker_id, tenant)
            else:
                logger.debug("actor's token attribute was False. Not generating client.")
        ch = SpawnerWorkerChannel(worker_id=worker_id)

        logger.debug("spawner attempting to start worker; worker_id: {}".format(worker_id))
        try:
            worker = self.start_worker(
                image,
                tenant,
                actor_id,
                worker_id,
                client_id,
                client_access_token,
                client_refresh_token,
                ch,
                api_server,
                client_secret
            )
        except Exception as e:
            msg = "Spawner got an exception from call to start_worker. Exception:{}".format(e)
            logger.error(msg)
            self.error_out_actor(actor_id, worker_id, msg)
            if client_id:
                self.delete_client(tenant, actor_id, worker_id, client_id, client_secret)
            return

        logger.debug("Returned from start_worker; Created new worker: {}".format(worker))
        ch.close()
        logger.debug("Client channel closed")

        if stop_existing:
            logger.info("Stopping existing workers: {}".format(worker_id))
            # TODO - update status to stop_requested
            self.stop_workers(actor_id, [worker_id])


    def client_generation(self, actor_id, worker_id, tenant):
        need_a_client = True
        client_attempts = 0
        while need_a_client and client_attempts < 10:
            client_attempts = client_attempts + 1
            # take a break between each subsequent attempt after the first one:
            if client_attempts > 1:
                time.sleep(2)
            client_ch = ClientsChannel()
            logger.debug(f"trying to generate a client for worker {worker_id}; attempt: {client_attempts}.")
            try:
                client_msg = client_ch.request_client(
                    tenant=tenant,
                    actor_id=actor_id,
                    worker_id=worker_id,
                    secret=self.secret
                )
            except Exception as e:
                logger.error("Got a ChannelTimeoutException trying to generate a client for "
                             "actor_id: {}; worker_id: {}; exception: {}".format(actor_id, worker_id, e))
                if client_attempts == 10:
                    # Update - 4/2020: we do NOT set the actor to an error statewhen client generation fails because
                    # put worker in an error state and return
                    # self.error_out_actor(actor_id, worker_id, "Abaco was unable to generate an OAuth client for new "
                    #                                           "worker {} for this actor. System administrators have been "
                    #                                           "notified. Actor will be put in error state and "
                    #                                           "must be updated before it will process".format(worker_id))
                    # Worker.update_worker_status(actor_id, worker_id, ERROR)
                    try:
                        client_ch.close()
                    except Exception as e:
                        logger.debug(f"got exception trying to close the client_ch: {e}")
                    self.kill_worker(actor_id, worker_id)
                    logger.critical("Client generation FAILED.")
                    raise e
            try:
                client_ch.close()
            except Exception as e:
                logger.debug(f"got exception trying to close the client_ch: {e}")

            if client_msg.get('status') == 'error':
                logger.error("Error generating client; worker_id: {}; message: {}".format(worker_id, client_msg.get('message')))
                # check to see if the error was an error that cannot be retried:
                if 'AgaveClientFailedDoNotRetry' in client_msg.get('message'):
                    logger.debug(f"got AgaveClientFailedDoNotRetry in message for worker {worker_id}. "
                                 f"Giving up and setting attempts directly to 10.")
                    client_attempts = 10
                if client_attempts == 10:
                    # Update - 4/2020: we do NOT set the actor to an error statewhen client generation fails because
                    # this is not something the user has control over.
                    # self.error_out_actor(actor_id, worker_id, "Abaco was unable to generate an OAuth client for new "
                    #                                           "worker {} for this actor. System administrators "
                    #                                           "have been notified. Actor will be put in error state and "
                    #                                           "must be updated before it will process "
                    #                                           "messages.".format(worker_id))
                    # Worker.update_worker_status(actor_id, worker_id, ERROR)
                    try:
                        client_ch.close()
                    except Exception as e:
                        logger.debug(f"got exception trying to close the client_ch: {e}")
                    self.kill_worker(actor_id, worker_id)
                    raise SpawnerException("Error generating client") #TODO - clean up error message
            # else, client was generated successfully:
            else:
                logger.info("Got a client: {}, {}, {}".format(client_msg['client_id'],
                                                              client_msg['access_token'],
                                                              client_msg['refresh_token']))
                return client_msg['client_id'], \
                       client_msg['access_token'],  \
                       client_msg['refresh_token'], \
                       client_msg['api_server'], \
                       client_msg['client_secret']

    def delete_client(self, tenant, actor_id, worker_id, client_id, secret):
        clients_ch = ClientsChannel()
        msg = clients_ch.request_delete_client(tenant=tenant,
                                               actor_id=actor_id,
                                               worker_id=worker_id,
                                               client_id=client_id,
                                               secret=secret)
        if msg['status'] == 'ok':
            logger.info("Client delete request completed successfully for "
                        "worker_id: {}, client_id: {}.".format(worker_id, client_id))
        else:
            logger.error("Error deleting client for "
                         "worker_id: {}, client_id: {}. Message: {}".format(worker_id, msg['message'], client_id, msg))
        clients_ch.close()

    def start_worker(self,
                     image,
                     tenant,
                     actor_id,
                     worker_id,
                     client_id,
                     client_access_token,
                     client_refresh_token,
                     ch,
                     api_server,
                     client_secret):

        # start an actor executor container and wait for a confirmation that image was pulled.
        attempts = 0
        # worker = get_worker(worker_id)
        # worker['status'] = PULLING_IMAGE
        Worker.update_worker_status(actor_id, worker_id, PULLING_IMAGE)
        try:
            logger.debug("Worker pulling image {}...".format(image))
            pull_image(image)
        except DockerError as e:
            # return a message to the spawner that there was an error pulling image and abort.
            # this is not necessarily an error state: the user simply could have provided an
            # image name that does not exist in the registry. This is the first time we would
            # find that out.
            logger.info("worker got a DockerError trying to pull image. Error: {}.".format(e))
            raise e
        logger.info("Image {} pulled successfully.".format(image))
        # Done pulling image
        # Run Worker Container
        while True:
            try:
                Worker.update_worker_status(actor_id, worker_id, CREATING_CONTAINER)
                logger.debug('spawner creating worker container')
                worker_dict = run_worker(
                    image,
                    actor_id,
                    worker_id,
                    client_id,
                    client_access_token,
                    client_refresh_token,
                    tenant,
                    api_server,
                    client_secret

                )
                logger.debug(f'finished run worker; worker dict: {worker_dict}')
            except DockerError as e:
                logger.error("Spawner got a docker exception from run_worker; Exception: {}".format(e))
                if 'read timeout' in e.message:
                    logger.info("Exception was a read timeout; trying run_worker again..")
                    time.sleep(5)
                    attempts = attempts + 1
                    if attempts > 20:
                        msg = "Spawner continued to get DockerError for 20 attempts. Exception: {}".format(e)
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
                        logger.info("Got WorkerException from delete_worker(). "
                                    "worker_id: {}"
                                    "Exception: {}".format(worker_id, e))

                    raise SpawnerException(message="Unable to start worker; error: {}".format(e))
            break
        logger.debug('finished loop')
        worker_dict['ch_name'] = WorkerChannel.get_name(worker_id)
        # if the actor is not already in READY status, set actor status to READY before worker status has been
        # set to READY.
        # it is possible the actor status is already READY because this request is the autoscaler starting a new worker
        # for an existing actor. It is also possible that another worker put the actor into state ERROR during the
        # time this spawner was setting up the worker; we check for that here.
        actor = Actor.from_db(actors_store[actor_id])
        if not actor.status == READY:
            # for now, we will allow a newly created, healthy worker to change an actor from status ERROR to READY,
            # but this policy is subject to review.
            if actor.status == ERROR:
                logger.info(f"actor {actor_id} was in ERROR status; new worker {worker_id} overriding it to READY...")
            try:
                Actor.set_status(actor_id, READY, status_message=" ")
            except KeyError:
                # it is possible the actor was already deleted during worker start up; if
                # so, the worker should have a stop message waiting for it. starting subscribe
                # as usual should allow this process to work as expected.
                pass
        # finalize worker with READY status
        worker = Worker(tenant=tenant, **worker_dict)
        logger.info("calling add_worker for worker: {}.".format(worker))
        Worker.add_worker(actor_id, worker)

        ch.put('READY')  # step 4
        logger.info('sent message through channel')

    def error_out_actor(self, actor_id, worker_id, message):
        """In case of an error, put the actor in error state and kill all workers"""
        logger.debug("top of error_out_actor for worker: {}_{}".format(actor_id, worker_id))
        Actor.set_status(actor_id, ERROR, status_message=message)
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
                logger.info("Spawner just stopped worker {}_{} in error_out_actor".format(actor_id, worker_id))
                return
            except Exception as e:
                logger.error("spawner got exception trying to run stop_workers. Exception: {}".format(e))
                logger.info("setting worker_status to ERROR so that kill_worker will run.")
                worker_status = ERROR

        # if the spawner was never able to start the worker container, we need to simply delete the worker record
        if worker_status == REQUESTED or worker_status == SPAWNER_SETUP or worker_status == PULLING_IMAGE or \
            worker_status == ERROR:
            logger.debug(f"worker status was: {worker_status}; trying to kill_worker")
            try:
                self.kill_worker(actor_id, worker_id)
                logger.info("Spawner just killed worker {}_{} in error_out_actor".format(actor_id, worker_id))
            except DockerError as e:
                logger.info("Received DockerError trying to kill worker: {}. Exception: {}".format(worker_id, e))
                logger.info("Spawner will continue on since this is exception processing.")

    def kill_worker(self, actor_id, worker_id):
        logger.debug(f"top of kill_worker: {actor_id}_{worker_id}")
        try:
            Worker.delete_worker(actor_id, worker_id)
            logger.debug(f"worker deleted; {actor_id}_{worker_id}")
        except WorkerException as e:
            logger.info("Got WorkerException from delete_worker(). "
                        "worker_id: {}"
                        "Exception: {}".format(worker_id, e))
        except Exception as e:
            logger.error("Got an unexpected exception from delete_worker(). "
                        "worker_id: {}"
                        "Exception: {}".format(worker_id, e))


def main():
    # todo - find something more elegant
    idx = 0
    while idx < 3:
        try:
            sp = Spawner()
            logger.info("spawner made connection to rabbit, entering main loop")
            logger.info("spawner using abaco_conf_host_path={}".format(os.environ.get('abaco_conf_host_path')))
            sp.run()
        except (rabbitpy.exceptions.ConnectionException, RuntimeError):
            # rabbit seems to take a few seconds to come up
            time.sleep(5)
            idx += 1
    logger.critical("spawner could not connect to rabbitMQ. Shutting down!")

if __name__ == '__main__':
    main()
