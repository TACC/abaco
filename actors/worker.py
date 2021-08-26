import copy
import os
import shutil
import sys
import threading
import _thread
import time

import channelpy

from __init__ import t
from auth import get_tenant_verify
from channels import ActorMsgChannel, CommandChannel, WorkerChannel, SpawnerWorkerChannel
from codes import SHUTDOWN_REQUESTED, SHUTTING_DOWN, ERROR, READY, BUSY, COMPLETE
from common.config import conf
from docker_utils import DockerError, DockerStartContainerError, DockerStopContainerError, execute_actor, pull_image
from errors import WorkerException
import globals
from models import Actor, Execution, Worker, site
from stores import actors_store, workers_store, executions_store

from common.logs import get_logger
logger = get_logger(__name__)

# keep_running will be updated by the thread listening on the worker channel when a graceful shutdown is
# required.
keep_running = True

# maximum number of consecutive errors a worker can encounter before giving up and moving itself into an ERROR state.
MAX_WORKER_CONSECUTIVE_ERRORS = 5

def shutdown_worker(actor_id, worker_id, delete_actor_ch=True):
    """Gracefully shutdown a single worker."
    actor_id (str) - the dbid of the associated actor.
    """
    logger.debug(f"top of shutdown_worker for worker_id: {worker_id}")
    # set the worker status to SHUTDOWN_REQUESTED:
    try:
        Worker.update_worker_status(actor_id, worker_id, SHUTDOWN_REQUESTED)
    except Exception as e:
        logger.error(f"worker got exception trying to update status to SHUTODWN_REQUESTED. actor_id: {actor_id};"
                     f"worker_id: {worker_id}; exception: {e}")
    ch = WorkerChannel(worker_id=worker_id)
    if not delete_actor_ch:
        ch.put("stop-no-delete")
    else:
        ch.put("stop")
    logger.info(f"A 'stop' message was sent to worker: {worker_id}")
    ch.close()

def shutdown_workers(actor_id, delete_actor_ch=True):
    """
    Graceful shutdown of all workers for an actor. Arguments:
    * actor_id (str) - the db_id of the actor
    * delete_actor_ch (bool) - whether the worker shutdown process should also delete the actor_ch. This should be true
      whenever the actor is being removed. This will also force quit any currently running executions.
    """
    logger.debug(f"shutdown_workers() called for actor: {actor_id}")
    try:
        workers = Worker.get_workers(actor_id)
    except Exception as e:
        logger.error(f"Got exception from get_workers: {e}")
    if not workers:
        logger.info(f"shutdown_workers did not receive any workers from Worker.get_workers for actor: {actor_id}")
    # @TODO - this code is not thread safe. we need to update the workers state in a transaction:
    for worker in workers:
        shutdown_worker(actor_id, worker['id'], delete_actor_ch)


def process_worker_ch(tenant, worker_ch, actor_id, worker_id, actor_ch):
    """ Target for a thread to listen on the worker channel for a message to stop processing.
    :param worker_ch:
    :return:
    """
    global keep_running
    logger.info(f"Worker subscribing to worker channel...{actor_id}_{worker_id}")
    while keep_running:
        try:
            msg, msg_obj = worker_ch.get_one()
        except Exception as e:
            logger.error(f"worker {worker_id} got exception trying to read the worker channel! "
                         f"sleeping for 10 seconds and then will try again; e: {e}")
            time.sleep(10)
            continue
        # receiving the message is enough to ack it - resiliency is currently handled in the calling code.
        msg_obj.ack()
        logger.debug(f"Received message in worker channel; msg: {msg}; {actor_id}_{worker_id}")
        logger.debug(f"Type(msg)={type(msg)}")
        if msg == 'status':
            # this is a health check, return 'ok' to the reply_to channel.
            logger.debug("received health check. updating worker_health_time.")
            try:
                Worker.update_worker_health_time(actor_id, worker_id)
            except Exception as e:
                logger.error(f"worker {worker_id} got exception trying to update its health time! "
                             f"sleeping for 10 seconds and then will try again; e: {e}")
                time.sleep(10)
                continue

        elif msg == 'force_quit':
            logger.info("Worker with worker_id: {} (actor_id: {}) received a force_quit message, "
                        "forcing the execution to halt...".format(worker_id, actor_id))
            globals.force_quit = True
            globals.keep_running = False

        elif msg == 'stop' or msg == 'stop-no-delete':
            logger.info("Worker with worker_id: {} (actor_id: {}) received stop message, "
                        "stopping worker...".format(worker_id, actor_id))
            globals.keep_running = False
            # set the worker status to SHUTTING_DOWN:
            try:
                Worker.update_worker_status(actor_id, worker_id, SHUTTING_DOWN)
            except Exception as e:
                logger.error(
                    f"worker got exception trying to update status to SHUTTING_DOWN. actor_id: {actor_id};"
                    f"worker_id: {worker_id}; exception: {e}")

            # when an actor's image is updated, old workers are deleted while new workers are
            # created. Deleting the actor msg channel in this case leads to race conditions
            delete_actor_ch = True
            if msg == 'stop-no-delete':
                logger.info(f"Got stop-no-delete; will not delete actor_ch. {actor_id}_{worker_id}")
                delete_actor_ch = False
            # if a `stop` was sent, the actor is being deleted, and so we want to immediately shutdown processing.
            else:
                globals.force_quit = True
            try:
                Worker.delete_worker(actor_id, worker_id)
            except WorkerException as e:
                logger.info("Got WorkerException from delete_worker(). "
                            "worker_id: {}"
                            "Exception: {}".format(worker_id, e))
            # delete associated channels:
            # it is possible the actor channel was already deleted, in which case we just keep processing
            if delete_actor_ch:
                try:
                    actor_ch.delete()
                    logger.info(f"ActorChannel deleted for actor: {actor_id} worker_id: {worker_id}")
                except Exception as e:
                    logger.info("Got exception deleting ActorChannel for actor: {} "
                                "worker_id: {}; exception: {}".format(actor_id, worker_id, e))
            try:
                worker_ch.delete()
                logger.info(f"WorkerChannel deleted for actor: {actor_id} worker_id: {worker_id}")
            except Exception as e:
                logger.info("Got exception deleting WorkerChannel for actor: {} "
                            "worker_id: {}; exception: {}".format(actor_id, worker_id, e))

            logger.info(f"Worker with worker_id: {worker_id} is now exiting.")
            _thread.interrupt_main()
            logger.info(f"main thread interrupted, worker {actor_id}_{worker_id} issuing os._exit()...")
            os._exit(0)

def get_execution_token(token_tenant, token_user, access_token_ttl=14400):
    """
    Process to generate OAuth2 tokens for workers.
    """
    try:
        start = time.time()
        token_res = t.tokens.create_token(account_type='user',
                                          token_tenant_id=token_tenant,
                                          token_username=token_user,
                                          access_token_ttl=access_token_ttl,
                                          generate_refresh_token=False,
                                          use_basic_auth=False)
        elasped_time = time.time() - start
        if elasped_time >= 2:
            logger.error(f"Time to create execution token: {elasped_time}")
        return token_res.access_token.access_token
    except Exception as e:
        logger.error(f"Time to fail at creating execution token: {time.time() - start}")
        msg = f"Got exception trying to create actor access_token; exception: {e}"
        logger.error(msg)
        raise WorkerException(msg)

def subscribe(tenant,
              actor_id,
              image,
              revision,
              worker_id,
              api_server,
              worker_ch):
    """
    Main loop for the Actor executor worker. Subscribes to the actor's inbox and executes actor
    containers when message arrive. Also launches a separate thread which ultimately subscribes to the worker channel
    for future communications.
    :return:
    """
    logger.debug(f"Top of subscribe(). worker_id: {worker_id}")
    actor_ch = ActorMsgChannel(actor_id)
    # establish configs for this worker -------
    leave_containers = conf.worker_leave_containers
    logger.debug(f"leave_containers: {leave_containers}")

    mem_limit = str(conf.worker_mem_limit)
    logger.debug(f"mem_limit: {mem_limit}")

    max_cpus = conf.worker_max_cpus
    logger.debug(f"max_cpus: {max_cpus}")

    # start a separate thread for handling messages sent to the worker channel ----
    logger.info("Starting the process worker channel thread.")
    t = threading.Thread(target=process_worker_ch, args=(tenant, worker_ch, actor_id, worker_id, actor_ch))
    t.start()

    # subscribe to the actor message queue -----
    logger.info(f"Worker subscribing to actor channel. worker_id: {worker_id}")
    # keep track of whether we need to update the worker's status back to READY; otherwise, we
    # will hit mongo with an UPDATE every time the subscription loop times out (i.e., every 2s)
    update_worker_status = True

    # global tracks whether this worker should keep running.
    globals.keep_running = True

    # consecutive_errors tracks the number of consecutive times a worker has gotten an error trying to process a
    # message. Even though the message will be requeued, we do not want the worker to continue processing
    # indefinitely when a compute node is unhealthy.
    consecutive_errors = 0

    # main subscription loop -- processing messages from actor's mailbox
    while globals.keep_running:
        logger.debug(f"top of keep_running; worker id: {worker_id}")
        if update_worker_status:
            Worker.update_worker_status(actor_id, worker_id, READY)
            logger.debug(f"updated worker status to READY in SUBSCRIBE; worker id: {worker_id}")
            update_worker_status = False

        # note: the following get_one() call blocks until a message is returned. this means it could be a long time
        # (i.e., many seconds, or even minutes) between the check above to globals.keep_running (line 252) and the
        # get_one() function returning. In this time, the worker channel thread could have received a stop. We need to
        # check this.
        try:
            msg, msg_obj = actor_ch.get_one()
        except channelpy.ChannelClosedException:
            logger.info(f"Channel closed, worker exiting. worker id: {worker_id}")
            globals.keep_running = False
            sys.exit()
        logger.info(f"worker {worker_id} processing new msg.")

        # worker ch thread has received a stop and is already shutting us down (see note above); we need to nack this
        # message and exit
        if not globals.keep_running:
            logger.info("got msg from get_one() but globals.keep_running was False! "
                        "Requeing message and worker will exit. {}+{}".format(actor_id, worker_id))
            msg_obj.nack(requeue=True)
            logger.info(f"message requeued; worker exiting:{actor_id}_{worker_id}")
            time.sleep(5)
            raise Exception()
        # if the actor revision is different from the revision assigned to this worker, the worker is stale, so we
        # need to nack this message and exit.
        # NOTE: we could also compare the worker's revision to the revision contained in the message itself so that
        # a given message was always processed by a worker of the same revision, but this would take more work and is
        # not the requirement.
        try:
            actor = Actor.from_db(actors_store[site()][actor_id])
        except Exception as e:
            logger.error("unexpected exception retrieving actor to check revision. Nacking message."
                         "actor_id: {}; worker_id: {}; status: {}; exception: {}".format(actor_id,
                                                                                         worker_id,
                                                                                         READY,
                                                                                         e))
            msg_obj.nack(requeue=True)
            logger.info(f"worker exiting. worker_id: {worker_id}")
            raise e
        if not revision == actor.revision:
            logger.info(f"got msg from get_one() but worker's revision ({revision}) was different "
                        f"from actor.revision ({actor.revision}). Requeing message and worker will "
                        f"exit. {actor_id}+{worker_id}")
            msg_obj.nack(requeue=True)
            logger.info(f"message requeued; worker exiting:{actor_id}_{worker_id}")
            time.sleep(5)
            raise Exception()

        try:
            Worker.update_worker_status(actor_id, worker_id, BUSY)
        except Exception as e:
            logger.error("unexpected exception from call to update_worker_status. Nacking message."
                         "actor_id: {}; worker_id: {}; status: {}; exception: {}".format(actor_id,
                                                                                         worker_id,
                                                                                         BUSY,
                                                                                         e))
            logger.info(f"worker exiting. {actor_id}_{worker_id}")
            msg_obj.nack(requeue=True)
            raise e
        update_worker_status = True
        logger.info(f"Received message {msg}. Starting actor container. worker id: {worker_id}")
        # the msg object is a dictionary with an entry called message and an arbitrary
        # set of k:v pairs coming in from the query parameters.
        message = msg.pop('message', '')
        try:
            execution_id = msg['_abaco_execution_id']
            content_type = msg['_abaco_Content_Type']
            mounts = actor.mounts
            logger.debug(f"actor mounts: {mounts}")
        except Exception as e:
            logger.error("unexpected exception retrieving execution, content-type, mounts. Nacking message."
                         "actor_id: {}; worker_id: {}; status: {}; exception: {}".format(actor_id,
                                                                                         worker_id,
                                                                                         BUSY,
                                                                                         e))
            msg_obj.nack(requeue=True)
            logger.info(f"worker exiting. worker_id: {worker_id}")
            raise e

        # if the actor revision is different from the revision assigned to this worker, the worker is stale, so we
        # need to nack this message and exit.
        # NOTE: we could also compare the worker's revision to the revision contained in the message itself so that
        # a given message was always processed by a worker of the same revision, but this would take more work and is
        # not the requirement.
        if not revision == actor.revision:
            logger.info(f"got msg from get_one() but worker's revision ({revision}) was different "
                        f"from actor.revision ({actor.revision}). Requeing message and worker will "
                        f"exit. {actor_id}+{worker_id}")
            msg_obj.nack(requeue=True)
            logger.info(f"message requeued; worker exiting:{actor_id}_{worker_id}")
            time.sleep(5)
            raise Exception()


        # for results, create a socket in the configured directory.
        # Paths should be formatted as host_path:container_path for split
        socket_host_path_dir, socket_container_path_dir = conf.worker_socket_paths.split(':')
        socket_host_path = f'{os.path.join(socket_host_path_dir, worker_id, execution_id)}.sock'
        socket_container_path = f'{os.path.join(socket_container_path_dir, worker_id, execution_id)}.sock'
        logger.info(f"Create socket at path: {socket_host_path}")
        # add the socket as a mount:
        mounts.append({'host_path': socket_host_path,
                       'container_path': '/_abaco_results.sock',
                       'format': 'ro'})

        # for binary data, create a fifo in the configured directory. The configured
        # fifo_host_path_dir is equal to the fifo path in the worker container:
        fifo_container_path = None
        if content_type == 'application/octet-stream':
            fifo_host_path_dir, fifo_container_path_dir = conf.worker_fifo_paths.split(':')
            fifo_host_path = os.path.join(fifo_host_path_dir, worker_id, execution_id)
            fifo_container_path = os.path.join(fifo_container_path_dir, worker_id, execution_id)
            try:
                os.mkfifo(fifo_container_path)
                logger.info(f"Created fifo at path: {fifo_host_path}")
            except Exception as e:
                logger.error(f"Could not create fifo_path at {fifo_host_path}. Nacking message. Exception: {e}")
                msg_obj.nack(requeue=True)
                logger.info(f"worker exiting. worker_id: {worker_id}")
                raise e
            # add the fifo as a mount:
            mounts.append({'host_path': fifo_host_path,
                           'container_path': '/_abaco_binary_data',
                           'format': 'ro'})

        # the execution object was created by the controller, but we need to add the worker id to it now that we
        # know which worker will be working on the execution.
        logger.debug(f"Adding worker_id to execution. worker_id: {worker_id}")
        try:
            Execution.add_worker_id(actor_id, execution_id, worker_id)
        except Exception as e:
            logger.error(f"Unexpected exception adding working_id to the Execution. Nacking message. Exception: {e}")
            msg_obj.nack(requeue=True)
            logger.info(f"worker exiting. worker_id: {worker_id}")
            raise e

        # privileged dictates whether the actor container runs in privileged mode and if docker daemon is mounted.
        privileged = False
        if type(actor['privileged']) == bool and actor['privileged']:
            privileged = True
        logger.debug(f"privileged: {privileged}; worker_id: {worker_id}")

        # overlay resource limits if set on actor:
        if actor.mem_limit:
            mem_limit = actor.mem_limit
        if actor.max_cpus:
            max_cpus = actor.max_cpus

        # retrieve the default environment registered with the actor.
        environment = actor['default_environment']
        logger.debug(f"Actor default environment: {environment}")

        # construct the user field from the actor's uid and gid:
        user = get_container_user(actor, execution_id, actor_id)
        logger.debug(f"Final user value: {user}")
        # overlay the default_environment registered for the actor with the msg
        # dictionary
        environment.update(msg)
        environment['_abaco_access_token'] = ''
        environment['_abaco_actor_dbid'] = actor_id
        environment['_abaco_actor_id'] = actor.id
        environment['_abaco_worker_id'] = worker_id
        environment['_abaco_container_repo'] = actor.image
        environment['_abaco_actor_state'] = actor.state
        environment['_abaco_api_server'] = api_server
        environment['_abaco_actor_name'] = actor.name or 'None'
        logger.debug(f"Overlayed environment: {environment}; worker_id: {worker_id}")

        # Creating token oauth2 token to be injected as environment variable for actor
        # execution so that user can use it to authenticate to Tapis.
        tenant_tenant_object = conf.get(f"{tenant}_tenant_object") or {}
        generate_clients = tenant_tenant_object.get("generate_clients") or conf.global_tenant_object.get('generate_clients')
        logger.debug(f"final generate_clients: {generate_clients}")
        logger.debug(actor)
        if generate_clients:
            logger.debug(f"execution token generation is configured on, creating token for user: {user} and tenant: {tenant}.")
            token = get_execution_token(actor.tenant, actor.owner)
            environment['_abaco_access_token'] = token
        logger.info(f"Passing update environment: {environment}")
        logger.info(f"About to execute actor; worker_id: {worker_id}")
        logger.info(f"Executed actor mounts: {mounts}")
        try:
            stats, logs, final_state, exit_code, start_time = execute_actor(actor_id,
                                                                            worker_id,
                                                                            execution_id,
                                                                            image,
                                                                            message,
                                                                            user,
                                                                            environment,
                                                                            privileged,
                                                                            mounts,
                                                                            leave_containers,
                                                                            fifo_container_path,
                                                                            socket_container_path,
                                                                            mem_limit,
                                                                            max_cpus,
                                                                            tenant)
        except DockerStartContainerError as e:
            logger.error("Worker {} got DockerStartContainerError: {} trying to start actor for execution {}."
                         "Placing message back on queue.".format(worker_id, e, execution_id))
            # if we failed to start the actor container, we leave the worker up and re-queue the original message
            msg_obj.nack(requeue=True)
            logger.debug('message requeued.')
            consecutive_errors += 1
            if consecutive_errors > MAX_WORKER_CONSECUTIVE_ERRORS:
                logger.error("Worker {} failed to successfully start actor for execution {} {} consecutive times; "
                             "Exception: {}. Putting the actor in error status and shutting "
                             "down workers.".format(worker_id, execution_id, MAX_WORKER_CONSECUTIVE_ERRORS, e))
                Actor.set_status(actor_id, ERROR, f"Error executing container: {e}; w")
                shutdown_workers(actor_id, delete_actor_ch=False)
                Execution.update_status(actor_id, execution_id, ERROR)
                # wait for worker to be shutdown..
                time.sleep(60)
                break
            else:
                # sleep five seconds before getting a message again to give time for the compute
                # node and/or docker health to recover
                time.sleep(5)
                continue
        except DockerStopContainerError as e:
            logger.error("Worker {} was not able to stop actor for execution: {}; Exception: {}. "
                         "Putting the actor in error status and shutting down workers.".format(worker_id, execution_id, e))
            Actor.set_status(actor_id, ERROR, f"Error executing container: {e}")
            # since the error was with stopping the actor, we will consider this message "processed"; this choice
            # could be reconsidered/changed
            msg_obj.ack()
            Execution.update_status(actor_id, execution_id, ERROR)
            shutdown_workers(actor_id, delete_actor_ch=False)
            # wait for worker to be shutdown..
            time.sleep(60)
            break
        except Exception as e:
            logger.error(f"Worker {worker_id} got an unexpected exception trying to run actor for execution: {execution_id}."
                         "Putting the actor in error status and shutting down workers. "
                         f"Exception: {e}; Exception type: {type(e)}")
            # updated 2/2021 -- we no longer set the actor to ERROR state for unrecognized exceptions. Most of the time
            # these exceptions are due to internal system errors, such as not being able to talk eo RabbitMQ or getting
            # socket timeouts from docker. these are not the fault of the actor, and putting it (but not other actors
            # who simply didn't happen to be executing at the time) in ERROR state is confusing to users and leads to
            # actors not procssing messages until the user notices and intervenes.
            # # # # # # # Actor.set_status(actor_id, ERROR, "Error executing container: {}".format(e))

            # the execute_actor function raises a DockerStartContainerError if it met an exception before starting the
            # actor container; if the container was started, then another exception should be raised. Therefore,
            # we can assume here that the container was at least started and we can ack the message.
            msg_obj.ack()
            Execution.update_status(actor_id, execution_id, ERROR)
            shutdown_workers(actor_id, delete_actor_ch=False)
            # wait for worker to be shutdown..
            time.sleep(60)
            break
        # ack the message
        msg_obj.ack()

        # Add the logs to the execution before finalizing it -- otherwise, there is a race condition for clients
        # waiting on the execution to be COMPLETE and then immediately retrieving the logs.
        try:
            logger.debug("Checking for get_actor_log_ttl")
            log_ex = Actor.get_actor_log_ttl(actor_id)
            logger.debug(f"log ex is {log_ex}")
            Execution.set_logs(execution_id, logs, actor_id, tenant, worker_id, log_ex)
            logger.debug(f"Successfully added execution logs of expiry {log_ex}.")
        except Exception as e:
            msg = "Got exception trying to set logs for exception {}; " \
                  "Exception: {}; worker_id: {}".format(execution_id, e, worker_id)
            logger.error(msg)

        logger.debug(f"container finished successfully; worker_id: {worker_id}")
        # Add the completed stats to the execution
        logger.info(f"Actor container finished successfully. Got stats object:{str(stats)}")
        Execution.finalize_execution(actor_id, execution_id, COMPLETE, stats, final_state, exit_code, start_time)
        logger.info(f"Added execution: {execution_id}; worker_id: {worker_id}")

        # Update the worker's last updated and last execution fields:
        try:
            Worker.update_worker_execution_time(actor_id, worker_id)
            logger.debug(f"worker execution time updated. worker_id: {worker_id}")
        except KeyError:
            # it is possible that this worker was sent a gracful shutdown command in the other thread
            # and that spawner has already removed this worker from the store.
            logger.info("worker {} got unexpected key error trying to update its execution time. "
                        "Worker better be shutting down! keep_running: {}".format(worker_id, globals.keep_running))
            if globals.keep_running:
                logger.error("worker couldn't update's its execution time but keep_running is still true!")

        # we completed an execution successfully; reset the consecutive_errors counter
        consecutive_errors = 0
        logger.info(f"worker time stamps updated; worker_id: {worker_id}")
    logger.info(f"global.keep_running no longer true. worker is now exited. worker id: {worker_id}")

def get_container_user(actor, execution_id, actor_id):
    try:
        logger.debug("top of get_container_user")
        uid = None
        gid = None
        if actor.get('use_container_uid'):
            logger.info("actor set to use_container_uid. Returning None for user")
            return None
        # if the run_as_executor attribute is turned on then we need to change the uid and gid of the worker before execution          
        if actor.get('run_as_executor'):
            exec = executions_store[site()][f'{actor_id}_{execution_id}']
            uid = exec['executor_uid']
            gid = exec['executor_gid']
            logger.debug(f"The uid: {uid} and gid: {gid} from the executor.")
        # if there is no executor_uid or gid and get_container_uid is false than we have to use the actor uid and gid
        if not uid:    
            uid = actor.get('uid')
            gid = actor.get('gid')
            logger.debug(f"The uid: {uid} and gid: {gid} from the actor.")
        if not uid:
            user = None
        elif not gid:
            user = uid
        else:
            user = f'{uid}:{gid}'
        return user
    except Exception as e:
        logger.critical(f"get_container_user failed. e: {e}")
        raise

def main():
    """
    Main function for the worker process.

    This function
    """
    worker_id = os.environ.get('worker_id')
    image = os.environ.get('image')
    actor_id = os.environ.get('actor_id')
    revision = os.environ.get('revision')
    try:
        revision = int(revision)
    except ValueError:
        logger.error(f"worker did not get an integer revision number; got: {revision}; "
                     f"worker {actor_id}+{worker_id} exiting.")
        sys.exit()
    client_id = os.environ.get('client_id', None)
    client_access_token = os.environ.get('client_access_token', None)
    client_refresh_token = os.environ.get('client_refresh_token', None)
    tenant = os.environ.get('tenant', None)
    api_server = os.environ.get('api_server', None)
    client_secret = os.environ.get('client_secret', None)

    logger.info(f"Top of main() for worker: {worker_id}, image: {image}; revision: {revision}"
                f"actor_id: {actor_id}; client_id:{client_id}; tenant: {tenant}; api_server: {api_server}")
    spawner_worker_ch = SpawnerWorkerChannel(worker_id=worker_id)

    logger.debug("Worker waiting on message from spawner...")
    result = spawner_worker_ch.get_one()
    logger.debug(f"Worker received reply from spawner. result: {result}.")

    # should be OK to close the spawner_worker_ch on the worker side since spawner was first
    # to open it.
    spawner_worker_ch.delete()
    logger.debug('spawner_worker_ch closed.')

    logger.info(f"Actor {actor_id} status set to READY. subscribing to inbox.")
    worker_ch = WorkerChannel(worker_id=worker_id)
    subscribe(tenant,
              actor_id,
              image,
              revision,
              worker_id,
              api_server,
              worker_ch)


if __name__ == '__main__':
    # This is the entry point for the worker container.
    # Worker containers are launched by spawners and spawners pass the initial configuration
    # data for the worker through environment variables.
    logger.info("Initial log for new worker.")

    # call the main() function:
    try:
        os.system(f'sudo /home/tapis/actors/folder_permissions.sh /home/tapis/runtime_files')
        os.system(f'sudo /home/tapis/actors/folder_permissions.sh /var/run/docker.sock')
        main()
    except Exception as e:
        try:
            worker_id = os.environ.get('worker_id')
        except:
            logger.error(f"worker main thread got exception trying to get worker id from environment."
                         f"not able to send stop-no-delete message to itself. worker_id is unknown.")
            worker_id = ''
        if worker_id:
            try:
                ch = WorkerChannel(worker_id=worker_id)
                # since this is an exception, we don't know that the actor has been deleted
                # don't delete the actor msg channel:
                ch.put('stop-no-delete')
                logger.info(f"Worker main loop sent 'stop-no-delete' message to itself; worker_id: {worker_id}.")
                ch.close()
                msg = f"worker caught exception from main loop. worker exiting. e" \
                      f"Exception: {e} worker_id: {worker_id}"
                logger.info(msg)
            except Exception as e2:
                logger.error(f"worker main thread got exception trying to send stop-no-delete message to itself;"
                             f"worker_id: {worker_id}. e1: {e} e2: {e2}")
    keep_running = False
    sys.exit()