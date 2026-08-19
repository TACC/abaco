import copy
import logging
import os
import shutil
import sys
import threading
import _thread
import time

import cloudpickle
import pika
import pika.exceptions
from pymongo.errors import ConnectionFailure, ExecutionTimeout

from __init__ import t
from auth import get_tenant_verify
from codes import SHUTDOWN_REQUESTED, SHUTTING_DOWN, ERROR, READY, BUSY, COMPLETE, RUNNING
from tapisservice.config import conf
from docker_utils import DockerError, DockerStartContainerError, DockerStopContainerError, execute_actor, pull_image
from errors import WorkerException
from queues import RabbitUnavailableError, connect_with_backoff
import globals
from models import Actor, Execution, Worker, site
from stores import actors_store, workers_store, executions_store, get_site_rabbitmq_uri
from queues import declare_queue, delete_queue, queue_declare_args

from tapisservice.logs import get_logger
logger = get_logger(__name__)

# keep_running will be updated by the thread listening on the worker channel when a graceful shutdown is
# required.
keep_running = True

# maximum number of consecutive errors a worker can encounter before giving up and moving itself into an ERROR state.
MAX_WORKER_CONSECUTIVE_ERRORS = 5

# how long a new worker waits for the spawner's 'READY' handshake before giving up. The spawner sends it
# right after creating the container, so this only trips if the spawner died mid-spawn.
SPAWNER_HANDSHAKE_TIMEOUT_S = 300

RABBIT_URI = get_site_rabbitmq_uri(site())

def channel_loads(body):
    """Decode a message body from a queue shared with the TaskQueue channels, which
    cloudpickle every payload, including the spawner's 'READY' handshake. Falls back
    to utf-8 for a producer that still sends raw bytes during a rolling upgrade."""
    try:
        return cloudpickle.loads(body)
    except Exception:
        return body.decode()


def get_pika_channel(queue_name, uri=None, *, durable=True, auto_delete=False, exclusive=False,
                     expires_ms=None, heartbeat=30, blocked_timeout=30,
                     deadline_s=None, declare=True):
    uri = uri or RABBIT_URI
    # never log `uri`: it carries the broker password
    logger.debug(f"top of get_pika_channel for queue_name: {queue_name}")
    params = pika.URLParameters(uri)
    params.heartbeat = heartbeat
    params.blocked_connection_timeout = blocked_timeout
    # same bounded exponential backoff as every other channel; raises RabbitUnavailableError once
    # deadline_s (default: queues.CONNECT_DEADLINE_S) has passed.
    connection = connect_with_backoff(params, deadline_s=deadline_s)
    channel = connection.channel()
    if declare:
        # must produce the same queue type as TaskQueue's declaration or the broker
        # rejects the redeclare (queue type is fixed at creation).
        arguments = dict(queue_declare_args(queue_name))
        if expires_ms:
            arguments['x-expires'] = expires_ms
        # declare_queue logs a 406 (queue type / argument mismatch) loudly before re-raising.
        declare_queue(channel, queue_name, durable=durable, arguments=arguments,
                      auto_delete=auto_delete, exclusive=exclusive)
    return connection, channel


def _close_quietly(connection):
    try:
        if connection is not None and connection.is_open:
            connection.close()
    except Exception:
        pass


def _reconnect_actor_channel(connection, queue_name, worker_id):
    """Replace the main loop's dead actor-inbox connection. Raises WorkerException when the queue no longer
    exists (deleted along with its actor -- a passive declare, so it is not resurrected) or the broker stays
    unreachable past the connect deadline; the caller lets that propagate to __main__, which sends
    stop-no-delete and terminates the process."""
    _close_quietly(connection)
    try:
        connection, channel = get_pika_channel(queue_name, declare=False)
    except Exception as e:
        raise WorkerException(f"worker {worker_id} could not reconnect to {queue_name}; "
                              f"exiting. {type(e).__name__}: {e}")
    try:
        channel.queue_declare(queue=queue_name, passive=True)
    except Exception as e:
        _close_quietly(connection)
        raise WorkerException(f"actor queue {queue_name} is gone; worker {worker_id} exiting. "
                              f"{type(e).__name__}: {e}")
    channel.confirm_delivery()  # release_unattempted() republishes on this channel
    logger.info(f"worker {worker_id} reconnected to {queue_name}.")
    return connection, channel


def _settle(channel, delivery_tag, ack=True, requeue=False):
    """basic_ack/basic_nack that survives a dead connection: the broker has then already put the delivery
    back and will redeliver it. Log it; the next basic_get fails and takes the reconnect path."""
    try:
        if ack:
            channel.basic_ack(delivery_tag=delivery_tag)
        else:
            channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue)
        return True
    except Exception as e:
        logger.error(f"could not {'ack' if ack else 'nack'} delivery {delivery_tag}: the connection is gone "
                     f"and the broker will redeliver it. {type(e).__name__}: {e}")
        return False


# Delivery accounting on actor_msg_* (quorum queues with a delivery limit, see queues.queue_declare_args and
# the abaco-actor-msg-quorum policy): RabbitMQ 3.x counts EVERY return of a message -- basic_nack/basic_reject with
# requeue=True, and a channel/connection closing while it is unacked -- and dead-letters it once the limit
# is exceeded. AMQP 0.9.1 has no uncounted "release". So:
#   * released, never run (shutting down, stale revision, container never started, or a service the worker
#     needs failing before the run: Mongo unreachable, no execution token): release_unattempted() -- not
#     counted. An outage must not dead-letter healthy messages.
#   * failed before the run for a reason that may be the message's own fault (bad payload, missing
#     execution record): counted nack(requeue=True) and the worker exits, so each worker spends at most one
#     delivery on it -- this is the poison protection.
#   * worker/broker died while holding it: counted by the broker.
MONGO_UNAVAILABLE = (ConnectionFailure, ExecutionTimeout)


def release_unattempted(channel, queue_name, delivery_tag, properties, body):
    """Give back a message this worker never started running without spending its delivery budget: publish
    a copy (publisher-confirmed, mandatory) and ack the original. The copy goes to the tail of the queue
    with a fresh delivery count. If the publish fails, fall back to a counted nack(requeue=True) -- never
    drop the message. A connection dying between the publish and the ack also redelivers the original (one
    duplicate run), a far smaller window than the counted-requeue loop this replaces."""
    props = copy.copy(properties) if properties is not None else pika.BasicProperties(delivery_mode=2)
    if props.headers:
        props.headers = {k: v for k, v in props.headers.items() if k != 'x-delivery-count'} or None
    try:
        channel.basic_publish(exchange='', routing_key=queue_name, body=body, properties=props, mandatory=True)
    except Exception as e:
        logger.error(f"could not republish delivery {delivery_tag} to {queue_name}; falling back to a "
                     f"counted requeue. {type(e).__name__}: {e}")
        return _settle(channel, delivery_tag, ack=False, requeue=True)
    return _settle(channel, delivery_tag)


# How long a worker holding a redelivered message waits for that execution's original worker to answer a ping.
OWNER_PING_WAIT_S = 30


def redelivery_already_handled(actor_id, execution_id, worker_id, ping, pump, wait_s=OWNER_PING_WAIT_S):
    """For a REDELIVERED actor message, return why it must not run again, or None to run it.

    The broker redelivers every unacked message when a connection drops (a broker restart, a missed heartbeat),
    including those whose executions are still running on their original worker: that worker's ack then fails on
    the dead connection, and running the copy executed the actor twice. Skip the copy when the execution already
    finished, or when it is RUNNING (its container started) on another worker that PROVES it is alive: we send
    'status' to that worker's control queue (`ping(owner)`; False if the queue is gone) and it must update its
    last_health_check_time within wait_s, while `pump(seconds)` keeps our own connection serviced. A worker
    record alone proves nothing: one that was OOM-killed or lost its host stays BUSY until health removes it,
    and skipping on it lost the execution. On any doubt, or any lookup failure, return None: the message runs
    again (at-least-once)."""
    try:
        ex = executions_store[site()][f'{actor_id}_{execution_id}']
    except KeyError:
        return None
    except Exception as e:
        logger.warning(f"could not check redelivered execution {execution_id}; running it. {type(e).__name__}: {e}")
        return None
    status = ex.get('status')
    if status in (COMPLETE, ERROR):
        return f"execution {execution_id} is already {status}"
    owner = ex.get('worker_id')
    if status != RUNNING or not owner or owner == worker_id:
        return None
    key = f'{actor_id}_{owner}'
    try:
        record = workers_store[site()].get(key)
        if not record or record.get('status') != BUSY:
            return None
        before = record.get('last_health_check_time')
        if not ping(owner):
            return None
        give_up_at = time.monotonic() + wait_s
        while time.monotonic() < give_up_at:
            pump(1)
            record = workers_store[site()].get(key)
            if not record or record.get('status') != BUSY:
                return None
            if record.get('last_health_check_time') != before:
                return f"execution {execution_id} is still running on worker {owner}, which answered a ping"
        logger.info(f"worker {owner} did not answer within {wait_s}s; running redelivered execution {execution_id}")
        return None
    except Exception as e:
        logger.warning(f"could not confirm worker {owner} is alive; running redelivered execution {execution_id}. "
                       f"{type(e).__name__}: {e}")
        return None


def _ping_worker(channel, owner):
    """Send 'status' to another worker's control queue on our own (confirmed) channel. False if the queue is gone
    (unroutable: that worker was reaped) or the publish failed."""
    try:
        channel.basic_publish(exchange='', routing_key=f'worker_{owner}', body=cloudpickle.dumps('status'),
                              properties=pika.BasicProperties(delivery_mode=2), mandatory=True)
        return True
    except Exception as e:
        logger.info(f"could not ping worker {owner}: {type(e).__name__}: {e}")
        return False


# Set while the main thread holds an actor message it has not started running yet. A stop makes the control
# thread os._exit() the process; doing that while a message is held killed it mid-release, so the requeue was
# counted against the delivery limit, or (between the republished copy and the ack) the message ran twice.
# The control thread waits for this to clear, at most STOP_RELEASE_WAIT_S.
HELD = threading.Event()
STOP_RELEASE_WAIT_S = 30


def _wait_for_held_message(grace_s=STOP_RELEASE_WAIT_S):
    give_up_at = time.monotonic() + grace_s
    while HELD.is_set() and time.monotonic() < give_up_at:
        time.sleep(0.2)


def _connection_alive(connection, channel):
    """Service the connection once and report whether it (and the channel) still work. The steps between taking
    a message and running it (Mongo, Tokens) do not service heartbeats; if they took long enough for the broker
    to drop the connection, the broker has already put the message back, and running it here would run it twice."""
    try:
        connection.process_data_events(time_limit=0)
        return bool(connection.is_open and channel.is_open)
    except Exception:
        return False


def run_with_heartbeats(connection, fn, *args):
    """Run fn(*args) on a helper thread while this thread -- the one that owns `connection` -- keeps
    servicing it. A BlockingConnection only answers heartbeats inside I/O calls, so an execution blocking
    this thread for more than two heartbeat intervals got the connection dropped by the broker: the unacked
    message was requeued (a duplicate run) and the final basic_ack raised. pika is not thread-safe, so the
    helper never touches the connection and acks/nacks stay on this thread. Returns fn's result or re-raises
    its exception here, so the caller's except clauses work unchanged."""
    outcome = {}

    def target():
        try:
            outcome['result'] = fn(*args)
        except BaseException as e:
            outcome['error'] = e

    helper = threading.Thread(target=target, name='execute_actor', daemon=True)
    helper.start()
    pumping = True
    while helper.is_alive():
        if pumping:
            try:
                connection.process_data_events(time_limit=1)
            except Exception as e:
                # the connection is gone; let the execution finish -- the ack after it fails and the
                # main loop reconnects.
                logger.error(f"actor connection failed during an execution; {type(e).__name__}: {e}")
                pumping = False
        else:
            helper.join(timeout=1)
    if 'error' in outcome:
        raise outcome['error']
    return outcome['result']

def shutdown_worker(actor_id, worker_id, delete_actor_ch=True):
    """Gracefully shutdown a single worker.
    actor_id (str) - the dbid of the associated actor.
    Returns False only when RabbitMQ could not be reached at all, so a caller stopping many workers can stop
    paying the connect deadline once per worker.
    """
    logger.debug(f"top of shutdown_worker for worker_id: {worker_id}")
    # set the worker status to SHUTDOWN_REQUESTED:
    try:
        Worker.update_worker_status(actor_id, worker_id, SHUTDOWN_REQUESTED)
    except Exception as e:
        logger.error(f"worker got exception trying to update status to SHUTODWN_REQUESTED. actor_id: {actor_id};"
                     f"worker_id: {worker_id}; exception: {e}")
    queue_name = f"worker_{worker_id}"
    if not delete_actor_ch:
        msg = "stop-no-delete"
    else:
        msg = "stop"
    connection = None
    try:
        # Declare the queue, as trunk's WorkerChannel(...).put() did. The spawner records a worker as READY a
        # few seconds before the worker process declares worker_<id>; a stop sent in that window (the
        # autoscaler scaling an idle actor down) was unroutable without the declare, and the worker came up
        # stuck in SHUTDOWN_REQUESTED, alive until its TTL. A queue declared for a worker that is already gone
        # is removed by health's orphan sweep (check_orphan_worker_queues).
        connection, channel = get_pika_channel(queue_name)
        channel.confirm_delivery()
        channel.basic_publish(exchange='', routing_key=queue_name, body=cloudpickle.dumps(msg), mandatory=True)
        logger.info(f"A '{msg}' message was sent to worker: {worker_id}")
    except RabbitUnavailableError as e:
        logger.error(f"Could not reach RabbitMQ to send '{msg}' to worker {worker_id}: {e}")
        return False
    except Exception as e:
        logger.info(f"Could not publish shutdown message to {queue_name}; it may not exist. worker_id: {worker_id}; e: {e}")
    finally:
        _close_quietly(connection)
    return True


def shutdown_workers(actor_id, delete_actor_ch=True):
    """
    Graceful shutdown of all workers for an actor. Arguments:
    * actor_id (str) - the db_id of the actor
    * delete_actor_ch (bool) - whether the worker shutdown process should also delete the actor_ch. This should be true
      whenever the actor is being removed. This will also force quit any currently running executions.
    """
    logger.debug(f"shutdown_workers() called for actor: {actor_id}")
    workers = []
    try:
        workers = Worker.get_workers(actor_id)
    except Exception as e:
        logger.error(f"Got exception from get_workers: {e}")
    if not workers:
        logger.info(f"shutdown_workers did not receive any workers from Worker.get_workers for actor: {actor_id}")
    # @TODO - this code is not thread safe. we need to update the workers state in a transaction:
    for idx, worker in enumerate(workers):
        if not shutdown_worker(actor_id, worker['id'], delete_actor_ch):
            # Each attempt waits out the connect deadline (10s on an API request), so with the broker down a
            # DELETE of an actor with many workers outlasted nginx. Mark the rest SHUTDOWN_REQUESTED without
            # messaging them, the state a failed message would have left them in anyway. Health hard-deletes
            # workers whose actor is gone, and its TTL check sends a live actor's workers a new stop.
            rest = workers[idx + 1:]
            if rest:
                logger.error(f"RabbitMQ unreachable; not messaging the remaining {len(rest)} worker(s) of actor "
                             f"{actor_id}, only marking them {SHUTDOWN_REQUESTED}.")
            for other in rest:
                try:
                    Worker.update_worker_status(actor_id, other['id'], SHUTDOWN_REQUESTED)
                except Exception as e:
                    logger.error(f"could not mark worker {other['id']} {SHUTDOWN_REQUESTED}; e: {e}")
            break


def _open_worker_channel(queue_name, worker_id):
    """(Re)connect the worker-channel thread. Returns (connection, channel), or None when the queue no longer
    exists (404): the system reaped this worker. Any other failure (broker down or restarting) is retried
    here. Only a 404 means "reaped"; an unreachable broker also breaks the main thread's actor channel, whose
    reconnect gives up after its deadline and takes the whole process down."""
    backoff = 2
    while True:
        connection = None
        try:
            connection, channel = get_pika_channel(queue_name, declare=False)
            # passive: a plain declare would resurrect a reaped worker's queue
            channel.queue_declare(queue=queue_name, passive=True)
            return connection, channel
        except pika.exceptions.ChannelClosedByBroker as e:
            _close_quietly(connection)
            if e.reply_code == 404:
                return None
            error = e
        except Exception as e:
            _close_quietly(connection)
            error = e
        logger.error(f"worker {worker_id} could not reopen {queue_name}; retrying in {backoff}s. "
                     f"{type(error).__name__}: {error}")
        time.sleep(backoff)
        backoff = min(backoff * 2, 30)


def process_worker_ch(tenant, worker_id, actor_id, actor_queue_name):
    """ Target for a thread to listen on the worker channel for a message to stop processing.
    :param worker_ch:
    :return:
    """
    global keep_running
    logger.info(f"Worker subscribing to worker channel...{actor_id}_{worker_id}")
    queue_name = f"worker_{worker_id}"
    connection, channel = get_pika_channel(queue_name)
    while keep_running:
        try:
            method_frame, header_frame, body = channel.basic_get(queue=queue_name, auto_ack=False)
            if not method_frame:
                time.sleep(1)
                continue
            # Receiving a control message is enough to ack it: health re-sends 'status', and a stop is acted on
            # below. Inside the try so that a connection which died here takes the reconnect path instead of
            # killing this thread (the worker would keep running with nobody listening for stop or status).
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        except Exception as e:
            # A closed channel never recovers by retrying the same handle: once the broker closes it, every
            # basic_get raises instantly, so retrying the same handle would spin forever while the worker
            # holds its Mongo pool. Reconnect; exit only if the queue is gone.
            logger.error(f"worker {worker_id} got exception ({type(e).__name__}) reading the "
                         f"worker channel; reconnecting. e: {e}")
            _close_quietly(connection)
            time.sleep(2)
            reopened = _open_worker_channel(queue_name, worker_id)
            if reopened is None:
                logger.info(f"worker channel {queue_name} is gone; worker was reaped -- exiting.")
                globals.keep_running = False
                globals.force_quit = True
                _wait_for_held_message()
                _thread.interrupt_main()
                os._exit(0)
            connection, channel = reopened
            continue
        try:
            msg = channel_loads(body)
        except Exception as e:
            # already acked: dropping it is the only way it does not come back forever
            logger.error(f"worker {worker_id} dropped an undecodable message on {queue_name}. "
                         f"{type(e).__name__}: {e}")
            continue
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
            logger.info(f"Worker with worker_id: {worker_id} (actor_id: {actor_id}) received a force_quit message, forcing the execution to halt...")
            globals.force_quit = True
            globals.keep_running = False

        elif msg == 'stop' or msg == 'stop-no-delete':
            logger.info(f"Worker with worker_id: {worker_id} (actor_id: {actor_id}) received stop message, stopping worker...")
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
            delete_actor_ch = msg != 'stop-no-delete' # get msg with status and compare
            # handle stop-no-delete vs stop semantics and remove Worker registry entry
            if msg == 'stop-no-delete':
                logger.info(f"Got stop-no-delete; will not delete actor_ch. {actor_id}_{worker_id}")
                delete_actor_ch = False
            # if a `stop` was sent, the actor is being deleted, and so we want to immediately shutdown processing.
            else:
                globals.force_quit = True
            try:
                Worker.delete_worker(actor_id, worker_id)
            except WorkerException as e:
                logger.info(f"Got WorkerException from delete_worker(). "
                            f"worker_id: {worker_id} Exception: {e}")
            # delete associated channels:
            # it is possible the actor channel was already deleted, in which case we just keep processing
            if delete_actor_ch:
                try:
                    # never declare in a delete path (see queues.delete_queue)
                    delete_queue(actor_queue_name)
                    logger.info(f"ActorChannel deleted for actor: {actor_id} worker_id: {worker_id}")
                except Exception as e:
                    logger.info(f"Got exception deleting ActorChannel for actor: {actor_id} worker_id: {worker_id}; exception: {e}")
            try:
                channel.queue_delete(queue=queue_name)
                logger.info(f"WorkerChannel deleted for actor: {actor_id} worker_id: {worker_id}")
            except Exception as e:
                logger.info(f"Got exception deleting WorkerChannel for actor: {actor_id} worker_id: {worker_id}; exception: {e}")
            _close_quietly(connection)
            # let the main thread give back a message it took but has not started (it sees keep_running=False)
            _wait_for_held_message()
            logger.info(f"Worker with worker_id: {worker_id} is now exiting.")
            _thread.interrupt_main()
            logger.info(f"main thread interrupted, worker {actor_id}_{worker_id} issuing os._exit()...")
            os._exit(0)
    _close_quietly(connection)

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
                                          use_basic_auth=False,
                                          _tapis_set_x_headers_from_service=True)
        elasped_time = time.time() - start
        if elasped_time >= 2:
            logger.error(f"Time to create execution token: {elasped_time}")
        return token_res.access_token.access_token
    except Exception as e:
        logger.error(f"Time to fail at creating execution token: {time.time() - start}")
        msg = f"Got exception trying to create actor access_token; exception: {e}"
        logger.error(msg)
        raise WorkerException(msg)

def subscribe(tenant, actor_id, image, revision, worker_id, api_server):
    """
    Main loop for the Actor executor worker. Subscribes to the actor's inbox and executes actor
    containers when message arrive. Also launches a separate thread which ultimately subscribes to the worker channel
    for future communications.
    :return:
    """
    logger.debug(f"Top of subscribe(). worker_id: {worker_id}")
    # Must match ActorMsgChannel in channels.py (actor_msg_<id>), the queue that the
    # messages API publishes to.
    actor_queue_name = f"actor_msg_{actor_id}"
    worker_queue_name = f"worker_{worker_id}"
    # establish configs for this worker -------
    leave_containers = conf.worker_leave_containers
    logger.debug(f"leave_containers: {leave_containers}")

    mem_limit = str(conf.worker_mem_limit)
    logger.debug(f"mem_limit: {mem_limit}")

    max_cpus = conf.worker_max_cpus
    logger.debug(f"max_cpus: {max_cpus}")

    # start a separate thread for handling messages sent to the worker channel ----
    logger.info("Starting the process worker channel thread.")
    t = threading.Thread(target=process_worker_ch, args=(tenant, worker_id, actor_id, actor_queue_name))
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
    connection, channel = get_pika_channel(actor_queue_name)
    channel.confirm_delivery()  # release_unattempted() republishes on this channel
    while globals.keep_running:
        HELD.clear()
        logger.debug(f"top of keep_running; worker id: {worker_id}")
        if update_worker_status:
            Worker.update_worker_status(actor_id, worker_id, READY)
            logger.debug(f"updated worker status to READY in SUBSCRIBE; worker id: {worker_id}")
            update_worker_status = False
        try:
            method_frame, header_frame, body = channel.basic_get(queue=actor_queue_name, auto_ack=False)
        except Exception as e:
            # A closed channel never recovers by retrying the same handle, and sys.exit() here only ended the
            # main thread: the non-daemon worker-channel thread kept answering health pings, a zombie that
            # looked READY forever. Reconnect instead; _reconnect_actor_channel raises (-> __main__ sends
            # stop-no-delete and terminates the process) if the queue is gone or the broker stays down.
            logger.warning(f"worker {worker_id} lost the actor channel ({type(e).__name__}: {e}); reconnecting.")
            connection, channel = _reconnect_actor_channel(connection, actor_queue_name, worker_id)
            continue
        if not method_frame:
            time.sleep(1)
            continue
        msg_obj = method_frame
        HELD.set()
        try:
            msg = channel_loads(body)
        except Exception as e:
            # A message that cannot be deserialized is poison by definition -- it will
            # never process no matter how many workers try. Dead-letter it immediately
            # (nack, no requeue) so it lands in abaco_dlq instead of looping.
            logger.error(f"worker {worker_id} could not deserialize a message on {actor_queue_name}; "
                         f"dead-lettering it. e: {e}")
            _settle(channel, msg_obj.delivery_tag, ack=False, requeue=False)
            continue
        logger.info(f"worker {worker_id} processing new msg.")
        # Load the actor for every message: the revision check below must see the
        # actor's current revision, and mounts can change between messages.
        try:
            actor = Actor.from_db(actors_store[site()][actor_id])
        except Exception as e:
            # Mongo unreachable, or the actor was just deleted (its stop message is on the way): not the
            # message's fault, so not counted against the delivery limit.
            logger.error(f"worker {worker_id} could not load actor {actor_id}; giving the message back and "
                         f"exiting. {type(e).__name__}: {e}")
            release_unattempted(channel, actor_queue_name, msg_obj.delivery_tag, header_frame, body)
            raise

        # worker ch thread has received a stop and is already shutting us down (see note above); we need to nack this
        # message and exit
        if not globals.keep_running:
            logger.info("got msg from get_one() but globals.keep_running was False! Requeing message and worker will exit. {}+{}".format(actor_id, worker_id))
            # never run: not counted against the delivery limit
            release_unattempted(channel, actor_queue_name, msg_obj.delivery_tag, header_frame, body)
            logger.info(f"message requeued; worker exiting:{actor_id}_{worker_id}")
            time.sleep(5)
            connection.close()
            raise Exception()
        if not revision == actor.revision:
            logger.info(f"got msg from get_one() but worker's revision ({revision}) was different "
                        f"from actor.revision ({actor.revision}). Requeing message and worker will "
                        f"exit. {actor_id}+{worker_id}")
            # never run: not counted against the delivery limit. During an actor update every stale
            # worker can grab the same message once, so counted requeues dead-lettered healthy messages.
            release_unattempted(channel, actor_queue_name, msg_obj.delivery_tag, header_frame, body)
            logger.info(f"message requeued; worker exiting:{actor_id}_{worker_id}")
            time.sleep(5)
            raise Exception()
        if msg_obj.redelivered and isinstance(msg, dict):
            skip = redelivery_already_handled(actor_id, msg.get('_abaco_execution_id'), worker_id,
                                              lambda owner: _ping_worker(channel, owner),
                                              lambda seconds: connection.process_data_events(time_limit=seconds))
            if skip:
                logger.info(f"worker {worker_id} dropping a redelivered message: {skip}")
                _settle(channel, msg_obj.delivery_tag)
                continue

        try:
            Worker.update_worker_status(actor_id, worker_id, BUSY)
        except Exception as e:
            logger.error("unexpected exception from call to update_worker_status. Nacking message."
                         "actor_id: {}; worker_id: {}; status: {}; exception: {}".format(actor_id,
                                                                                         worker_id,
                                                                                         BUSY,
                                                                                         e))
            logger.info(f"worker exiting. {actor_id}_{worker_id}")
            # the worker's own record, not the message: not counted (see delivery accounting above)
            release_unattempted(channel, actor_queue_name, msg_obj.delivery_tag, header_frame, body)
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
            # counted on purpose: a message without these fields is malformed and should reach the DLQ
            channel.basic_nack(delivery_tag=msg_obj.delivery_tag, requeue=True)
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
            # never run: not counted against the delivery limit. During an actor update every stale
            # worker can grab the same message once, so counted requeues dead-lettered healthy messages.
            release_unattempted(channel, actor_queue_name, msg_obj.delivery_tag, header_frame, body)
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
                logger.error(f"Could not create fifo_path at {fifo_host_path}. Releasing message. Exception: {e}")
                # a problem on this host, not with the message: not counted; this worker exits
                release_unattempted(channel, actor_queue_name, msg_obj.delivery_tag, header_frame, body)
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
            if isinstance(e, MONGO_UNAVAILABLE):
                release_unattempted(channel, actor_queue_name, msg_obj.delivery_tag, header_frame, body)
            else:
                # counted on purpose: e.g. the execution record is gone; this worker exits
                channel.basic_nack(delivery_tag=msg_obj.delivery_tag, requeue=True)
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
        try:
            user = get_container_user(actor, execution_id, actor_id)
        except Exception as e:
            logger.error(f"worker {worker_id} could not determine the container user for execution "
                         f"{execution_id}; exiting. {type(e).__name__}: {e}")
            if isinstance(e, MONGO_UNAVAILABLE):
                release_unattempted(channel, actor_queue_name, msg_obj.delivery_tag, header_frame, body)
            else:
                # counted on purpose: the execution record lacks the executor's uid/gid
                channel.basic_nack(delivery_tag=msg_obj.delivery_tag, requeue=True)
            raise
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
            try:
                token = get_execution_token(actor.tenant, actor.owner)
            except Exception as e:
                # the Tokens service, not the message: not counted, so an outage cannot dead-letter it
                logger.error(f"worker {worker_id} could not create an execution token; giving the message back "
                             f"and exiting. {type(e).__name__}: {e}")
                release_unattempted(channel, actor_queue_name, msg_obj.delivery_tag, header_frame, body)
                raise
            environment['_abaco_access_token'] = token
        logger.info(f"Passing update environment: {environment}")
        if not globals.keep_running:
            # a stop arrived while this message was being prepared (the BUSY transition above fails quietly
            # once the record is SHUTDOWN_REQUESTED): give it back instead of starting a run the stop kills
            logger.info(f"worker {worker_id} stopping before the run; giving the message back.")
            release_unattempted(channel, actor_queue_name, msg_obj.delivery_tag, header_frame, body)
            break
        if not _connection_alive(connection, channel):
            logger.warning(f"worker {worker_id} lost the actor connection before running execution {execution_id}; "
                           f"the broker has requeued the message, so not running it here.")
            continue
        HELD.clear()
        logger.info(f"About to execute actor; worker_id: {worker_id}")
        logger.info(f"Executed actor mounts: {mounts}")
        try:
            # executions run for minutes; run_with_heartbeats keeps the connection holding the unacked
            # message alive meanwhile.
            stats, logs, final_state, exit_code, start_time = run_with_heartbeats(connection,
                                                                                  execute_actor,
                                                                                  actor_id,
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
            # if we failed to start the actor container, we leave the worker up and re-queue the original message.
            # The container never started (execute_actor's contract), so this is not counted against the
            # delivery limit: up to MAX_WORKER_CONSECUTIVE_ERRORS+1 counted requeues per worker exceeded it.
            # consecutive_errors still bounds the retries and puts the actor in ERROR.
            release_unattempted(channel, actor_queue_name, msg_obj.delivery_tag, header_frame, body)
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
            _settle(channel, msg_obj.delivery_tag)
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
            _settle(channel, msg_obj.delivery_tag)
            Execution.update_status(actor_id, execution_id, ERROR)
            shutdown_workers(actor_id, delete_actor_ch=False)
            # wait for worker to be shutdown..
            time.sleep(60)
            break
        # ack the message. If the connection died meanwhile the broker redelivers it (a duplicate run, the
        # unavoidable cost of a genuine broker failure); the next basic_get reconnects.
        _settle(channel, msg_obj.delivery_tag)

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
    HELD.clear()
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
            user = f'{uid}:{uid}'
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

    logger.info(f"Top of main() for worker: {worker_id}, image: {image}; revision: {revision} "
                f"actor_id: {actor_id}; client_id:{client_id}; tenant: {tenant}; api_server: {api_server}")
    # Must match SpawnerWorkerChannel in channels.py (spawner_worker_<id>), the queue
    # the spawner sends READY to; any other name leaves the worker waiting forever.
    spawner_queue_name = f"spawner_worker_{worker_id}"
    connection, channel = get_pika_channel(spawner_queue_name)

    logger.debug("Worker waiting on message from spawner...")
    give_up_at = time.monotonic() + SPAWNER_HANDSHAKE_TIMEOUT_S
    next_log_at = time.monotonic() + 30
    while True:
        method_frame, header_frame, body = channel.basic_get(queue=spawner_queue_name, auto_ack=True)
        if method_frame:
            result = channel_loads(body)
            break
        now = time.monotonic()
        if now >= give_up_at:
            _close_quietly(connection)
            raise WorkerException(f"worker {worker_id} got no handshake from the spawner on {spawner_queue_name} "
                                  f"within {SPAWNER_HANDSHAKE_TIMEOUT_S}s; exiting.")
        if now >= next_log_at:
            logger.info(f"worker {worker_id} still waiting for the spawner handshake on {spawner_queue_name}...")
            next_log_at = now + 30
        time.sleep(1)
    logger.debug(f"Worker received reply from spawner. result: {result}.")

    # should be OK to close the spawner_worker_ch on the worker side since spawner was first
    # to open it.
    channel.queue_delete(queue=spawner_queue_name)
    connection.close()
    logger.debug('spawner_worker_ch closed.')
    logger.info(f"Actor {actor_id} status set to READY. subscribing to inbox.")
    subscribe(tenant, actor_id, image, revision, worker_id, api_server)


def exit_process(code, grace_s=60):
    """Terminate the whole worker process. sys.exit() only ends the main thread: the interpreter then waits
    forever on the non-daemon worker-channel thread, which keeps answering health 'status' pings -- a zombie
    that looks READY. Give that thread grace_s to act on a stop-no-delete (it deletes the worker record and
    queue, then os._exit()s itself), then flush the logs and exit regardless."""
    give_up_at = time.monotonic() + grace_s
    for thread in threading.enumerate():
        if thread is not threading.current_thread() and not thread.daemon:
            thread.join(timeout=max(0.0, give_up_at - time.monotonic()))
    logger.info(f"worker process exiting; code: {code}")
    logging.shutdown()
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(code)


if __name__ == '__main__':
    # This is the entry point for the worker container.
    # Worker containers are launched by spawners and spawners pass the initial configuration
    # data for the worker through environment variables.
    logger.info("Initial log for new worker.")

    exit_code = 0
    # call the main() function:
    try:
        os.system(f'sudo /usr/local/sbin/folder_permissions.sh /home/tapis/runtime_files')
        os.system(f'sudo /usr/local/sbin/folder_permissions.sh /var/run/docker.sock')
        main()
    except Exception as e:
        exit_code = 1
        HELD.clear()  # every raise out of subscribe() released or nacked its message first
        try:
            worker_id = os.environ.get('worker_id')
        except:
            logger.error(f"worker main thread got exception trying to get worker id from environment. not able to send stop-no-delete message to itself. worker_id is unknown.")
            worker_id = ''
        if worker_id:
            try:
                queue_name = f"worker_{worker_id}"
                # since this is an exception, we don't know that the actor has been deleted; don't delete the
                # actor msg channel. declare=False: a worker queue that was already reaped is not resurrected.
                connection, channel = get_pika_channel(queue_name, declare=False)
                channel.basic_publish(exchange='', routing_key=queue_name, body=cloudpickle.dumps('stop-no-delete'))
                logger.info(f"Worker main loop sent 'stop-no-delete' message to itself; worker_id: {worker_id}.")
                connection.close()
                msg = f"worker caught exception from main loop. worker exiting. eException: {e} worker_id: {worker_id}"
                logger.info(msg)
            except Exception as e2:
                logger.error(f"worker main thread got exception trying to send stop-no-delete message to itself;"
                             f"worker_id: {worker_id}. e1: {e} e2: {e2}")
    else:
        # process_worker_ch polls `while keep_running` (this module's global, not globals.keep_running), so
        # this ends that thread within ~1s. Only on a clean return: after an exception the thread must stay up
        # long enough to act on the stop-no-delete sent above (worker record + queue cleanup).
        keep_running = False
    exit_process(exit_code)