import cloudpickle
import json
import time

import pika
import pika.exceptions

from flask import g, has_request_context
from tapisservice.config import conf
from tapisservice.logs import get_logger
logger = get_logger(__name__)
from errors import ResourceError
from stores import get_site_rabbitmq_uri

# How long a connect may keep retrying before giving up. Daemons (spawner, events,
# health) run their own reconnect loops, so they only need a window that rides out a
# broker restart; a gunicorn request thread must give up well before nginx does
# (proxy_read_timeout 125s), otherwise every API thread piles up behind a dead broker.
# The old contract was 300 attempts at up to 30s backoff: ~2.5 hours per thread.
CONNECT_DEADLINE_S = float(conf.get('rabbit_connect_deadline_s') or 60)
REQUEST_CONNECT_DEADLINE_S = float(conf.get('rabbit_request_connect_deadline_s') or 10)


def site():
    try:
        return g.site_id
    except:
        return conf.get('site_id')


class ChannelClosedException(Exception):
    pass


class ChannelTimeoutException(Exception):
    pass


# Every error the pika transport can raise on a dead/dying broker connection. Call
# sites that used to catch rabbitpy.exceptions.RabbitpyException should catch this.
# ConnectionResetError/OSError cover the socket dying under pika at moments where it
# surfaces the raw error; RuntimeError preserves the old RabbitConnection contract.
# ChannelClosedException is what get_one() raises when it finds its channel already
# closed; the daemons must treat it as "reconnect", not die on it.
RABBIT_ERRORS = (pika.exceptions.AMQPError, ConnectionResetError, OSError, RuntimeError,
                 ChannelClosedException)


# Errors from a confirmed basic_publish that PROVE the message was not enqueued: the
# broker returned it (Unroutable) or refused it (Nack), or the channel was already
# closed so nothing was sent (WrongState). Any other RABBIT_ERRORS out of a publish
# (StreamLostError, ConnectionClosed, ...) is ambiguous: the frames may have reached the
# broker before the connection died and the confirm was lost.
PUBLISH_REFUSED_ERRORS = (pika.exceptions.UnroutableError, pika.exceptions.NackError,
                          pika.exceptions.ChannelWrongStateError)


class RabbitUnavailableError(ResourceError, RuntimeError):
    """No connection within the deadline. A ResourceError so the API layer answers 503
    instead of 500; a RuntimeError so daemon loops catching RABBIT_ERRORS still work."""

    def __init__(self, msg):
        ResourceError.__init__(self, msg, 503)
        RuntimeError.__init__(self, msg)


def _default_deadline_s():
    return REQUEST_CONNECT_DEADLINE_S if has_request_context() else CONNECT_DEADLINE_S


def connect_with_backoff(params, deadline_s=None, retries=300):
    """Open a pika BlockingConnection, retrying with exponential backoff (0.5s..30s)
    until it succeeds, `retries` attempts are used, or `deadline_s` seconds have
    elapsed, whichever comes first. The default deadline depends on who is asking: a
    flask request thread gets REQUEST_CONNECT_DEADLINE_S, everything else
    CONNECT_DEADLINE_S. Each attempt is itself bounded by pika's socket/stack timeouts
    (10s/15s), so the deadline can overshoot by about that much. Raises
    RabbitUnavailableError when giving up."""
    if deadline_s is None:
        deadline_s = _default_deadline_s()
    give_up_at = time.monotonic() + deadline_s
    tries = 0
    backoff = 0.5
    max_backoff = 30
    while True:
        try:
            return pika.BlockingConnection(params)
        except Exception as e:
            # A failed connect attempt is always retryable. pika does not raise every
            # connect failure as AMQPConnectionError: a paused or booting broker gives
            # 'Timeout during AMQP handshake' through a connection-workflow error outside
            # the AMQPError tree, so this catches Exception.
            tries += 1
            remaining = give_up_at - time.monotonic()
            if tries >= retries or remaining <= 0:
                raise RabbitUnavailableError(f"Could not connect to RabbitMQ within {deadline_s:.0f}s "
                                             f"({tries} attempt(s)); last error "
                                             f"{type(e).__name__}: {e}")
            wait = min(backoff, remaining)
            logger.warning(f"RabbitMQ connection attempt {tries} failed ({type(e).__name__}): {e}. "
                           f"Retrying in {wait:.1f}s ({remaining:.0f}s left before giving up)...")
            time.sleep(wait)
            backoff = min(backoff * 2, max_backoff)


class RabbitConnection(object):
    """One pika BlockingConnection + channel with a bounded retry/backoff contract.
    heartbeat=30 means a dead broker is *detected* within ~2 intervals instead of the
    connection silently rotting -- rabbitpy connections here ran without an applied
    heartbeat, which is how stale connections accumulated across broker restarts.
    BlockingConnection services heartbeats only inside its own calls; TaskQueue.get_one()
    keeps calling it while waiting, so long-idle readers (the spawner command queue)
    stay alive."""

    def __init__(self, retries=300, heartbeat=30, deadline_s=None):
        self._uri = get_site_rabbitmq_uri(site())
        params = pika.URLParameters(self._uri)
        params.heartbeat = heartbeat
        params.blocked_connection_timeout = 30
        self._conn = connect_with_backoff(params, deadline_s=deadline_s, retries=retries)
        self._ch = self._conn.channel()
        self._ch.basic_qos(prefetch_count=1)

    def close(self):
        """Close this instance of the connection. Must be called from the thread that
        uses the connection -- pika BlockingConnection is not thread-safe."""
        try:
            if self._ch.is_open:
                self._ch.close()
        except pika.exceptions.AMQPError as e:
            logger.debug(f"error closing rabbit channel (already closing?): {e}")
        try:
            if self._conn.is_open:
                self._conn.close()
        except pika.exceptions.AMQPError as e:
            logger.debug(f"error closing rabbit connection (already closing?): {e}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        del exc_type, exc_val, exc_tb
        self.close()


class Message(object):
    """The slice of the rabbitpy Message surface that call sites actually use:
    .body, .ack(), .nack(requeue=). Wraps one pika delivery."""

    __slots__ = ('_ch', 'delivery_tag', 'redelivered', 'body')

    def __init__(self, ch, method, body):
        self._ch = ch
        self.delivery_tag = method.delivery_tag
        self.redelivered = bool(getattr(method, 'redelivered', False))
        self.body = body

    def ack(self):
        self._ch.basic_ack(self.delivery_tag)

    def nack(self, requeue=False):
        self._ch.basic_nack(self.delivery_tag, requeue=requeue)


class _QueueHandle(object):
    """Backs the legacy `ch._queue._queue` attribute chain. The old code exposed the
    rabbitpy Queue there and call sites use exactly two things: len() (queue depth,
    via passive declare) and purge()."""

    def __init__(self, taskq):
        self._tq = taskq

    def __len__(self):
        frame = self._tq._ch.queue_declare(queue=self._tq.name, passive=True)
        return frame.method.message_count

    def purge(self):
        self._tq._ch.queue_purge(self._tq.name)


class LegacyQueue(object):
    """
    This class is here to support existing code that expects an _queue object on the various channel
    objects (e.g., ActorMsgChannel object). Client code makes use of the ch._queue._queue attribute.
    """
    pass


# Queues that must never lose a message get the quorum treatment (Raft-replicated on
# 3.8+; delivery-counted, so DLX delivery-limit policies work). Short-lived per-worker
# channels stay classic: creating/destroying a Raft group per worker handshake is churn
# for no durability benefit. Off unless rabbit_queue_type: quorum is set in config --
# requires a 3.8+ broker, and a queue's type is fixed at first declaration.
QUORUM_PREFIXES = ('actor_msg_', 'command_channel_', 'events_channel_')


def queue_declare_args(name):
    if (conf.get('rabbit_queue_type') or 'classic') != 'quorum':
        return {}
    if name and name.startswith(QUORUM_PREFIXES):
        # Poison protection (the delivery limit -> abaco_dlx -> abaco_dlq) is applied by
        # the abaco-actor-msg-quorum POLICY from stores.rabbit_initialization, deliberately not
        # as an x-delivery-limit argument here: queue arguments are fixed at declaration,
        # so tuning rabbit_delivery_limit would 406 every existing queue. See docs/rabbitmq.md.
        return {'x-queue-type': 'quorum'}
    return {}


QUEUE_MIGRATION_DOC = 'docs/rabbitmq.md'


def declare_queue(channel, name, durable=True, arguments=None, **kwargs):
    """channel.queue_declare that makes an argument mismatch loud and specific. A queue's
    type and x-arguments are fixed when it is first declared; redeclaring it with
    different ones (rabbit_queue_type flipped on a live broker, a stale x-delivery-limit)
    makes the broker close the channel with 406 PRECONDITION_FAILED -- which otherwise
    surfaces as a generic rabbit error and an endless reconnect loop. Still re-raises: the
    fix is operational (see QUEUE_MIGRATION_DOC), never an automatic delete here."""
    try:
        return channel.queue_declare(queue=name, durable=durable, arguments=arguments, **kwargs)
    except pika.exceptions.ChannelClosedByBroker as e:
        if e.reply_code == 406:
            logger.critical(f"queue type mismatch: RabbitMQ refused to redeclare '{name}' with arguments "
                            f"{arguments or {}}: {e.reply_text}. The queue already exists with other "
                            f"arguments (was rabbit_queue_type changed on a live broker?); every use of "
                            f"this queue will fail until it is migrated: see {QUEUE_MIGRATION_DOC}")
        raise


def delete_queue(name, deadline_s=None):
    """Delete a queue WITHOUT declaring it first. TaskQueue(...).delete() and a declaring get_pika_channel()
    re-create the queue before deleting it: during an actor DELETE every worker (on 'stop') and the API
    would re-create and delete the same quorum inbox at once, racing Raft cluster creation against
    deletion (541 INTERNAL_ERROR on the racing calls; with at-least-once dead-lettering it also took down
    RabbitMQ 3.12's dead-letter supervisor, see stores.py). Deleting a queue that is already gone is not
    an error."""
    with RabbitConnection(deadline_s=deadline_s) as conn:
        conn._ch.queue_delete(queue=name)


class TaskQueue(object):
    def __init__(self, name=None, deadline_s=None):
        # one connection per TaskQueue instance, owned by the thread that made it --
        # same lifecycle as before, but the transport underneath is pika.
        self.conn = RabbitConnection(deadline_s=deadline_s)
        self._ch = self.conn._ch
        self.name = name
        try:
            declare_queue(self._ch, name, durable=True, arguments=queue_declare_args(name))
        except Exception:
            # the constructor is failing, so nobody will ever close() this connection
            self.close()
            raise
        # publisher confirms: basic_publish blocks until the broker accepts the message
        # and raises (UnroutableError/NackError) instead of silently dropping it. The
        # rabbitpy version published fire-and-forget.
        self._ch.confirm_delivery()
        self._queue = LegacyQueue()
        self._queue._queue = _QueueHandle(self)

    @staticmethod
    def _pre_process(msg):
        """
        Processing that should happen before putting a message on the queue. Override in subclass.
        """
        return msg

    @staticmethod
    def _post_process(msg):
        """
        Processing that should happen after retrieving a message from the queue. Override in subclass.
        :param msg:
        :return:
        """
        return msg

    def put(self, m):
        self._ch.basic_publish(exchange='',
                               routing_key=self.name,
                               body=self._pre_process(m),
                               properties=pika.BasicProperties(delivery_mode=2),
                               mandatory=True)

    def close(self):
        # The rabbitpy version closed on a background thread because rabbitpy's close
        # could block indefinitely. pika's close is bounded (heartbeat/socket timeouts),
        # and BlockingConnection is not thread-safe, so close synchronously here.
        try:
            self.conn.close()
        except Exception as e:
            logger.warning(f"Error closing RabbitMQ connection: {e}")

    def delete(self):
        try:
            self._ch.queue_delete(queue=self.name)
        finally:
            # deleting the queue does not release this object's TCP connection; without
            # this close, every delete() call site leaks a connection for the life of
            # the process.
            self.close()

    # Seconds between empty polls in get_one(). Bounds the extra latency a new message can see.
    POLL_INTERVAL_S = 0.5

    def get_one(self, timeout=None):
        """Wait for one message and return (payload, msg); msg supports .ack()/.nack(requeue=).

        Polls with basic_get instead of holding a consumer. A consumer keeps the next message delivered and
        unacked while the caller works on the current one, on a connection nobody services in between: the
        spawner sat on the next command for as long as a spawn took, and a message a k8s worker gave back
        was delivered straight back to it. process_data_events between polls answers heartbeats.

        A message that cannot be decoded is rejected without requeue (dead-lettered where a policy applies,
        dropped otherwise) rather than redelivered forever. Raises ChannelClosedException when the channel
        is closed and ChannelTimeoutException after `timeout` seconds without a message (None: wait
        forever); pika errors from a dead connection propagate."""
        give_up_at = None if timeout is None else time.monotonic() + timeout
        while True:
            if self._ch is None or not self._ch.is_open:
                raise ChannelClosedException()
            method, _properties, body = self._ch.basic_get(self.name, auto_ack=False)
            if method is not None:
                msg = Message(self._ch, method, body)
                try:
                    return self._post_process(msg), msg
                except Exception as e:
                    logger.error(f"could not decode a message on {self.name}; rejecting it without requeue. "
                                 f"{type(e).__name__}: {e}")
                    msg.nack(requeue=False)
                    continue
            wait = self.POLL_INTERVAL_S
            if give_up_at is not None:
                remaining = give_up_at - time.monotonic()
                if remaining <= 0:
                    raise ChannelTimeoutException()
                wait = min(wait, remaining)
            self.conn._conn.process_data_events(time_limit=wait)


class JsonTaskQueue(TaskQueue):
    """
    Task Queue where the message payloads are all JSON
    """

    @staticmethod
    def _pre_process(msg):
        return json.dumps(msg).encode('utf-8')

    @staticmethod
    def _post_process(msg):
        return json.loads(msg.body.decode('utf-8'), object_hook=str)


class BinaryTaskQueue(TaskQueue):
    """
    Task Queue where the message payloads are python objects.
    """
    @staticmethod
    def _pre_process(msg):
        return cloudpickle.dumps(msg)

    @staticmethod
    def _post_process(msg):
        return cloudpickle.loads(msg.body)
