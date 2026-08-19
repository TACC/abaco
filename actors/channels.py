import time

import cloudpickle
import pika
import pika.exceptions

from queues import ChannelClosedException, ChannelTimeoutException, connect_with_backoff

from tapisservice.config import conf
from flask import g
from stores import get_site_rabbitmq_uri

def site():
    try:
        return g.site_id
    except:
        return conf.get('site_id')

RABBIT_URI = get_site_rabbitmq_uri(site())

from queues import BinaryTaskQueue

class EventsChannel(BinaryTaskQueue):
    """Work with events on the events channel."""

    event_queue_names = ('default',)

    def __init__(self, name='default'):
        self.uri = RABBIT_URI
        if name not in EventsChannel.event_queue_names:
            raise Exception('Invalid Events Channel Queue name.')

        super().__init__(name=f'events_channel_{name}')

    def put_event(self, json_data):
        """Put a new event on the events channel."""
        self.put(json_data)


class CommandChannel(BinaryTaskQueue):
    """Work with commands on the spawner command channel."""

    def __init__(self, name='default'):
        self.uri = RABBIT_URI
        queues_list = conf.get('spawner_host_queues')
        if name not in queues_list:
            raise Exception('Invalid Queue name.')

        super().__init__(name=f'command_channel_{name}')

    def put_cmd(self, actor_id, worker_id, image, revision, tenant, site_id, stop_existing=True):
        """Put a new command on the command channel."""
        msg = {'actor_id': actor_id,
               'worker_id': worker_id,
               'image': image,
               'revision': revision,
               'tenant': tenant,
               'site_id': site_id,
               'stop_existing': stop_existing}

        self.put(msg)


class SpawnerWorkerChannel(BinaryTaskQueue):
    """Channel facilitating communication between a spawner and a worker during startup. Pass the name of the worker to communicate with an
    existing worker.
    """
    def __init__(self, worker_id=None):
        self.uri = RABBIT_URI
        ch_name = None
        if worker_id:
            ch_name = f'spawner_worker_{worker_id}'
        super().__init__(name=ch_name)


class WorkerChannel(BinaryTaskQueue):
    """Channel for communication with a worker. Pass the id of the worker to communicate with an
    existing worker.
    """
    @classmethod
    def get_name(cls, worker_id):
        """Return the name of the channel that would be used for this worker_id."""
        return f'worker_{worker_id}'

    def __init__(self, worker_id=None):
        self.uri = RABBIT_URI
        ch_name = None
        if worker_id:
            ch_name = WorkerChannel.get_name(worker_id)
        super().__init__(name=ch_name)


class ActorMsgChannel(BinaryTaskQueue):
    def __init__(self, actor_id):
        super().__init__(name=f'actor_msg_{actor_id}')

    def put_msg(self, message, d={}, **kwargs):
        d['message'] = message
        for k, v in kwargs:
            d[k] = v
        self.put(d)


RESULTS_QUEUE_EXPIRES_MS = int(conf.get('results_queue_expires_ms') or 1200000)


class ExecutionResultsChannel:
    """Pika-based channel for execution results for a specific actor execution."""
    POLL_INTERVAL_S = 0.1

    def __init__(self, actor_id, execution_id, uri=None, deadline_s=None):
        self.queue_name = f'results_{actor_id}_{execution_id}'
        self.uri = uri or RABBIT_URI
        self._deadline_s = deadline_s
        self._connection = None
        self._channel = None
        self._connect()

    class _QueueHandle:
        """docker_utils checks len(results_ch._queue._queue) before deciding whether to
        delete the queue; this preserves that attribute chain over pika."""
        def __init__(self, outer):
            self._outer = outer

        def __len__(self):
            try:
                # the connection may have been dropped while the execution ran (see
                # _ensure_open); reconnect rather than raise out of the cleanup.
                self._outer._ensure_open()
                frame = self._outer._channel.queue_declare(queue=self._outer.queue_name, passive=True)
            except pika.exceptions.ChannelClosedByBroker as e:
                if e.reply_code != 404:
                    raise
                # the queue already expired (x-expires): nothing left to read. Raising
                # here would escape docker_utils' end-of-execution cleanup.
                return 0
            return frame.method.message_count

    def _connect(self):
        params = pika.URLParameters(self.uri)
        params.heartbeat = 30
        params.blocked_connection_timeout = 30
        # same bounded retry as every other channel; a single failed attempt used to
        # surface as a 500 on the synchronous-message and results endpoints.
        self._connection = connect_with_backoff(params, deadline_s=self._deadline_s)
        self._channel = self._connection.channel()
        try:
            # x-expires: the broker deletes the queue once it has gone unused (no
            # consumer, no basic.get, no redeclare) for this long, so results nobody
            # reads do not accumulate forever. Same 20 minutes channelpy's
            # FiniteRabbitConnection applied. NB: publishing does not count as use.
            self._channel.queue_declare(queue=self.queue_name, durable=True,
                                        arguments={'x-expires': RESULTS_QUEUE_EXPIRES_MS})
        except pika.exceptions.ChannelClosedByBroker as e:
            if e.reply_code != 406:
                raise
            # PRECONDITION_FAILED: the queue already exists with other arguments (declared
            # by a pre-x-expires process during a rolling deploy). It is still the right
            # queue; attach to it as-is instead of failing the execution.
            self._channel = self._connection.channel()
            self._channel.queue_declare(queue=self.queue_name, passive=True)
        handle = ExecutionResultsChannel._QueueHandle(self)
        self._queue = type('LegacyQueue', (), {})()
        self._queue._queue = handle

    # Connection-level failures after which a fresh connection is the fix.
    _RECONNECTABLE = (pika.exceptions.AMQPConnectionError, pika.exceptions.ChannelWrongStateError,
                      pika.exceptions.ChannelClosed, pika.exceptions.StreamLostError)

    def _ensure_open(self):
        """The worker opens this channel when an execution starts and next uses it when the
        actor emits a result -- possibly minutes or days later. Nothing services the
        connection's heartbeats in between (it belongs to the thread running the
        execution), so the broker drops it after ~2 heartbeat intervals. Reconnect lazily
        on next use instead of losing the result and failing the execution's cleanup."""
        if (self._connection is None or not self._connection.is_open
                or self._channel is None or not self._channel.is_open):
            self.close()
            self._connect()

    def _publish(self, body):
        self._channel.basic_publish(
            exchange='',
            routing_key=self.queue_name,
            body=body,
            properties=pika.BasicProperties(delivery_mode=2)  # make message persistent
        )

    def put(self, value):
        body = cloudpickle.dumps(value)
        self._ensure_open()
        try:
            self._publish(body)
        except self._RECONNECTABLE:
            # is_open can still read True on a connection the broker already dropped for
            # missed heartbeats; the first write finds out. Reconnect once and retry.
            self.close()
            self._connect()
            self._publish(body)

    def get(self, timeout=float('inf')):
        """Return the next result, waiting up to `timeout` seconds for one to arrive.
        Raises ChannelTimeoutException when none arrived in time (the channelpy
        contract every caller is written against: do_synch_message treats a return as
        "got the result" and the results endpoint passes the return value straight to
        make_response) and ChannelClosedException when the channel is gone."""
        give_up_at = time.monotonic() + (timeout if timeout is not None else float('inf'))
        while True:
            if self._channel is None or not self._channel.is_open:
                raise ChannelClosedException()
            method_frame, header_frame, body = self._channel.basic_get(self.queue_name, auto_ack=True)
            if method_frame:
                return cloudpickle.loads(body)
            remaining = give_up_at - time.monotonic()
            if remaining <= 0:
                raise ChannelTimeoutException()
            # process_data_events rather than time.sleep so heartbeats are serviced
            # while waiting on a long timeout.
            self._connection.process_data_events(time_limit=min(self.POLL_INTERVAL_S, remaining))

    def delete(self):
        try:
            # a channel the broker closed (e.g. the 404 above) cannot delete anything,
            # and an expired queue needs no deleting.
            if self._channel is not None and self._channel.is_open:
                self._channel.queue_delete(queue=self.queue_name)
        finally:
            self.close()

    def close(self):
        try:
            if self._connection and self._connection.is_open:
                self._connection.close()
        except Exception:
            # closing a connection the broker already dropped can raise; it is gone either way
            pass