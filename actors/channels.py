import time

from channelpy import BasicChannel, Channel, RabbitConnection
from channelpy.chan import checking_events
from channelpy.exceptions import ChannelClosedException, ChannelTimeoutException
import cloudpickle
import rabbitpy

from config import Config

class WorkerChannel(Channel):
    """Channel for communication with a worker. Pass the id of the worker to communicate with an
    existing worker.
    """
    @classmethod
    def get_name(cls, worker_id):
        """Return the name of the channel that would be used for this worker_id."""
        return 'worker_{}'.format(worker_id)

    def __init__(self, worker_id=None):
        self.uri = Config.get('rabbit', 'uri')
        ch_name = None
        if worker_id:
            ch_name = WorkerChannel.get_name(worker_id)
        super().__init__(name=ch_name,
                         connection_type=RabbitConnection,
                         uri=self.uri)


class SpawnerWorkerChannel(Channel):
    """Channel facilitating communication between a spawner and a worker during startup. Pass the name of the worker to communicate with an
    existing worker.
    """
    def __init__(self, worker_id=None):
        self.uri = Config.get('rabbit', 'uri')
        ch_name = None
        if worker_id:
            ch_name = 'spawner_worker_{}'.format(worker_id)
        super().__init__(name=ch_name,
                         connection_type=RabbitConnection,
                         uri=self.uri)


class ClientsChannel(Channel):
    """Channel for communicating with the clients generator."""

    def __init__(self, name='clients'):
        self.uri = Config.get('rabbit', 'uri')
        super().__init__(name=name,
                         connection_type=RabbitConnection,
                         uri=self.uri)

    def request_client(self, tenant, actor_id, worker_id, secret):
        """Request a new client for a specific tenant and worker."""
        msg = {'command': 'new',
               'tenant': tenant,
               'actor_id': actor_id,
               'worker_id': worker_id,
               'secret': secret}
        return self.put_sync(msg, timeout=60)

    def request_delete_client(self, tenant, actor_id, worker_id, client_id, secret):
        """Request a client be deleted as part of shutting down a worker."""
        msg = {'command': 'delete',
               'tenant': tenant,
               'actor_id': actor_id,
               'worker_id': worker_id,
               'client_id': client_id,
               'secret': secret}
        return self.put_sync(msg, timeout=60)


class CommandChannel(Channel):
    """Work with commands on the command channel."""

    def __init__(self, name='default'):
        self.uri = Config.get('rabbit', 'uri')
        queues_list = Config.get('spawner', 'host_queues').replace(' ', '')
        valid_queues = queues_list.split(',')
        if name not in valid_queues:
            raise Exception('Invalid Queue name.')


        super().__init__(name='command_channel_{}'.format(name),
                         connection_type=RabbitConnection,
                         uri=self.uri)

    def put_cmd(self, actor_id, worker_id, image, tenant, stop_existing=True):
        """Put a new command on the command channel."""
        msg = {'actor_id': actor_id,
               'worker_id': worker_id,
               'image': image,
               'tenant': tenant,
               'stop_existing': stop_existing}

        self.put(msg)


class BinaryChannel(BasicChannel):
    """Override BaseChannel methods to handle binary messages."""

    @checking_events
    def put(self, value):
        """Override the channelpy.Channel.put so that we can pass binary."""
        if self._queue is None:
            raise ChannelClosedException()
        self._queue.put(cloudpickle.dumps(value))

    @staticmethod
    def _process(msg):
        return cloudpickle.loads(msg)

    @checking_events
    def get(self, timeout=float('inf')):
        start = time.time()
        while True:
            if self._queue is None:
                raise ChannelClosedException()
            msg = self._queue.get()
            if msg is not None:
                return self._process(msg)
            if time.time() - start > timeout:
                raise ChannelTimeoutException()
            time.sleep(self.POLL_FREQUENCY)

    def get_one(self):
        """Blocking method to get a single message without polling."""
        if self._queue is None:
            raise ChannelClosedException()
        for msg in self._queue._queue.consume(prefetch=1):
            return self._process(msg.body), msg


from queues import BinaryTaskQueue


class ActorMsgChannel(BinaryTaskQueue):
    def __init__(self, actor_id):
        super().__init__(name='actor_msg_{}'.format(actor_id))

    def put_msg(self, message, d={}, **kwargs):
        d['message'] = message
        for k, v in kwargs:
            d[k] = v
        self.put(d)


class ActorMSSgChannel(BinaryChannel):
    """Work with messages sent to a specific actor.
    """
    def __init__(self, actor_id):
        self.uri = Config.get('rabbit', 'uri')
        super().__init__(name='actor_msg_{}'.format(actor_id),
                         connection_type=RabbitConnection,
                         uri=self.uri)

    def put_msg(self, message, d={}, **kwargs):
        """Pass a message to an actor's inbox, thereby invoking it. `message` is the request
        body msg parameter; `d` is a dictionary built from the request query parameters;
        additional metadata (e.g. jwt, username) can be passed through kwargs.
        """
        d['message'] = message
        for k, v in kwargs:
            d[k] = v
        self.put(d)


class FiniteRabbitConnection(RabbitConnection):
    """Override the channelpy.connections.RabbitConnection to provide TTL functionality,"""

    def create_queue(self, name=None, expires=1200000):
    # def create_queue(self, name=None, expires=100000):
        """Create queue for messages.
        :type name: str
        :type expires: int (time, in milliseconds, for queue to expire) 
        :rtype: queue
        """
        _queue = rabbitpy.Queue(self._ch, name=name, durable=True, expires=expires)
        _queue.declare()
        return _queue


class ExecutionResultsChannel(BinaryChannel):
    """Work with the results for a specific actor execution.
    """
    def __init__(self, actor_id, execution_id):
        self.uri = Config.get('rabbit', 'uri')
        super().__init__(name='results_{}_{}'.format(actor_id, execution_id),
                         connection_type=FiniteRabbitConnection,
                         uri=self.uri)


class ExecutionJSONResultsChannel(Channel):
    """Work with the results for a specific actor execution when actor type==json.
    """
    def __init__(self, actor_id, execution_id):
        self.uri = Config.get('rabbit', 'uri')
        super().__init__(name='results_{}_{}'.format(actor_id, execution_id),
                         connection_type=FiniteRabbitConnection,
                         uri=self.uri)

