import time

from channelpy import BasicChannel, Channel, RabbitConnection
from channelpy.chan import checking_events
from channelpy.exceptions import ChannelClosedException, ChannelTimeoutException
import cloudpickle
import rabbitpy

from tapisservice.config import conf
from flask import g
from stores import get_site_rabbitmq_uri

def site():
    try:
        return g.site_id
    except:
        return conf.get('site_id')

RABBIT_URI = get_site_rabbitmq_uri(site())

# class WorkerChannel(Channel):
#     """Channel for communication with a worker. Pass the id of the worker to communicate with an
#     existing worker.
#     """
#     @classmethod
#     def get_name(cls, worker_id):
#         """Return the name of the channel that would be used for this worker_id."""
#         return 'worker_{}'.format(worker_id)
#
#     def __init__(self, worker_id=None):
#         self.uri = RABBIT_URI
#         ch_name = None
#         if worker_id:
#             ch_name = WorkerChannel.get_name(worker_id)
#         super().__init__(name=ch_name,
#                          connection_type=RabbitConnection,
#                          uri=self.uri)


# class SpawnerWorkerChannel(Channel):
#     """Channel facilitating communication between a spawner and a worker during startup. Pass the name of the worker to communicate with an
#     existing worker.
#     """
#     def __init__(self, worker_id=None):
#         self.uri = RABBIT_URI
#         ch_name = None
#         if worker_id:
#             ch_name = 'spawner_worker_{}'.format(worker_id)
#         super().__init__(name=ch_name,
#                          connection_type=RabbitConnection,
#                          uri=self.uri)


class ClientsChannel(Channel):
    """Channel for communicating with the clients generator."""

    def __init__(self, name='clients'):
        self.uri = RABBIT_URI
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


# class CommandChannel(Channel):
#     """Work with commands on the command channel."""
#
#     def __init__(self, name='default'):
#         self.uri = RABBIT_URI
#         queues_list = conf.get('spawner_host_queues')
#         if name not in queues_list:
#             raise Exception('Invalid Queue name.')
#
#
#         super().__init__(name='command_channel_{}'.format(name),
#                          connection_type=RabbitConnection,
#                          uri=self.uri)
#
#     def put_cmd(self, actor_id, worker_id, image, revision, tenant, stop_existing=True):
#         """Put a new command on the command channel."""
#         msg = {'actor_id': actor_id,
#                'worker_id': worker_id,
#                'image': image,
#                'revision': revision,
#                'tenant': tenant,
#                'stop_existing': stop_existing}
#
#         self.put(msg)


# class EventsChannel(Channel):
#     """Work with events on the events channel."""
#
#     event_queue_names = ('default',
#                          )
#
#     def __init__(self, name='default'):
#         self.uri = RABBIT_URI
#         if name not in EventsChannel.event_queue_names:
#             raise Exception('Invalid Events Channel Queue name.')
#
#         super().__init__(name='events_channel_{}'.format(name),
#                          connection_type=RabbitConnection,
#                          uri=self.uri)
#
#     def put_event(self, json_data):
#         """Put a new event on the events channel."""
#         self.put(json_data)


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


class EventsChannel(BinaryTaskQueue):
    """Work with events on the events channel."""

    event_queue_names = ('default',
                         )

    def __init__(self, name='default'):
        self.uri = RABBIT_URI
        if name not in EventsChannel.event_queue_names:
            raise Exception('Invalid Events Channel Queue name.')

        super().__init__(name=f'events_channel_{name}')

    def put_event(self, json_data):
        """Put a new event on the events channel."""
        self.put(json_data)


class CommandChannel(BinaryTaskQueue):
    """Work with commands on the command channel."""

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

    def put_adapter_cmd(self, adapter_id, server_id, image, revision, tenant, site_id, stop_existing=True):
        """Put a new command on the command channel."""
        msg = {'adapter_id': adapter_id,
               'server_id': server_id,
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

class ServerChannel(BinaryTaskQueue):
    """Channel for communication with a server. Pass the id of the server to communicate with an
    existing server.
    """
    @classmethod
    def get_name(cls, server_id):
        """Return the name of the channel that would be used for this server_id."""
        return f'server_{server_id}'

    def __init__(self, server_id=None):
        self.uri = RABBIT_URI
        ch_name = None
        if server_id:
            ch_name = WorkerChannel.get_name(server_id)
        super().__init__(name=ch_name)

class ActorMsgChannel(BinaryTaskQueue):
    def __init__(self, actor_id):
        super().__init__(name=f'actor_msg_{actor_id}')

    def put_msg(self, message, d={}, **kwargs):
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
        self.uri = RABBIT_URI
        super().__init__(name=f'results_{actor_id}_{execution_id}',
                         connection_type=FiniteRabbitConnection,
                         uri=self.uri)


class ExecutionJSONResultsChannel(Channel):
    """Work with the results for a specific actor execution when actor type==json.
    """
    def __init__(self, actor_id, execution_id):
        self.uri = RABBIT_URI
        super().__init__(name=f'results_{actor_id}_{execution_id}',
                         connection_type=FiniteRabbitConnection,
                         uri=self.uri)

