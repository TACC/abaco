import cloudpickle
import json
import rabbitpy
import threading
import time

from config import Config


class ChannelClosedException(Exception):
    pass


class RabbitConnection(object):
    def __init__(self, retries=100):
        self._uri = Config.get('rabbit', 'uri')
        tries = 0
        connected = False
        while tries < retries and not connected:
            try:
                self._conn = rabbitpy.Connection(self._uri)
                connected = True
            except RuntimeError:
                time.sleep(0.1)
        if not connected:
            raise RuntimeError("Could not connect to RabbitMQ.")
        self._ch = self._conn.channel()
        self._ch.prefetch_count(value=1, all_channels=True)

    def close(self):
        """Close this instance of the connection. """

        self._ch.close()
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        del exc_type, exc_val, exc_tb
        self.close()

# rconn = RabbitConnection()

class LegacyQueue(object):
    """
    This class is here to support existing code that expects an _queue object on the Various channel objects (e.g.,
    ActorMsgChannel object). Client code that makes use of the _queue._queue
    """
    pass


class TaskQueue(object):
    def __init__(self, name=None):
        # reuse the singleton rconn
        # self.conn = rconn
        # NOTE -
        # reusing the singleton creates the issue that, once closed, it cannot be used further. in order for the
        # singleton pattern to work, we would need fof individual functions to not call close() directly, but rather
        # have an automated way at the end of each process/thread execution to close the connection.

        # create a new RabbitConnection for this instance of the task queue.
        self.conn = RabbitConnection()
        self._ch = self.conn._ch
        self.name = name
        self.queue = rabbitpy.Queue(self._ch, name=name, durable=True)
        self.queue.declare()
        # the following added for backwards compatibility so that client code using the ch._queue._queue attribute
        # will continue to work.
        self._queue = LegacyQueue()
        self._queue._queue = self.queue

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
        msg = rabbitpy.Message(self.conn._ch, self._pre_process(m), {})
        msg.publish('', self.name)

    # def close(self):
    #     self.conn.close()

    def close(self):
        def _close(this):
            this.conn.close()

        t = threading.Thread(target=_close, args=(self,))
        t.start()

    def delete(self):
        self.conn.close()

    def get_one(self):
        """Blocking method to get a single message without polling."""
        if self._queue is None:
            raise ChannelClosedException()
        for msg in self.queue.consume(prefetch=1):
            return self._post_process(msg), msg


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
