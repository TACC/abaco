import ast
import getpass
import json
import os
import socket
import time

import cloudpickle

from . import tapis
from . import errors
from . import util


def reserved_environment_vars():
    return ['MSG', '_abaco_Content-Type', '_abaco_execution_id',
            '_abaco_username', '_abaco_actor_state', '_abaco_actor_dbid', '_abaco_actor_id']


def get_client():
    """Returns a pre-authenticated Tapis client using the abaco environment variables."""
    # if we have an access token, use that:
    if os.environ.get('_abaco_access_token'):
        tp = tapis.Tapis(base_url=os.environ.get('_abaco_api_server'),
                         jwt=os.environ.get('_abaco_access_token'))
    elif os.environ.get('_abaco_api_server'):
        # otherwise, create a client with a fake JWT. this will only work if the actor is supplying its
        # own token to itself via a config object or the message, etc.
        tp = tapis.Tapis(base_url=os.environ.get('_abaco_api_server'), jwt='123')
    else:
        raise errors.BaseTapyException("Unable to instantiate a Tapis client: no token found.")

    return tp


def get_context():
    """
    Returns a context dictionary with message and metadata about the message
    """
    context = {'raw_message': os.environ.get('MSG'), 'content_type': os.environ.get('_abaco_Content-Type'),
               'actor_repo': os.environ.get('_abaco_container_repo'), 'actor_name': os.environ.get('_abaco_actor_name'),
               'actor_id': os.environ.get('_abaco_actor_id'), 'actor_dbid': os.environ.get('_abaco_actor_dbid'),
               'execution_id': os.environ.get('_abaco_execution_id'), 'worker_id': os.environ.get('_abaco_worker_id'),
               'username': os.environ.get('_abaco_username'), 'state': os.environ.get('_abaco_actor_state'),
               'raw_message_parse_log': [], 'message_dict': {}}

    # Set up message_dict and error log preemptively
    # message_dict is actually an AttrDict so users can use
    # dot notation when programming against it

    # Try the safer AST eval to load the raw message into a dict,
    # falling back to trying json.loads() if that fails
    try:
        temp_dict = ast.literal_eval(context['raw_message'])
        if isinstance(temp_dict, dict):
            context['message_dict'] = temp_dict
    except Exception as e_ast:
        context['raw_message_parse_log'].append(str(e_ast))
        try:
            temp_dict = json.loads(context['raw_message'])
            context['message_dict'] = temp_dict
        except Exception as e_json:
            context['raw_message_parse_log'].append(str(e_json))
            pass

    context.update(os.environ)
    return util.AttrDict(context)


def get_binary_message():
    """Read the full binary message sent via the abaco named pipe."""
    fd = os.open('/_abaco_binary_data', os.O_RDONLY | os.O_NONBLOCK)
    msg = b''
    while True:
        frame = _read_bytes(fd)
        if frame:
            msg += frame
        else:
            return msg


def _read_bytes(fifo, n=4069):
    """Read at most n bytes from a pipe at path `fifo`."""
    try:
        return os.read(fifo, n)
    except OSError:
        # an empty fifo will return a Resource temporarily unavailable OSError (Errno 11)
        # since we explicitly support a single message protocol, this means we are done
        return None


def update_state(state):
    """Update the actor's state with the new value of `state`. The `state` variable should be JSON serializable."""
    tp = get_client()
    actor_id = get_context()['actor_id']
    tp.actors.updateState(actor_id=actor_id, body=state)


def send_python_result(obj):
    """
    Send an arbitrary python object, `obj`
    :param obj: a python object to return as a result.
    :return:
    """
    try:
        b = cloudpickle.dumps(obj)
    except Exception as e:
        msg = "Could not serialize {}; got exception: {}".format(obj, e)
        print(msg)
        raise errors.BaseTapyException(msg)
    send_bytes_result(b)


def send_bytes_result(b):
    """
    Send a result `b` which should be a bytes object to the Abaco system.
    `b` must be shorter than MAX_RESULT_LENGTH configured for the Abaco instance
    or else
    """
    if not isinstance(b, bytes):
        msg = "send_bytes_result did not receive bytes, got: {}".format(b)
        print(msg)
        raise errors.BaseTapyException(msg)
    sock = _get_results_socket()
    try:
        sock.send(b)
    except Exception as e:
        msg = "Got exception sending bytes over results socket: {}".format(e)
        print(msg)
        raise errors.BaseTapyException(msg)


def _get_results_socket():
    """Instantiate the results socket for sending binary results."""
    client = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    sock = '/_abaco_results.sock'
    try:
        client.connect(sock)
    except (FileNotFoundError, ConnectionError) as e:
        msg = "Exception connecting to results socket: {}".format(e)
        print(msg)
        raise errors.BaseTapyException(msg)
    return client


def is_tapis_notebook():
    """
    Determine whether this code is running from within a Tapis notebook. This can be used for automatically
    determining the image to use for the asynchronous executor.
    """
    # are we running as the jupyter user
    if os.environ.get('JUPYTERHUB_API_TOKEN'):
        return True
    return False


def get_tapis_abaco_image():
    """
    Determine the docker image name for a Jupyter notebook running on TACC infrastructure.
    """
    # the TACC JupyterHub sets the environment variable JUPYTER_IMAGE in the user notebooks (this is actually
    # default behavior by the KubeSpawner).
    return os.environ.get('JUPYTER_IMAGE')


class AbacoExecutor(object):
    """Executor class that leverages an Abaco actor for executions"""
    def __init__(
            self,
            # the Tapis client to use to connect to the abaco instance
            tp,
            # use an existing actor; it must have been defined with an allowable image.
            actor_id=None,
            # specify an image to use for the abaco actor. it must be able to accept a message that contains a
            # callable and parameters and execute the callable.
            image=None,
            # if not specifying an image, one of a pre-defined set of strings that determines the image to use for the
            # actor
            context=None,
            # timeout (in seconds) to wait for actor to be READY; for very large images, may need to increase this
            # to allow for longer download times on the abaco side.
            timeout=60):
        self.tp = tp
        self.actor_id = None
        self.timeout = timeout
        if actor_id:
            # make sure it exists:
            rsp = tp.actors.get(actor_id=actor_id)
            self.actor_id = actor_id
            self.status = rsp.get('status')
            self.image = rsp.get('image')
        else:
            # first, determine the image that should be used:
            if not image:
                # if no image provided, check to see if we are running from within a Tapis jupyter notebook:
                if is_tapis_notebook():
                    image = get_tapis_abaco_image()

            # next, check if a user already has an tppy_abaco_executor for the correct image:
            actors = tp.actors.listActors()
            for a in actors:
                if a.get('name') == 'tppy_abaco_executor':
                    # check fot the image:
                    if not image or image == a.get('image'):
                        self.actor_id = a.get('id')
            if not self.actor_id:
                # this is a new actor:
                self.image = None
                if image:
                    self.image = image
                elif context and hasattr(context, 'lower'):
                    # basic py3 image:
                    if context.lower() == 'py3':
                        self.image = 'abacosamples/py3_func_v3'
                    # docker image with many scientific libraries pre-installed
                    elif context.lower() == 'py3-scipy':
                        self.image = 'abacosamples/py3_sci_base_func_v3'
                    # docker image used for the sd2e jupyter hub
                    elif context.lower() == 'sd2e-jupyter':
                        self.image = 'sd2e/jupyteruser_func'
                    # add support for other contexts as needed...
                # provide a sensible default image
                if not self.image:
                    self.image = 'abacosamples/py3_func_v3'
                # register an Abaco actor with the appropriate image:
                try:
                    rsp = tp.actors.createActor(image=self.image,
                                                stateless=True,
                                                name='agpy_abaco_executor')
                except Exception as e:
                    raise errors.BaseTapyException(
                        "Unable to register the actor; exception: {}".format(
                            e))
                self.actor_id = rsp.id
                self.status = 'SUBMITTED'

                self.wait_until_ready()

    def _update_status(self):
        status = self.tp.actors.getActor(actor_id=self.actor_id).status
        self.status = status
        return status

    def _is_ready(self):
        return self.status == 'READY'

    def wait_until_ready(self, timeout=None):
        now = time.time()
        if timeout:
            future = now + timeout
        else:
            future = float("inf")
        while not self._is_ready() and time.time() < future:
            self._update_status()
            time.sleep(2)

    def submit(self, fn, *args, **kwargs):
        """Schedule the callable fn to run as fn(*args, **kwargs) and returns a
        AbacoAsyncReponse Future object representing the execution of the callable."""

        if not args:
            args = []
        if not kwargs:
            kwargs = {}
        message = cloudpickle.dumps({
            'cwd': os.getcwd(),
            'func': fn,
            'args': args,
            'kwargs': kwargs
        })
        headers = {'Content-Type': 'application/octet-stream'}
        rsp = self.tp.actors.sendMessage(actor_id=self.actor_id,
                                         message=message,
                                         headers=headers)
        execution_id = rsp.get('executionId')
        if not execution_id:
            raise errors.BaseTapyException(
                "Error submitting function call. Did not get an execution id; response: {}"
                .format(rsp))
        return AbacoAsyncResponse(
            self.tp,
            self.actor_id,
            execution_id,
        )

    def map(self, fn, args_list=[], kwargs_list=None):
        """
        Map a function, fn, over input data args_list and kwargs_list. If kwargs_list is provided,
        it must have the same length as args list.
        """
        if not kwargs_list:
            kwargs_list = [{} for i in args_list]
        if not len(args_list) == len(kwargs_list):
            raise errors.BaseTapyException("map requires lists of equal length")
        return [
            self.submit(fn, *args, **kwargs)
            for args, kwargs in zip(args_list, kwargs_list)
        ]

    def delete(self):
        """ Delete this executor completely."""
        self.tp.actors.deleteActor(actor_id=self.actor_id)

    def blocking_call(self, fn, *args, **kwargs):
        """Execute fn(*args, **kwargs) and block until the result completes. """
        arsp = self.submit(fn, *args, **kwargs)
        return arsp.result()


# max time, in seconds, to sleep between status check calls for an execution
MAX_SLEEP = 60


def exp_backoff(prev, count):
    # exponentially increase the sleep amount via the equation 2^(0.2*prev) every 10th time:
    next = prev
    if (count % 10) == 0:
        next = 2.0**(prev * 0.2)
    if next > MAX_SLEEP:
        return MAX_SLEEP
    return next


class AbacoAsyncResponse(object):
    """
    Future class encapsulating an asynchronous execution performed via an Abaco actor.
    """
    def __init__(self, tp, actor_id, execution_id):
        self.tp = tp
        self.actor_id = actor_id
        self.execution_id = execution_id
        self.status = 'SUBMITTED'

    def _update_status(self):
        status = self.tp.actors.getExecution(
            actor_id=self.actor_id, execution_id=self.execution_id).status
        self.status = status
        return status

    def _is_done(self):
        return self.status == 'COMPLETE'

    def done(self):
        """Return True if the call was successfully cancelled or finished running."""
        self._update_status()
        return self._is_done()

    def running(self):
        self._update_status()
        return not self._is_done()

    def result(self, timeout=None):
        """
        :param timeout: int,
        :return:
        """
        now = time.time()
        sleep = 0.5
        count = 0
        if timeout:
            future = now + timeout
        else:
            future = float("inf")
        while not self._is_done() and time.time() < future:
            self._update_status()
            count += 1
            time.sleep(exp_backoff(sleep, count))
        if time.time() > future and not self._is_done():
            raise TimeoutError()
        # result should be ready:
        results = []
        while True:
            result = self.tp.actors.getExecutionResult(
                actor_id=self.actor_id, execution_id=self.execution_id)
            if not result:
                break
            results.append(cloudpickle.loads(result))
        return results
