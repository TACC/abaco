import configparser
import json
import os
import socket
import time
import timeit

import docker
from requests.packages.urllib3.exceptions import ReadTimeoutError
from requests.exceptions import ReadTimeout, ConnectionError

from agaveflask.logs import get_logger, get_log_file_strategy
logger = get_logger(__name__)

from channels import ExecutionResultsChannel
from config import Config
from codes import BUSY
from models import Worker, get_current_utc_time

TAG = os.environ.get('TAG') or Config.get('general', 'TAG') or ''
if not TAG[0] == ':':
    TAG = ':{}',format(TAG)
AE_IMAGE = '{}{}'.format(os.environ.get('AE_IMAGE', 'abaco/core'), TAG)

# timeout (in seconds) for the socket server
RESULTS_SOCKET_TIMEOUT = 0.1

# max frame size, in bytes, for a single result
MAX_RESULT_FRAME_SIZE = 131072

max_run_time = int(Config.get('workers', 'max_run_time'))

dd = Config.get('docker', 'dd')
host_id = os.environ.get('SPAWNER_HOST_ID', Config.get('spawner', 'host_id'))
logger.debug("host_id: {}".format(host_id))
host_ip = Config.get('spawner', 'host_ip')


class DockerError(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = message


class DockerStartContainerError(DockerError):
    pass


def rm_container(cid):
    """
    Remove a container.
    :param cid:
    :return:
    """
    cli = docker.APIClient(base_url=dd, version="auto")
    try:
        rsp = cli.remove_container(cid, force=True)
    except Exception as e:
        logger.info("Got exception trying to remove container: {}. Exception: {}".format(cid, e))
        raise DockerError("Error removing container {}, exception: {}".format(cid, str(e)))
    logger.info("container {} removed.".format(cid))

def pull_image(image):
    """
    Update the local registry with an actor's image.
    :param actor_id:
    :return:
    """
    logger.debug("top of pull_image()")
    cli = docker.APIClient(base_url=dd, version="auto")
    try:
        rsp = cli.pull(repository=image)
    except Exception as e:
        msg = "Error pulling image {} - exception: {} ".format(image, e)
        logger.info(msg)
        raise DockerError(msg)
    if '"message":"Error' in rsp:
        if '{} not found'.format(image) in rsp:
            msg = "Image {} was not found on the public registry.".format(image)
            logger.info(msg)
            raise DockerError(msg)
        else:
            msg = "There was an error pulling the image: {}".format(rsp)
            logger.error(msg)
            raise DockerError(msg)
    return rsp


def list_all_containers():
    """Returns a list of all containers """
    cli = docker.APIClient(base_url=dd, version="auto")
    # todo -- finish

def container_running(image=None, name=None):
    """Check if there is a running container for an image.
    image should be fully qualified; e.g. image='jstubbs/abaco_core'
    Can pass wildcards in name using * character; e.g. name='abaco_spawner*'
    """
    logger.debug("top of container_running().")
    filters = {}
    if name:
        filters['name'] = name
    if image:
        filters['image'] = image
    cli = docker.APIClient(base_url=dd, version="auto")
    try:
        containers = cli.containers(filters=filters)
    except Exception as e:
        msg = "There was an error checking container_running for image: {}. Exception: {}".format(image, e)
        logger.error(msg)
        raise DockerError(msg)
    logger.debug("found containers: {}".format(containers))
    return len(containers) > 0

def run_container_with_docker(image,
                              command,
                              name=None,
                              environment={},
                              mounts=[],
                              log_file='service.log',
                              auto_remove=False):
    """
    Run a container with docker mounted in it.
    Note: this function always mounts the abaco conf file so it should not be used by execute_actor().
    """
    logger.debug("top of run_container_with_docker().")
    cli = docker.APIClient(base_url=dd, version="auto")

    # bind the docker socket as r/w since this container gets docker.
    volumes = ['/var/run/docker.sock']
    binds = {'/var/run/docker.sock': {'bind': '/var/run/docker.sock', 'ro': False}}
    # add a bind key and dictionary as well as a volume for each mount
    for m in mounts:
        binds[m.get('host_path')] = {'bind': m.get('container_path'),
                                     'ro': m.get('format') == 'ro'}
        volumes.append(m.get('host_path'))

    # mount the abaco conf file. first we look for the environment variable, falling back to the value in Config.
    try:
        abaco_conf_host_path = os.environ.get('abaco_conf_host_path')
        if not abaco_conf_host_path:
            abaco_conf_host_path = Config.get('spawner', 'abaco_conf_host_path')
        logger.debug("docker_utils using abaco_conf_host_path={}".format(abaco_conf_host_path))
        # mount config file at the root of the container as r/o
        volumes.append('/service.conf')
        binds[abaco_conf_host_path] = {'bind': '/service.conf', 'ro': True}
    except configparser.NoOptionError as e:
        # if we're here, it's bad. we don't have a config file. better to cut and run,
        msg = "Did not find the abaco_conf_host_path in Config. Exception: {}".format(e)
        logger.error(msg)
        raise DockerError(msg)

    # mount the logs file.
    volumes.append('/var/log/service.log')
    # first check to see if the logs directory config was set:
    try:
        logs_host_dir = Config.get('logs', 'host_dir')
    except (configparser.NoSectionError, configparser.NoOptionError):
        # if the directory is not configured, default it to abaco_conf_host_path
        logs_host_dir = os.path.dirname(abaco_conf_host_path)
    binds['{}/{}'.format(logs_host_dir, log_file)] = {'bind': '/var/log/service.log', 'rw': True}

    host_config = cli.create_host_config(binds=binds, auto_remove=auto_remove)
    logger.debug("binds: {}".format(binds))

    # create and start the container
    try:
        container = cli.create_container(image=image,
                                         environment=environment,
                                         volumes=volumes,
                                         host_config=host_config,
                                         command=command)
        cli.start(container=container.get('Id'))
    except Exception as e:
        msg = "Got exception trying to run container from image: {}. Exception: {}".format(image, e)
        logger.info(msg)
        raise DockerError(msg)
    logger.info("container started successfully: {}".format(container))
    return container

def run_worker(image, worker_id):
    """
    Run an actor executor worker with a given channel and image.
    :return:
    """
    logger.debug("top of run_worker()")
    command = 'python3 -u /actors/worker.py'
    logger.debug("docker_utils running worker. image:{}, command:{}".format(
        image, command))

    # determine what log file to use
    if get_log_file_strategy() == 'split':
        log_file = 'worker.log'
    else:
        log_file = 'abaco.log'

    # mount the directory on the host for creating fifos
    try:
        fifo_host_path_dir = Config.get('workers', 'fifo_host_path_dir')
        logger.info("Using fifo_host_path_dir: {}".format(fifo_host_path_dir))
    except (configparser.NoSectionError, configparser.NoOptionError) as e:
        logger.error("Got exception trying to look up fifo_host_path_dir. Setting to None. Exception: {}".format(e))
        fifo_host_path_dir = None
    if fifo_host_path_dir:
        mounts = [{'host_path': os.path.join(fifo_host_path_dir, worker_id),
                   'container_path': os.path.join(fifo_host_path_dir, worker_id),
                   'format': 'rw'}]
    else:
        mounts = []

    # mount the directory on the host for creating result sockets
    try:
        socket_host_path_dir = Config.get('workers', 'socket_host_path_dir')
        logger.info("Using socket_host_path_dir: {}".format(socket_host_path_dir))
    except (configparser.NoSectionError, configparser.NoOptionError) as e:
        logger.error("Got exception trying to look up fifo_host_path_dir. Setting to None. Exception: {}".format(e))
        socket_host_path_dir = None
    if socket_host_path_dir:
        mounts.append({'host_path': os.path.join(socket_host_path_dir, worker_id),
                       'container_path': os.path.join(socket_host_path_dir, worker_id),
                       'format': 'rw'})

    logger.info("Final fifo_host_path_dir: {}; socket_host_path_dir: {}".format(fifo_host_path_dir,
                                                                                socket_host_path_dir))
    try:
        auto_remove = Config.get('workers', 'auto_remove')
    except (configparser.NoSectionError, configparser.NoOptionError) as e:
        logger.debug("no auto_remove in the workers stanza.")
        auto_remove = True
    if hasattr(auto_remove, 'lower'):
        if auto_remove.lower() == 'false':
            auto_remove = False
    elif not auto_remove == True:
        auto_remove = False
    container = run_container_with_docker(image=AE_IMAGE,
                                          command=command,
                                          environment={'image': image,
                                                       'worker_id': worker_id,
                                                       '_abaco_secret': os.environ.get('_abaco_secret')},
                                          mounts=mounts,
                                          log_file=log_file,
                                          auto_remove=auto_remove)
    # don't catch errors -- if we get an error trying to run a worker, let it bubble up.
    # TODO - determines worker structure; should be placed in a proper DAO class.
    logger.info("worker container running. worker_id: {}. container: {}".format(worker_id, container))
    return { 'image': image,
             # @todo - location will need to change to support swarm or cluster
             'location': dd,
             'id': worker_id,
             'cid': container.get('Id'),
             'status': BUSY,
             'host_id': host_id,
             'host_ip': host_ip,
             'last_execution_time': 0,
             'last_health_check_time': get_current_utc_time() }

def execute_actor(actor_id,
                  worker_id,
                  execution_id,
                  image,
                  msg,
                  user=None,
                  d={},
                  privileged=False,
                  mounts=[],
                  leave_container=False,
                  fifo_host_path=None,
                  socket_host_path=None):
    """
    Creates and runs an actor container and supervises the execution, collecting statistics about resource consumption
    from the Docker daemon.

    :param actor_id: the dbid of the actor; for updating worker status
    :param worker_id: the worker id; also for updating worker status
    :param execution_id: the id of the execution.
    :param image: the actor's image; worker must have already downloaded this image to the local docker registry.
    :param msg: the message being passed to the actor.
    :param user: string in the form {uid}:{gid} representing the uid and gid to run the command as.
    :param d: dictionary representing the environment to instantiate within the actor container.
    :param privileged: whether this actor is "privileged"; i.e., its container should run in privileged mode with the
    docker daemon mounted.
    :param mounts: list of dictionaries representing the mounts to add; each dictionary mount should have 3 keys:
    host_path, container_path and format (which should have value 'ro' or 'rw').
    :param fifo_host_path: If not None, a string representing a path on the host to a FIFO used for passing binary data to the actor.
    :param socket_host_path: If not None, a string representing a path on the host to a socket used for collecting results from the actor.
    :return: result (dict), logs (str) - `result`: statistics about resource consumption; `logs`: output from docker logs.
    """
    logger.debug("top of execute_actor()")

    # initial stats object, environment, binds and volumes
    result = {'cpu': 0,
              'io': 0,
              'runtime': 0 }

    # instantiate docker client
    cli = docker.APIClient(base_url=dd, version="auto")

    # don't try to pass binary messages through the environment as these can cause
    # broken pipe errors. the binary data will be passed through the FIFO momentarily.
    if not fifo_host_path:
        d['MSG'] = msg
    binds = {}
    volumes = []

    # if container is privileged, mount the docker daemon so that additional
    # containers can be started.
    logger.debug("privileged: {}".format(privileged))
    if privileged:
        binds = {'/var/run/docker.sock':{
                    'bind': '/var/run/docker.sock',
                    'ro': False }}
        volumes = ['/var/run/docker.sock']

    # add a bind key and dictionary as well as a volume for each mount
    for m in mounts:
        binds[m.get('host_path')] = {'bind': m.get('container_path'),
                                     'ro': m.get('format') == 'ro'}
        volumes.append(m.get('host_path'))
    host_config = cli.create_host_config(binds=binds, privileged=privileged)

    # write binary data to FIFO if it exists:
    if fifo_host_path:
        try:
            fifo = os.open(fifo_host_path, os.O_RDWR)
            os.write(fifo, msg)
        except Exception as e:
            logger.error("Error writing the FIFO. Exception: {}".format(e))
            os.remove(fifo_host_path)
            raise DockerStartContainerError("Error writing to fifo: {}".format(e))

    # set up results socket
    try:
        server = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        server.bind(socket_host_path)
        server.settimeout(RESULTS_SOCKET_TIMEOUT)
    except Exception as e:
        logger.error("could not instantiate or bind socket. Exception: {}".format(e))
        raise e

    # instantiate the results channel:
    results_ch = ExecutionResultsChannel(actor_id, execution_id)

    # create and start the container
    logger.debug("Final container environment: {}".format(d))
    logger.debug("Final binds: {} and host_config: {} for the container.".format(binds, host_config))
    container = cli.create_container(image=image,
                                     environment=d,
                                     user=user,
                                     volumes=volumes,
                                     host_config=host_config)
    # get the URC time stampe
    start_time = get_current_utc_time()
    # start the timer to track total execution time.
    start = timeit.default_timer()
    logger.debug("right before cli.start: {}".format(start))
    try:
        cli.start(container=container.get('Id'))
    except Exception as e:
        # if there was an error starting the container, user will need to debug
        logger.info("Got exception starting actor container: {}".format(e))
        raise DockerStartContainerError("Could not start container {}. Exception {}".format(container.get('Id'), str(e)))

    # local bool tracking whether the actor container is still running
    running = True

    logger.debug("right before creating stats_cli: {}".format(timeit.default_timer()))
    # create a separate cli for checking stats objects since these should be fast and we don't want to wait
    stats_cli = docker.APIClient(base_url=dd, timeout=1, version="auto")
    logger.debug("right after creating stats_cli: {}".format(timeit.default_timer()))

    # under load, we can see UnixHTTPConnectionPool ReadTimeout's trying to create the stats_obj
    # so here we are trying up to 3 times to create the stats object for a possible total of 3s
    # timeouts
    ct = 0
    while ct < 3:
        try:
            stats_obj = stats_cli.stats(container=container.get('Id'), decode=True)
            break
        except ReadTimeout:
            ct += 1
        except Exception as e:
            logger.error("Unexpected exception creating stats_obj. Exception: {}".format(e))
            # in this case, we need to kill the container since we cannot collect stats;
            running = False
    logger.debug("right after creating stats_obj: {}".format(timeit.default_timer()))

    while running:
        datagram = None
        try:
            datagram = server.recv(MAX_RESULT_FRAME_SIZE)
        except socket.timeout:
            pass
        except Exception as e:
            logger.error("got exception from server.recv: {}".format(e))
        logger.debug("right after try/except datagram block: {}".format(timeit.default_timer()))
        if datagram:
            try:
                results_ch.put(datagram)
            except Exception as e:
                logger.error("Error trying to put datagram on results channel. Exception: {}".format(e))
        logger.debug("right after results ch.put: {}".format(timeit.default_timer()))
        try:
            logger.debug("waiting on a stats obj: {}".format(timeit.default_timer()))
            stats = next(stats_obj)
            logger.debug("got the stats obj: {}".format(timeit.default_timer()))
        except ReadTimeoutError:
            # this is a ReadTimeoutError from docker, not requests. container is finished.
            logger.debug("next(stats) just timed out: {}".format(timeit.default_timer()))
            # container stopped before another stats record could be read, just ignore and move on
            running = False
            break
        try:
            result['cpu'] += stats['cpu_stats']['cpu_usage']['total_usage']
        except KeyError as e:
            logger.info("Got a KeyError trying to fetch the cpu object: {}".format(e))
        try:
            result['io'] += stats['networks']['eth0']['rx_bytes']
        except KeyError as e:
            logger.info("Got KeyError exception trying to grab the io object. running: {}; Exception: {}".format(running, e))
        if running:
            logger.debug("about to check container status: {}".format(timeit.default_timer()))
            # we need to wait for the container id to be available
            i = 0
            while i < 10:
                try:
                    c = cli.containers(all=True, filters={'id': container.get('Id')})[0]
                    break
                except IndexError:
                    logger.error("Got an IndexError trying to get the container object.")
                    time.sleep(0.1)
                    i += 1
            logger.debug("done checking status: {}; i: {}".format(timeit.default_timer(), i))
            if i == 10:
                logger.error("Never could retrieve the container object! container id: {}".format(container.get('Id')))
                try:
                    cli.stop(container.get('Id'))
                except Exception as e:
                    logger.error("Got another exception trying to stop the actor container. Exception: {}".format(e))
                finally:
                    running = False
                continue
            state = c.get('State')
            if not state == 'running':
                logger.debug("container finished, final state: {}".format(state))
                running = False
            else:
                # container still running; check if we are beyond the max_run_time
                runtime = timeit.default_timer() - start
                if max_run_time > 0 and max_run_time < runtime:
                    logger.info("hit runtime limit: {}".format(timeit.default_timer()))
                    cli.stop(container.get('Id'))
                    running = False
            logger.debug("right after checking container state: {}".format(timeit.default_timer()))
    logger.info("container stopped:{}".format(timeit.default_timer()))
    stop = timeit.default_timer()
    # get info from container execution, including exit code
    try:
        container_info = cli.inspect_container(container.get('Id'))
        try:
            container_state = container_info['State']
            try:
                exit_code = container_state['ExitCode']
            except KeyError as e:
                logger.error("Could not determine ExitCode for container {}. e: {}".format(container.get('Id'), e))
                exit_code = 'undetermined'
        except KeyError as e:
            logger.error("Could not determine final state for container {}. e: {} ".format(container.get('Id')), e)
            container_state = {'unavailable': True}
    except docker.errors.APIError as e:
        logger.error("Could not inspect container {}. e: {}".format(container.get('Id'), e))

    logger.debug("right after getting container_info: {}".format(timeit.default_timer()))
    # get logs from container
    logs = cli.logs(container.get('Id'))
    logger.debug("right after getting container logs: {}".format(timeit.default_timer()))

    # get any additional results from the execution:
    while True:
        datagram = None
        try:
            datagram = server.recv(MAX_RESULT_FRAME_SIZE)
        except socket.timeout:
            break
        except Exception as e:
            logger.error("Got exception from server.recv: {}".format(e))
        if datagram:
            try:
                results_ch.put(datagram)
            except Exception as e:
                logger.error("Error trying to put datagram on results channel. Exception: {}".format(e))
    logger.debug("right after getting last execution results from datagram socket: {}".format(timeit.default_timer()))
    if socket_host_path:
        server.close()
        os.remove(socket_host_path)
    logger.debug("right after removing socket: {}".format(timeit.default_timer()))

    # remove container, ignore errors
    if not leave_container:
        try:
            cli.remove_container(container=container)
            logger.info("Container removed.")
        except Exception as e:
            logger.error("Exception trying to remove actor: {}".format(e))
    else:
        logger.debug("leaving actor container since leave_container was True.")
    logger.debug("right after removing actor container: {}".format(timeit.default_timer()))

    if fifo_host_path:
        os.close(fifo)
        os.remove(fifo_host_path)
    result['runtime'] = int(stop - start)
    logger.debug("right after removing fifo; about to return: {}".format(timeit.default_timer()))
    return result, logs, container_state, exit_code, start_time
