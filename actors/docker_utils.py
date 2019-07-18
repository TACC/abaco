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
from codes import BUSY, READY, RUNNING
import globals
from models import Execution, get_current_utc_time

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

class DockerStopContainerError(DockerError):
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

def container_running(name=None):
    """Check if there is a running container whose name contains the string, `name`. Note that this function will
    return True if any running container has a name which contains the input `name`.

    """
    logger.debug("top of container_running().")
    filters = {}
    if name:
        filters['name'] = name
    cli = docker.APIClient(base_url=dd, version="auto")
    try:
        containers = cli.containers(filters=filters)
    except Exception as e:
        msg = "There was an error checking container_running for name: {}. Exception: {}".format(name, e)
        logger.error(msg)
        raise DockerError(msg)
    logger.debug("found containers: {}".format(containers))
    return len(containers) > 0

def run_container_with_docker(image,
                              command,
                              name=None,
                              environment={},
                              mounts=[],
                              log_file=None,
                              auto_remove=False,
                              client_id=None,
                              client_access_token=None,
                              client_refresh_token=None,
                              actor_id=None,
                              tenant=None,
                              api_server=None,
                              client_secret=None

):
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
    # also add it to the environment if not already there
    if 'abaco_conf_host_path' not in environment:
        environment['abaco_conf_host_path'] = abaco_conf_host_path

    if 'client_id' not in environment:
        environment['client_id'] = client_id

    if 'client_access_token' not in environment:
        environment['client_access_token'] = client_access_token

    if 'actor_id' not in environment:
        environment['actor_id'] = actor_id

    if 'tenant' not in environment:
        environment['tenant'] = tenant

    if 'api_server' not in environment:
        environment['api_server'] = api_server

    if 'client_secret' not in environment:
        environment['client_secret'] = client_secret

    if 'client_refresh_token' not in environment:
        environment['client_refresh_token'] = client_refresh_token

    # if not passed, determine what log file to use
    if not log_file:
        if get_log_file_strategy() == 'split':
            log_file = 'worker.log'
        else:
            log_file = 'abaco.log'

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

    # add the container to a specific docker network, if configured
    netconf = None
    try:
        docker_network = Config.get('spawner', 'docker_network')
    except Exception:
        docker_network = None
    if docker_network:
        netconf = cli.create_networking_config({docker_network: cli.create_endpoint_config()})

    # create and start the container
    try:
        container = cli.create_container(image=image,
                                         environment=environment,
                                         volumes=volumes,
                                         host_config=host_config,
                                         command=command,
                                         name=name,
                                         networking_config=netconf)
        cli.start(container=container.get('Id'))
        logger.info('LOOK HERE - container successfully created! ')
    except Exception as e:
        msg = "Got exception trying to run container from image: {}. Exception: {}".format(image, e)
        logger.info(msg)
        raise DockerError(msg)
    logger.info("container started successfully: {}".format(container))
    return container


def run_worker(image,
               actor_id,
               worker_id,
               client_id,
               client_access_token,
               client_refresh_token,
               tenant,
               api_server,
               client_secret):
    """
    Run an actor executor worker with a given channel and image.
    :return:
    """
    logger.debug("top of run_worker()")
    command = 'python3 -u /actors/worker.py'
    logger.debug("docker_utils running worker. image:{}, command:{}".format(
        image, command))

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
        else:
            auto_remove = True
    elif not auto_remove == True:
        auto_remove = False
    container = run_container_with_docker(
        image=AE_IMAGE,
        command=command,
        environment={
            'image': image,
            'worker_id': worker_id,
            '_abaco_secret': os.environ.get('_abaco_secret')},
            mounts=mounts,
            log_file=None,
            auto_remove=auto_remove,
            name='worker_{}_{}'.format(actor_id, worker_id),
            client_id=client_id,
            client_access_token=client_access_token,
            client_refresh_token=client_refresh_token,
            actor_id=actor_id,
            tenant=tenant,
            api_server=api_server,
            client_secret=client_secret
    )
    # don't catch errors -- if we get an error trying to run a worker, let it bubble up.
    # TODO - determines worker structure; should be placed in a proper DAO class.
    logger.info("worker container running. worker_id: {}. container: {}".format(worker_id, container))
    return { 'image': image,
             # @todo - location will need to change to support swarm or cluster
             'location': dd,
             'id': worker_id,
             'cid': container.get('Id'),
             'status': READY,
             'host_id': host_id,
             'host_ip': host_ip,
             'last_execution_time': 0,
             'last_health_check_time': get_current_utc_time() }

def stop_container(cli, cid):
    """
    Attempt to stop a running container, with retry logic. Should only be called with a running container.
    :param cli: the docker cli object to use.
    :param cid: the container id of the container to be stopped.
    :return:
    """
    i = 0
    while i < 10:
        try:
            cli.stop(cid)
            return True
        except Exception as e:
            logger.error("Got another exception trying to stop the actor container. Exception: {}".format(e))
            i += 1
            continue
    raise DockerStopContainerError

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
                  socket_host_path=None,
                  mem_limit=None,
                  max_cpus=None):
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
    :param mem_limit: The maximum amount of memory the Actor container can use; should be the same format as the --memory Docker flag.
    :param max_cpus: The maximum number of CPUs each actor will have available to them. Does not guarantee these CPU resources; serves as upper bound.
    :return: result (dict), logs (str) - `result`: statistics about resource consumption; `logs`: output from docker logs.
    """
    logger.debug("top of execute_actor(); (worker {};{})".format(worker_id, execution_id))

    # initially set the global force_quit variable to False
    globals.force_quit = False

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
    logger.debug("privileged: {};(worker {};{})".format(privileged, worker_id, execution_id))
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

    # mem_limit
    # -1 => unlimited memory
    if mem_limit == '-1':
        mem_limit = None

    # max_cpus
    try:
        max_cpus = int(max_cpus)
    except:
        max_cpus = None
    # -1 => unlimited cpus
    if max_cpus == -1:
        max_cpus = None

    host_config = cli.create_host_config(binds=binds, privileged=privileged, mem_limit=mem_limit, nano_cpus=max_cpus)
    logger.debug("host_config object created by (worker {};{}).".format(worker_id, execution_id))

    # write binary data to FIFO if it exists:
    if fifo_host_path:
        try:
            fifo = os.open(fifo_host_path, os.O_RDWR)
            os.write(fifo, msg)
        except Exception as e:
            logger.error("Error writing the FIFO. Exception: {};(worker {};{})".format(e, worker_id, execution_id))
            os.remove(fifo_host_path)
            raise DockerStartContainerError("Error writing to fifo: {}; "
                                            "(worker {};{})".format(e, worker_id, execution_id))

    # set up results socket -----------------------
    # make sure socket doesn't already exist:
    try:
        os.unlink(socket_host_path)
    except OSError as e:
        if os.path.exists(socket_host_path):
            logger.error("socket at {} already exists; Exception: {}; (worker {};{})".format(socket_host_path, e,
                                                                                             worker_id, execution_id))
            raise DockerStartContainerError("Got an OSError trying to create the results docket; "
                                            "exception: {}".format(e))

    # use retry logic since, when the compute node is under load, we see errors initially trying to create the socket
    # server object.
    keep_trying = True
    count = 0
    server = None
    while keep_trying and count < 10:
        keep_trying = False
        count = count + 1
        try:
            server = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        except Exception as e:
            keep_trying = True
            logger.info("Could not instantiate socket at {}. "
                         "Count: {}; Will keep trying. "
                         "Exception: {}; type: {}; (worker {};{})".format(socket_host_path, count, e, type(e),
                                                                          worker_id, execution_id))
        try:
            server.bind(socket_host_path)
        except Exception as e:
            keep_trying = True
            logger.info("Could not bind socket at {}. "
                         "Count: {}; Will keep trying. "
                         "Exception: {}; type: {}; (worker {};{})".format(socket_host_path, count, e, type(e),
                                                                          worker_id, execution_id))
        try:
            server.settimeout(RESULTS_SOCKET_TIMEOUT)
        except Exception as e:
            keep_trying = True
            logger.info("Could not set timeout for socket at {}. "
                         "Count: {}; Will keep trying. "
                         "Exception: {}; type: {}; (worker {};{})".format(socket_host_path, count, e, type(e),
                                                                          worker_id, execution_id))
    if not server:
        msg = "Failed to instantiate results socket. " \
              "Abaco compute host could be overloaded. Exception: {}; (worker {};{})".format(e, worker_id, execution_id)
        logger.error(msg)
        raise DockerStartContainerError(msg)

    logger.debug("results socket server instantiated. (worker {};{})".format(worker_id, execution_id))

    # instantiate the results channel:
    results_ch = ExecutionResultsChannel(actor_id, execution_id)

    # create and start the container
    logger.debug("Final container environment: {};(worker {};{})".format(d, worker_id, execution_id))
    logger.debug("Final binds: {} and host_config: {} for the container.(worker {};{})".format(binds, host_config,
                                                                                               worker_id, execution_id))
    container = cli.create_container(image=image,
                                     environment=d,
                                     user=user,
                                     volumes=volumes,
                                     host_config=host_config)
    # get the UTC time stamp
    start_time = get_current_utc_time()
    # start the timer to track total execution time.
    start = timeit.default_timer()
    logger.debug("right before cli.start: {}; container id: {}; "
                 "(worker {};{})".format(start, container.get('Id'), worker_id, execution_id))
    try:
        cli.start(container=container.get('Id'))
    except Exception as e:
        # if there was an error starting the container, user will need to debug
        logger.info("Got exception starting actor container: {}; (worker {};{})".format(e, worker_id, execution_id))
        raise DockerStartContainerError("Could not start container {}. Exception {}".format(container.get('Id'), str(e)))

    # local bool tracking whether the actor container is still running
    running = True
    Execution.update_status(actor_id, execution_id, RUNNING)

    logger.debug("right before creating stats_cli: {}; (worker {};{})".format(timeit.default_timer(),
                                                                              worker_id, execution_id))
    # create a separate cli for checking stats objects since these should be fast and we don't want to wait
    stats_cli = docker.APIClient(base_url=dd, timeout=1, version="auto")
    logger.debug("right after creating stats_cli: {}; (worker {};{})".format(timeit.default_timer(),
                                                                             worker_id, execution_id))

    # under load, we can see UnixHTTPConnectionPool ReadTimeout's trying to create the stats_obj
    # so here we are trying up to 3 times to create the stats object for a possible total of 3s
    # timeouts
    ct = 0
    stats_obj = None
    logs = None
    while ct < 3:
        try:
            stats_obj = stats_cli.stats(container=container.get('Id'), decode=True)
            break
        except ReadTimeout:
            ct += 1
        except Exception as e:
            logger.error("Unexpected exception creating stats_obj. Exception: {}; (worker {};{})".format(e, worker_id,
                                                                                                         execution_id))
            # in this case, we need to kill the container since we cannot collect stats;
            # UPDATE - 07-2018: under load, a errors can occur attempting to create the stats object.
            # the container could still be running; we need to explicitly check the container status
            # to be sure.
    logger.debug("right after attempting to create stats_obj: {}; (worker {};{})".format(timeit.default_timer(),
                                                                                         worker_id, execution_id))
    # a counter of the number of iterations through the main "running" loop;
    # this counter is used to determine when less frequent actions, such as log aggregation, need to run.
    loop_idx = 0
    while running and not globals.force_quit:
        loop_idx += 1
        logger.debug("top of while running loop; loop_idx: {}".format(loop_idx))
        datagram = None
        stats = None
        try:
            datagram = server.recv(MAX_RESULT_FRAME_SIZE)
        except socket.timeout:
            pass
        except Exception as e:
            logger.error("got exception from server.recv: {}; (worker {};{})".format(e, worker_id, execution_id))
        logger.debug("right after try/except datagram block: {}; (worker {};{})".format(timeit.default_timer(),
                                                                                        worker_id, execution_id))
        if datagram:
            try:
                results_ch.put(datagram)
            except Exception as e:
                logger.error("Error trying to put datagram on results channel. "
                             "Exception: {}; (worker {};{})".format(e, worker_id, execution_id))
        logger.debug("right after results ch.put: {}; (worker {};{})".format(timeit.default_timer(),
                                                                             worker_id, execution_id))

        # only try to collect stats if we have a stats_obj:
        if stats_obj:
            logger.debug("we have a stats_obj; trying to collect stats. (worker {};{})".format(worker_id, execution_id))
            try:
                logger.debug("waiting on a stats obj: {}; (worker {};{})".format(timeit.default_timer(),
                                                                                 worker_id, execution_id))
                stats = next(stats_obj)
                logger.debug("got the stats obj: {}; (worker {};{})".format(timeit.default_timer(),
                                                                            worker_id, execution_id))
            except StopIteration:
                # we have read the last stats object - no need for processing
                logger.debug("Got StopIteration; no stats object. (worker {};{})".format(worker_id, execution_id))
            except ReadTimeoutError:
                # this is a ReadTimeoutError from docker, not requests. container is finished.
                logger.info("next(stats) just timed out: {}; (worker {};{})".format(timeit.default_timer(),
                                                                                    worker_id, execution_id))
                # UPDATE - 07-2018: under load, a ReadTimeoutError from the attempt to get a stats object
                # does NOT imply the container has stopped; we need to explicitly check the container status
                # to be sure.

        # if we got a stats object, add it to the results; it is possible stats collection timed out and the object
        # is None
        if stats:
            logger.debug("adding stats to results; (worker {};{})".format(worker_id, execution_id))
            try:
                result['cpu'] += stats['cpu_stats']['cpu_usage']['total_usage']
            except KeyError as e:
                logger.info("Got a KeyError trying to fetch the cpu object: {}; "
                            "(worker {};{})".format(e, worker_id, execution_id))
            try:
                result['io'] += stats['networks']['eth0']['rx_bytes']
            except KeyError as e:
                logger.info("Got KeyError exception trying to grab the io object. "
                            "running: {}; Exception: {}; (worker {};{})".format(running, e, worker_id, execution_id))

        # grab the logs every 5th iteration --
        if loop_idx % 5 == 0:
            logs = cli.logs(container.get('Id'))
            Execution.set_logs(execution_id, logs)
            logs = None

        # checking the container status to see if it is still running ----
        if running:
            logger.debug("about to check container status: {}; (worker {};{})".format(timeit.default_timer(),
                                                                                      worker_id, execution_id))
            # we need to wait for the container id to be available
            i = 0
            while i < 10:
                try:
                    c = cli.containers(all=True, filters={'id': container.get('Id')})[0]
                    break
                except IndexError:
                    logger.error("Got an IndexError trying to get the container object. "
                                 "(worker {};{})".format(worker_id, execution_id))
                    time.sleep(0.1)
                    i += 1
            logger.debug("done checking status: {}; i: {}; (worker {};{})".format(timeit.default_timer(), i,
                                                                                  worker_id, execution_id))
            # if we were never able to get the container object, we need to stop processing and kill this
            # worker; the docker daemon could be under heavy load, but we need to not launch another
            # actor container with this worker, because the existing container may still be running,
            if i == 10 or not c:
                # we'll try to stop the container
                logger.error("Never could retrieve the container object! Attempting to stop container; "
                             "container id: {}; (worker {};{})".format(container.get('Id'), worker_id, execution_id))
                # stop_container could raise an exception - if so, we let it pass up and have the worker
                # shut itself down.
                stop_container(cli, container.get('Id'))
                logger.info("container {} stopped. (worker {};{})".format(container.get('Id'), worker_id, execution_id))

                # if we were able to stop the container, we can set running to False and keep the
                # worker running
                running = False
                continue
            state = c.get('State')
            if not state == 'running':
                logger.debug("container finished, final state: {}; (worker {};{})".format(state,
                                                                                          worker_id, execution_id))
                running = False
                continue
            else:
                # container still running; check if a force_quit has been sent OR
                # we are beyond the max_run_time
                runtime = timeit.default_timer() - start
                if globals.force_quit or (max_run_time > 0 and max_run_time < runtime):
                    logs = cli.logs(container.get('Id'))
                    if globals.force_quit:
                        logger.info("issuing force quit: {}; (worker {};{})".format(timeit.default_timer(),
                                                                               worker_id, execution_id))
                    else:
                        logger.info("hit runtime limit: {}; (worker {};{})".format(timeit.default_timer(),
                                                                               worker_id, execution_id))
                    cli.stop(container.get('Id'))
                    running = False
            logger.debug("right after checking container state: {}; (worker {};{})".format(timeit.default_timer(),
                                                                                           worker_id, execution_id))
    logger.info("container stopped:{}; (worker {};{})".format(timeit.default_timer(), worker_id, execution_id))
    stop = timeit.default_timer()
    globals.force_quit = False

    # get info from container execution, including exit code; Exceptions from any of these commands
    # should not cause the worker to shutdown or prevent starting subsequent actor containers.
    try:
        container_info = cli.inspect_container(container.get('Id'))
        try:
            container_state = container_info['State']
            try:
                exit_code = container_state['ExitCode']
            except KeyError as e:
                logger.error("Could not determine ExitCode for container {}. "
                             "Exception: {}; (worker {};{})".format(container.get('Id'), e, worker_id, execution_id))
                exit_code = 'undetermined'
        except KeyError as e:
            logger.error("Could not determine final state for container {}. "
                         "Exception: {}; (worker {};{})".format(container.get('Id')), e, worker_id, execution_id)
            container_state = {'unavailable': True}
    except docker.errors.APIError as e:
        logger.error("Could not inspect container {}. "
                     "Exception: {}; (worker {};{})".format(container.get('Id'), e, worker_id, execution_id))

    logger.debug("right after getting container_info: {}; (worker {};{})".format(timeit.default_timer(),
                                                                                 worker_id, execution_id))
    # get logs from container
    if not logs:
        logs = cli.logs(container.get('Id'))
    if not logs:
        # there are issues where container do not have logs associated with them when they should.
        logger.info("Container id {} had NO logs associated with it. "
                    "(worker {};{})".format(container.get('Id'), worker_id, execution_id))
    logger.debug("right after getting container logs: {}; (worker {};{})".format(timeit.default_timer(),
                                                                                 worker_id, execution_id))

    # get any additional results from the execution:
    while True:
        datagram = None
        try:
            datagram = server.recv(MAX_RESULT_FRAME_SIZE)
        except socket.timeout:
            break
        except Exception as e:
            logger.error("Got exception from server.recv: {}; (worker {};{})".format(e, worker_id, execution_id))
        if datagram:
            try:
                results_ch.put(datagram)
            except Exception as e:
                logger.error("Error trying to put datagram on results channel. "
                             "Exception: {}; (worker {};{})".format(e, worker_id, execution_id))
    logger.debug("right after getting last execution results from datagram socket: {}; "
                 "(worker {};{})".format(timeit.default_timer(), worker_id, execution_id))
    if socket_host_path:
        server.close()
        os.remove(socket_host_path)
    logger.debug("right after removing socket: {}; (worker {};{})".format(timeit.default_timer(),
                                                                          worker_id, execution_id))

    # remove actor container with retrying logic -- check for specific filesystem errors from the docker daemon:
    if not leave_container:
        keep_trying = True
        count = 0
        while keep_trying and count < 10:
            keep_trying = False
            count = count + 1
            try:
                cli.remove_container(container=container)
                logger.info("Actor container removed. (worker {};{})".format(worker_id, execution_id))
            except Exception as e:
                # if the container is already gone we definitely want to quit:
                if 'No such container' in str(e):
                    logger.info("Got 'no such container' exception - quiting. "
                                "Exception: {}; (worker {};{})".format(e, worker_id, execution_id))
                    break
                # if we get a resource busy/internal server error from docker, we need to keep trying to remove the
                # container.
                elif 'device or resource busy' in str(e) or 'failed to remove root filesystem' in str(e):
                    logger.error("Got resource busy/failed to remove filesystem exception trying to remove "
                                 "actor container; will keep trying."
                                 "Count: {}; Exception: {}; (worker {};{})".format(count, e, worker_id, execution_id))
                    time.sleep(1)
                    keep_trying = True
                else:
                    logger.error("Unexpected exception trying to remove actor container. Giving up."
                                 "Exception: {}; type: {}; (worker {};{})".format(e, type(e), worker_id, execution_id))
    else:
        logger.debug("leaving actor container since leave_container was True. "
                     "(worker {};{})".format(worker_id, execution_id))
    logger.debug("right after removing actor container: {}; (worker {};{})".format(timeit.default_timer(),
                                                                                   worker_id, execution_id))

    if fifo_host_path:
        os.close(fifo)
        os.remove(fifo_host_path)
    if results_ch:
        results_ch.close()
    result['runtime'] = int(stop - start)
    logger.debug("right after removing fifo; about to return: {}; (worker {};{})".format(timeit.default_timer(),
                                                                                         worker_id, execution_id))
    return result, logs, container_state, exit_code, start_time
