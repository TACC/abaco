import json
import os
import socket
import time
import timeit
import datetime
import random

import docker
from requests.packages.urllib3.exceptions import ReadTimeoutError
from requests.exceptions import ReadTimeout, ConnectionError

from common.logs import get_logger
logger = get_logger(__name__)

from channels import ExecutionResultsChannel
from common.config import conf
from codes import BUSY, READY, RUNNING
import encrypt_utils
import globals
from models import Actor, Adapter, AdapterServer, Execution, get_current_utc_time, display_time, site, ActorConfig
from stores import workers_store, alias_store, configs_store, adapter_servers_store
import encrypt_utils


TAG = os.environ.get('TAG') or conf.version or ''
if not TAG[0] == ':':
    TAG = f':{TAG}'
AE_IMAGE = f"{os.environ.get('AE_IMAGE', 'abaco/core-v3')}{TAG}"

# timeout (in seconds) for the socket server
RESULTS_SOCKET_TIMEOUT = 0.1

# max frame size, in bytes, for a single result
MAX_RESULT_FRAME_SIZE = 131072

max_run_time = conf.worker_max_run_time

dd = conf.docker_dd
host_id = os.environ.get('SPAWNER_HOST_ID', conf.spawner_host_id)
logger.debug(f"host_id: {host_id}")
host_ip = conf.spawner_host_ip


class DockerError(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = message


class DockerStartContainerError(DockerError):
    pass

class DockerStopContainerError(DockerError):
    pass

def get_docker_credentials():
    """
    Get the docker credentials from the config.
    """
    # we try to get as many credentials as have been
    creds = []
    cnt = 1
    while True:
        try:
            # Format in config should be
            # dockerhub_username_1
            # dockerhub_password_1
            # dockerhub_username_2
            # dockerhub_password_2 
            # See stache entry "CIC (Abaco) DockerHub credentials" for credentials.
            username = conf.get(f'dockerhub_username_{cnt}')
            password = conf.get(f'dockerhub_password_{cnt}')
        except:
            break
        if not username or not password:
            break
        creds.append({'username': username, 'password': password})
        cnt = cnt + 1
    return creds


dockerhub_creds = get_docker_credentials()


def get_random_dockerhub_cred():
    """
    Chose a dockerhub credential at random
    """
    if len(dockerhub_creds) == 0:
        return None, None
    creds = random.choice(dockerhub_creds)
    try:
        username = creds['username']
        password = creds['password']
    except Exception as e:
        logger.debug(f"Got exception trying to get dockerhub credentials. e: {e}")
        return None, None
    return username, password


def cli_login(cli, username, password):
    """
    Try to login a dockerhub cli with a username and password
    """
    try:
        cli.login(username=username, password=password)
    except Exception as e:
        logger.error(f"Could not login using dockerhub creds; username: {username}."
                     f"Exception: {e}")


def rm_container(cid):
    """
    Remove a container.
    :param cid:
    :return:
    """
    cli = docker.APIClient(base_url=dd, version="auto")
    username, password = get_random_dockerhub_cred()
    if username and password:
        cli_login(cli, username, password)
    try:
        rsp = cli.remove_container(cid, force=True)
    except Exception as e:
        logger.info(f"Got exception trying to remove container: {cid}. Exception: {e}")
        raise DockerError(f"Error removing container {cid}, exception: {str(e)}")
    logger.info(f"container {cid} removed.")

def pull_image(image):
    """
    Update the local registry with an actor's image.
    :param actor_id:
    :return:
    """
    logger.debug(f"top of pull_image(), dd: {dd}")
    cli = docker.APIClient(base_url=dd, version="auto")
    username, password = get_random_dockerhub_cred()
    if username and password:
        cli_login(cli, username, password)
    try:
        rsp = cli.pull(repository=image)
    except Exception as e:
        msg = f"Error pulling image {image} - exception: {e} "
        logger.info(msg)
        raise DockerError(msg)
    if '"message":"Error' in rsp:
        if f'{image} not found' in rsp:
            msg = f"Image {image} was not found on the public registry."
            logger.info(msg)
            raise DockerError(msg)
        else:
            msg = f"There was an error pulling the image: {rsp}"
            logger.error(msg)
            raise DockerError(msg)
    return rsp


def list_all_containers():
    """Returns a list of all containers """
    cli = docker.APIClient(base_url=dd, version="auto")
    return cli.containers()


def get_current_worker_containers():
    worker_containers = []
    containers = list_all_containers()
    for c in containers:
        if 'worker' in c['Names'][0]:
            container_name = c['Names'][0]
            # worker container names have format "/worker_<tenant>_<actor-id>_<worker-id>
            # so split on _ to get parts
            try:
                parts = container_name.split('_')
                tenant_id = parts[1]
                actor_id = parts[2]
                worker_id = parts[3]
                worker_containers.append({'container': c,
                                          'tenant_id': tenant_id,
                                          'actor_id': actor_id,
                                          'worker_id': worker_id
                                          })
            except:
                pass
    return worker_containers

def get_current_server_containers():
    server_containers = []
    containers = list_all_containers()
    for c in containers:
        if 'adapterserver' in c['Names'][0]:
            container_name = c['Names'][0]
            # server container names have format "/adapterserver_<tenant>_<adapter-id>_<server-id>
            # so split on _ to get parts
            try:
                parts = container_name.split('_')
                tenant_id = parts[1]
                adapter_id = parts[2]
                server_id = parts[3]
                server_containers.append({'container': c,
                                          'tenant_id': tenant_id,
                                          'adapter_id': adapter_id,
                                          'server_id': server_id
                                          })
            except:
                pass
    return server_containers

def check_worker_containers_against_store():
    """
    cheks the existing worker containers on a host against the status of the worker in the workers_store.
    """
    worker_containers = get_current_worker_containers()
    for idx, w in enumerate(worker_containers):
        try:
            # try to get the worker from the store:
            store_key = f"{w['tenant_id']}_{w['actor_id']}_{w['worker_id']}"
            worker = workers_store[site()][store_key]
        except KeyError:
            worker = {}
        status = worker.get('status')
        try:
            last_execution_time = display_time(worker.get('last_execution_time'))
        except:
            last_execution_time = None
        print(idx, '). ', w['actor_id'], w['worker_id'], status, last_execution_time)


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
        msg = f"There was an error checking container_running for name: {name}. Exception: {e}"
        logger.error(msg)
        raise DockerError(msg)
    logger.debug(f"found containers: {containers}")
    return len(containers) > 0

def run_container_with_docker(image,
                              command,
                              name=None,
                              environment={},
                              mounts=[],
                              log_file=None,
                              auto_remove=False,
                              actor_id=None,
                              tenant=None,
                              api_server=None):
    """
    Run a container with docker mounted in it.
    Note: this function always mounts the abaco conf file so it should not be used by execute_actor().
    """
    logger.debug("top of run_container_with_docker().")
    cli = docker.APIClient(base_url=dd, version="auto")

    # bind the docker socket as r/w since this container gets docker.
    binds = {'/var/run/docker.sock': {'bind': '/var/run/docker.sock', 'ro': False}}
    # add a bind key and dictionary as well as a volume for each mount
    for m in mounts:
        binds[m.get('host_path')] = {'bind': m.get('container_path'),
                                     'ro': m.get('format') == 'ro'}

    # This should exist if config.json is using environment variables.
    # But there's a chance that it's not being used and isn't needed.
    abaco_host_path = os.environ.get('abaco_host_path')
    logger.debug(f"docker_utils using abaco_host_path={abaco_host_path}")

    try:
        # Get config path and mount to root of the container as r/o
        abaco_conf_host_path = conf.spawner_abaco_conf_host_path
        logger.debug(f"docker_utils using abaco_conf_host_path={abaco_conf_host_path}")
        binds[abaco_conf_host_path] = {'bind': '/home/tapis/config.json', 'ro': True}
    except AttributeError as e:
        # if we're here, it's bad. we don't have a config file. better to cut and run,
        msg = f"Did not find the abaco_conf_host_path in Config. Exception: {e}"
        logger.error(msg)
        raise DockerError(msg)

    if 'abaco_host_path' not in environment:
        environment['abaco_host_path'] = abaco_host_path

    if 'actor_id' not in environment:
        environment['actor_id'] = actor_id

    if 'tenant' not in environment:
        environment['tenant'] = tenant

    if 'api_server' not in environment:
        environment['api_server'] = api_server

    # if not passed, determine what log file to use
    if not log_file:
        if conf.log_filing_strategy == 'split' and conf.get('worker_log_file'):
            log_file = conf.get('worker_log_file')
        else:
            log_file = 'abaco.log'

    # first check to see if the logs directory config was set:
    logs_host_dir = conf.get("logs_host_dir") or None
    if not logs_host_dir:
        # if the directory is not configured, default it to abaco_conf_host_path
        logs_host_dir = os.path.dirname(abaco_conf_host_path)
    binds[f'{logs_host_dir}/{log_file}'] = {'bind': '/home/tapis/runtime_files/logs/service.log', 'rw': True}

    host_config = cli.create_host_config(binds=binds, auto_remove=auto_remove)
    logger.debug(f"binds: {binds}")

    # add the container to a specific docker network, if configured
    netconf = None
    docker_network = conf.spawner_docker_network
    if docker_network:
        netconf = cli.create_networking_config({docker_network: cli.create_endpoint_config()})

    # create and start the container
    try:
        container = cli.create_container(image=image,
                                         environment=environment,
                                         host_config=host_config,
                                         command=command,
                                         name=name,
                                         networking_config=netconf)
        cli.start(container=container.get('Id'))
        logger.debug('container successfully started')
    except Exception as e:
        msg = f"Got exception trying to run container from image: {image}. Exception: {e}"
        logger.info(msg)
        raise DockerError(msg)
    logger.info(f"container started successfully: {container}")
    return container


def run_worker(image,
               revision,
               actor_id,
               worker_id,
               tenant,
               api_server):
    """
    Run an actor executor worker with a given channel and image.
    :return:
    """
    logger.debug("top of run_worker()")
    command = 'python3 -u /actors/worker.py'
    logger.debug(f"docker_utils running worker. actor_id: {actor_id}; worker_id: {worker_id};"
                 f"image:{image}; revision: {revision}; command:{command}")

    mounts = []
    if conf.mongo_tls:
        # mount the directory on the host containing Mongo TLS certs.
        # Paths should be formatted as host_path:container_path for split
        mongo_certs_host_path_dir, mongo_certs_container_path_dir = conf.mongo_tls_certs_path.split(':')
        logger.info(f"Using mongo certs paths - {mongo_certs_host_path_dir}:{mongo_certs_container_path_dir}")
        mounts.append({'host_path': mongo_certs_host_path_dir,
                       'container_path': mongo_certs_container_path_dir,
                       'format': 'rw'})

    # mount the directory on the host for creating fifos
    # Paths should be formatted as host_path:container_path for split
    fifo_host_path_dir, fifo_container_path_dir = conf.worker_fifo_paths.split(':')
    logger.info(f"Using fifo paths - {fifo_host_path_dir}:{fifo_container_path_dir}")
    mounts.append({'host_path': os.path.join(fifo_host_path_dir, worker_id),
                   'container_path': os.path.join(fifo_container_path_dir, worker_id),
                   'format': 'rw'})

    # mount the directory on the host for creating result sockets
    # Paths should be formatted as host_path:container_path for split
    socket_host_path_dir, socket_container_path_dir = conf.worker_socket_paths.split(':')
    logger.info(f"Using socket paths - {socket_host_path_dir}:{socket_container_path_dir}")
    mounts.append({'host_path': os.path.join(socket_host_path_dir, worker_id),
                   'container_path': os.path.join(socket_container_path_dir, worker_id),
                   'format': 'rw'})

    logger.info(f"Final fifo and socket mounts: {mounts}")
    auto_remove = conf.worker_auto_remove
    container = run_container_with_docker(
        image=AE_IMAGE,
        command=command,
        environment={
            'image': image,
            'revision': revision,
            'worker_id': worker_id,
            'abaco_host_path': os.environ.get('abaco_host_path'),
            '_abaco_secret': os.environ.get('_abaco_secret')},
            mounts=mounts,
            log_file=None,
            auto_remove=auto_remove,
            name=f'worker_{actor_id}_{worker_id}',
            actor_id=actor_id,
            tenant=tenant,
            api_server=api_server
    )
    # don't catch errors -- if we get an error trying to run a worker, let it bubble up.
    # TODO - determines worker structure; should be placed in a proper DAO class.
    logger.info(f"worker container running. worker_id: {worker_id}. container: {container}")
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
            logger.error(f"Got another exception trying to stop the actor container. Exception: {e}")
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
                  max_cpus=None,
                  tenant=None):
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
    logger.debug(f"top of execute_actor(); actor_id: {actor_id}; tenant: {tenant} (worker {worker_id};{execution_id})")

    # get any configs for this actor
    actor_configs = {}
    config_list = []
    # list of all aliases for the actor
    alias_list = []
    # the actor_id passed in is the dbid
    actor_human_id = Actor.get_display_id(tenant, actor_id)

    for alias in alias_store[site()].items():
        logger.debug(f"checking alias: {alias}")
        if actor_human_id == alias['actor_id'] and tenant == alias['tenant']:
            alias_list.append(alias['alias'])
    logger.debug(f"alias_list: {alias_list}")
    # loop through configs to look for any that apply to this actor
    for config in configs_store[site()].items():
        # first look for the actor_id itself
        if actor_human_id in config['actors']:
            logger.debug(f"actor_id matched; adding config {config}")
            config_list.append(config)
        else:
            logger.debug("actor id did not match; checking aliases...")
            # if we didn't find the actor_id, look for ay of its aliases
            for alias in alias_list:
                if alias in config['actors']:
                    # as soon as we find it, append and get out (only want to add once)
                    logger.debug(f"alias {alias} matched; adding config: {config}")
                    config_list.append(config)
                    break
    logger.debug(f"got config_list: {config_list}")
    # for each config, need to check for secrets and decrypt ---
    for config in config_list:
        logger.debug('checking for secrets')
        try:
            if config['is_secret']:
                value = encrypt_utils.decrypt(config['value'])
                actor_configs[config['name']] = value
            else:
                actor_configs[config['name']] = config['value']

        except Exception as e:
            logger.error(f'something went wrong checking is_secret for config: {config}; e: {e}')

    logger.debug(f"final actor configs: {actor_configs}")
    d['_actor_configs'] = actor_configs


    # initially set the global force_quit variable to False
    globals.force_quit = False

    # initial stats object, environment and binds
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

    # if container is privileged, mount the docker daemon so that additional
    # containers can be started.
    logger.debug(f"privileged: {privileged};(worker {worker_id};{execution_id})")
    if privileged:
        binds = {'/var/run/docker.sock':{
                    'bind': '/var/run/docker.sock',
                    'ro': False }}

    # add a bind key and dictionary as well as a volume for each mount
    for m in mounts:
        binds[m.get('host_path')] = {'bind': m.get('container_path'),
                                     'ro': m.get('format') == 'ro'}

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
    logger.debug(f"host_config object created by (worker {worker_id};{execution_id}).")

    # write binary data to FIFO if it exists:
    fifo = None
    if fifo_host_path:
        try:
            fifo = os.open(fifo_host_path, os.O_RDWR)
            os.write(fifo, msg)
        except Exception as e:
            logger.error(f"Error writing the FIFO. Exception: {e};(worker {worker_id};{execution_id})")
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
            os.chmod(socket_host_path, 0o777)
            logger.debug(f"results socket permissions set to 777. socket_host_path: {socket_host_path}")
        except Exception as e:
            msg = f"Got exception trying to set permissions on the results socket. Not sure what to do. e: {e}"
            logger.error(msg)
            # for now, we'll just swallow it but this is really a TODO.

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
              "Abaco compute host could be overloaded. (worker {};{})".format(worker_id, execution_id)
        logger.error(msg)
        raise DockerStartContainerError(msg)

    logger.debug("results socket server instantiated. path: {} (worker {};{})".format(socket_host_path,
                                                                                      worker_id, execution_id))

    # instantiate the results channel:
    results_ch = ExecutionResultsChannel(actor_id, execution_id)

    # create and start the container
    logger.debug(f"Final container environment: {d};(worker {worker_id};{execution_id})")
    logger.debug("Final binds: {} and host_config: {} for the container.(worker {};{})".format(binds, host_config,
                                                                                               worker_id, execution_id))
    container = cli.create_container(image=image,
                                     environment=d,
                                     user=user,
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
        logger.info(f"Got exception starting actor container: {e}; (worker {worker_id};{execution_id})")
        raise DockerStartContainerError(f"Could not start container {container.get('Id')}. Exception {str(e)}")

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
    log_ex = Actor.get_actor_log_ttl(actor_id)
    while running and not globals.force_quit:
        loop_idx += 1
        logger.debug(f"top of while running loop; loop_idx: {loop_idx}")
        datagram = None
        stats = None
        try:
            datagram = server.recv(MAX_RESULT_FRAME_SIZE)
        except socket.timeout:
            pass
        except Exception as e:
            logger.error(f"got exception from server.recv: {e}; (worker {worker_id};{execution_id})")
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
            logger.debug(f"we have a stats_obj; trying to collect stats. (worker {worker_id};{execution_id})")
            try:
                logger.debug("waiting on a stats obj: {}; (worker {};{})".format(timeit.default_timer(),
                                                                                 worker_id, execution_id))
                stats = next(stats_obj)
                logger.debug("got the stats obj: {}; (worker {};{})".format(timeit.default_timer(),
                                                                            worker_id, execution_id))
            except StopIteration:
                # we have read the last stats object - no need for processing
                logger.debug(f"Got StopIteration; no stats object. (worker {worker_id};{execution_id})")
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
            logger.debug(f"adding stats to results; (worker {worker_id};{execution_id})")
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
            Execution.set_logs(execution_id, logs, actor_id, tenant, worker_id, log_ex)
            logs = None

        # checking the container status to see if it is still running ----
        if running:
            logger.debug("about to check container status: {}; (worker {};{})".format(timeit.default_timer(),
                                                                                      worker_id, execution_id))
            # we need to wait for the container id to be available
            i = 0
            c = None
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
                logger.info(f"container {container.get('Id')} stopped. (worker {worker_id};{execution_id})")

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
    logger.info(f"container stopped:{timeit.default_timer()}; (worker {worker_id};{execution_id})")
    stop = timeit.default_timer()
    globals.force_quit = False

    # get info from container execution, including exit code; Exceptions from any of these commands
    # should not cause the worker to shutdown or prevent starting subsequent actor containers.
    exit_code = 'undetermined'
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
            # Converting ISO8601 times to unix timestamps
            try:
                # Slicing to 23 to account for accuracy up to milliseconds and replace to get rid of ISO 8601 'Z'
                startedat_ISO = container_state['StartedAt'].replace('Z', '')[:23]
                finishedat_ISO = container_state['FinishedAt'].replace('Z', '')[:23]

                container_state['StartedAt'] = datetime.datetime.strptime(startedat_ISO, "%Y-%m-%dT%H:%M:%S.%f")
                container_state['FinishedAt'] = datetime.datetime.strptime(finishedat_ISO, "%Y-%m-%dT%H:%M:%S.%f")
            except Exception as e:
                logger.error(f"Datetime conversion failed for container {container.get('Id')}. ",
                             f"Exception: {e}; (worker {worker_id};{execution_id})")
                container_state = {'unavailable': True}
        except KeyError as e:
            logger.error(f"Could not determine final state for container {container.get('Id')}. ",
                         f"Exception: {e}; (worker {worker_id};{execution_id})")
            container_state = {'unavailable': True}
    except docker.errors.APIError as e:
        logger.error(f"Could not inspect container {container.get('Id')}. ",
                     f"Exception: {e}; (worker {worker_id};{execution_id})")
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
            logger.error(f"Got exception from server.recv: {e}; (worker {worker_id};{execution_id})")
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
                logger.info(f"Actor container removed. (worker {worker_id};{execution_id})")
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
        try:
            os.close(fifo)
            os.remove(fifo_host_path)
        except Exception as e:
            logger.debug(f"got Exception trying to clean up fifo_host_path; e: {e}")
    if results_ch:
        # check if the length of the results channel is empty and if so, delete it --
        if len(results_ch._queue._queue) == 0:
            try:
                results_ch.delete()
            except Exception as e:
                logger.warn(f"Got exception trying to delete the results_ch, swallowing it; Exception: {e}")
        else:
            try:
                results_ch.close()
            except Exception as e:
                logger.warn(f"Got exception trying to close the results_ch, swallowing it; Exception: {e}")
    result['runtime'] = int(stop - start)
    logger.debug("right after removing fifo; about to return: {}; (worker {};{})".format(timeit.default_timer(),
                                                                                         worker_id, execution_id))
    return result, logs, container_state, exit_code, start_time

def start_adapter_server(adapter_id,
                  server_id,
                  image,
                  user=None,
                  d={},
                  privileged=False,
                  mounts=[],
                  mem_limit=None,
                  max_cpus=None,
                  tenant=None):
    """
    Creates and runs an adapter container and supervises the execution, collecting statistics about resource consumption
    from the Docker daemon.

    :param adapter_id: the dbid of the adapter; for updating server status
    :param server_id: the worker id; also for updating server status
    :param image: the adapter's image; worker must have already downloaded this image to the local docker registry.
    :param user: string in the form {uid}:{gid} representing the uid and gid to run the command as.
    :param d: dictionary representing the environment to instantiate within the adapter container.
    :param privileged: whether this adapter is "privileged"; i.e., its container should run in privileged mode with the
    docker daemon mounted.
    :param mounts: list of dictionaries representing the mounts to add; each dictionary mount should have 3 keys:
    host_path, container_path and format (which should have value 'ro' or 'rw').
    :param mem_limit: The maximum amount of memory the adapter container can use; should be the same format as the --memory Docker flag.
    :param max_cpus: The maximum number of CPUs each adapter will have available to them. Does not guarantee these CPU resources; serves as upper bound.
    :return: server_ports
    """
    logger.debug(f"top of start_adapter_server(); adapter_id: {adapter_id}; tenant: {tenant}")

    # get any configs for this adapter
    adapter_configs = {}
    config_list = []
    # list of all aliases for the adapter
    alias_list = []
    # the adapter_id passed in is the dbid
    adapter_human_id = Adapter.get_display_id(tenant, adapter_id)

    for alias in alias_store[site()].items():
        logger.debug(f"checking alias: {alias}")
        if adapter_human_id == alias['adapter_id'] and tenant == alias['tenant']:
            alias_list.append(alias['alias'])
    logger.debug(f"alias_list: {alias_list}")
    # loop through configs to look for any that apply to this adapter
    for config in configs_store[site()].items():
        # first look for the adapter_id itself
        if adapter_human_id in config['adapters']:
            logger.debug(f"adapter_id matched; adding config {config}")
            config_list.append(config)
        else:
            logger.debug("adapter id did not match; checking aliases...")
            # if we didn't find the adapter_id, look for ay of its aliases
            for alias in alias_list:
                if alias in config['adapters']:
                    # as soon as we find it, append and get out (only want to add once)
                    logger.debug(f"alias {alias} matched; adding config: {config}")
                    config_list.append(config)
                    break
    logger.debug(f"got config_list: {config_list}")
    # for each config, need to check for secrets and decrypt ---
    for config in config_list:
        logger.debug('checking for secrets')
        try:
            if config['is_secret']:
                value = encrypt_utils.decrypt(config['value'])
                adapter_configs[config['name']] = value
            else:
                adapter_configs[config['name']] = config['value']

        except Exception as e:
            logger.error(f'something went wrong checking is_secret for config: {config}; e: {e}')

    logger.debug(f"final adapter configs: {adapter_configs}")
    d['_adapter_configs'] = adapter_configs


    # initially set the global force_quit variable to False
    globals.force_quit = False
    # initial stats object, environment and binds
    result = {'cpu': 0,
              'io': 0,
              'runtime': 0 }

    # instantiate docker client
    cli = docker.APIClient(base_url=dd, version="auto")

    binds = {}

    # if container is privileged, mount the docker daemon so that additional
    # containers can be started.
    logger.debug(f"privileged: {privileged}")
    if privileged:
        binds = {'/var/run/docker.sock':{
                    'bind': '/var/run/docker.sock',
                    'ro': False }}

    # add a bind key and dictionary as well as a volume for each mount
    for m in mounts:
        binds[m.get('host_path')] = {'bind': m.get('container_path'),
                                     'ro': m.get('format') == 'ro'}

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

    host_config = cli.create_host_config(binds=binds, privileged=privileged, mem_limit=mem_limit, nano_cpus=max_cpus, port_bindings={8080: None})
    logger.debug(f"host_config object created.")

    spn = conf.spawner_host_ip      #the ip address of the spawner
    # create and start the container
    logger.debug(f"Final container environment: {d}")
    logger.debug("Final binds: {} and host_config: {} for the container.".format(binds, host_config))
    cont = cli.create_container(image=image,
                                     environment=d,
                                     user=user,
                                     name=f'adapterserver_{adapter_id}_{server_id}',
                                     host_config=host_config)

    start = timeit.default_timer()
    logger.debug("right before cli.start: {}; container id: {}; ".format(start, container.get('Id')))
    try:
        container = cli.start(container=cont.get('Id'))
        idx = 0
        while idx<25:
            try:
                port=cli.inspect_container(container)['NetworkSettings']['Ports']['8080/tcp'][0]['HostPort']
                break
            except:
                idx = idx+1
    except Exception as e:
        # if there was an error starting the container, user will need to debug
        logger.info(f"Got exception starting adapter container: {e}")
        raise DockerStartContainerError(f"Could not start container {container.get('Id')}. Exception {str(e)}")

    AdapterServer.update_status(adapter_id, server_id, RUNNING)

    result = f'https://{spn}:{port}'
    result2 = conf.spawner_host_id


    return result, result2
