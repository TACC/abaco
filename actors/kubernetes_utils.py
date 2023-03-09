import json
import os
import socket
import time
import timeit
import datetime
import random

from kubernetes import client, config
from requests.packages.urllib3.exceptions import ReadTimeoutError
from requests.exceptions import ReadTimeout, ConnectionError

from tapisservice.logs import get_logger
logger = get_logger(__name__)

from channels import ExecutionResultsChannel
from tapisservice.config import conf
from codes import BUSY, READY, RUNNING, CREATING_CONTAINER
import globals
from models import Actor, Execution, get_current_utc_time, display_time, site, ActorConfig
from stores import workers_store, alias_store, configs_store
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

# k8 client creation
config.load_incluster_config()
k8 = client.CoreV1Api()

host_id = os.environ.get('SPAWNER_HOST_ID', conf.spawner_host_id)
logger.debug(f"host_id: {host_id}")
host_ip = conf.spawner_host_ip


class KubernetesError(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = message

class KubernetesStartContainerError(KubernetesError):
    pass

class KubernetesStopContainerError(KubernetesError):
    pass


def get_kubernetes_namespace():
    """
    Attempt to get namespace from filesystem
    Should be in file /var/run/secrets/kubernetes.io/serviceaccount/namespace
    
    We first take config, if not available, we grab from filesystem. Meaning
    config should usually be empty.
    """
    namespace = conf.get("kubernetes_namespace", None)
    if not namespace:
        try:
            logger.debug("Attempting to get kubernetes_namespace from file.")
            with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace") as f:
                content = f.readlines()
                namespace = content[0].strip()
        except Exception as e:
            logger.debug(f"Couldn't grab kubernetes namespace from filesystem. e: {e}")
        
    if not namespace:
        msg = "In get_kubernetes_namespace(). Failed to get namespace."
        logger.debug(msg)
        raise KubernetesError(msg)
    logger.debug(f"In get_kubernetes_namespace(). Got namespace: {namespace}.")
    return namespace

# Get k8 namespace for future use.
NAMESPACE = get_kubernetes_namespace()


def rm_container(cid):
    """
    Remove a container.
    :param cid:
    :return:
    """    
    try:
        k8.delete_namespaced_pod(name=cid, namespace=NAMESPACE)
    except Exception as e:
        logger.info(f"Got exception trying to remove pod: {cid}. Exception: {e}")
        raise KubernetesError(f"Error removing pod {cid}, exception: {str(e)}")
    logger.info(f"pod {cid} removed.")


def list_all_containers():
    """Returns a list of all containers in a particular namespace """
    pods = k8.list_namespaced_pod(NAMESPACE).items
    return pods


def get_current_worker_containers():
    """Get all containers, filter for just workers, and display in style docker_utils uses."""
    worker_pods = []
    for pod in list_all_containers():
        pod_name = pod.metadata.name
        if 'actors-worker' in pod_name:
            # worker name format = "actors-worker-<tenant>-<actor-id>-<worker-id>
            # so split on - to get parts (containers use _, pods use -)
            try:
                parts = pod_name.split('-')
                tenant_id = parts[2]
                actor_id = parts[3]
                worker_id = parts[4]
                worker_pods.append({'pod': pod,
                                    'tenant_id': tenant_id,
                                    'actor_id': actor_id,
                                    'worker_id': worker_id})
            except:
                pass
    return worker_pods


def check_worker_pods_against_store():
    """
    Checks the existing worker pods on a host against the status of the worker in the workers_store.
    """
    worker_pods = get_current_worker_containers()
    for idx, w in enumerate(worker_pods):
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
    """
    Check if there is a running pods whose name contains the string, `name`. Note that this function will
    return True if any running container has a name which contains the input `name`.
    """
    logger.debug("top of kubernetes_utils.container_running().")
    if not name:
        raise KeyError(f"kubernetes_utils.container_running received name: {name}")
    name = name.replace('_', '-').lower()
    try:
        if k8.read_namespaced_pod(namespace=NAMESPACE, name=name).status.phase == 'Running':
            return True
    except client.rest.ApiException:
        # pod not found
        return False
    except Exception as e:
        msg = f"There was an error checking kubernetes_utils.container_running for name: {name}. Exception: {e}"
        logger.error(msg)
        raise KubernetesError(msg)
    

def run_container(image,
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
    Run a container with access to Kubernetes cluster controls.
    Note: this function always mounts the abaco conf file so it should not be used by execute_actor().
    Note: this function gives express permissions to the pod to mess with kube, only used for workers.
    """
    logger.debug("top of kubernetes_utils.run_container().")

    # This should exist if config.json is using environment variables.
    abaco_host_path = os.environ.get('abaco_host_path')
    logger.debug(f"kubernetes_utils using abaco_host_path={abaco_host_path}")

    ## environment variables
    if 'abaco_host_path' not in environment:
        environment['abaco_host_path'] = abaco_host_path
    if 'actor_id' not in environment:
        environment['actor_id'] = actor_id
    if 'tenant' not in environment:
        environment['tenant'] = tenant
    if 'api_server' not in environment:
        environment['api_server'] = api_server

    # NAME NOTE Worker name can be 123-abc, ., and -. No underscores! (lowercase only)
    name = name.replace('_', '-').lower()

    # Get mounts ready for k8 spec
    if mounts:
        volumes, volume_mounts = mounts
    else:
        volumes = []
        volume_mounts = []
    logger.debug(f"volumes: {volumes}")
    logger.debug(f"volume_mounts: {volume_mounts}")

    # Create and start the pod
    try:
        # This is just a thing that's needed
        metadata = client.V1ObjectMeta(name=name)

        # Environment variable declaration
        env = []
        for env_name, env_val in environment.items():
            env.append(client.V1EnvVar(name=env_name, value=str(env_val)))
        env.append(client.V1EnvVar(name='abaco_host_path', value=os.environ.get('abaco_host_path')))
        env.append(client.V1EnvVar(name='_abaco_secret', value=os.environ.get('_abaco_secret')))

        # Password environment variable declaration
        mongo_pass_source = client.V1EnvVarSource(secret_key_ref = client.V1SecretKeySelector(name="tapis-abaco-secrets", key='mongo-password'))
        env.append(client.V1EnvVar(name="MONGO_PASSWORD", value_from=mongo_pass_source))
        rabbit_pass_source = client.V1EnvVarSource(secret_key_ref = client.V1SecretKeySelector(name="tapis-abaco-secrets", key='rabbitmq-password'))
        env.append(client.V1EnvVar(name="RABBITMQ_PASSWORD", value_from=rabbit_pass_source))
        service_pass_source = client.V1EnvVarSource(secret_key_ref = client.V1SecretKeySelector(name="tapis-abaco-secrets", key='service-password'))
        env.append(client.V1EnvVar(name="SERVICE_PASSWORD", value_from=service_pass_source))
        logger.debug(f"pod env variables: {env}")
        
        # Create container and pod
        containers = [client.V1Container(name=name, command=command, image=image, volume_mounts=volume_mounts, env=env)]
        pod_spec = client.V1PodSpec(service_account_name="actors-serviceaccount", restart_policy="Never", containers=containers, volumes=volumes)
        pod_body = client.V1Pod(metadata=metadata, spec=pod_spec, kind="Pod", api_version='v1')
        pod = k8.create_namespaced_pod(namespace=NAMESPACE, body=pod_body)
    except Exception as e:
        msg = f"Got exception trying to start pod with image: {image}. Exception: {e}"
        logger.info(msg)
        raise KubernetesError(msg)
    logger.info(f"pod started successfully")
    return pod


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
    logger.debug("top of kubernetes_utils.run_worker()")
    command = ["python3", "-u", "/home/tapis/actors/kubernetes_worker.py"]
    logger.debug(f"kubernetes_utils running worker. actor_id: {actor_id}; worker_id: {worker_id};"
                 f"image:{image}; revision: {revision}; command: {command}")

    ## Mounts - require creating a volume and then mounting it. FYI.
    # mount_example (volume type)
    # abaco config (configMap)
    # fifo and sockets (nfs mount to pod in cluster - requires ReadWriteMany)
    # /work (???)
    # logging file (Deployment prints to stdout. Ignoring for now.)
    volumes = []
    volume_mounts = []

    # This is basically run_worker. So we'll leave this here.
    # Get the actors-nfs cluster IP address so you can mount it on workers.
    nfs_cluster_ip = k8.read_namespaced_service(namespace='default', name='actors-nfs').spec.cluster_ip

    ## fifo mount - Read by all pods. Created by worker. Written to by spawner. Read by worker.
    nfs = client.V1NFSVolumeSource(server=nfs_cluster_ip, path="/_abaco_fifos")
    volumes.append(client.V1Volume(name='actors-nfs-fifos', nfs = nfs))
    volume_mounts.append(client.V1VolumeMount(name="actors-nfs-fifos", mount_path="/home/tapis/runtime_files/_abaco_fifos", read_only=False))

    ## socket mount - Read by all pods. Created by worker. Written to by spawner. Read by worker.
    nfs = client.V1NFSVolumeSource(server=nfs_cluster_ip, path="/_abaco_results_sockets")
    volumes.append(client.V1Volume(name='actors-nfs-sockets', nfs = nfs))
    volume_mounts.append(client.V1VolumeMount(name="actors-nfs-sockets", mount_path="/home/tapis/runtime_files/_abaco_results_sockets", read_only=False))

    ## actors-config mount - Have to mount the config
    config_map = client.V1ConfigMapVolumeSource(name='actors-config')
    volumes.append(client.V1Volume(name='actors-config', config_map = config_map))
    volume_mounts.append(client.V1VolumeMount(name="actors-config", mount_path="/home/tapis/config.json", sub_path='config.json', read_only=True))

    #### Should look into if k8 has any auto_remove akin thing to delete pods or jobs.
    #### ttlSecondsAfterFinished does exist for jobs.
    auto_remove = conf.worker_auto_remove
    pod = run_container(
        image=AE_IMAGE,
        command=command,
        environment={
            'image': image,
            'revision': revision,
            'worker_id': worker_id,
            'abaco_host_path': os.environ.get('abaco_host_path'),
            '_abaco_secret': os.environ.get('_abaco_secret')},
        mounts=[volumes, volume_mounts],
        log_file=None,
        auto_remove=auto_remove,
        name=f'actors_worker_{actor_id}_{worker_id}',
        actor_id=actor_id,
        tenant=tenant,
        api_server=api_server
    )
    # don't catch errors -- if we get an error trying to run a worker, let it bubble up.
    # TODO - determines worker structure; should be placed in a proper DAO class.
    logger.info(f"worker pod running. worker_id: {worker_id}.")
    return { 'image': image,
             # @todo - location will need to change to support swarm or cluster
             'location': "kubernetes",
             'id': worker_id,
             'cid': worker_id, #Pods don't have an id field
             'status': READY,
             'host_id': host_id,
             'host_ip': host_ip,
             'last_execution_time': 0,
             'last_health_check_time': get_current_utc_time() }

def stop_container(name):
    """
    Attempt to stop a running pod, with retry logic. Should only be called with a running pod.
    :param name: the pod name of the pod to be stopped.
    :return:
    """
    if not name:
        raise KeyError(f"kubernetes_utils.container_running received name: {name}")
    name = name.replace('_', '-').lower()

    i = 0
    while i < 10:        
        try:
            k8.delete_namespaced_pod(namespace=NAMESPACE, name=name)
            return True
        except client.rest.ApiException:
            # pod not found
            return False
        except Exception as e:
            logger.error(f"Got another exception trying to stop the actor container. Exception: {e}")
            i += 1
            continue
    raise KubernetesStopContainerError


def execute_actor(actor_id,
                  worker_id,
                  execution_id,
                  image,
                  msg,
                  user=None,
                  environment={},
                  privileged=False,
                  mounts=[],
                  leave_container=False,
                  fifo_host_path=None,
                  socket_host_path=None,
                  mem_limit=None,
                  max_cpus=None,
                  tenant=None):
    """
    Creates and runs an actor pod and supervises the execution, collecting statistics about resource consumption
    with kubernetes utils.

    :param actor_id: the dbid of the actor; for updating worker status
    :param worker_id: the worker id; also for updating worker status
    :param execution_id: the id of the execution.
    :param image: the actor's image;
    :param msg: the message being passed to the actor.
    :param user: string in the form {uid}:{gid} representing the uid and gid to run the command as.
    :param environment: dictionary representing the environment to instantiate within the actor container.
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
    logger.debug(f"top of kubernetes_utils.execute_actor(); actor_id: {actor_id}; tenant: {tenant} (worker {worker_id}; {execution_id})")

    name=f'actors_exec_{actor_id}_{execution_id}'.replace('_', '-').lower()


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
                if not conf.get("web_apim_public_key"):
                    mes = ("Got exception using actor config object. Config was set to is_secret. But conf.web_apim_public_key is not set",
                           "Please let a system admin know. More than likely the conf was set, but is now not set.")
                    logger.info(mes)
                    raise Exception(mes)
                value = encrypt_utils.decrypt(config['value'])
                actor_configs[config['name']] = value
            else:
                actor_configs[config['name']] = config['value']
        except Exception as e:
            logger.error(f'something went wrong checking is_secret for config: {config}; e: {e}')

    logger.debug(f"final actor configs: {actor_configs}")
    environment['_actor_configs'] = actor_configs
    
    # We set all of these to overwrite default kubernetes env vars. There's two sets of default vars.
    # These, the required, and 
    environment['KUBERNETES_PORT'] = ""
    environment['KUBERNETES_SERVICE_HOST'] = ""
    environment['KUBERNETES_SERVICE_PORT'] = ""
    environment['KUBERNETES_SERVICE_PORT_HTTPS'] = ""
    environment['KUBERNETES_PORT_443_TCP'] = ""
    environment['KUBERNETES_PORT_443_TCP_ADDR'] = ""
    environment['KUBERNETES_PORT_443_TCP_PORT'] = ""
    environment['KUBERNETES_PORT_443_TCP_PROTO'] = ""

    # initially set the global force_quit variable to False
    globals.force_quit = False

    # initial stats object, environment and vols
    result = {'cpu': 0,
              'io': 0,
              'runtime': 0 }

    # TODO: From docker_utils.py - If privileged, then mount docker daemon so container can create
    # more containers. We don't do that here, because kubernetes.
    # if privileged:
    # ...


    # Get mounts ready for k8 spec
    if mounts:
        volumes, volume_mounts = mounts
    else:
        volumes = []
        volume_mounts = []
    logger.debug(f"Final volumes: {volumes};(worker {worker_id}; {execution_id})")
    logger.debug(f"Final volume_mounts: {volume_mounts};(worker {worker_id}; {execution_id})")

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

    ## FIFOs
    # don't try to pass binary messages through the environment as these can cause
    # broken pipe errors. the binary data will be passed through the FIFO momentarily.
    if not fifo_host_path:
        environment['MSG'] = msg

    # write binary data to FIFO if it exists:
    fifo = None
    if fifo_host_path:
        # fifo_host_path is just /_abaco_fifos/{worker_id}/{execution_id}. Need to add fifo itself.
        fifo_host_path = f"{fifo_host_path}/_abaco_binary_data" 

        try:
            fifo = os.open(fifo_host_path, os.O_RDWR)
            os.write(fifo, msg)
        except Exception as e:
            logger.error(f"Error writing the FIFO. Exception: {e};(worker {worker_id}; {execution_id})")
            os.remove(fifo_host_path)
            raise KubernetesStartContainerError(f"Error writing to fifo: {e}; (worker {worker_id}; {execution_id})")

    ## Sockets
    # socket_host_path is just /_abaco_results_sockets/{worker_id}/{execution_id}. Need to add filename.sock
    socket_host_path = f"{socket_host_path}/_abaco_results.sock" 
    # set up results socket -----------------------
    # make sure socket doesn't already exist:
    try:
        os.unlink(socket_host_path)
    except OSError as e:
        if os.path.exists(socket_host_path):
            logger.error(f"socket at {socket_host_path} already exists; Exception: ",
                         f"{e}; (worker {worker_id}; {execution_id})")
            raise KubernetesStartContainerError(f"Got an OSError trying to create the results socket; exception: {e}")

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
            logger.info(f"Could not instantiate socket at {socket_host_path}. Count: {count}; Will keep trying. "
                        f"Exception: {e}; type: {type(e)}; (worker {worker_id};{execution_id})")
        try:
            server.bind(socket_host_path)
        except Exception as e:
            keep_trying = True
            logger.info(f"Could not bind socket at {socket_host_path}. Count: {count}; Will keep trying. "
                        f"Exception: {e}; type: {type(e)}; (worker {worker_id};{execution_id})")
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
            logger.info(f"Could not set timeout for socket at {socket_host_path}. Count: {count}; Will keep trying. "
                        f"Exception: {e}; type: {type(e)}; (worker {worker_id};{execution_id})")
    if not server:
        msg = "Failed to instantiate results socket. " \
              "Abaco compute host could be overloaded. (worker {};{})".format(worker_id, execution_id)
        logger.error(msg)
        raise DockerStartContainerError(msg)

    logger.debug(f"results socket server instantiated. path: {socket_host_path} (worker {worker_id};{execution_id})")

    # instantiate the results channel:
    results_ch = ExecutionResultsChannel(actor_id, execution_id)

    # use retry logic since, when the compute node is under load, we see errors initially trying to create the socket
    # server object.
    keep_trying = True
    count = 0
    server = None

    # instantiate the results channel:
    results_ch = ExecutionResultsChannel(actor_id, execution_id)

    # create and start the container
    logger.debug(f"Final container environment: {environment};(worker {worker_id}; {execution_id})")

    # Just something we need for kubernetes pods
    metadata = client.V1ObjectMeta(name=name)
    
    # Environment variable declaration
    env = []
    for env_name, env_val in environment.items():
        env.append(client.V1EnvVar(name=env_name, value=str(env_val)))
    logger.debug(f"exec pod env variables: {env}")
    
    ## Mem limit and max cpus - K8 resources
    resource_limits = {}
    # Memory
    # docker uses b/or no suffix, k/kb, m/mb, g/gb for units (technically also takes Ki, Mi, Gi fyi)
    # k8 uses no suffix (for bytes), Ki, Mi, Gi, Ti, Pi, or Ei (Does not accept kb, mb, or gb at all)
    # k/kb/ki->Ki, m/mb/mi->Mi, g/gb/gi->Gi
    if mem_limit:
        units = {'Ki': ['ki', 'kb', 'k'],
                    'Mi': ['mi', 'mb', 'm'],
                    'Gi': ['gi', 'gb', 'g']}
        converted = False
        for new_unit, old_units in units.items():
            for old_unit in old_units:
                if old_unit in str(mem_limit):
                    mem_limit = mem_limit.replace(old_unit, new_unit)
                    resource_limits["memory"] =  mem_limit
                    converted = True
                    break
            if converted:
                break
    ## CPU
    # max_cpu should be a int representing nanocpus. We can use the k8 'n' suffix for nanocpus.
    if max_cpus:
        max_cpus = f"{max_cpus}n"
        resource_limits["cpu"] = max_cpus

    ## Create resource and container
    if resource_limits:
        resources = client.V1ResourceRequirements(limits = resource_limits)
        container = client.V1Container(name=name, image=image, volume_mounts=volume_mounts, env=env, resources=resources)
    else:
        container = client.V1Container(name=name, image=image, volume_mounts=volume_mounts, env=env)

    ## Security Context
    uid = None
    gid = None
    if user:
        try:
            # user should be None or 223232:323232
            uid, gid = user.split(":")
        except Exception as e:
            # error starting the pod, user will need to debug
            msg = f"Got exception getting user uid/gid: {e}; (worker {worker_id}; {execution_id})"
            logger.info(msg)
            raise KubernetesStartContainerError(msg)

    # Add in security context if uid and gid are found
    if uid and gid:
        security = client.V1SecurityContext(run_as_user=uid, run_as_group=gid)
        pod_spec = client.V1PodSpec(security_context=security, restart_policy="Never", containers=[container], enable_service_links=False)
    else:
        pod_spec = client.V1PodSpec(restart_policy="Never", containers=[container], volumes=volumes, enable_service_links=False)
        
    try:
        # Start pod
        start_time = get_current_utc_time()
        # start the timer to track total execution time.
        start = timeit.default_timer()
        logger.debug(f"right before k8.create_namespaced_pod: {start}; pod name: {name}; (worker {worker_id}; {execution_id})")
        pod_body = client.V1Pod(metadata=metadata, spec=pod_spec, kind="Pod", api_version='v1')
        pod = k8.create_namespaced_pod(namespace=NAMESPACE, body=pod_body)
    except Exception as e:
        # if there was an error starting the pod, user will need to debug
        msg = f"Got exception starting actor exec pod: {e}; (worker {worker_id}; {execution_id})"
        logger.info(msg)
        raise KubernetesStartContainerError(msg)
    logger.info(f"pod started successfully")

    # Wait for pod to start running. Unique to kube as docker container start command is blocking. Not here.
    # To check for availablity we first check if pod is in Succeeded phase (possible with fast execs). If not we can't trust
    # pod.phase of "Running" as that means nothing. Need to look at container status. Container status is outputted as
    # state: {"running": None, "terminated": None, "waiting": None}
    # None is an object containing container_id, exit_code, start/finish time, message (error if applicable), reason, and signal.
    # Reason can be "CrashLoopBackOff" in waiting, "Completed" in terminated (should mean Succeeded pod phase),
    # "ContainerCreating" in waiting, etc. Only one state object is ever specified, we need to error in the case of errors,
    # and mark running if running.
    
    # local bool tracking whether the actor pod is still running
    container_creating = False
    running = False
    loop_idx = 0
    while True:
        loop_idx += 1
        pod = k8.read_namespaced_pod(namespace=NAMESPACE, name=name)
        pod_phase = pod.status.phase
        if pod_phase == "Succeeded":
            logger.debug(f"Kube exec in succeeded phase. (worker {worker_id}; {execution_id})")
            running = True
            break
        elif pod_phase in ["Running", "Pending"]:
            # Check if container running or in error state (exec pods are always only one container (so far))
            # Container can be in waiting state due to ContainerCreating ofc
            # We try to get c_state. container_status when pending is None for a bit.
            try:
                c_state = pod.status.container_statuses[0].state
            except:
                c_state = None
            logger.debug(f'state: {c_state}')
            if c_state:
                if c_state.waiting and c_state.waiting.reason != "ContainerCreating":
                    msg = f"Found kube container waiting with reason: {c_state.waiting.reason} (worker {worker_id}; {execution_id})"
                    logger.error(msg)
                    raise KubernetesStartContainerError(msg)
                elif c_state.waiting and c_state.waiting.reason == "ContainerCreating":
                    if not container_creating:
                        container_creating = True
                        Execution.update_status(actor_id, execution_id, CREATING_CONTAINER)
                elif c_state.running:
                    running = True
                    break
        
        # TODO: Add some more checks here, check for kube container error statuses.
        if loop_idx % 60:
            logger.debug(f"Waiting for kube exec to get to running. {loop_idx} sec. (worker {worker_id}; {execution_id})")
        if loop_idx == 300:
            msg = f"Kube exec not ready after 5 minutes. shutting it down. (worker {worker_id}; {execution_id})"
            logger.warning(msg)
            raise KubernetesStartContainerError(msg)
        time.sleep(1)
            
    if running:
        Execution.update_status(actor_id, execution_id, RUNNING)

    # Stats loop waiting for execution to end
    # a counter of the number of iterations through the main "running" loop;
    # this counter is used to determine when less frequent actions, such as log aggregation, need to run.
    loop_idx = 0
    log_ex = Actor.get_actor_log_ttl(actor_id)
    logs = None
    while running and not globals.force_quit:
        loop_idx += 1
        logger.debug(f"top of kubernetes_utils while running loop; loop_idx: {loop_idx}")

        # grab the logs every 3rd iteration --
        if loop_idx % 3 == 0:
            logs = None
            logs = k8.read_namespaced_pod_log(namespace=NAMESPACE, name=name)
            Execution.set_logs(execution_id, logs, actor_id, tenant, worker_id, log_ex)

        ## Check pod to see if we're still running
        logger.debug(f"about to check pod status: {timeit.default_timer()}; (worker {worker_id}; {execution_id})")
        # Waiting for pod availability
        i = 0
        pod = None
        while i < 10:
            try:
                pod = k8.read_namespaced_pod(namespace=NAMESPACE, name=name)                    
                break # pod was found
            except client.rest.ApiException: # pod not found                    
                logger.error(f"Got an IndexError trying to get the pod object. (worker {worker_id}; {execution_id})")
                time.sleep(0.1)
                i += 1
        logger.debug(f"done checking status: {timeit.default_timer()}; i: {i}; (worker {worker_id}; {execution_id})")
        if not pod: # Couldn't find pod
            logger.error(f"Couldn't retrieve pod! Stopping pod; name: {name}; (worker {worker_id}; {execution_id})")
            stop_container(name)
            logger.info(f"pod {name} stopped. (worker {worker_id}; {execution_id})")
            running = False
            continue

        # Get pod state
        try:
            state = pod.status.phase
        except:
            state = "broken"
            logger.error(f"KUBE BUG:couldn't get status.phase. pod: {pod}")
        if state != 'Running': # If we're already in Running, Success is the only option.
            logger.debug(f"pod finished, final state: {state}; (worker {worker_id}; {execution_id})")
            running = False
            continue
        else:
            # pod still running; check for force_quit OR max_run_time
            runtime = timeit.default_timer() - start
            if globals.force_quit:
                logger.warning(f"issuing force quit: {timeit.default_timer()}; (worker {worker_id}; {execution_id})")
                stop_container
                running = False
            if max_run_time > 0 and max_run_time < runtime:
                logger.warning(f"hit runtime limit: {timeit.default_timer()}; (worker {worker_id}; {execution_id})")
                stop_container
                running = False
        logger.debug(f"right after checking pod state: {timeit.default_timer()}; (worker {worker_id}; {execution_id})")

    logger.info(f"pod stopped:{timeit.default_timer()}; (worker {worker_id}; {execution_id})")
    stop = timeit.default_timer()
    globals.force_quit = False

    # get info from pod execution, including exit code; Exceptions from any of these commands
    # should not cause the worker to shutdown or prevent starting subsequent actor pods.
    logger.debug("Pod finished")
    exit_code = 'undetermined'
    try:
        pod = k8.read_namespaced_pod(namespace=NAMESPACE, name=name)
        try:
            c_state = pod.status.container_statuses[0].state # to be used to set final_state
            # Sets final state equal to whichever c_state object exists (only 1 exists at a time ever)
            pod_state = c_state.running or c_state.terminated or c_state.waiting
            pod_state = pod_state.to_dict()
            try:
                exit_code = pod_state.get('exit_code', 'No Exit Code')
                startedat_ISO = pod_state.get('started_at', 'No "started at" time (k8 - request feature on github)')
                finishedat_ISO = pod_state.get('finished_at' 'No "finished at" time (k8 - request feature on github)')
                # if times exist, converting ISO8601 times to unix timestamps
                if not 'github' in startedat_ISO:
                    # Slicing to 23 to account for accuracy up to milliseconds and replace to get rid of ISO 8601 'Z'
                    startedat_ISO = startedat_ISO.replace('Z', '')[:23]
                    pod_state['StartedAt'] = datetime.datetime.strptime(startedat_ISO, "%Y-%m-%dT%H:%M:%S.%f")

                if not 'github' in finishedat_ISO:
                    finishedat_ISO = pod.finishedat_ISO.replace('Z', '')[:23]
                    pod_state['FinishedAt'] = datetime.datetime.strptime(finishedat_ISO, "%Y-%m-%dT%H:%M:%S.%f")
            except Exception as e:
                logger.error(f"Datetime conversion failed for pod {name}. "
                             f"Exception: {e}; (worker {worker_id}; {execution_id})")
        except Exception as e:
            logger.error(f"Could not determine final state for pod {name}. "
                         f"Exception: {e}; (worker {worker_id}; {execution_id})")
            pod_state = {'unavailable': True}
    except client.rest.ApiException:
        logger.error(f"Could not get pod info for name: {name}. "
                     f"Exception: {e}; (worker {worker_id}; {execution_id})")

    logger.debug(f"right after getting pod object: {timeit.default_timer()}; (worker {worker_id}; {execution_id})")
    # get logs from pod
    try:
        if not logs:
            logs = k8.read_namespaced_pod_log(namespace=NAMESPACE, name=name)
        if not logs:
            # there are issues where container do not have logs associated with them when they should.
            logger.info(f"Pod: {name} had NO logs associated with it. (worker {worker_id}; {execution_id})")
    except Exception as e:
        logger.error(f"Unable to get logs for exec. error: {e}. (worker {worker_id}; {execution_id})")
    logger.debug(f"right after getting pod logs: {timeit.default_timer()}; (worker {worker_id}; {execution_id})")

    # remove actor container with retrying logic -- check for specific filesystem errors from kube
    if not leave_container:
        logger.debug("deleting container")
        keep_trying = True
        count = 0
        while keep_trying and count < 10:
            keep_trying = False
            count = count + 1
            try:
                stop_container(name)
                logger.info(f"Actor pod removed. (worker {worker_id}; {execution_id})")
            except Exception as e:
                # if the pod is already gone we definitely want to quit:
                if "Reason: Not Found" in str(e):
                    logger.info("Got 'Not Found' exception - quiting. "
                                f"Exception: {e}; (worker {worker_id}; {execution_id})")
                    break
                else:
                    logger.error("Unexpected exception trying to remove actor pod. Giving up."
                                 f"Exception: {e}; type: {type(e)}; (worker {worker_id}; {execution_id})")
    else:
        logger.debug(f"leaving actor pod since leave_container was True. (worker {worker_id}; {execution_id})")
    logger.debug(f"right after removing actor container: {timeit.default_timer()}; (worker {worker_id}; {execution_id})")
    result['runtime'] = int(stop - start)
    return result, logs, pod_state, exit_code, start_time
