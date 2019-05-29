
Abaco - Developer Documentation
-------------------------------

This section goes into more technical details about the configuration and implementation of Abaco.


Configurations
--------------
The Abaco system has several configuration options. Configurations are placed in the abaco.conf file and
mounted into the initial containers for the web applications, spawners, health supervisors, and client generators.
For containers started dynamically, such as the worker containers, the conf file must be present on the host
and the path to the file must be known by the agents starting the containers (such as the spawners in the case of
workers). Abaco agents look for the environment variable "abaco_conf_host_path" to know where to mount the conf
file from on the host. In the local development docker-compose file, we set the abaco_conf_host_path based on the
variable "abaco_path", which should be a directory containing a config file (for example, the github project
root). For example, to get started locally, once can put `export abaco_path=$(pwd)` from within the project
root and the compose file will use the local-dev.conf file there (see the Abaco README.md in the project root).

The abaco.conf file in the project root should be self documenting and contain all possible options. Here we want
to highlight some of the bigger, more important config options

1) Logging: Abaco has two kinds of logging configuration. First is the log strategy to use, which can be either 'combined' so that all logs go into one file or 'split' so that each process logs to a separate file. Second, logging can be configured by level ('CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'). Log level can be set for the entire Abaco instance all at once (e.g. level = WARN) or for specific modules (e.g level.spawner = DEBUG).

2) Whether to generate clients: The Abaco system can be configured to dynamically generate an OAuth client as well as
a set of access tokens whenever it starts a worker. In this situation, the worker will maintain a valid access token
(using a refresh token, as needed) representing the owner of the actor, and inject that access token into the actor
container. (Note that the actor currently has no way of refreshing the token herself, so such executions should be
restricted to the life of the token, typically 4 hours). It is possible to turn off client generation by setting
`generate_clients: False` within the workers stanza of the config file. See "Client Generation" below.



Development
-----------

To develop the core Abaco services, find or create an issue on github to work in. Checkout the project from github and
create a local feature branch from the development branch with name I-xyz where xyz is the number
of the issue on github.

All core modules live in the actors package. The modules ending in _api.py define Flask applications
 and associated resources. The resource implementations are all defined in the controllers.py module. The models.py
 module defines data access classes and methods for interacting with the persistence layer.

The store.py module defines high-level interfaces for interacting with databases, and stores.py provides specific
definitions of Abaco stores used in the models.py module.

The backend processes (e.g. spawners, workers, health checks and clientg) are defined in their own modules with a
 corresponding main method used for start up.

Channels used for communication between Abaco processes are implemented in the channels.py module.

Once code changes are in place, build a new abaco_core image (all core Abaco processes run out of the same
core image), relaunch the Abaco container stack, and run the tests. Add new tests as appropriate.

Building the Images, Launching the Stack, and Running the Tests (Linux)
-----------------------------------------------------------------------
Note: These instructions work for Linux. OS X and Windows will require different approaches.

**Building the Images**

All core Abaco processes run out of the same abaco/core image. It should be build using the Dockerfile at the root
of the repository and tagged with a tag equal to the branch name. Export a variable called $TAG with the tag name
preceded by a colon (:) character in it.

For example,

```shell
$ export TAG=:I-12
$ docker build -t abaco/core$TAG .
```
Auxiliary images can be built from the Dockerfiles within the images folder. In particular, the abaco/nginx image required
to start up the stack can built from within the images/nginx directory. For example

```shell
$ cd images/nginx
$ docker build -t abaco/nginx$TAG .
```

**Starting the Development Stack**

Once the images are built, start the development stack by first changing into the project root and
exporting the abaco_path and TAG variables. You also need to create an abaco.log file in the root:

```shell
$ export abaco_path=$(pwd)
$ export TAG=:I-12
$ touch abaco.log
```
The abaco_path variable needs to contain the path to a directory with an abaco.conf file. It is used by Abaco containers to
know where to find the config file. Note that the abaco.conf contains the default IP address of the Docker0 gateway for
Mongo and Redis. If your Docker0 gateway IP is different, you will need to updte the abaco.conf file.

Several optional aspects of the Abaco development stack require additional configuration. By default, the databases
(Redis, Mongo and RabbitMQ) do not use authentication. To test with authentication, uncomment the relevant stanzas from
the docker-comppose-local-db.yml file and export the following variables:
```shell
$ export mongo_password=password
$ export redi_password=password
```

In order to run containers with UIDs generated from TAS, put
```shell
use_tas_uid: True
```
in the local-dev.conf file and export the following:
```shell
$ export TAS_ROLE_ACCT=the_account
$ export TAS_ROLE_PASS=the_password
```

Finally, start the Abaco containers with the following command:

```shell
$ docker-compose up -d
```
Note that this will command will still run the default (production) versions of the auxiliary images, such as the
databases.


**Running the Tests**
There are two kinds of tests in the Abaco test suite: unit tests and functional tests. Unit tests target a specific
module while functional tests target the entire stack. The tests leverage py.test framework and each test module starts
with "test_" followed by either the name of a module or a description of the functions being tested. For example,
test_store.py contains unit tests for the store.py module while test_abaco_core.py contains functional tests for
the Abaco core functionality.

The tests are packaged into their own Docker image for convenience. To run the tests, first build the tests image:

```shell
$ docker build -f Dockerfile-test -t abaco/testsuite$TAG .
```

To run the functional tests, execute the following:

```shell
$ docker run -e base_url=http://172.17.0.1:8000 -e case=camel -v /:/host -v $(pwd)/local-dev.conf:/etc/service.conf -it --rm abaco/testsuite$TAG
```

Run the unit tests with a command similar to the following, changing the test module as the end as necessary:

```shell
$ docker run -e base_url=http://172.17.0.1:8000 -v $(pwd)/local-dev.conf:/etc/service.conf --entrypoint=py.test -it --rm abaco/testsuite$TAG /tests/test_store.py
```

Dev, Staging and Master Branches and Environments
-------------------------------------------------
The dev, staging and master branches are long-lived branches in the Abaco repository corresponding to the three
permanent environments: Dev, Staging and Production, respectively. Feature branches should be merged into dev
once tests are passing. Pushing dev to origin will then trigger an automated CI pipeline job in Jenkins which
will run tests and automatically deploy to the Dev environment. Deployment is done via Ansible scripts contained
in the ansible directory.


Actors and Communication via Channels
-------------------------------------

Abaco itself is largely architected using an Actor model. Actors communicate to each other by passing messages over
a "channel". Channels are first class objects that enable communication across Python threads, processes, or hosts, and
the Channel objects themselves can be passed through the channels. This approach enables both pub-sub as well as
direct request-reply patterns. We use the channelpy library (https://github.com/TACC/channelpy) as the basis for our
channels.

The channels in use in the system are defined in the channels.py module (within the actors package).


Database Considerations
-----------------------

We divide up the state persisted in the Abaco system into "stores" defined in the stores.py module. Each of
these stores represent an independent collection of data (though there are relations) and thus can be
stored in different databases.

We currently use both Redis and MongoDB for different stores. We use Redis for current application state
(e.g. actors_store and workers_store) where transaction semantics are needed and the size of the
dataset will be relatively small. We use Mongo for the permissions, accounting and logging data (permissions_store,
executions_store and logs_store). Naturally, the log data (actual raw logs from the container executions) presents a
challenge in space, and was one of the primary reasons for adding support for Mongo. We already use a set_with_expiry
method to store the logs for a fixed period of time.


Spawners Starting Workers and Client Generation
-----------------------------------------------

As mentioned in the architecture document, Abaco uses Spawners and Workers to facilitate execution of the actor Docker
containers. These processes communicate via channels.

Additionally, the Abaco system uses a Clientg process for managing OAuth clients. Each worker gets a new
OAuth client when it is started, and its client is deleted whenever the worker is destroyed. This way, the actor
containers can be started with a fresh OAuth access token. Currently, the clients.py modules leverages the agave.py
module, a Python 3-compatible Agave SDK for managing clients.

The following algorithm is used to start workers with client generation happening when configured accordingly:

1. Spawner receives a message on the Command channel to start one or more new workers.
2. Spawner starts the worker containers using the configured docker daemon (for now, the local unix socket). It passes the image to use and waits for a message indicating the workers were able to pull the image and start up successfully.
3. If the actor already had workers and Spawner was instructed to stop the existing workers, it sends messages to them at this point to shut down.
4. Spawner checks the `generate_clients` config within the `workers` stanza to see if client generation is enabled.
5. Spawner sends a message to the clientg agent via the `ClientsChannel` requesting a new client.
6. Clientg responds to Spawner with a message containing the client key and secret, access token, and refresh token if client generation was successful.
7. Once all workers have responded healthy and all clients have been generated, Spawner sends a final message to each worker instructing them to subscribe to the actor mailbox channel. This message will contain the client credentials and tokens if those were generated.

Dependence on Docker Version
----------------------------

Various functions within Abaco are implemented by making calls to the docker daemon API using the docker-py client.
These calls are implemented in the docker_utils.py module. Different versions of the docker-py client library as
well as the Docker daemon itself present different APIs, and this can impact performance and correctness within
Abaco. As a result, strict attention to Docker and docker-py versions is necessary.

Developing the Dashboard
------------------------

The Abaco Dashboard is a web application geared towards administrative users. It is currently based on Django
and Bootstrap and the source code lives in the `dashboard` directory. Because it leverages TACC OAuth for
authentication and authorization, the simplest approach to developing the dashboard is to configure it to point
at a remote instance; for example, the develop or production instance.

Complete these steps before beginning work on the dashboard app:

1. Update the docker-compose-dashboard file with a client key and secret for the remote instance you intend to interact with.
NOTE: this client myst be subscribed to the Headers api. You can add a subscription to the Headers using the following:

```shell
$ curl -u <username>:<password> -d "apiName=Headers&apiVersion=v0.1&apiProvider=admin" https://api.tacc.utexas.edu/clients/v2/<client_name>/subscriptions
```
2. Modify your local /etc/hosts file to add an entry like this:

```shell
127.0.0.1 reactors.tacc.cloud
```
3. Build the dashboard image by running the following from the project root:

```shell
$ docker build -t abaco/dashboard -f Dockerfile-dashboard .
```

4. Start the dashboard app with docker-compose by running the following from the project root:

```shell
$ docker-compose -f docker-compose-dashboard.yml up -d
```


Remote Deployment
-----------------

Using the following commands, a new Abaco instance can be installed on Centos7 VMs running, for instance, in JetStream.

Install dependencies on the hosts (e.g., Docker, docker-compose, etc):

```shell
$ docker run --rm -v ~/.ssh/cic-iu.pem:/root/.ssh/cic-iu.pem -v /:/host -v $(pwd)/ansible/dev/hosts:/deploy/hosts agaveapi/deployer -i /deploy/hosts /deploy/docker_host.plbk -e update_docker_version=True -e update_docker_compose_version=True -e docker_version=1.12.6-1.el7.centos
```

Deploy Abaco:

```shell
$ docker run --rm -v ~/.ssh/jenkins-prod:/root/.ssh/id_rsa -v $(pwd)/ansible:/deploy agaveapi/deployer -i /deploy/dev/hosts /deploy/deploy_abaco.plbk
```


Production Release
------------------

Deployment checklist:
* logon to build server (typically, megajenkins, 129.114.6.149) and tag and push the images:
  - docker tag abaco/core:dev abaco/core:$TAG
    docker tag abaco/core:dev abaco/core
  - docker tag abaco/testsuite:dev abaco/testsuite
    docker tag abaco/testsuite:dev abaco/testsuite:$TAG
  - docker tag abaco/nginx:dev abaco/nginx:$TAG
    docker tag abaco/nginx:dev abaco/nginx

- change the tag in the compose and abaco.conf files on each production host
- pull the image (abaco/core:$TAG)
- update the TAG value in the abaco.conf file (e.g. TAG: 0.5.1)

- prep the env:

```shell
$ export abaco_path=$(pwd)
$ export TAG=0.5.1
```

1) shutdown all abaco containers
2) shutdown and restart rabbitmq
3) restart abaco containers
4) run a clean up:

```shell
$ docker rm `docker ps -aq`
```

Debug container
---------------

It can be useful to run a test container with all of the abaco code as well as iPython installed
when investigating an Abaco host. The following command will create such a container. NOTE: make sure
to update the SPAWNER_HOST_ID when using the health.py module in the test container.

```shell
$ docker run -v /:/host -v /var/run/docker.sock:/var/run/docker.sock -it -e case=camel -e SPAWNER_HOST_ID=0 -e base_url=http://172.17.0.1:8000 -v $(pwd)/abaco.conf:/etc/service.conf --rm --entrypoint=bash abaco/testsuite:$TAG
```

Additionally, when investigating a local development stack, consider using leveraging the `util` module from within
the tests directory once inside the test container:

```shell
$ cd /tests
$ ipython
```

Making requests to the local development stack is easy using the requests library:

```shell
>>> from util import *
>>> import requests
>>> requests.get('{}/actors'.format(base_url), headers=headers).json()
>>> requests.post('{}/actors'.format(base_url), data={'image': 'abacosamples/py3_func:dev'}, headers=headers).json()

```
Auto-Scaling
------------

Autoscaling uses [Prometheus](https://prometheus.io), which also provides a convenient dashboard.

Prometheus works by doing a GET request to the `/metrics` endpoint of Abaco, which occurs every 5 seconds. When this GET request happens, the following chain of events occurs:
1. All current Actors are cycled through, and given a metric for counting the current number of messages in its queue. 
     * An actor that already has a metric will not receive another one. 
2. All of the Actor metrics are cycled through, and updated with the Actor's current message count
3. Each Actor is checked. If an actor has 0 workers, it is given 1 worker. 
4. Each Actor metric is checked. 
     * If the message count for the actor is >=1, then that actor is given a new worker. 
     * If the message count for the actor is 0, then 1 worker is removed from that actor (assuming it has at least 1 worker)
5. The `/metrics` endpoint is updated with the current values of each metric
6. Prometheus scrapes the `/metrics` endpoint and saves the metrics data to its time-series database. 

Prometheus has some configuration files, found in the prometheus directory. Here, there is also a Dockerfile. The autoscaling feature uses a separate docker-compose file, `docker-compose-prom`.

