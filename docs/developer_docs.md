
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

The backend processes (e.g. spawners, workers, health checks, and clientg) are defined in their own modules with a
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
Mongo. If your Docker0 gateway IP is different, you will need to updte the abaco.conf file.

Several optional aspects of the Abaco development stack require additional configuration. By default, the databases
(Mongo and RabbitMQ) do not use authentication. To test with authentication, uncomment the relevant stanzas from
the docker-comppose-local-db.yml file and export the following variables:
```shell
$ export mongo_password=password
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
$ docker run --network=abaco_abaco -e base_url=http://nginx -e case=camel -v /:/host -v $(pwd)/local-dev.conf:/etc/service.conf -it --rm abaco/testsuite$TAG
```

You can pass arguments to the testsuite container just like you would to py.test. For example, you can run
just one test (in this case, the `test_create_actor_with_webhook` test) from within the `test_abaco_core.py` 
file with:

```shell
$ docker run --network=abaco_abaco -e base_url=http://nginx -e case=camel -v /:/host -v $(pwd)/local-dev.conf:/etc/service.conf -it --rm abaco/testsuite$TAG /tests/test_abaco_core.py::test_create_actor_with_webhook
``` 

Run the unit tests with a command similar to the following, changing the test module as the end as necessary:

```shell
$ docker run --network=abaco_abaco -e base_url=http://nginx -v $(pwd)/local-dev.conf:/etc/service.conf --entrypoint=py.test -it --rm abaco/testsuite$TAG /tests/test_store.py
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

Previously Redis and MongoDB shared different stores. We now use MongoDB for all stores. The DB is used to store current application state
(e.g. actors_store and workers_store) and information about the permissions, accounting, nonces, clients, pre-gen clients, and logging data
(permissions_store, executions_store, nonce_store, clients_store, pregen_clients, and logs_store). Naturally, the log data (actual raw logs
from the container executions) presented a challenge in space and meant MongoDB would be a suitable DB. We use a set_with_expiry method
to store the logs for a fixed period of time.


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
2. Spawner checks the `generate_clients` config within the `workers` stanza to see if client generation is enabled.
3. Spawner sends a message to the clientg agent via the `ClientsChannel` requesting a new client.
4. Clientg responds to Spawner with a message containing the client key and secret, access token, and refresh token if client generation was successful.
5. Spawner pulls the docker image
6. Spawner starts the worker containers using the configured docker daemon (for now, the local unix socket). It passes the image to use and worker_id as environment variables and waits for a message on the SpawnerWorker channel for that worker indicating the workers were able to pull the image and start up successfully.
7. Spawner updates worker store with container ID and status of READY
8. Spawner sends a message on the spawnerworker channel (which only the worker is subscribed to) to let it know that it is ready.

Each worker goes through different states, depending on where it is in the creation process. A finite state machine can be used to describe these states: 
![Worker State Diagram](https://github.com/TACC/abaco/blob/worker-management/docs/worker-state-diagram.png "Worker State diagram")


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
>>> hs = get_jwt_headers()
>>> import requests
>>> requests.get('{}/actors'.format(base_url), headers=hs).json()
>>> requests.post('{}/actors'.format(base_url), data={'image': 'abacosamples/py3_func:dev'}, headers=hs).json()

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

Mongo Conversion
----------------
Version 1.6 of Abaco restructures data storage by removing Redis and storing all data in MongoDB. This conversion to solely
MongoDB was done for simplicity, atomicity, and the ability to implement search with more ease. The conversion required updating
the data structure of saved documents to match between stores that were previously part of Redis. An example for each store is below:

logs_store (db=1):
```
{
    "_id" : "8oA5YDobwyK1",
    "exp" : ISODate("2020-03-20T17:05:50.872Z"),
    "logs" : "These are the logs",
    "actor_id" : "DEV-DEVELOP_AKeo3XGAGyRr",
    "tenant" : "DEV-DEVELOP"
}
```

permissions_store (db=2):
```
{
    "_id" : "DEV-DEVELOP_AKeo3XGAGyRr",
    "testuser" : "UPDATE"
}
```

executions_store (db=3):
```
{
    "_id" : "DEV-DEVELOP_AKeo3XGAGyRr",
    "jN6gDD0y8Lmg" : {
        "tenant" : "DEV-DEVELOP",
        "api_server" : "https://dev.tenants.develop.tacc.cloud",
        "actor_id" : "DEV-DEVELOP_AKeo3XGAGyRr",
        "executor" : "Abaco Event",
        "worker_id" : "BoN4DR6WZvWG",
        "id" : "jN6gDD0y8Lmg",
        "message_received_time" : "1584723940.696552",
        "start_time" : "1584723940.982222",
        "runtime" : 2,
        "cpu" : 0,
        "io" : 0,
        "status" : "COMPLETE",
        "exit_code" : 0,
        "final_state" : {
            "Status" : "exited",
            "Running" : false,
            "Paused" : false,
            "Restarting" : false,
            "OOMKilled" : false,
            "Dead" : false,
            "Pid" : 0,
            "ExitCode" : 0,
            "Error" : "",
            "StartedAt" : "2020-03-20T17:05:41.467527Z",
            "FinishedAt" : "2020-03-20T17:05:43.4702601Z"
        }
    },
    ...
}
```

clients_store (db=4):
```
Will be updated
``` 

actors_store (db=5):
```
{
    "_id" : "DEV-DEVELOP_AKeo3XGAGyRr",
    "api_server" : "https://dev.tenants.develop.tacc.cloud",
    "create_time" : "1584723930.184325",
    "db_id" : "DEV-DEVELOP_AKeo3XGAGyRr",
    "default_environment" : {},
    "description" : "",
    "executions" : {},
    "gid" : null,
    "hints" : [],
    "id" : "AKeo3XGAGyRr",
    "image" : "jstubbs/abaco_test",
    "last_update_time" : "1584723930.184325",
    "link" : "aylY75G7axxb",
    "max_cpus" : null,
    "max_workers" : null,
    "mem_limit" : null,
    "mounts" : [ 
        {
            "host_path" : "/data1",
            "container_path" : "/_abaco_data1",
            "mode" : "ro"
        }
    ],
    "name" : "abaco_test_suite_create_link",
    "owner" : "testuser",
    "privileged" : false,
    "queue" : "default",
    "state" : {},
    "stateless" : true,
    "status" : "READY",
    "status_message" : " ",
    "tasdir" : null,
    "tenant" : "DEV-DEVELOP",
    "token" : "false",
    "type" : "none",
    "uid" : null,
    "use_container_uid" : false,
    "webhook" : ""
}
```

workers_store (db=6):
```
{
    "_id" : "DEV-DEVELOP_AKeo3XGAGyRr",
    "BoN4DR6WZvWG" : {
        "tenant" : "DEV-DEVELOP",
        "id" : "BoN4DR6WZvWG",
        "status" : "READY",
        "ch_name" : "worker_BoN4DR6WZvWG",
        "image" : "jstubbs/abaco_test",
        "location" : "unix://var/run/docker.sock",
        "cid" : "91fd463de7ed72fa2f8f3e32e2377c897f874ad895cc5b4fbba89c504a656301",
        "host_id" : "0",
        "host_ip" : "172.17.0.1",
        "create_time" : "1584723938.432633",
        "last_execution_time" : "1584723950.874809",
        "last_health_check_time" : "1584724483.159131"
    },
    ...
}
```

nonce_store (db=7):
```
{
    "_id" : "DEV-DEVELOP_jane",
    "DEV-DEVELOP_AKeo3XGAGyRr" : {
        "tenant" : "DEV-DEVELOP",
        "db_id" : null,
        "roles" : [ 
            "Internal/AGAVEDEV_testuser_postman-test-client-1496345350_PRODUCTION", 
            "Internal/AGAVEDEV_testuser_postman-test-client-1497902074_PRODUCTION", 
            ...
        ],
        "owner" : "testuser",
        "api_server" : "https://dev.tenants.develop.tacc.cloud",
        "level" : "EXECUTE",
        "max_uses" : -1,
        "description" : "",
        "alias" : "DEV-DEVELOP_jane",
        "actor_id" : null,
        "id" : "DEV-DEVELOP_AKeo3XGAGyRr",
        "create_time" : "1584723876.437177",
        "last_use_time" : "1584723876.494796",
        "current_uses" : 1,
        "remaining_uses" : -1
    }
}
```

alias_store (db=8):
```
{
    "_id" : "DEV-DEVELOP_jane",
    "actor_id" : "7W6JJkepYbwRm",
    "alias" : "jane",
    "alias_id" : "DEV-DEVELOP_jane",
    "api_server" : "https://dev.tenants.develop.tacc.cloud",
    "db_id" : "DEV-DEVELOP_AKeo3XGAGyRr",
    "owner" : "testuser",
    "tenant" : "DEV-DEVELOP"
}
```

pregen_clients (db=9):
```
Pregen clients are currently not utilized in Abaco. The database is set for future work though.
```

As you can see, most stores are organized by actor id first, as "_id", followed by either information concerning that actor, or another object linked to that actor, for instance, workers or executions. The logs store deviates from this schema and instead is organized by execution_id as the top-level with actor_id as a field. This is due to data expiry. Logs require an ability to expire over time which is done with Mongo's expiry indexes. However, these indexes delete an entire document, not a subdocument, so each log must be contained in it's own document for that reason.

With the conversion, the Mongo store was expanded upon in terms of functionality. `__getitem__()`, `\__setitem__()`, and `\__delitem__()` are now capable of multilevel gets, sets, and dels. Previously modifying a field required a new function, but as the following example shows, gets, sets, and deletes are now multilevel with a new syntax. Additionally error handling has been integrated into these functions.
```
# Getting the owner field of an object in the nonce_store
nonce_store['DEV-DEVELOP_AKeo3XGAGyRr', 'owner']

# Setting a field 5 levels down equal to new_data. The field will be created if it does not exist
logs_store[level1, level2, level3, level4, level5] = new_data.

# Deleting a the 'name' field for an actor in the actors_store
del actors_store['DEV-DEVELOP_AKeo3XGAGyRr', 'name']
```

These multilevel functions are made possible with the `_process_inputs()` function in store.py. This function takes in the fields specified and parses them properly for all operations.

Along with these "normal" functions come recreations of old functions, such as `pop_field()`, `set_with_expiry()`, `__iter__()`, `__len__()`, `_prepset()`, `getset()`, `add_if_empty()`, and `getset()`. All of these functions act in the same way as they previously did. `getset()`, `add_if_empty()`, `set_with_expiry()`, and `pop_field()` also inherit the benefits of `_process_inputs()` and work on multiple levels. For example:
```
# The following would create a new field 3 levels deep in the logs_store if the field did not yet exists
logs_store.add_if_empty([execution_id, 'field1', 'field2'], 'new_value')
```
This other functions follow the same format as well for multilevel modifications.

A function who's result stayed the same, but features changed was `items()`. Items returns a MongoDB `find()` function. Without any input, something like `logs_store.items()` would return the entirety of logs_store in the format Redis returned data in. As in, just the data. `items()` by default has a projection input set to `{'_id': False}`. This gets rid of returning the '_id' fields shown in the example stores above. This was done to follow the same formatting as Redis. But in order to return this '_id' field, which is usually the actor_id, a developer must run `logs_store.items(proj_inp=None)`. Additionally `items()` allows for `filter_inp`, this parameter allows a developer to filter results with a Mongo query. 

Additionally one new function was created and is named `full_update()`. This function is just that and is essentially a passthrough for the key and value fields in MongoDB's  `update_one()`. This allows access for intricate updates in the code that allow for atomic processes to take place with the use of one Mongo function. An example of this function implemented in code is below and is on line 798 of `actors/models.py`:

```
try:
            # Check for remaining uses equal to -1
            res = nonce_store.full_update(
                {'_id': nonce_key, nonce_id + '.remaining_uses': {'$eq': -1}},
                {'$inc': {nonce_id + '.current_uses': 1},
                '$set': {nonce_id + '.last_use_time': get_current_utc_time()}})
            if res.raw_result['updatedExisting'] == True:
                logger.debug("nonce has infinite uses. updating nonce.")
                return

            # Check for remaining uses greater than 0
            res = nonce_store.full_update(
                {'_id': nonce_key, nonce_id + '.remaining_uses': {'$gt': 0}},
                {'$inc': {nonce_id + '.current_uses': 1,
                        nonce_id + '.remaining_uses': -1},
                '$set': {nonce_id + '.last_use_time': get_current_utc_time()}})
            if res.raw_result['updatedExisting'] == True:
                logger.debug("nonce still has uses remaining. updating nonce.")
                return
            
            logger.debug("nonce did not have at least 1 use remaining.")
            raise errors.PermissionsException("No remaining uses left for this nonce.")
        except KeyError:
            logger.debug("nonce did not have a remaining_uses attribute.")
            raise errors.PermissionsException("No remaining uses left for this nonce.")
```

Here you can see `full_update()` implemented to either increment or decrement a field, all in one function call. Doing this allows for a truly atomic procedure. For example, the first case of full_update checks for a particular `nonce_key`, if that key exists and the `remaining_uses` field for that key is equal to -1, the `current_uses` field is incremented by one and the `last_use_time` field is set to the current UTC time.

A more advanced example comes on line 1335 of `actors/models.py`
```
          elif status == ERROR:
              res = workers_store.full_update(
                  {'_id': actor_id},
                  [{'$set': {worker_id + '.status': {"$concat": [ERROR, " (PREVIOUS ", f"${worker_id}.status", ")"]}}}])
```

Here, if the status to be set is `ERROR` then we use the recently introduced, aggregation pipeline inside of a MongoDB `update_one()` function. This allows us to update a field in the database with information in the about to be written over field. In this case it results in us being able to set a field equal to "ERROR (PREVIOUS READY)". Where ready is what that field was set to. This is done all in one completely atomic function call instead of a previous two.

There were talks to do even more with full_update, such as a, "check if actor exists and then do work if True" query. While possible, this would usually require a large join between collections which is inadvisable by Mongo. So if a query is completely in one collection or not too many steps are required during a join, then `full_update()` can give you atomicity. Several calls to check actor existance are done in Abaco, but they usually consists of several steps before being able to actually update a field.

