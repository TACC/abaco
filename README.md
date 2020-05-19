Abaco
=====

Actor Based Co(mputing)ntainers: Functions-as-a-service using the Actor model.

[![DOCS](https://readthedocs.org/projects/abaco/badge/)](https://tacc-cloud.readthedocs.io/projects/abaco/en/latest/index.html)
[![DOI](https://zenodo.org/badge/39394579.svg)](https://zenodo.org/badge/latestdoi/39394579)


Intro
-----

Abaco is a web service and distributed system that combines the actor model of concurrent computation, Linux containers into a web services platform that provides functions-as-a-service. In Abaco, actor registered in the system is associated with a Docker image. Actor containers are executed in response to messages posted to their inbox which itself is given by a URI exposed via Abaco. In the process of executing the actor container, state, logs and execution statistics are collected. Many aspects of the
system are configurable. 

Quickstart
----------

![Usage Workflow](docs/Figure2.png "Abaco Usage, Illustrated")

1.  Deploy a development version of the Abaco stack using
    `docker-compose`. First, change into the project root and export the
    following variable so that abaco containers know where to find the
    example configuration file

    ```shell
    $ export abaco_path=$(pwd)
    ```

    Then start the Abaco containers with the following two commands:

    ```shell
    $ docker-compose up -d
    ```

    If all went well, the services will be running behind `nginx` on 8000. We assume the Docker Gateway is running on the default IP for Docker 1.9.1+ which is 172.17.0.1. If this is not the case for your setup, you will need to update the value of host within the store stanza of the all.conf file with the IP address of the Gateway. It also may take several seconds for the mongo db to be ready to accept connections.

2.  Register an actor -- Initially, the Abaco system is empty with no actors defined which can be seen by making a GET request to the root collection:

    ```shell
    $ curl localhost:8000/actors
    ```

    Abaco will respond with a JSON message that looks something like:

    > ```json
    > {
    >     "msg": "Actors retrieved successfully.",
    >     "result": [],
    >     "status": "success",
    >     "version": "0.01"
    > }
    > ```

    To define an actor, post an image available on the public Docker Hub. You can also optionally pass a name attribute.

    ```shell
    $ curl -X POST --data "image=abacosamples/test" "localhost:8000/actors"
    ```

    Abaco responds in JSON. You should see something like this:

    ```json
    {
      "message": "Actor created successfully.",
      "result": {
        "_links": {
          "executions": "https://dev.tenants.develop.agaveapi.co/actors/v2/yP0PDpZWG38Bg/executions",
          "owner": "https://dev.tenants.develop.agaveapi.co/profiles/v2/testshareuser",
          "self": "https://dev.tenants.develop.agaveapi.co/actors/v2/yP0PDpZWG38Bg"
        },
        "createTime": "2017-09-21 22:22:52.482495",
        "defaultEnvironment": {},
        "description": "",
        "id": "yP0PDpZWG38Bg",
        "image": "abacosamples/test",
        "lastUpdateTime": "2017-09-21 22:22:52.482495",
        "owner": "testshareuser",
        "privileged": false,
        "state": {},
        "stateless": false,
        "status": "SUBMITTED",
        "statusMessage": ""
      },
      "status": "success",
      "version": ":dev"
    }    ```

    Abaco assigned an id to the actor (in this case `yP0PDpZWG38Bg`) and associated it with the image abacosamples/test which it began pulling from the public Docker hub.

3.  Notice that Abaco returned a status of `SUBMITTED` for the actor; behind the scenes, Abaco is starting a worker container to handle messages passed to this actor. The worker must initialize itself (download the image, etc) before the actor is ready. You can retrieve the details about an actor (including the status) by making a `GET` request to a specific actor using its uuid like so:

    ```shell
    $ curl localhost:8000/actors/yP0PDpZWG38Bg
    ```

    When the actor's worker is initialized, you will see a response like
    this:

    ```json
    {
      "message": "Actor retrieved successfully.",
      "result": {
        "_links": {
          "executions": "https://dev.tenants.develop.agaveapi.co/actors/v2/yP0PDpZWG38Bg/executions",
          "owner": "https://dev.tenants.develop.agaveapi.co/profiles/v2/testshareuser",
          "self": "https://dev.tenants.develop.agaveapi.co/actors/v2/yP0PDpZWG38Bg"
        },
        "createTime": "2017-09-21 22:29:40.549968",
        "defaultEnvironment": {},
        "description": "",
        "id": "yP0PDpZWG38Bg",
        "image": "abacosamples/test",
        "lastUpdateTime": "2017-09-21 22:29:40.549968",
        "owner": "testshareuser",
        "privileged": false,
        "state": {},
        "stateless": false,
        "status": "READY",
        "statusMessage": ""
      },
      "status": "success",
      "version": ":dev"
    }    
    ```

    A status of "READY" indicates that actor is capable of processing messages by launching containers from the image. Note that any messages sent before the actor is ready will still be queued up.

4.  We're now ready to execute our actor. To do so, make a `POST` request to the messages collection for the actor and pass a message string as the payload.

    ```bash
    $ curl -X POST --data "message=test execution"  localhost:8000/actors/yP0PDpZWG38Bg/messages
    ```

    abaco executes the image registered for `yP0PDpZWG38Bg`, in this case, jstubbs/abaco\_test, and passes in the string `"test execution"` as an environmental variable (`$MSG`). abaco will use the default command included in the image when executing the container. The response will look like this:

    ```json
    {
      "message": "The request was successful",
      "result": {
        "_links": {
          "messages": "https://dev.tenants.develop.agaveapi.co/actors/v2/yP0PDpZWG38Bg/messages",
          "owner": "https://dev.tenants.develop.agaveapi.co/profiles/v2/testshareuser",
          "self": "https://dev.tenants.develop.agaveapi.co/actors/v2/yP0PDpZWG38Bg/executions/JN0Boakk0VzKX"
        },
        "executionId": "JN0Boakk0VzKX",
        "msg": "test execution"
      },
      "status": "success",
      "version": ":dev"
    }
    ```

    Note that the execution id (in this case, `JN0Boakk0VzKX`) is returned in the response. This execution id can be used to retrieve information about the execution.

5.  An actor's executions can be retrieved using the `executions` sub-collection.

    ```shell
    $ curl localhost:8000/actors/yP0PDpZWG38Bg/executions
    ```

    The response will include summary statistics of all executions for the actor as well as the id's of each execution. As expected, we see the execution id returned from the previous step.

    ```json
    {
      "message": "Actor executions retrieved successfully.",
      "result": {
        "_links": {
          "owner": "https://dev.tenants.develop.agaveapi.co/profiles/v2/testshareuser",
          "self": "https://dev.tenants.develop.agaveapi.co/actors/v2/yP0PDpZWG38Bg/executions"
        },
        "actorId": "yP0PDpZWG38Bg",
        "apiServer": "https://dev.tenants.develop.agaveapi.co",
        "ids": [
          "JN0Boakk0VzKX"
        ],
        "owner": "testshareuser",
        "totalCpu": 50299836,
        "totalExecutions": 1,
        "totalIo": 1498,
        "totalRuntime": 2
      },
      "status": "success",
      "version": ":dev"
    }
    ```

    The `abacosamples/test` image simply echo's the environment and does a sleep for 2 seconds. We can query the executions collection with the execution id at any to get status information about the execution. When the execution finishes, its status will be returned as "COMPLETE" and details about the execution including runtime, cpu, and io usage will be available. For example:

    > ```shell
    > $ curl localhost:8000/actors/yP0PDpZWG38Bg/executions/JN0Boakk0VzKX
    > ```

    The response will look something like:

    ```json
    {
      "message": "Actor execution retrieved successfully.",
      "result": {
        "_links": {
          "logs": "https://dev.tenants.develop.agaveapi.co/actors/v2/DEV-DEVELOP_yP0PDpZWG38Bg/executions/JN0Boakk0VzKX/logs",
          "owner": "https://dev.tenants.develop.agaveapi.co/profiles/v2/testshareuser",
          "self": "https://dev.tenants.develop.agaveapi.co/actors/v2/DEV-DEVELOP_yP0PDpZWG38Bg/executions/JN0Boakk0VzKX"
        },
        "actorId": "yP0PDpZWG38Bg",
        "apiServer": "https://dev.tenants.develop.agaveapi.co",
        "cpu": 50299836,
        "executor": "testshareuser",
        "exitCode": 0,
        "finalState": {
          "Dead": false,
          "Error": "",
          "ExitCode": 0,
          "FinishedAt": "2017-09-21T22:35:16.921702662Z",
          "OOMKilled": false,
          "Paused": false,
          "Pid": 0,
          "Restarting": false,
          "Running": false,
          "StartedAt": "2017-09-21T22:35:14.852960209Z",
          "Status": "exited"
        },
        "id": "JN0Boakk0VzKX",
        "io": 1498,
        "messageReceivedTime": "2017-09-21 22:35:14.374600",
        "runtime": 2,
        "startTime": "2017-09-21 22:35:14.637897",
        "status": "COMPLETE",
        "workerId": "vrbr8PeWLXYEQ"
      },
      "status": "success",
      "version": ":dev"
    }
    ```

6.  You can also retrieve the logs (as in docker logs) for any execution:

    ```shell
    $ curl localhost:8000/actors/yP0PDpZWG38Bg/executions/JN0Boakk0VzKX/logs
    ```

    Response:

    ```json
    {
      "message": "Logs retrieved successfully.",
      "result": {
        "_links": {
          "execution": "https://dev.tenants.develop.agaveapi.co/actors/v2/yP0PDpZWG38Bg/executions/JN0Boakk0VzKX",
          "owner": "https://dev.tenants.develop.agaveapi.co/profiles/v2/testshareuser",
          "self": "https://dev.tenants.develop.agaveapi.co/actors/v2/yP0PDpZWG38Bg/executions/JN0Boakk0VzKX/logs"
        },
        "logs": "Contents of MSG: test execution\nEnvironment:\nHOSTNAME=48cb805f9af5\nSHLVL=1\nHOME=/root\n_abaco_actor_id=yP0PDpZWG38Bg\n_abaco_access_token=\n_abaco_api_server=https://dev.tenants.develop.agaveapi.co\n_abaco_actor_dbid=DEV-DEVELOP_yP0PDpZWG38Bg\nMSG=test execution\n_abaco_execution_id=JN0Boakk0VzKX\nPATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin\n_abaco_Content_Type=str\nPWD=/\n_abaco_jwt_header_name=X-Jwt-Assertion-Dev-Develop\n_abaco_username=testshareuser\n_abaco_actor_state={}\nContents of root file system: \n_abaco_data1\n_abaco_data2\nbin\ndata1\ndata2\ndev\netc\nhome\nproc\nroot\nsys\ntest.sh\ntmp\nusr\nvar\nChecking for contents of mounts:\nMount exsits. Contents:\n"
      },
      "status": "success",
      "version": ":dev"
    }
    ```

    As mentioned earlier, this test container simply echo's the environment, and we see that from the logs. Notice that our `MSG` variable showed up, as well as a couple other variables: `_abaco_api_server` and `_abaco_username`. The username is showing up as "anonymous" since the development configuration is using no authentication; however, the abaco system has a configurable authentication mechanism for securing the services with standards such as JWT (<https://tools.ietf.org/html/rfc7519>), and when such authentication mechanisms are configured, the username will be populated.

Additional Features
-------------------

The quick start introduced the basic features of abaco. Here we list
some of the more advanced features.

-   **Admin API**: In Abaco, messages sent to an actor for execution are
    queued and processed by the actor's "workers". Workers are processes
    that have access to the docker daemon and the actor's image, and
    workers take care of launching the actor containers, reading the
    docker stats api for the execution, storing logs for the execution,
    etc. Abaco has a separate administration api which can be used to
    manage the workers for an actor. This API is available via the
    `workers` collection for any given actor: for example, to retrieve
    the workers for our actor from the quickstart we would make a GET
    request like so:

    ```shell
    $ curl localhost:8000/actors/yP0PDpZWG38Bg/workers
    ```

    The response contains the list of all workers and supporting
    metadata including the worker's container id, the host ip where the
    working is running and its status.

    ```json
    {
      "message": "Workers retrieved successfully.",
      "result": [
        {
          "chName": "bd8fc7f5e14743a48cb3835886d64d44",
          "cid": "094456dfa8b5f711be89deffba0b514ec047ccd907b60f4a88a5b951bf12d17f",
          "hostId": "0",
          "hostIp": "172.17.0.1",
          "id": "vrbr8PeWLXYEQ",
          "image": "abacosamples/test",
          "lastExecutionTime": "2017-09-21 22:35:17.156459",
          "lastHealthCheckTime": "2017-09-21 22:46:27.741725",
          "location": "unix://var/run/docker.sock",
          "status": "READY",
          "tenant": "DEV-DEVELOP"
        }
      ],
      "status": "success",
      "version": ":dev"
    }
    ```

    We can add workers for an actor by making POST requests to the
    collection, optionally passing an argument `num` to specify a number
    of workers to have (default is 1). Note that when an actor has
    multiple workers, messages will be processed in parallel. We can
    also delete a worker by making a DELETE request to the worker's URI.

-   **Privileged containers**: By default, all actor containers are
    started in non-privileged mode, but when registering an actor, the
    user can specify that the actor is `privileged`, in which case the
    actor's containers will be started in privileged mode with the
    docker daemon mounted. This can be used, for example, to kick off
    automated Docker builds of other images.
-   **Default environments**: When registering an actor, the operator
    can provide an arbitrary JSON collection of key/value pairs in the
    `default_environment` parameter. These variables and values will be
    injected into the environment when executing a container. This can
    be useful for providing sensitive information such as credentials
    that cannot be stored in the actor's Docker image.
-   **Custom container environments**: When making a POST request to the
    actor's messages collection to execute an actor container, users can
    supply additional environment variables and values as query
    parameters. Abaco will update the actor's default environment with
    these query parameter variables and values, with the latter
    overriding the former.
-   **Stateless actors**: By default, actors are assumed to be statefull
    (that is, have side effects or maintain state between executions),
    but when registered, an actor can be set as "stateless" indicating
    that they can be automatically scaled (that is, add additional
    workers) without race conditions (see below).
-   **Health checks and auto-scaling**: currently, abaco runs health
    check processes to ensure that workers are healthy and available for
    processing messages in an actor's queue. Health check agents can
    create new workers and/or destroy existing workers as needed,
    depending on an actor's queue size. We are currently working on
    formalized policies that can be set in the `abaco.conf` file to
    allow for more robust auto-scaling, including that of stateless
    actors.
-   **Hot updates and graceful shutdowns**: workers can be sent a
    "shutdown" command which will cause the worker to exit. If the
    worker is currently processing an actor execution, the execution
    will conclude before the worker exits. When updating an actor's
    image, abaco first gracefully shuts down all workers before
    launching new workers with the updated image so that actors are in
    effect updated in real time with no downtime or execution
    interruption.
-   **Scalable architecture and Multihost deployments**: Abaco was
    architected to scale easily to meet the demands of large workloads
    across thousands of actors. While the quickstart launched all abaco
    processes (or actors!) on a single host, the system can be easily
    deployed and scaled up across any number of hosts. See the `ansible`
    directory for scripts used to deploy abaco in production-like
    environments. For more information on the abaco architecture see
    (<https://github.com/TACC/abaco/blob/master/docs/architecture.md>).
    UPDATE: with the announcement of Docker 1.12 and embedded
    orchestration, parts of this section will be updated to make
    deploying on a swarm cluster seamless and automatic from the compose
    file.
-   **Configurable**: Many aspects of the Abaco system are configurable
    via the abaco.conf file. The example contained in this repository is
    self-documenting.
-   **Multi-tenant**: A single Abaco instance can serve multiple
    organization or "tenants" which have logical separation within the
    system. The tenants can be configured in the `abaco.conf` file and
    read out of the request through either a JWT or a special tenant
    header.
-   **Basic Permissions System**: When configured to run with JWT authentication,
    Abaco parses the JWT for the user's identity. Actors are "owned" by the user who
    registers them, and initially actors are private to their owner. Users can share
    actors with other user by making a POST to the actor's permissions endpoint. Three
    permissions are availale: READ, EXECUTE and UPDATE, and currently, higher permission
    levels imply lower ones. Actors can be made public by granting a permission to the
    ABACO_WORLD user.
-   **Role Based Access Control**: When configured to run with JWT authentication, Abaco
    parses the JWT for the user's occupied roles. Four specific roles are recognized: admin,
    privileged, user, and limited. Users with the admin role have full access to all actors,
    can create or update actors to be "privileged", and can add or remove workers for any actor.
    Users with the privileged role essentially have admin rights to the actors they own or
    have update permission on. Users with the user role have basic access to Abaco: they can
    create and execute actors, but they cannot create privileged actors and they cannot modify
    the workers associated with their actors. Finally, the limited role is a place holder for
    future work. We plan to enable users with the limited role to make a (configurable) limited
    number of executions to actors that are shared with them.
-   **Integration with the Agave (<http://agaveapi.co/>)
    science-as-a-service API platform**: Abaco can be used as an "event
    processor" in conjunction with the Agave API platform. When deployed
    and configured with Agave's JWT authentication, abaco will inject
    the necessary authentication tokens needed for making requests
    directly to Agave APIs on behalf of the original end-user.
    Additionally, we are developing base images that contain Agave
    language SDKs and other tools so that processing an event can be as
    easy as writing a function or extending a class.
