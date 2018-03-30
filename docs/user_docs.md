# Abaco User Documentation #

Welcome to the Abaco user documentation. Three higher-level capabilities are being developed on top of the Abaco web
service and computing platform; *reactors* for event-driven programming, *asynchronous executors* for scaling out
function calls within running applications, and *data adapters* for creating rationalized microservices from disparate
and heterogeneous sources of data. These capabilities are under active development: as features become available
the documentation below will be updated accordingly.


# Reactors Documentation #

Abaco reactors enable developers to implement event-driven programming models that are well integrated into the TACC
ecosystem. Here we cover the basics of creating your own reactor Docker images and working with the Reactors service to
register, execute and retrieve data about your reactors.

A TACC reactor is a Docker image that can be executed by making a POST request to its assigned inbox URL. When a reactor is executed, the service injects the contents of the POST message as well as authentication tokens and other data to help reactor containers accomplish computational tasks on behalf of the user or application that registered it.

A reactor can be any Docker image whatsoever, and its execution time can be up to 12 hours. If additional requests are made to the reactor's inbox URL while an execution is taking place, the messages will be queued and processed in the order received.

Contents:
1. Creating Reactor Images
2. Registering and Managing Reactors
3. Executing Reactors
4. Retrieving Data About Reactors

## Creating Reactors ##

As mentioned in the introduction, a reactor can be associated with any Docker image whatsoever.

### A Basic Reactor Image ###
Perhaps the simplest possible reactor image is one with just a single BASH script built off a minimal base such as busybox. The abacosamples/test image is an example. Here is the Dockerfile:

```
from busybox
add test.sh /test.sh
run chmod +x /test.sh
CMD ["sh", "/test.sh"]
```
The `test.sh` script could be anything. Here is an example:
```
#!/bin/bash

# print the special MSG variable:
echo "Contents of MSG: "$MSG

# do a sleep to slow things down
sleep 2

# print the full environment
echo "Environment:"
env

# print the root file system:
echo "Contents of root file system: "
ls /
```
In the test script we echo various strings and variables to standard out. The output sent to standard out will be captured in the logs for the reactor execution, following the Docker convention.

Note also that we echo the contents of the `MSG` variable. This is a special environment variable created by the reactor system when a raw string is passed as the payload to the reactor's inbox. The Reactors service facilitates communication with reactor containers through environment variables. The section below gives a complete list of environment variables set by the service.


### Complete List of Reactor Environment Variables ###
Here is a complete list of all possible environment variables.

* _abaco_execution_id: The id of the current execution.
* _abaco_access_token: An OAuth2 access token representing the user who registered the reactor.
* _abaco_actor_dbid: The internal id of the reactor.
* _abaco_actor_state: The value of the reactor's state at the start of the execution.
* _abaco_Content-Type: (either 'str' or 'application/json') - The data type of the message.
* _abaco_username: The username of the "executor", i.e., the user who sent the message.
* _abaco_api_server: The base URL for the Reactor's service.
* MSG: the message sent to the reactor, as a raw string.


### Sample Images and Python library ###
In order to simplify the creation of reactors that use Python, the TACC team has created a Python library and associated Docker image that provides convenience utilities. There is also a growing catalogue of public example images within the abacosamples Docker organization.

### Getting the Python Utilities ###
To use the Python base image, create a Dockerfile with the following:

```
from abacosamples/base-ubu
```

Note: The base image is based on Ubuntu 16.04.

The primary utility offered by the base image is the `agavepy` Python package. Users can add this to their own Docker image by installing agavepy, either from source or via pip. For example, one could add the following to their Dockerfile:

```
RUN pip install agavepy
```

### The actor module ###

The agavepy package is a complete Python binding for TACC's Agave API. Here, we focus on the actor module. To import the module, add the following line in your Python program:

```
from agavepy import actors
```

Currently, the actors module provides the following utilities:
```
  * `get_context()` - returns a Python dictionary with the following fields:
    * `raw_message` - the original message, either string or JSON depending on the Contetnt-Type.
    * `content_type` - derived from the original message request.
    * `message_dict` - A Python dictionary representing the message (for Content-Type: application/json)
    * `execution_id` - the ID of this execution.
    * `username` - the username of the user that requested the execution.
    * `state` - (for stateful actors) state value at the start of the execution.
    * `actor_id` - the actor's id.
  * `get_client()` - returns a pre-authenticated agavepy.Agave object.
  * `update_state(val)` - Atomically, update the actor's state to the value `val`.
```

Creating simple Python reactors is easy. For example, one could write a small function in a `main.py` module and invoke it when `__name == __main__`.

Here we use that approach to write a small "accumulator" reactor. The reactor pulls the variable `count` out of the message that it receives and adds it to its state:

```
from agavepy.actors import get_context, update_state

def main():
    context = get_context()
    count = context['message_dict']['count']
    state = context['state']
    update_state(count+state)

if __name__ == '__main__':
    main()
```

In order to create a Docker image with this reactor, create a Dockerfile that descends from the base image and add the Python module:
```
from abacosamples/base
ADD myactor.py /myactor.py

CMD ["python", "/myactor.py"]
```

### Leveraging TACC APIs from Within Reactors ###
One of the powerful features of reactors is that they can make authenticated requests to other TACC APIs during their execution. This is also straight-forward using the agavepy library.

To create an agavepy client, use the following code:

```
from agavepy.actors import get_client

ag = get_client()
```

The `ag` object is a pre-authenticated Agave client with full access to the APIs. For example, the following code is a complete example to create a directory within a specific path on an Agave storage system. The code assumes a `dir_name` is always passed but defaults the system id and path (in this case, `reactors.storage.sample` and `/data/samples`, respectively).

```
from agavepy.actors import get_context, get_client

def main():
    context = get_context()
    dir_name = context['message_dict']['dir_name']
    path = context['message_dict'].get('path', '/data/samples')
    system_id = context['message_dict'].get('system_id', 'reactors.storage.sample')
    ag = get_client()
    ag.files.manage(systemId=system_id, filePath=path, body={'action':'mkdir', 'path': dir_name})

if __name__ == '__main__':
    main()
```


## Registering and Managing Reactors ##

The hosted TACC reactors service sits on top of a RESTful HTTP API caled Abaco. To work with the service, any HTTP client can be used. In this documentation we will focus on two clients: curl, which can be run from the command line in most Unix like environments; and the agavepy Python library.

In this section we cover registering reactors from Docker images.

### Prerequisite - Permissions ###
To register a reactor, you need permission to access the underlying API service. You can check your access level by logging in with your TACC account to https://reactors.tacc.cloud. We will be making a web form available at https://reactors.tacc.cloud/request_access where users can request access.

### Initial Registration ###

Once you have your Docker image build and pushed to the Docker Hub, you can register a reactor from it by making a POST request to the API. The only required POST parameter is the image to use, but there are several other optional arguments.

### Complete List of Registration Parameters ###
The following parameters can be passed when registering a new reactor.

Required parameters:
* image - The Docker image to associate with the reactor. This should be a fully qualified image available on the public Docker Hub. We encourage users to use to image tags to version control their reactors.

Optional parameters:
* name - A user defined name for the reactor.
* description - A user defined description for the reactor.
* default_environment - The default environment is a set of key/value pairs to be injected into every execution of the reactor. The values can also be overridden when passing a message to the reactor in the query parameters (see the Executing Reactors section below).
* stateless (True/False) - Whether the reactor stores private state as part of its execution. If True, the state API will not be available, but in a future release, the Reactors service will be able to automatically scale reactor processes to execute messages in parallel. The default value is False.
* privileged (True/False) - Whether the reactor runs in privileged mode and has access to the Docker daemon. *Note: Setting this parameter to True requires elevated permissions.*



Here is an example using curl:

```
$ curl -H "Authorization: Bearer $TOKEN" \
-H "Content-Type: application/json" \
-d '{"image": "abacosamples/test", "name": "test", "description": "My test actor using the abacosamples image.", "default_environment":{"key1": "value1", "key2": "value2"} }' \
https://api.tacc.cloud/actors/v2
```

To register a reactor using the agavepy library, we use the `actors.add()` method and pass the same arguments through the `body` parameter. For example,

```
>>> from agavepy.agave import Agave
>>> ag = Agave(api_server='https://api.tacc.utexas.edu', token='<access_token>')
>>> reactor = {"image": "abacosamples/test", "name": "test", "description": "My test actor using the abacosamples image registered using agavepy.", "default_environment":{"key1": "value1", "key2": "value2"} }
>>> ag.actors.add(body=reactor)
```

### Updating Reactors ###
You can update a reactor at any time by issuing a PUT request to the reactor's id. A complete description of the reactor is required when making a PUT request. The available parameters are therefore the same as for registering the reactor.

The image associated with the reactor will be pulled from the Docker Hub if the image name or tag differ from what is currently registered. If the image name and tag match what is registered, the Reactors service will not automatically pull the image from Docker Hub. If you want the Reactors service to pull the latest image in this case, you can pass `force=True` parameter.

In this first example of updating a reactor using curl, we do not force the update. In this case, since the image (`abacosamples/test`) has not changed, it will not be updated:

```
$ curl -X PUT -H "Authorization: Bearer $TOKEN" \
-H "Content-Type: application/json" \
-d '{"image": "abacosamples/test", "name": "test", "description": "My test actor using the abacosamples image.", "default_environment":{"key1": "value1", "key2": "new_value"} }' \
https://api.tacc.cloud/actors/v2/$REACTOR_ID
```

Here is an example where we pass the `force` parameter, forcing the Reactors service to pull the latest image even when the image and tag are identical.

```
$ curl -X PUT -H "Authorization: Bearer $TOKEN" \
-H "Content-Type: application/json" \
-d '{"image": "abacosamples/test", "name": "test", "force": True, "description": "My test actor using the abacosamples image.", "default_environment":{"key1": "value1", "key2": "new_value"} }' \
https://api.tacc.cloud/actors/v2/$REACTOR_ID
```

Here is an example using agavepy; we use the `actors.update()` method passing the `actorId` and the description as the `body` parameter.

```
 >>> reactor = {'default_environment': {'key1': 'value1', 'key2': 'value2'},
 'description': 'My test actor using the abacosamples image registered with agavepy.',
 'image': 'abacosamples/test',
 'name': 'test'}

 >>> ag.actors.update(actorId='lJbeXR8bMXARG', body=reactor)
```


## Executing Reactors ##
To execute a Docker container associated with a reactor, we send the reactor a message by making a POST request to the reactor's inbox URI which is of the form:
```
https://api.tacc.cloud/reactors/v2/<reactor_id>/messages
```

Currently, two types of messages are supported: "raw" strings and JSON messages.

### Executing Reactors with Raw Strings ###

To execute a reactor passing a raw string, make a POST request with a single argument in the message body of `message`. Here is an example using curl:

```
$ curl -H "Authorization: Bearer $TOKEN" -d "message=some test message" https://api.tacc.cloud/actors/v2/$REACTOR_ID/messages
```

When this request is successful, the Reactors service will put a single message on the reactor's message queue which will ultimately result in one container execution with the `$MSG` environment variable having the value `some test message`.

The same execution could be made using the Python library like so:

```
>>> ag.actors.sendMessage(actorId='NolBaJ5y6714M', body={'message': 'test'})
```

### Executing Reactors by Passing JSON ###

You can also send pure JSON as the reactor message. To do so, specify a Content-Type of "application/json". Here is an example using curl:

```
$ curl -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{"username": "jdoe", "application_id": "print-env-cli-DggdGbK-0.1.0" }' https://api.tacc.cloud/actors/v2/$REACTOR_ID/messages
```

One advantage to passing JSON is that the python library will automatically attempt to deserialize it into a pure Python object. This shows up in the `context` object under the `message_dict` key. For example, for the example above, the corresponding rector (if written in Python) could retrieve the application_id from the message with the following code:

```
from agavepy.actors import get_context
context = get_context()
application_id = context['message_dict']['application_id']
```

The same reactor execution could be made using the Python library like so:

```
>>> message_dict = {"username": "jdoe", "application_id": "print-env-cli-DggdGbK-0.1.0" }
>>> ag.actors.sendMessage(actorId='NolBaJ5y6714M', body=message_dict)
```

### Overwriting the Default Environment ###

Reactors can (optionally) be registered with a set of key/value pairs to always be injected into the reactor's containers as environment variables. This is called the reactor's default environment. However, any of these values can be overwritten for a specific execution by passing query string parameters to the POST request.

For example, if we had a reactor with a default environment of `{"key1": "value1", "key2": "value2"}` (this was the case in one of our previous examples), we could overwrite the value of `key1` by passing a different value in the query string like so:

```
$ curl -H "Authorization: Bearer $TOKEN" -d "message=some test message" https://api.tacc.cloud/actors/v2/$REACTOR_ID/messages?key1=valueABC
```
In this case, the reactor container for this one execution would have the following environment variables and values (in addition to the standard ones injected by the Reactors service):
```
key1=valueABC
key2=value2
MSG=test message
```

## Retrieving Reactor Execution and Log Data ##

One can also retrieve data about a reactor's executions and the logs generated by the execution. Logs are anything written to standard out during the container execution. Note that logs are purged from the database on a regular interval (usually 24 hours) so be sure to retrieve important log data in a timely fashion.

To get details about an execution, make a GET request to the reactor's executions collection with the execution id:

```
$ curl -H "Authorization: Bearer $TOKEN" https://api.tacc.cloud/actors/v2/$REACTOR_ID/executions/$EXECUTION_ID
```
Here is an example response:
```
{
  "message": "Actor execution retrieved successfully.",
  "result": {
    "_links": {
      "logs": "https://api.sd2e.org/actors/v2/SD2E_NolBaJ5y6714M/executions/ZgeLeGGQDaZjj/logs",
      "owner": "https://api.sd2e.org/profiles/v2/jstubbs",
      "self": "https://api.sd2e.org/actors/v2/SD2E_NolBaJ5y6714M/executions/ZgeLeGGQDaZjj"
    },
    "actorId": "NolBaJ5y6714M",
    "apiServer": "https://api.sd2e.org",
    "cpu": 271632236,
    "executor": "jstubbs",
    "exitCode": 0,
    "finalState": {
      "Dead": false,
      "Error": "",
      "ExitCode": 0,
      "FinishedAt": "2017-10-06T15:54:00.019411149Z",
      "OOMKilled": false,
      "Paused": false,
      "Pid": 0,
      "Restarting": false,
      "Running": false,
      "StartedAt": "2017-10-06T15:53:57.845747189Z",
      "Status": "exited"
    },
    "id": "ZgeLeGGQDaZjj",
    "io": 766,
    "messageReceivedTime": "2017-10-06 15:53:55.615958",
    "runtime": 2,
    "startTime": "2017-10-06 15:53:57.088129",
    "status": "COMPLETE",
    "workerId": "lzD51vqrYaL8g"
  },
  "status": "success",
  "version": ":dev"
}
```
The equivalent request in Python looks like:

```
>>> ag.actors.getExecution(actorId=aid, executionId=exid)
```

Finally, to retrieve an execution's logs, make a GET request to the logs collection for the execution. For example:

```
$ curl -H "Authorization: Bearer $TOKEN" https://api.tacc.cloud/actors/v2/$REACTOR_ID/executions/$EXECUTION_ID/logs
```
Here is an example response:
```
{
  "message": "Logs retrieved successfully.",
  "result": {
    "_links": {
      "execution": "https://api.sd2e.org/actors/v2/NolBaJ5y6714M/executions/ZgeLeGGQDaZjj",
      "owner": "https://api.sd2e.org/profiles/v2/jstubbs",
      "self": "https://api.sd2e.org/actors/v2/NolBaJ5y6714M/executions/ZgeLeGGQDaZjj/logs"
    },
    "logs": "Contents of MSG: {'application_id': 'print-env-cli-DggdGbK-0.1.0', 'username': 'jdoe'}\nEnvironment:\nHOSTNAME=ba45cf7c68d5\nSHLVL=1\nHOME=/root\n_abaco_actor_id=NolBaJ5y6714M\n_abaco_access_token=9562ff7763cb6a21a0851f5e19bea67\n_abaco_api_server=https://api.sd2e.org\n_abaco_actor_dbid=SD2E_NolBaJ5y6714M\nMSG={'application_id': 'print-env-cli-DggdGbK-0.1.0', 'username': 'jdoe'}\n_abaco_execution_id=ZgeLeGGQDaZjj\nPATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin\nkey1=value1\n_abaco_Content_Type=application/json\nkey2=value2\nPWD=/\n_abaco_jwt_header_name=X-Jwt-Assertion-Sd2E\n_abaco_username=jstubbs\n_abaco_actor_state={}\nContents of root file system: \nbin\ndev\netc\nhome\nproc\nroot\nsys\ntest.sh\ntmp\nusr\nvar\nChecking for contents of mounts:\nMount does not exist\n"
  },
  "status": "success",
  "version": ":dev"
}
```

The equivalent request in Python looks like:
```
>>> ag.actors.getExecutionLogs(actorId=aid, executionId=exid)
```


# Asynchronous Executors #
Abaco asynchronous executors enable developers to asynchronously execute functions on the Abaco cluster from
directly within a running application; for example, when scaling out pleasantly parallel workloads such as parameter
sweeps. Currently, asynchronous executors are only available in Python 3.

Working with an Abaco asynchronous executor is similar to using a Threadpool or Processpool executor, but instead of
the executions running in separate threads or processes, they run on the Abaco cluster. When an Abaco asynchronous
executor is instantiated, an actor is registered with a special image. The methods that invoke remote executions do so
by sending a binary message to the registered actor; the message consists of a binary serialization of the callable and
data. When the actor receives the message, it deserializes it back into the callable and data, executes the callable,
and registers the return as a result for the actor.

The first step to using Abaco asynchronous executors is to import the `AbacoExecutor` class and instantiate it;
instantiating an `AbacoExecutor` requires an authenticated `agavepy.Agave` client, so that should be instantiated first.
For example:
```
from agavepy.agave import Agave
from agavepy.actors import AbacoExecutor

ag = Agave(api_server='https://api.tacc.cloud', token='my_token')
ex = AbacoExecutor(ag, image='abacosamples/py3_sci_base_func')
```

The `image` attribute is optional; if no image is passed, the Abaco library will provide a default. The key is that
the image used must contain all dependencies (Python packages, extensions, etc.) needed for the function to be executed.
If your function uses a custom code or library, you will need to build a Docker image containing the dependencies as
well as the Abaco executor library for processing messages and responses. A set of base images exist as starting points,
and the Abaco development team can help build any custom images needed.

Instantaiting the executor can take several seconds as it registers an actor and waits for it to be ready behind the
scenes. Once the executor is instantiated we have both blocking and non-blocking methods for launching remote executions:

## Blocking Calls ##

The simplest way to use the AbacoExecutor is with the `blocking_call()` method. This launches the execution on the Abaco
cluster and blocks until the result is ready. Once the result is ready, it fetches it from the actor's result endpoint
and returns.

For example, if we have the following function, `f`, defined below, we can use `blocking_call()` as follows to execute
`f` in the cloud.
```
def f(x):
    return 3*x + 5

ex = AbacoExecutor(ag)
ex.blocking_call(f, 3)
# ...blocks until result is ready..
Out[]: [14]

```

## Nonblocking Calls ##

To invoke a function once in a nonblocking manner, use the `submit()` method. For example, suppose we have the
following function, `f`, defined below. We can use `submit()` as follows to execute `f` in the cloud.
```
def f(x):
    return 3*x + 5

ex = AbacoExecutor(ag)
# ..returns an AbacoAsynchronousResponse object immediately
arsp = ex.submit(f, 5)
```

The `submit()` method returns an `AbacoAsyncResponse` object immediately. This does not mean our function execution has
 finished, but we can use the `arsp` object to check the status of the execution and, eventually, retrieve the result:
```
# use done() to check the status; are we done?
arsp.done()
# .. returns True or False immediately..
Out[]: True

# Now that the execution is done, use result() to retrieve the result (if not done, this method blocks until the result
# is ready)
arsp.result()
Out[]: [20]
```

Finally, we can use the executor's `map()` method to map a function over a set of input data. The `map()` call will
return a list of `AbacoAsyncResponse` objects, one for each execution. The results can then be retrieved individually.

In the following example, we make a single nonblocking call to the function `g` to compute the product of two two
square numpy arrays:
```
import numpy as np
def g(std_dev, size):
    A = np.random.normal(0, std_dev, (size, size))
    B = np.random.normal(0, std_dev, (size, size))
    C = np.dot(A, B)
    return C[0]

arsp = ex.submit(g, 100, 4096)
arsp.result()
Out[]:
[array([ -54028.76305778,  106760.76856284, -242740.05860707, ...,
         125689.34792646,  309953.84157049,  554118.2633921 ])]
```


Here, we use `map()` to submit 8 such executions. We also add time the execution and add some error handling:
```
import time
start=time.time()
arsp = ex.map(g, [[100, 1024] for i in range(8)])
for a in arsp:
    try:
        print(a.result())
    except Exception:
        print("error for execution: {}".format(a.execution_id))
end=time.time()
```
When done, executors should be deleted to remove the actor and supporting resources on the Abaco cluster:
```
ex.delete()
```

In a future release, Abaco will automatically remove actors represetning executors that have been idle for more than
12 hours.


# Data Adapters #
The data adpaters functionality is still in early development stages.