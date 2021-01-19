<<<<<<< HEAD
## Image: abacosamples/py3_func_v3 ##

This actor will execute an arbitrary Python3 function that is passed to it after serialization via the cloudpickle
library. At the moment, it is important that the function is built in Python 3.7 (or is based on the py3_base_v3
image) and serialized using cloudpickle.

In time, we will build up tooling to automate the process below

### Serialize the function and execute the actor ###

1. create some function:
    ```shell
    >>> def f(a,b, c=1):
            return a+b+c
    ```

2. create a "message" containing the function and the parameters to pass it. Put the function,
the args and the kwargs into separate keys in a dictionary using the keys `func`, `args`,
and `kwargs`, respectively:
    ```shell
    >>> import cloudpickle
    >>> l = [5,7]
    >>> message = cloudpickle.dumps({'func': f, 'args': l, 'kwargs': {'c': 5}})
    ```
Note: here, the `message` object has type bytes

3. Make a request, passing the message as binary data, and make sure to set the Content-Type
header as `application/octet-stream``:
    ```shell
    >>> url = 'https://{}/actors/v2/{}/messages'.format(base_url, actor_id)
    >>> headers = {'Authorization': 'Bearer {}'.format(access_token)}
    >>> headers['Content-Type'] = 'application/octet-stream'
    >>> rsp = requests.post(url, headers=headers, data=message)

    ```

4. Capture the execution id returned from the response and retrieve the result:
    ```shell
    >>> ex_id = rsp.json().get('result').get('executionId')
    >>> r_url = 'https://{}/actors/v2/{}/executions/{}/results'.format(base_url, actor_id, ex_id)
    >>> rsp = requests.get(r_url, headers=headers)
    >>> result = cloudpickle.loads(rsp.content)
    ```
   
### Using tapypi
The Tapis python SDK, tapipy, contains convenience methods for performing the above steps. Using
tapipy, the approach is as follows

```shell
from tapipy.tapis import Tapis
from tapipy.actors import AbacoExecutor

# create the tapipy client and the AbacoExecutor object
t = Tapis(base_url='https://dev.develop.tapis.io', username='...', password='...')
ex = AbacoExecutor(tp=t)

# by default, the AbacoExecutor uses the abacosamples/py3_func_v3. You can change this by passing
# the "image" parameter or passing a recognized value for "context", such as "py3", "sd2e-jupyter", etc.

# define some function you want to call on the Abaco cloud
def f(a, b):
    return a+b
    
# submit an invocation of the function, passing in args:
rs = ex.submit(f, 5, 10)

# rs is an tapipy.actors.AbacoAsyncResponse object; you can do things like check if the execution
# is complete with:
rs.done()
Out[]: True

# when done, retrieve the result:
rs.result()                                                                                                                                     
Out[]: [15]

```
