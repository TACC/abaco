## Image: abacosamples/py3_func ##

This actor will execute an arbitrary Python3 function that is passed to it after serialization via the cloudpickle
library. At the moment, it is important that the function is built in Python 3.5 (or is based on the py3_base
image) and serialized using cloudpickle.

In time, we will build up tooling to automate the process below

### Serialize the funtion and execute the actor ###

1. create some function:
    ```shell
    >>> def f(a,b, c=1):
            return a+b+c
    ```

2. create a "message" containg the function and the parameters to pass it. Put the function,
the args and the kwargs into separate keys in a dictionary using the keys `func`, `args`,
and `kwargs`, repectively:
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
