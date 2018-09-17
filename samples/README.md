## Sample Actor Images ##

This directory contains code samples that can be used to create actors with various behaviors. We intend to grow this
repository over time to contain actors representing many of the common tasks facing gateways and digital labs built
on Agave.


### Base Image ###
The `abacosamples/base-ubu` image provides the following convenience utilities for writing (Python2) actor functions:
  * `get_context()` - returns a Python dictionary with the following fields:
    * `raw_message` - the original message, either string or JSON depending on the Content-Type.
    * `content_type` - derived from the original message request.
    * `message_dict` - A Python dictionary representing the message (for Content-Type: application/json)
    * `execution_id` - the ID of this execution.
    * `username` - the username of the user that requested the execution.
    * `state` - (for stateful actors) state value at the start of the execution.
    * `actor_id` - the actor's id.
  * `get_client()` - returns a pre-authenticated agavepy.Agave object.
  * `update_state(val)` - Atomically, update the actor's state to the value `val`.


### Creating New Actor Images ###
Create a python module and import the desired functions from the `agavepy.actors` module:
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

Create a Dockerfile that descends from the base image and add your module:
```
from abacosamples/base
ADD myactor.py /myactor.py

CMD ["python", "/myactor.py"]
```


