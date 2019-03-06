Image: abacosamples/binary_message_classifier
---------------------------------------------

`Originally from TACCster tutorial.
<https://github.com/TACC/taccster18_Cloud_Tutorial/tree/master/classifier>`_

Directory contains a "self-contained" image classifier based on the TensorFlow library.  
Modified to take two additional inputs, binary messages, and binary image data.

Binary image data can be used as an input to use the code locally, while binary message  
input allows a connection to Abaco's FIFO pipeline and allows for binary message data to
be read in.

Building image
~~~~~~~~~~~~~~

Image creation can be done with:

.. code-block:: bash

    docker build .

Executing the actor with Python and AgavePy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Setting up an ``AgavePy`` object with token and API address information:

.. code-block:: python

    from agavepy.agave import Agave
    ag = Agave(api_server='https://api.tacc.utexas.edu',
               username='<username>', password='<password>',
               client_name='JPEG_classifier',
               api_key='<api_key>',
               api_secret='<api_secret>')

    ag.get_access_token()
    ag = Agave(api_server='https://api.tacc.utexas.edu/', token=ag.token)

Creating actor with the TensorFlow image classifier docker image:

.. code-block:: python

    my_actor = {'image': 'abacosamples/binary_message_classifier',
                'name': 'JPEG_classifier',
                'description': 'Labels a read in binary image'}
    actor_data = ag.actors.add(body=my_actor)

The following creates a binary message from a JPEG image file:

.. code-block:: python
    
    with open('<path to jpeg image here>', 'rb') as file:
        binary_image = file.read()

Sending binary JPEG file to actor as message with the ``application/octet-stream`` header:

.. code-block:: python

    result = ag.actors.sendMessage(actorId=actor_data['id'],
                                   body={'binary': binary_image},
                                   headers={'Content-Type': 'application/octet-stream'})

The following returns information pertaining to the execution:

.. code-block:: python

    execution = ag.actors.getExecution(actorId=actor_data['id'],
                                       executionId = result['executionId'])

Once the execution has complete, the logs can be called with the following:

.. code-block:: python
    
    exec_info = requests.get('{}/actors/v2/{}/executions/{}'.format(url, actor_id, exec_id),
                             headers={'Authorization': 'Bearer {}'.format(token)})

Extra info
~~~~~~~~~~

There is a non-used entry.sh file in this folder, you can use that along with
uncommenting the final line of the Dockerfile in order to use image urls as
input. The classify_image.py file takes more inputs as well from command line!
