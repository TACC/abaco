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

    # Import the Tapis object
    from tapipy import Tapis

    # Log into you the Tapis service by providing user/pass and url.
    t = Tapis(base_url='https://master.tapis.io',
              tenant_id='master',
              account_type='user',
              username='myuser',
              password='mypass')
        
    # Get tokens that will be used for authentication function calls
    t.get_tokens()

    Creating actor with the TensorFlow image classifier docker image:

.. code-block:: python

    actor_data = t.actors.createActor(image='abacosamples/binary_message_classifier',
                                      name='JPEG_classifier',
                                      description='Labels a read in binary image')

The following creates a binary message from a JPEG image file:

.. code-block:: python
    
    with open('<path to jpeg image here>', 'rb') as file:
        binary_image = file.read()

Sending binary JPEG file to actor as message with the ``application/octet-stream`` header:

.. code-block:: python

    result = t.actors.sendMessage(actor_id = actor_data.id,
                                  request_body = {'body': 'd'},
                                  headers = {'Content-Type': 'application/octet-stream'})

The following returns information pertaining to the execution:

.. code-block:: python

    execution = t.actors.getExecution(actor_id = actor_data.id,
                                      execution_id = result.executionId)

Once the execution has complete, the logs can be called with the following:

.. code-block:: python
    
    logs = t.actors.getExecutionLogs(actor_id = actor_data.id,
                                     execution_id = result.executionId)

Extra info
~~~~~~~~~~

There is a non-used entry.sh file in this folder, you can use that along with
uncommenting the final line of the Dockerfile in order to use image urls as
input. The classify_image.py file takes more inputs as well from command line!
