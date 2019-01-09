## Image: abacosamples/nonce ##

This image illustrates the use of nonces for sending messages to actors. To start the process, register
an actor with this image and send an initial "start" message. The initial start message requires two
fields:
  * "start": True - tells the nonce actor to start the
  * "iterations": int - the number of iterations between actors.

The first time the actor is messaged, it does the following things:
1. Creates a second actor with the nonce image.
2. Creates a nonce to itself;
3. Sends a message to the second actor with the nonce to itself.

After this initial setup, the two actors message each other, back and forth, until the number of iterations
set in the initial message is met. The first actor tracks these iterations using its state API.
