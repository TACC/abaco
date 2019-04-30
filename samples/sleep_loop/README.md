# Sleep Loop Sample #
# Image: abacosamples/sleep_loop

This sample issues sleep statements in a loop and prints a message within each iteration of the loop. Both the
total number of iterations and the sleep time (in seconds) are optional configurations that can be sent in the message.

 In general, this actor sample xan be used to test longer running executions.

# Example Usage #

```bash
$ curl -H "x-jwt-assertion-DEV-DEVELOP: $jwt" localhost:8000/actors/$aid/messages?PYTHONUNBUFFERED=0 -H "Content-type: application/json" -d '{"sleep": 1, "iterations": 3}'
```


