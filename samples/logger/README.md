# Logger Sample #
# Image: abacosamples/logger

This sample prints a long string to test Abaco's logging facility. The length of the string can be controlled by a 
parameter sent via the message.


# Example Usage #
Use the default length (1 MB)
```bash
$ curl -H "x-jwt-assertion-DEV-DEVELOP: $jwt" localhost:8000/actors/$aid/messages?PYTHONUNBUFFERED=0
```

Specify the a message length of 500K 
```bash
$ curl -H "x-jwt-assertion-DEV-DEVELOP: $jwt" localhost:8000/actors/$aid/messages?PYTHONUNBUFFERED=0 -H "Content-type: application/json" -d '{"length": 500000}'
```


