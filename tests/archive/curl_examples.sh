#!/bin/sh

# Some simple examples exercising the APIs with curl
curl localhost:8000/actors
curl -X POST --data "name=foo&image=abacosamples/test" localhost:8000/actors
curl -X POST --data "name=bar&image=bar_image&description=The bar actor" localhost:8000/actors
curl -X POST --data "name=foo&image=busybox&description=A second foo actor" localhost:8000/actors
curl localhost:8000/actors/foo_0
curl localhost:8000/actors/foo_0/messages
curl -X POST --data "message=testing execution" localhost:8000/actors/foo_0/messages
curl localhost:8000/actors/foo_0/state
curl -X POST --data "state=some state" localhost:8000/actors/foo_0/state
curl localhost:8000/actors/foo_0/state
curl localhost:8000/actors/foo_0/executions
curl -X POST --data "cpu=31&io=114&runtime=8256" localhost:8000/actors/foo_0/executions
curl -X POST --data "cpu=9&io=5&runtime=1000" localhost:8000/actors/foo_0/executions
curl localhost:8000/actors/foo_0/executions
curl -X DELETE localhost:8000/actors/foo_1

# pass json in
curl -X POST -H "Content-Type: application/json" -d '{"image": "jstubbs/abaco_test", "name": "foo", "default_environment":{"key1": "value1"} }' localhost:8000/actors

# CORS support
# basic GET - response should have Access-Control-Allow-Origin
curl -v -H "Origin: http://example.com"  localhost:8000/actors

# options request - response should have Access-Control-Allow-[Origin, Methods, Headers]
curl -v -H "Origin: http://example.com" -H "Access-Control-Request-Method: POST" -H "Access-Control-Request-Headers: X-Requested-With" -X OPTIONS  localhost:8000/actors

# add, message, and update example

curl -X POST --data "name=foo&image=jstubbs/abaco_test" localhost:8000/actors
curl -X POST --data "name=foo&image=jstubbs/abaco_test_nosleep" localhost:8000/actors
curl localhost:8000/actors/foo_0
curl -X POST --data "message=testing execution" localhost:5001/actors/AGAVE_PROD_5vBqV7wRLg6Jr/messages
# AGAVE_PROD_5vBqV7wRLg6Jr
curl -X PUT --data "name=foo&image=jstubbs/abaco_test2" localhost:8000/actors/foo_0

# send JSON data as the message
curl -H "Content-Type: application/json" -d '{"key1": "value1", "key2": "value2" }' -H "Authorization: Bearer $tok" $base/actors/v2/$uuid/messages

# hit the admin endpoint
curl -H "X-Jwt-Assertion-TACC-PROD: $jwta" localhost:8000/actors/admin

# register a bunch of actors
for i in {1..20}; do curl -H "X-Jwt-Assertion-AGAVE-PROD: $jwt" -X POST --data "name=test&image=jstubbs/abaco_test" localhost:8000/actors; done

# send off a bunch of messages:
for i in {1..10}; do for j in {1..20}; do curl -H "X-Jwt-Assertion-AGAVE-PROD: $jwt" -X POST --data "message=test_$i_$j" localhost:8000/actors/test_$i/messages; done; done

for i in {1..10}; do for j in {1..20}; do curl -H "X-Jwt-Assertion-AGAVE-PROD: $jwt" -X POST --data "message=test_$i_$j" "$base/actors/test_$i/messages"; done; done

for i in {1..40}; do curl -H "X-Jwt-Assertion-AGAVE-PROD: $jwt" -X POST --data "message=test number $i" localhost:8000/actors/test_0/messages; done

for i in {1..100}; do echo "Sending message $i"; curl -d "message=test $i" -H "X-Jwt-Assertion-TACC-PROD: $jwt" localhost:8001/actors/6vQrl6LKDlwX6/messages; sleep 1; done

curl -H "X-Jwt-Assertion-AGAVE-PROD: $jwt" -X POST --data "message=testing" localhost:5001/actors/6vQrl6LKDlwX6/messages

for i in {1..100}; do curl -H "X-Jwt-Assertion-AGAVE-PROD: $jwt" -X POST --data "message=testing $i" localhost:5001/actors/MyD3yJBe705Km/messages; sleep 1; done


# start a container with pyredis
docker run --rm -it -v $(pwd)/local-dev.conf:/etc/service.conf jstubbs/abaco_core bash


