#!/bin/bash

for i in `seq 1 100`; do
  curl -X POST --data "message=testing execution" localhost:5001/actors/foo_0/messages
done

# nice clean up:
# docker ps -a | grep 'Exited' | awk '{print $1}' | xargs --no-run-if-empty docker rm
