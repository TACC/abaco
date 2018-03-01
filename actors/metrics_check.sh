#!/bin/bash
#
# Entrypoint for a health check worker process. Runs every ten minutes

# give initial processes some time to launch
sleep 120

# main loop runs every 10 minutes
while :
do
    python3 -u /actors/metrics.py
    sleep 10
done