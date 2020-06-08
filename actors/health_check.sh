#!/bin/bash
#
# Entrypoint for a health check worker process. Runs every ten minutes

# give initial processes some time to launch
sleep 30 #120

# main loop runs every 10 minutes/20 seconds
while :
do
    python3 -u /home/tapis/actors/health.py
    sleep 600  #20
done