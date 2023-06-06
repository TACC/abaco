#!/bin/bash
#
# Entrypoint for a health check worker process. Runs every ten minutes


# give initial processes some time to launch
sleep 30

# autoscaling metrics loop runs every 5 seconds
metrics_loop() {
  while true; do
    python3 -u /home/tapis/actors/metrics.py
    sleep 5
  done
}

# cron metrics loop runs every 50 seconds
cron_loop() {
  while true; do
    python3 -u /home/tapis/actors/cron.py
    sleep 50
  done
}

# start the functions in the background
metrics_loop &
cron_loop &

# wait for both functions to exit (which they never will, in this case)
wait