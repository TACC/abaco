#!/bin/bash

# print the special MSG variable:
echo "Contents of MSG: "$MSG

# do a sleep to slow things down
sleep 2

# print the full environment
echo "Environment:"
env

# print the root file system:
echo "Contents of root file system: "
ls /

# check for the default mounts
echo "Checking for contents of mounts:"
if [ -e /_abaco_data1 ]; then
  echo "Mount exsits. Contents:"; ls /_abaco_data1
else
  echo "Mount does not exist"
fi;

# check for work and corral
echo "****Contents of jstubbs work:****"
ls -l /work/01837/jstubbs

echo "****Contents of /corral:****"
ls -l /corral

# Optionally report back: when webhook_url is set, PUT the output there as JSON. It is
# optional so the image keeps working for the plain test suite (tests/test_actors.py
# registers abacosamples/test without a webhook).
if [ -z "$webhook_url" ]; then
  echo "webhook_url not set; not posting results."
  exit 0
fi

# escape MSG for JSON: backslashes FIRST (otherwise the escapes added for quotes and
# newlines get doubled), then quotes, then newlines
escaped_msg=$(printf '%s' "$MSG" | sed 's/\\/\\\\/g' | sed 's/"/\\"/g' | sed ':a;N;$!ba;s/\n/\\n/g')
json_msg="{\"_abaco_actor_id\": \"$_abaco_actor_id\", \"_abaco_execution_id\": \"$_abaco_execution_id\", \"message\": \"$escaped_msg\"}"
# send the JSON to the webhook URL
curl -sS --max-time 30 -X PUT -H "Content-Type: application/json" -d "$json_msg" "$webhook_url"
