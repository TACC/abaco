#!/bin/bash
#
# Docker entrypoint for the test suite. This script needs the docker daemon mounted. It will
# To run specific tests, pass the same argument as would be passed to py.test.
#

# Parameter to the entrypoint.
TEST="$1"



# if nothing passed, run the full suite
if [ -z $TEST ]; then
  pytest /tests/test_abaco_core.py
elif [ "$#" -eq 2 ]; then
  TEST="$1 $2"
  echo $TEST
  pytest $TEST
elif [ "$#" -eq 3 ]; then
  TEST="$1 $2 $3"
  pytest $TEST
else
  pytest $TEST
fi