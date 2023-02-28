#!/bin/bash
#
# Docker entrypoint for the test suite. This script needs the docker daemon mounted.

# tests is an environment variable set in the makefile and defaults to 'tests' to run the entire folder.
# edit to 'tests/test_actors.py' or something else to only run specific test.
pytest --rootdir=/home/tapis/tests --maxfail $maxErrors $TESTS