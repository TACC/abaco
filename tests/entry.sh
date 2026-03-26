#!/bin/bash
#
# Docker entrypoint for the test suite. This script needs the docker daemon mounted.

# tests is an environment variable set in the makefile and defaults to 'tests' to run the entire folder.
# edit to 'tests/test_actors.py' or something else to only run specific test.
#
# tests/test_resilience.py imports the real service modules (stores, auth and the service
# client), which _called_from_within_test switches off -- and the rest of the suite needs
# that flag. So the resilience file runs on its own, without the flag, whenever $TESTS
# includes it; everything else runs as before.
RESILIENCE=/home/tapis/tests/test_resilience.py
rc=0
case "$TESTS" in
  */tests|*/tests/|"$RESILIENCE")
    env -u _called_from_within_test pytest --rootdir=/home/tapis/tests --maxfail $maxErrors "$RESILIENCE" || rc=$?
    ;;
esac
if [ "$TESTS" != "$RESILIENCE" ]; then
  pytest --rootdir=/home/tapis/tests --maxfail $maxErrors --ignore="$RESILIENCE" $TESTS || rc=$?
fi
exit $rc
