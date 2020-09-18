#!/bin/bash

# print the special MSG variable:
echo "Contents of MSG: "$MSG

# do a sleep to slow things down
sleep 1

# print the full environment
echo "Environment:"
env

# print the root file system:
echo "Contents of root file system: "
ls /