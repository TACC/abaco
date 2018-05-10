#!/bin/bash

# print the special MSG variable:
echo "Contents of MSG: "$MSG

if [ "$MSG" = "fruit" ] ; then
  echo "apple"
elif [ "$MSG" = "vegetable" ] ; then
  echo "carrot"
elif [ "$MSG" = "color" ] ; then
  echo "green"
elif [ "$MSG" = "month" ] ; then
  echo "december"
else
  echo "unknown"
fi
