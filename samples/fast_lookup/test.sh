#!/bin/bash

# print the special MSG variable:
echo "Contents of MSG: "$MSG

if [ "$MSG" = "fruit" ]
  echo "apple"
elif [ "$MSG" = "vegetable" ]
  echo "carrot"
elif [ "$MSG" = "color" ]
  echo "green"
elif [ "$MSG" = "month" ]
  echo "december"
else
  echo "unknown"
fi
