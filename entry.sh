#!/bin/bash

# Give permissions to Docker copied folders and files.
# Have to do this as we are running as Tapis user, not root.
# This script requires no permissions.
sudo /home/tapis/actors/folder_permissions.sh /home/tapis/runtime_files

if [ $api = "reg" ]; then
    if [ $server = "dev" ]; then
        python3 -u /home/tapis/actors/reg_api.py
    else
        cd /home/tapis/actors; /usr/local/bin/gunicorn -w $threads -b :5000 reg_api:app
    fi
elif [ $api = "admin" ]; then
    if [ $server = "dev" ]; then
        python3 -u /home/tapis/actors/admin_api.py
    else
        cd /home/tapis/actors; /usr/local/bin/gunicorn -w $threads -b :5000 admin_api:app
    fi
elif [ $api = "metrics" ]; then
    if [ $server = "dev" ]; then
        python3 -u /home/tapis/actors/metrics_api.py
    else
        cd /home/tapis/actors; /usr/local/bin/gunicorn -w $threads -b :5000 metrics_api:app
    fi
elif [ $api = "mes" ]; then
    if [ $server = "dev" ]; then
        python3 -u /home/tapis/actors/message_api.py
    else
        cd /home/tapis/actors; /usr/local/bin/gunicorn -w $threads -b :5000 message_api:app
    fi
fi

while true; do sleep 86400; done
