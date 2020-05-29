#!/bin/bash

# If the python code errors, container will exit. As exit code 1
# corresponds with error and exit code 0 corresponds with success.
if ! python3 -u /actors/conf_check.py; then
	echo "Config error. Stopping Container."
	exit
fi

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
