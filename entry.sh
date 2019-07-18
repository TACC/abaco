#!/bin/bash

if [ $api = "reg" ]; then
    if [ $server = "dev" ]; then
        python3 -u /actors/reg_api.py
    else
        cd /actors; /usr/bin/gunicorn -w $threads -b :5000 reg_api:app
    fi
elif [ $api = "admin" ]; then
    if [ $server = "dev" ]; then
        python3 -u /actors/admin_api.py
    else
        cd /actors; /usr/bin/gunicorn -w $threads -b :5000 admin_api:app
    fi
elif [ $api = "metrics" ]; then
    if [ $server = "dev" ]; then
        python3 -u /actors/metrics_api.py
    else
        cd /actors; /usr/bin/gunicorn -w $threads -b :5000 metrics_api:app
    fi
elif [ $api = "mes" ]; then
    if [ $server = "dev" ]; then
        python3 -u /actors/message_api.py
    else
        cd /actors; /usr/bin/gunicorn -w $threads -b :5000 message_api:app
    fi
fi

while true; do sleep 86400; done