import os
import sys

# these paths allow for importing modules from the actors package both in the docker container and native when the test
# suite is launched from the command line.
sys.path.append(os.path.split(os.getcwd())[0])
sys.path.append('/actors')
import time

import requests
import json
import cloudpickle
import datetime

from util import headers, base_url, case, \
    response_format, basic_response_checks, get_actor_id, check_execution_details, \
    execute_actor, get_tenant, privileged_headers, regular_headers, \
    get_tapis_token_headers, alternative_tenant_headers, delete_actors

def test_register_actor_with_cron(headers):
    url = f"{base_url}/actors"
    if case == 'camel':
        cron_field = 'cronSchedule'
    else:
        cron_field = 'cron_schedule'
    # we need to use "tomorrow" because abaco uses UTC which could be the next day compared to this
    # machine's "now()"
    tomorrow_dt = datetime.datetime.now() + datetime.timedelta(days=1)
    tomorrow_str = tomorrow_dt.strftime("%Y-%m-%d")
    cron_str = f'{tomorrow_str} 23 + 6 hours'

    data = {'image': 'jstubbs/abaco_test', 'name': 'abaco_test_suite_cron', cron_field: cron_str}
    rsp = requests.post(url, json=data, headers=headers)
    result = basic_response_checks(rsp)
    assert result[cron_field] == cron_str

def test_register_actor_with_incorrect_cron(headers):
    url = f"{base_url}/actors"
    if case == 'camel':
        cron_field = 'cronSchedule'
    else:
        cron_field = 'cron_schedule'
    data = {'image': 'jstubbs/abaco_test', 'name': 'abaco_test_suite_cron', cron_field: '2099-06-21 14 + 6 flimflams'}
    rsp = requests.post(url, data=data, headers=headers)
    #result = basic_response_checks(rsp)
    assert rsp.status_code == 500

def test_update_cron(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_cron')
    url = f'{base_url}/actors/{actor_id}'
    if case == 'camel':
        cron_field = 'cronSchedule'
    else:
        cron_field = 'cron_schedule'
    data = {'image': 'jstubbs/abaco_test', 'stateless': False, cron_field: '2099-09-6 20 + 3 months'}
    rsp = requests.put(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert result[cron_field] == '2099-09-6 20 + 3 months'

def test_update_cron_switch(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_cron')
    url = f'{base_url}/actors/{actor_id}'
    if case == 'camel':
        cron_field = 'cronSchedule'
        cron_switch = 'cronOn'
    else:
        cron_field = 'cron_schedule'
        cron_switch = 'cron_on'
    data = {'image': 'jstubbs/abaco_test', 'stateless': False, cron_field: '2099-09-6 20 + 3 months', cron_switch: False}
    rsp = requests.put(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert result['image'] == 'jstubbs/abaco_test'
    assert result[cron_switch] == False


# Clean up
def test_delete_actors(headers):
    delete_actors(headers)