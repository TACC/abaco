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

from util import headers, base_url, case, \
    response_format, basic_response_checks, get_actor_id, check_execution_details, \
    execute_actor, get_tenant, privileged_headers, regular_headers, \
    get_tapis_token_headers, alternative_tenant_headers, delete_actors

def test_register_actor_with_cron(headers):
    url = '{}/{}'.format(base_url, 'actors')
    data = {'image': 'jstubbs/abaco_test', 'name': 'abaco_test_suite', 'cronSchedule': '2020-06-21 14 + 6 hours'} 
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert result['cronSchedule'] == '2020-06-21 14 + 6 hours'

def test_register_actor_with_incorrect_cron(headers):
    url = '{}/{}'.format(base_url, 'actors')
    data = {'image': 'jstubbs/abaco_test', 'name': 'abaco_test_suite', 'cronSchedule': '2020-06-21 14 + 6 flimflams'} 
    rsp = requests.post(url, data=data, headers=headers)
    #result = basic_response_checks(rsp)
    assert rsp.status_code == 500

def test_update_cron(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite')
    url = '{}/actors/{}'.format(base_url, actor_id)
    data = {'image': 'jstubbs/abaco_test', 'stateless': False, 'cronSchedule': '2021-09-6 20 + 3 months'}
    rsp = requests.put(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert result['cronSchedule'] == '2021-09-6 20 + 3 months'

def test_update_cron_switch(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite')
    url = '{}/actors/{}'.format(base_url, actor_id)
    data = {'image': 'jstubbs/abaco_test', 'stateless': False, 'cronSchedule': '2021-09-6 20 + 3 months', 'cronOn': False}
    rsp = requests.put(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert result['image'] == 'jstubbs/abaco_test'
    assert result['cronOn'] == False



# Clean up
def test_delete_actors(headers):
    delete_actors(headers)