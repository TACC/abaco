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


# Initialization
def test_register_with_log_ex(headers):
    url = '{}/{}'.format(base_url, '/actors')
    field = 'log_ex'
    if case == 'camel':
        field = 'logEx'
    data = {'image': 'jstubbs/abaco_test', 
            'name': 'abaco_test_suite',
            field: '16000'}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    if case == 'camel':
        assert result['logEx'] == 16000
    else:
        assert result['log_ex'] == 16000

def test_update_log_ex(headers):
    actor_id = get_actor_id(headers)
    field = 'log_ex'
    if case == 'camel':
        field = 'logEx'
    url = '{}/actors/{}'.format(base_url, actor_id)
    data = {'image': 'jstubbs/abaco_test',
            field: '20000'}
    rsp = requests.put(url, headers=headers, data=data)
    result = basic_response_checks(rsp)
    assert result['image'] == 'jstubbs/abaco_test'
    if case == 'camel':
        assert result['logEx'] == 20000
    else:
        assert result['log_ex'] == 20000


# Clean up
def test_delete_actors(headers):
    delete_actors(headers)