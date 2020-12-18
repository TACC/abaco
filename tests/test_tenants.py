import os
import sys

# these paths allow for importing modules from the actors package both in the docker container and native when the test
# suite is launched from the command line.
sys.path.append(os.path.split(os.getcwd())[0])
sys.path.append('/actors')
import time

import requests
import json

from util import headers, base_url, case, \
    response_format, basic_response_checks, get_actor_id, check_execution_details, \
    execute_actor, get_tenant, privileged_headers, regular_headers, \
    get_tapis_token_headers, alternative_tenant_headers, check_worker_fields, delete_actors


# ##############################
# tenancy - tests for bleed over
# ##############################
# Testing things, this time with the usual headers, but on the 'admin' tenant rather than 'dev'.

def test_tenant_list_actors(alternative_tenant_headers):
    # passing another tenant should result in 0 actors.
    url = f"{base_url}/actors"
    rsp = requests.get(url, headers=alternative_tenant_headers)
    result = basic_response_checks(rsp)
    assert len(result) == 0

def test_tenant_register_actor(alternative_tenant_headers):
    url = f"{base_url}/actors"
    data = {'image': 'jstubbs/abaco_test', 'name': 'abaco_test_suite_other_tenant', 'stateless': False}
    rsp = requests.post(url, json=data, headers=alternative_tenant_headers)
    result = basic_response_checks(rsp)
    assert 'description' in result
    assert result['image'] == 'jstubbs/abaco_test'
    assert result['name'] == 'abaco_test_suite_other_tenant'
    assert result['id'] is not None

def test_tenant_actor_is_ready(alternative_tenant_headers):
    count = 0
    actor_id = get_actor_id(alternative_tenant_headers, name='abaco_test_suite_other_tenant')
    while count < 10:
        url = f'{base_url}/actors/{actor_id}'
        rsp = requests.get(url, headers=alternative_tenant_headers)
        result = basic_response_checks(rsp)
        if result['status'] == 'READY':
            return
        time.sleep(3)
        count += 1
    assert False

def test_tenant_list_registered_actors(alternative_tenant_headers):
    # passing another tenant should result in 1 actor.
    url = f"{base_url}/actors"
    rsp = requests.get(url, headers=alternative_tenant_headers)
    result = basic_response_checks(rsp)
    assert len(result) == 1

def test_tenant_list_actor(alternative_tenant_headers):
    actor_id = get_actor_id(alternative_tenant_headers, name='abaco_test_suite_other_tenant')
    url = f'{base_url}/actors/{actor_id}'
    rsp = requests.get(url, headers=alternative_tenant_headers)
    result = basic_response_checks(rsp)
    assert 'description' in result
    assert result['image'] == 'jstubbs/abaco_test'
    assert result['name'] == 'abaco_test_suite_other_tenant'
    assert result['id'] is not None

def test_tenant_list_executions(alternative_tenant_headers):
    actor_id = get_actor_id(alternative_tenant_headers, name='abaco_test_suite_other_tenant')
    url = f'{base_url}/actors/{actor_id}/executions'
    rsp = requests.get(url, headers=alternative_tenant_headers)
    result = basic_response_checks(rsp)
    assert len(result.get('executions')) == 0

def test_tenant_list_messages(alternative_tenant_headers):
    actor_id = get_actor_id(alternative_tenant_headers, name='abaco_test_suite_other_tenant')
    url = f'{base_url}/actors/{actor_id}/messages'
    rsp = requests.get(url, headers=alternative_tenant_headers)
    result = basic_response_checks(rsp)
    assert result.get('messages') == 0

def test_tenant_list_workers(alternative_tenant_headers):
    actor_id = get_actor_id(alternative_tenant_headers, name='abaco_test_suite_other_tenant')
    url = f'{base_url}/actors/{actor_id}/workers'
    rsp = requests.get(url, headers=alternative_tenant_headers)
    # workers collection returns the tenant_id since it is an admin api
    result = basic_response_checks(rsp, check_tenant=False)
    assert len(result) > 0
    # get the first worker
    worker = result[0]
    check_worker_fields(worker)



# Clean up
def test_delete_actors(alternative_tenant_headers):
    delete_actors(alternative_tenant_headers)