import os
import sys

# these paths allow for importing modules from the actors package both in the docker container and native when the test
# suite is launched from the command line.
sys.path.append(os.path.split(os.getcwd())[0])
sys.path.append('/actors')
import time

import requests
import json
import pytest

from util import headers, base_url, case, \
    response_format, basic_response_checks, get_actor_id, check_execution_details, \
    execute_actor, get_tenant, privileged_headers, regular_headers, \
    get_tapis_token_headers, alternative_tenant_headers, delete_actors, cycling_headers



# Initialization
def check_worker_fields(worker):
    assert worker.get('status') in ['READY', 'BUSY']
    assert worker.get('image') == 'jstubbs/abaco_test' or worker.get('image') == 'jstubbs/abaco_test2'
    assert worker.get('location')
    assert worker.get('cid')
    assert worker.get('tenant')
    if case == 'snake':
        assert worker.get('ch_name')
        assert 'last_execution_time' in worker
        assert 'last_health_check_time' in worker
    else:
        assert worker.get('chName')
        assert 'lastExecutionTime' in worker
        assert 'lastHealthCheckTime' in worker

def test_register_actor(headers):
    url = f"{base_url}/actors"
    data = {'image': 'jstubbs/abaco_test', 'name': 'abaco_test_suite', 'stateless': False}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert 'description' in result
    assert 'owner' in result
    assert result['owner'] == '_abaco_testuser_admin'
    assert result['image'] == 'jstubbs/abaco_test'
    assert result['name'] == 'abaco_test_suite'
    assert result['id'] is not None

def test_wait_for_worker_to_pull_image(headers):
    time.sleep(5)



# Testing
def test_list_workers(headers):
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/{actor_id}/workers'
    rsp = requests.get(url, headers=headers)
    # workers collection returns the tenant_id since it is an admin api
    result = basic_response_checks(rsp, check_tenant=False)
    assert len(result) > 0
    # get the first worker
    worker = result[0]
    check_worker_fields(worker)

def test_invalid_method_list_workers(headers):
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/{actor_id}/workers'
    rsp = requests.put(url, headers=headers)
    assert rsp.status_code == 405
    response_format(rsp)

def test_cors_list_workers(headers):
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/{actor_id}/workers'
    headers['Origin'] = 'http://example.com'
    rsp = requests.get(url, headers=headers)
    basic_response_checks(rsp)
    assert 'Access-Control-Allow-Origin' in rsp.headers

def test_cors_options_list_workers(headers):
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/{actor_id}/workers'
    headers['Origin'] = 'http://example.com'
    headers['Access-Control-Request-Method'] = 'POST'
    headers['Access-Control-Request-Headers'] = 'X-Requested-With'
    rsp = requests.options(url, headers=headers)
    assert rsp.status_code == 200
    assert 'Access-Control-Allow-Origin' in rsp.headers
    assert 'Access-Control-Allow-Methods' in rsp.headers
    assert 'Access-Control-Allow-Headers' in rsp.headers


def test_ensure_one_worker(headers):
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/{actor_id}/workers'
    rsp = requests.post(url, headers=headers)
    # workers collection returns the tenant_id since it is an admin api
    assert rsp.status_code in [200, 201]
    time.sleep(8)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp, check_tenant=False)
    assert len(result) == 1

def test_ensure_two_worker(headers):
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/{actor_id}/workers'
    data = {'num': '2'}
    rsp = requests.post(url, data=data, headers=headers)
    # workers collection returns the tenant_id since it is an admin api
    assert rsp.status_code in [200, 201]
    time.sleep(8)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp, check_tenant=False)
    assert len(result) == 2

def test_delete_worker(headers):
    # get the list of workers
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/{actor_id}/workers'
    rsp = requests.get(url, headers=headers)
    # workers collection returns the tenant_id since it is an admin api
    result = basic_response_checks(rsp, check_tenant=False)

    # delete the first one
    id = result[0].get('id')
    url = f'{base_url}/actors/{actor_id}/workers/{id}'
    rsp = requests.delete(url, headers=headers)
    result = basic_response_checks(rsp, check_tenant=False)
    time.sleep(4)

    # get the update list of workers
    url = f'{base_url}/actors/{actor_id}/workers'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp, check_tenant=False)
    assert len(result) == 1

def test_list_permissions(headers):
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/{actor_id}/permissions'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result) == 1

def test_invalid_method_list_permissions(headers):
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/{actor_id}/permissions'
    rsp = requests.put(url, headers=headers)
    assert rsp.status_code == 405
    response_format(rsp)

def test_add_permissions(headers):
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/{actor_id}/permissions'
    data = {'user': 'tester', 'level': 'UPDATE'}
    rsp = requests.post(url, data=data, headers=headers)
    basic_response_checks(rsp)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result) == 2

def test_modify_user_perissions(headers):
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/{actor_id}/permissions'
    data = {'user': 'tester', 'level': 'READ'}
    rsp = requests.post(url, data=data, headers=headers)
    basic_response_checks(rsp)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    # should still only have 2 results; previous call should have
    # modified the user's permission to READ
    assert len(result) == 2


# #########################
# role based access control
# #########################
# The above tests were done with an admin user. In the following tests, we check RBAC with users with different Abaco
# roles. The following defines the role types we check. These strings need to much the sufixes on the jwt files in this
# tests directory.
def test_other_users_can_create_basic_actor(cycling_headers):
    for username, headers in cycling_headers.items():
        url = f"{base_url}/actors"
        data = {'image': 'jstubbs/abaco_test', 'name': f'abaco_test_suite_{username}'}
        rsp = requests.post(url, data=data, headers=headers)
        print(rsp)
        print(rsp.content)
        result = basic_response_checks(rsp)

def test_other_users_actor_list(cycling_headers):
    for username, headers in cycling_headers.items():
        url = f"{base_url}/actors"
        rsp = requests.get(url, headers=headers)
        print(rsp)
        print(rsp.content)
        result = basic_response_checks(rsp)
        # this list should only include the actors for this user.
        assert len(result) == 1

def test_other_users_get_actor(cycling_headers):
    for username, headers in cycling_headers.items():
        actor_id = get_actor_id(headers, f'abaco_test_suite_{username}')
        url = f'{base_url}/actors/{actor_id}'
        rsp = requests.get(url, headers=headers)
        basic_response_checks(rsp)

def test_other_users_can_delete_basic_actor(cycling_headers):
    for username, headers in cycling_headers.items():
        actor_id = get_actor_id(headers, f'abaco_test_suite_{username}')
        url = f'{base_url}/actors/{actor_id}'
        rsp = requests.delete(url, headers=headers)
        basic_response_checks(rsp)

# regular role:
def test_regular_user_cannot_create_priv_actor(regular_headers):
    url = f"{base_url}/actors"
    data = {'image': 'jstubbs/abaco_test', 'name': 'abaco_test_suite', 'privileged': True}
    rsp = requests.post(url, data=data, headers=regular_headers)
    assert rsp.status_code not in range(1, 399)

# privileged role:
def test_priv_user_can_create_priv_actor(privileged_headers):
    url = f"{base_url}/actors"
    data = {'image': 'jstubbs/abaco_test', 'name': 'abaco_test_suite_priv_delete', 'privileged': True}
    rsp = requests.post(url, data=data, headers=privileged_headers)
    result = basic_response_checks(rsp)
    actor_id = result.get('id')
    url = f"{base_url}/actors/{actor_id}"
    rsp = requests.delete(url, headers=privileged_headers)
    basic_response_checks(rsp)



# Clean up
def test_delete_actors(headers, regular_headers, privileged_headers):
    delete_actors(headers)
    delete_actors(regular_headers)
    delete_actors(privileged_headers)