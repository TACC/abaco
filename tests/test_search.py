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
    get_tapis_token_headers, alternative_tenant_headers, delete_actors



# Initialization
def test_register_search_admin_actor_1(headers):
    # 1 worker, 2 executions
    url = f'{base_url}/actors'
    data = {'image': 'jstubbs/abaco_test', 'name': 'search_admin_actor_1', 'stateless': False}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert 'description' in result
    assert 'owner' in result
    assert result['owner'] == '_abaco_testuser_admin'
    assert result['image'] == 'jstubbs/abaco_test'
    assert result['name'] == 'search_admin_actor_1'
    assert result['id'] is not None
    time.sleep(5)

def test_execute_search_admin_actor_1(headers):
    # Execute twice
    actor_id = get_actor_id(headers, name='search_admin_actor_1')
    data = {'message': 'testing execution'}
    execute_actor(headers, actor_id, data=data)
    execute_actor(headers, actor_id, data=data)

def test_register_search_admin_actor_2(headers):
    # 1 worker, 0 execution
    url = f'{base_url}/actors'
    data = {'image': 'abacosamples/sleep_loop', 'name': 'search_admin_actor_2', 'stateless': False}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert 'description' in result
    assert 'owner' in result
    assert result['owner'] == '_abaco_testuser_admin'
    assert result['image'] == 'abacosamples/sleep_loop'
    assert result['name'] == 'search_admin_actor_2'
    assert result['id'] is not None
    time.sleep(5)

def test_register_search_privileged_actor_1(privileged_headers):
    # 2 workers, 3 executions
    url = f'{base_url}/actors'
    data = {'image': 'abacosamples/test', 'name': 'search_privileged_actor_1'}
    rsp = requests.post(url, data=data, headers=privileged_headers)
    result = basic_response_checks(rsp)
    assert 'description' in result
    assert 'owner' in result
    assert result['owner'] == '_abaco_testuser_privileged'
    assert result['image'] == 'abacosamples/test'
    assert result['name'] == 'search_privileged_actor_1'
    assert result['id'] is not None
    time.sleep(5)

def test_create_two_workers_for_search_privileged_actor_1(headers, privileged_headers):
    actor_id = get_actor_id(privileged_headers, name='search_privileged_actor_1')
    url = f'{base_url}/actors/{actor_id}/workers'
    data = {'num': '2'}
    # Headers here are admin_headers and are needed to update workers.
    rsp = requests.post(url, data=data, headers=headers)
    print(f'{rsp.status_code}: {rsp.content}')
    # workers collection returns the tenant_id since it is an admin api
    assert rsp.status_code in [200, 201]
    time.sleep(4)
    rsp = requests.get(url, headers=privileged_headers)
    result = basic_response_checks(rsp, check_tenant=False)
    assert len(result) == 2

def test_execute_search_privileged_actor_1(privileged_headers):
    # Execute thrice
    actor_id = get_actor_id(privileged_headers, name='search_privileged_actor_1')
    data = {'message': 'testing execution'}
    execute_actor(privileged_headers, actor_id, data=data)
    execute_actor(privileged_headers, actor_id, data=data)
    execute_actor(privileged_headers, actor_id, data=data)

def test_register_search_regular_actor_1(regular_headers):
    # 1 worker, 0 executions
    url = f'{base_url}/actors'
    data = {'image': 'abacosamples/test', 'name': 'search_regular_actor_1'}
    rsp = requests.post(url, data=data, headers=regular_headers)
    result = basic_response_checks(rsp)
    assert 'description' in result
    assert 'owner' in result
    assert result['owner'] == '_abaco_testuser_regular'
    assert result['image'] == 'abacosamples/test'
    assert result['name'] == 'search_regular_actor_1'
    assert result['id'] is not None
    time.sleep(5)



# Testing
# #####################################
# search and search authorization tests
# #####################################
# Checking that search is performing correctly for each database and returning correct projections.
# Also checking that permissions work correctly; one user can't read anothers information and 
# admins can read everything. These tests do no work without prior tests.
def test_search_logs_details(headers):
    url = f'{base_url}/actors/search/logs'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    if case == 'snake':
        assert result['_metadata']['count_returned'] == 2
        assert result['_metadata']['record_limit'] == 100
        assert result['_metadata']['records_skipped'] == 0
        assert result['_metadata']['total_count'] == 2
        assert len(result['search']) == result['_metadata']['count_returned']
    else:
        assert result['_metadata']['countReturned'] == 2
        assert result['_metadata']['recordLimit'] == 100
        assert result['_metadata']['recordsSkipped'] == 0
        assert result['_metadata']['totalCount'] == 2
        assert len(result['search']) == result['_metadata']['countReturned']
    assert '_links' in result['search'][0]
    assert 'logs' in result['search'][0]
    assert not '_id' in result['search'][0]
    assert not 'permissions' in result['search'][0]
    assert not 'tenant' in result['search'][0]
    assert not 'exp' in result['search'][0]

def test_search_executions_details(headers):
    url = f'{base_url}/actors/search/executions'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    if case == 'snake':
        assert result['_metadata']['count_returned'] == 2
        assert result['_metadata']['record_limit'] == 100
        assert result['_metadata']['records_skipped'] == 0
        assert result['_metadata']['total_count'] == 2
        assert len(result['search']) == result['_metadata']['count_returned']
    else:
        assert result['_metadata']['countReturned'] == 2
        assert result['_metadata']['recordLimit'] == 100
        assert result['_metadata']['recordsSkipped'] == 0
        assert result['_metadata']['totalCount'] == 2
        assert len(result['search']) == result['_metadata']['countReturned']
    assert '_links' in result['search'][0]
    assert 'executor' in result['search'][0]
    assert 'id' in result['search'][0]
    assert 'io' in result['search'][0]
    assert 'runtime' in result['search'][0]
    assert 'status' in result['search'][0]
    assert result['search'][0]['status'] == 'COMPLETE'
    assert not '_id' in result['search'][0]
    assert not 'permissions' in result['search'][0]
    assert not 'api_server' in result['search'][0]
    assert not 'tenant' in result['search'][0]

def test_search_actors_details(headers):
    url = f'{base_url}/actors/search/actors'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    if case == 'snake':
        assert result['_metadata']['count_returned'] <= 2
        assert result['_metadata']['record_limit'] == 100
        assert result['_metadata']['records_skipped'] == 0
        assert result['_metadata']['total_count'] <= 2
        assert len(result['search']) == result['_metadata']['count_returned']
    else:
        assert result['_metadata']['countReturned'] <= 2
        assert result['_metadata']['recordLimit'] == 100
        assert result['_metadata']['recordsSkipped'] == 0
        assert result['_metadata']['totalCount'] <= 2
        assert len(result['search']) == result['_metadata']['countReturned']
    assert '_links' in result['search'][0]
    assert 'description' in result['search'][0]
    assert 'gid' in result['search'][0]
    assert 'hints' in result['search'][0]
    assert 'link' in result['search'][0]
    assert 'mounts' in result['search'][0]
    assert 'name' in result['search'][0]
    assert 'owner' in result['search'][0]
    assert 'privileged' in result['search'][0]
    assert 'status' in result['search'][0]
    assert result['search'][0]['token'] == 'false'
    assert not '_id' in result['search'][0]
    assert not 'permissions' in result['search'][0]
    assert not 'api_server' in result['search'][0]
    assert not 'executions' in result['search'][0]
    assert not 'tenant' in result['search'][0]
    assert not 'db_id' in result['search'][0]

def test_search_workers_details(headers):
    url = f'{base_url}/actors/search/workers'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    # We don't check _metadata count because autoscaling could change numbers.
    assert 'status' in result['search'][0]
    assert 'id' in result['search'][0]
    assert not '_id' in result['search'][0]
    assert not 'permissions' in result['search'][0]
    assert not 'tenant' in result['search'][0]

# privileged role
def test_search_permissions_priv(privileged_headers):
    # Logs
    url = f'{base_url}/actors/search/logs'
    rsp = requests.get(url, headers=privileged_headers)
    result = basic_response_checks(rsp)
    print(result['_metadata'])
    if case == 'snake':
        assert result['_metadata']['count_returned'] == 3
        assert result['_metadata']['record_limit'] == 100
        assert result['_metadata']['records_skipped'] == 0
        assert result['_metadata']['total_count'] == 3
    else:
        assert result['_metadata']['countReturned'] == 3
        assert result['_metadata']['recordLimit'] == 100
        assert result['_metadata']['recordsSkipped'] == 0
        assert result['_metadata']['totalCount'] == 3
    
    # Executions
    url = f'{base_url}/actors/search/executions'
    rsp = requests.get(url, headers=privileged_headers)
    result = basic_response_checks(rsp)
    print(result['_metadata'])
    if case == 'snake':
        assert result['_metadata']['count_returned'] == 3
        assert result['_metadata']['record_limit'] == 100
        assert result['_metadata']['records_skipped'] == 0
        assert result['_metadata']['total_count'] == 3
    else:
        assert result['_metadata']['countReturned'] == 3
        assert result['_metadata']['recordLimit'] == 100
        assert result['_metadata']['recordsSkipped'] == 0
        assert result['_metadata']['totalCount'] == 3

    # Actors
    url = f'{base_url}/actors/search/actors'
    rsp = requests.get(url, headers=privileged_headers)
    result = basic_response_checks(rsp)
    print(result['_metadata'])
    if case == 'snake':
        assert result['_metadata']['count_returned'] == 1
        assert result['_metadata']['record_limit'] == 100
        assert result['_metadata']['records_skipped'] == 0
        assert result['_metadata']['total_count'] == 1
    else:
        assert result['_metadata']['countReturned'] == 1
        assert result['_metadata']['recordLimit'] == 100
        assert result['_metadata']['recordsSkipped'] == 0
        assert result['_metadata']['totalCount'] == 1

    # Workers
    url = f'{base_url}/actors/search/workers'
    rsp = requests.get(url, headers=privileged_headers)
    result = basic_response_checks(rsp)
    print(result['_metadata'])
    # We don't check _metadata count because autoscaling could change numbers.

# regular role
def test_search_permissions_regular(regular_headers):
    # Logs
    url = f'{base_url}/actors/search/logs'
    rsp = requests.get(url, headers=regular_headers)
    result = basic_response_checks(rsp)
    print(result['_metadata'])
    if case == 'snake':
        assert result['_metadata']['count_returned'] == 0
        assert result['_metadata']['record_limit'] == 100
        assert result['_metadata']['records_skipped'] == 0
        assert result['_metadata']['total_count'] == 0
    else:
        assert result['_metadata']['countReturned'] == 0
        assert result['_metadata']['recordLimit'] == 100
        assert result['_metadata']['recordsSkipped'] == 0
        assert result['_metadata']['totalCount'] == 0
    
    # Executions
    url = f'{base_url}/actors/search/executions'
    rsp = requests.get(url, headers=regular_headers)
    result = basic_response_checks(rsp)
    print(result['_metadata'])
    if case == 'snake':
        assert result['_metadata']['count_returned'] == 0
        assert result['_metadata']['record_limit'] == 100
        assert result['_metadata']['records_skipped'] == 0
        assert result['_metadata']['total_count'] == 0
    else:
        assert result['_metadata']['countReturned'] == 0
        assert result['_metadata']['recordLimit'] == 100
        assert result['_metadata']['recordsSkipped'] == 0
        assert result['_metadata']['totalCount'] == 0

    # Actors
    url = f'{base_url}/actors/search/actors'
    rsp = requests.get(url, headers=regular_headers)
    result = basic_response_checks(rsp)
    print(result['_metadata'])
    if case == 'snake':
        assert result['_metadata']['count_returned'] == 1
        assert result['_metadata']['record_limit'] == 100
        assert result['_metadata']['records_skipped'] == 0
        assert result['_metadata']['total_count'] == 1
    else:
        assert result['_metadata']['countReturned'] == 1
        assert result['_metadata']['recordLimit'] == 100
        assert result['_metadata']['recordsSkipped'] == 0
        assert result['_metadata']['totalCount'] == 1

    # Workers
    url = f'{base_url}/actors/search/workers'
    rsp = requests.get(url, headers=regular_headers)
    result = basic_response_checks(rsp)
    print(result['_metadata'])
    # We don't check _metadata count because autoscaling could change numbers.

def test_search_datetime(headers):
    url = f'{base_url}/actors/search/executions?final_state.StartedAt.gt=2000-05:00'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    print(result)
    assert len(result['search']) == 2

    url = f'{base_url}/actors/search/executions?final_state.StartedAt.gt=2000-01-01T01:00:00.000Z'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result['search']) == 2

    url = f'{base_url}/actors/search/executions?final_state.StartedAt.lt=2000-01-01T01:00'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result['search']) == 0

    url = f'{base_url}/actors/search/executions?message_received_time.between=2000-01-01T01:00,2200-12-30T23:59:59.999Z'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result['search']) == 2

    url = f'{base_url}/actors/search/actors?create_time.between=2000-01-01,2200-01-01'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result['search']) == 2

def test_search_exactsearch_search(headers):
    url = f'{base_url}/actors/search/actors?exactsearch=jstubbs/abaco_test'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result['search']) == 1

    url = f'{base_url}/actors/search/actors?search=sleep_loop'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert result['search'][0]['image'] == 'abacosamples/sleep_loop'

    url = f'{base_url}/actors/search/actors?exactsearch=NOTATHINGWORD'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result['search']) == 0

def test_search_in_nin(headers):
    url = f'{base_url}/actors/search/executions?status.in=["COMPLETE", "READY"]'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result['search']) == 2

    url = f'{base_url}/actors/search/executions?status.nin=["COMPLETE", "READY"]'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result['search']) == 0

def test_search_eq_neq(headers):
    url = f'{base_url}/actors/search/actors?image=abacosamples/sleep_loop'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result['search']) == 1

    url = f'{base_url}/actors/search/actors?image.neq=abacosamples/sleep_loop'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result['search']) == 1

def test_search_like_nlike(headers):
    url = f'{base_url}/actors/search/actors?image.like=sleep_loop'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result['search']) == 1
    
    url = f'{base_url}/actors/search/actors?image.nlike=sleep_loop'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result['search']) == 1

def test_search_skip_limit(headers):
    url = f'{base_url}/actors/search/actors?skip=1&limit=23'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    if case == 'snake':
        assert result['_metadata']['count_returned'] == 1
        assert result['_metadata']['record_limit'] == 23
        assert result['_metadata']['records_skipped'] == 1
        assert result['_metadata']['total_count'] == 2
    else:
        assert result['_metadata']['countReturned'] == 1
        assert result['_metadata']['recordLimit'] == 23
        assert result['_metadata']['recordsSkipped'] == 1
        assert result['_metadata']['totalCount'] == 2



# Clean up
def test_delete_actors(headers, regular_headers, privileged_headers):
    delete_actors(headers)
    delete_actors(regular_headers)
    delete_actors(privileged_headers)