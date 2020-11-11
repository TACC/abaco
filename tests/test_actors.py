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



# Testing
def test_list_actors(headers):
    url = f"{base_url}/actors"
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result) == 0

def test_cors_list_actors(headers):
    url = f"{base_url}/actors"
    headers['Origin'] = 'http://example.com'
    rsp = requests.get(url, headers=headers)
    basic_response_checks(rsp)
    assert 'Access-Control-Allow-Origin' in rsp.headers

def test_cors_options_list_actors(headers):
    url = f"{base_url}/actors"
    headers['Origin'] = 'http://example.com'
    headers['Access-Control-Request-Method'] = 'POST'
    headers['Access-Control-Request-Headers'] = 'X-Requested-With'
    rsp = requests.options(url, headers=headers)
    assert rsp.status_code == 200
    assert 'Access-Control-Allow-Origin' in rsp.headers
    assert 'Access-Control-Allow-Methods' in rsp.headers
    assert 'Access-Control-Allow-Headers' in rsp.headers

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

def test_register_stateless_actor(headers):
    url = f"{base_url}/actors"
    # stateless actors are the default now, so stateless tests should pass without specifying "stateless": True
    data = {'image': 'jstubbs/abaco_test', 'name': 'abaco_test_suite_statelesss'}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert 'description' in result
    assert 'owner' in result
    assert result['owner'] == '_abaco_testuser_admin'
    assert result['image'] == 'jstubbs/abaco_test'
    assert result['name'] == 'abaco_test_suite_statelesss'
    assert result['id'] is not None

def test_register_hints_actor(headers):
    url = f"{base_url}/actors"
    data = {'image': 'abacosamples/wc', 'name': 'abaco_test_suite_hints', 'hints': ['sync', 'test', 'hint_1']}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert 'description' in result
    assert 'owner' in result
    assert result['owner'] == '_abaco_testuser_admin'
    assert result['image'] == 'abacosamples/wc'
    assert result['name'] == 'abaco_test_suite_hints'
    assert result['id'] is not None

def test_register_actor_default_env(headers):
    url = f"{base_url}/actors"
    data = {'image': 'abacosamples/test',
            'name': 'abaco_test_suite_default_env',
            'stateless': True,
            'default_environment': {'default_env_key1': 'default_env_value1',
                                    'default_env_key2': 'default_env_value2'}
            }
    if case == 'camel':
        data.pop('default_environment')
        data['defaultEnvironment']= {'default_env_key1': 'default_env_value1',
                                     'default_env_key2': 'default_env_value2'}
    rsp = requests.post(url, json=data, headers=headers)
    result = basic_response_checks(rsp)
    assert 'description' in result
    assert 'owner' in result
    assert result['owner'] == '_abaco_testuser_admin'
    assert result['image'] == 'abacosamples/test'
    assert result['name'] == 'abaco_test_suite_default_env'
    assert result['id'] is not None

def test_register_actor_func(headers):
    url = f"{base_url}/actors"
    data = {'image': 'abacosamples/py3_func', 'name': 'abaco_test_suite_func'}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert 'description' in result
    assert 'owner' in result
    assert result['owner'] == '_abaco_testuser_admin'
    assert result['image'] == 'abacosamples/py3_func'
    assert result['name'] == 'abaco_test_suite_func'
    assert result['id'] is not None

def test_register_actor_regular_user(regular_headers):
    url = f"{base_url}/actors"
    data = {'image': 'abacosamples/test', 'name': 'abaco_test_suite_regular_user'}
    rsp = requests.post(url, data=data, headers=regular_headers)
    result = basic_response_checks(rsp)
    assert 'description' in result
    assert 'owner' in result
    assert result['owner'] == '_abaco_testuser_regular'
    assert result['image'] == 'abacosamples/test'
    assert result['name'] == 'abaco_test_suite_regular_user'
    assert result['id'] is not None

def test_register_actor_sleep_loop(headers):
    url = f"{base_url}/actors"
    data = {'image': 'abacosamples/sleep_loop', 'name': 'abaco_test_suite_sleep_loop'}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert 'description' in result
    assert 'owner' in result
    assert result['owner'] == '_abaco_testuser_admin'
    assert result['image'] == 'abacosamples/sleep_loop'
    assert result['name'] == 'abaco_test_suite_sleep_loop'
    assert result['id'] is not None

def test_list_actor(headers):
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/{actor_id}'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert 'description' in result
    assert 'owner' in result
    assert 'create_time' or 'createTime' in result
    assert 'last_update_time' or 'lastUpdateTime' in result
    assert result['image'] == 'jstubbs/abaco_test'
    assert result['name'] == 'abaco_test_suite'
    assert result['id'] is not None

def test_list_actor_state(headers):
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/{actor_id}/state'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert 'state' in result

def test_update_actor_state_string(headers):
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/{actor_id}/state'
    rsp = requests.post(url, headers=headers, json='abc')
    result = basic_response_checks(rsp)
    assert 'state' in result
    assert result['state'] == 'abc'

def test_update_actor_state_dict(headers):
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/{actor_id}/state'
    # update the state
    rsp = requests.post(url, headers=headers, json={'foo': 'abc', 'bar': 1, 'baz': True})
    result = basic_response_checks(rsp)
    # retrieve the actor's state:
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert 'state' in result
    assert result['state'] == {'foo': 'abc', 'bar': 1, 'baz': True}



# Invalid Requests - Ensuring proper response when sending invalid requests
def test_invalid_method_list_actors(headers):
    url = f"{base_url}/actors"
    rsp = requests.put(url, headers=headers)
    assert rsp.status_code == 405
    response_format(rsp)

def test_list_nonexistent_actor(headers):
    url = f"{base_url}/actors/bad_actor_id"
    rsp = requests.get(url, headers=headers)
    assert rsp.status_code == 404
    data = json.loads(rsp.content.decode('utf-8'))
    assert data['status'] == 'error'

def test_invalid_method_get_actor(headers):
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/{actor_id}'
    rsp = requests.post(url, headers=headers)
    assert rsp.status_code == 405
    response_format(rsp)

def test_register_without_image(headers):
    url = f'{base_url}/actors'
    rsp = requests.post(url, headers=headers, data={})
    response_format(rsp)
    assert rsp.status_code not in range(1, 399)
    data = json.loads(rsp.content.decode('utf-8'))
    message = data['message']
    assert 'image' in message

def test_register_with_invalid_stateless(headers):
    url = f"{base_url}/actors"
    data = {'image': 'abacosamples/test',
            'name': 'abaco_test_suite_invalid',
            'stateless': "abcd",
            }
    rsp = requests.post(url, json=data, headers=headers)
    response_format(rsp)
    assert rsp.status_code not in range(1, 399)
    data = json.loads(rsp.content.decode('utf-8'))
    message = data['message']
    assert 'stateless' in message

def test_register_with_invalid_container_uid(headers):
    url = f"{base_url}/actors"
    field = 'use_container_uid'
    if case == 'camel':
        field = 'useContainerUid'
    data = {'image': 'abacosamples/test',
            'name': 'abaco_test_suite_invalid',
            'stateless': False,
            field: "abcd"
            }
    rsp = requests.post(url, json=data, headers=headers)
    response_format(rsp)
    assert rsp.status_code not in range(1, 399)
    data = json.loads(rsp.content.decode('utf-8'))
    message = data['message']
    assert field in message

def test_register_with_invalid_def_env(headers):
    url = f"{base_url}/actors"
    field = 'default_environment'
    if case == 'camel':
        field = 'defaultEnvironment'
    data = {'image': 'abacosamples/test',
            'name': 'abaco_test_suite_invalid',
            'stateless': False,
            field: "abcd"
            }
    rsp = requests.post(url, json=data, headers=headers)
    response_format(rsp)
    assert rsp.status_code not in range(1, 399)
    data = json.loads(rsp.content.decode('utf-8'))
    message = data['message']
    assert field in message

def test_cant_register_max_workers_stateful(headers):
    url = f"{base_url}/actors"
    field = 'max_workers'
    if case == 'camel':
        field = 'maxWorkers'
    data = {'image': 'abacosamples/test',
            'name': 'abaco_test_suite_invalid',
            'stateless': False,
            field: 3,
            }
    rsp = requests.post(url, json=data, headers=headers)
    response_format(rsp)
    assert rsp.status_code not in range(1, 399)
    data = json.loads(rsp.content.decode('utf-8'))
    message = data['message']
    assert "stateful actors can only have 1 worker" in message

def test_register_with_put(headers):
    url = f'{base_url}/actors'
    rsp = requests.put(url, headers=headers, data={'image': 'abacosamples/test'})
    response_format(rsp)
    assert rsp.status_code not in range(1, 399)

def test_cant_update_stateless_actor_state(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_statelesss')
    url = f'{base_url}/actors/{actor_id}/state'
    rsp = requests.post(url, headers=headers, data={'state': 'abc'})
    response_format(rsp)
    assert rsp.status_code not in range(1, 399)



# Invalid authorization - Ensuring invalid authorization results in errors
def test_cant_set_max_workers_regular(regular_headers):
    url = f"{base_url}/actors"
    field = 'max_workers'
    if case == 'camel':
        field = 'maxWorkers'
    data = {'image': 'abacosamples/test',
            'name': 'abaco_test_suite_invalid',
            field: 3,
            }
    rsp = requests.post(url, json=data, headers=regular_headers)
    response_format(rsp)
    assert rsp.status_code not in range(1, 399)

def test_cant_set_max_cpus_regular(regular_headers):
    url = f"{base_url}/actors"
    field = 'max_cpus'
    if case == 'camel':
        field = 'maxCpus'
    data = {'image': 'abacosamples/test',
            'name': 'abaco_test_suite_invalid',
            field: 3000000000,
            }
    rsp = requests.post(url, json=data, headers=regular_headers)
    response_format(rsp)
    assert rsp.status_code not in range(1, 399)

def test_cant_set_mem_limit_regular(regular_headers):
    url = f"{base_url}/actors"
    field = 'mem_limit'
    if case == 'camel':
        field = 'memLimit'
    data = {'image': 'abacosamples/test',
            'name': 'abaco_test_suite_invalid',
            field: '3g',
            }
    rsp = requests.post(url, json=data, headers=regular_headers)
    response_format(rsp)
    assert rsp.status_code not in range(1, 399)



# Clean up
def test_delete_actors(headers, regular_headers):
    delete_actors(headers)
    delete_actors(regular_headers)