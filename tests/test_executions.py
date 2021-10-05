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
    data = {'image': 'abacosamples/py3_func_v3', 'name': 'abaco_test_suite_func'}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert 'description' in result
    assert 'owner' in result
    assert result['owner'] == '_abaco_testuser_admin'
    assert result['image'] == 'abacosamples/py3_func_v3'
    assert result['name'] == 'abaco_test_suite_func'
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



# Testing
def test_list_executions(headers):
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/{actor_id}/executions'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result.get('executions')) == 0

def test_invalid_method_list_executions(headers):
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/{actor_id}/executions'
    rsp = requests.put(url, headers=headers)
    assert rsp.status_code == 405
    response_format(rsp)

def test_list_messages(headers):
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/{actor_id}/messages'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert result.get('messages') == 0

def test_invalid_method_list_messages(headers):
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/{actor_id}/messages'
    rsp = requests.put(url, headers=headers)
    assert rsp.status_code == 405
    response_format(rsp)

def test_cors_list_messages(headers):
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/{actor_id}/messages'
    headers['Origin'] = 'http://example.com'
    rsp = requests.get(url, headers=headers)
    basic_response_checks(rsp)
    assert 'Access-Control-Allow-Origin' in rsp.headers

def test_cors_options_list_messages(headers):
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/{actor_id}/messages'
    headers['Origin'] = 'http://example.com'
    headers['Access-Control-Request-Method'] = 'POST'
    headers['Access-Control-Request-Headers'] = 'X-Requested-With'
    rsp = requests.options(url, headers=headers)
    assert rsp.status_code == 200
    assert 'Access-Control-Allow-Origin' in rsp.headers
    assert 'Access-Control-Allow-Methods' in rsp.headers
    assert 'Access-Control-Allow-Headers' in rsp.headers

def check_execution_details_function(result, actor_id, exc_id):
    check_execution_details(result, actor_id, exc_id)

def test_execute_basic_actor(headers):
    actor_id = get_actor_id(headers)
    data = {'message': 'testing execution'}
    execute_actor(headers, actor_id, data=data)

def test_execute_default_env_actor(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_default_env')
    data = {'message': 'testing execution'}
    result = execute_actor(headers, actor_id, data=data)
    exec_id = result['id']
    # get logs
    url = f'{base_url}/actors/{actor_id}/executions/{exec_id}/logs'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    logs = result.get('logs')
    assert 'default_env_key1' in logs
    assert 'default_env_key2' in logs
    assert 'default_env_value1' in logs
    assert 'default_env_value1' in logs
    assert '_abaco_container_repo' in logs
    assert '_abaco_worker_id' in logs
    assert '_abaco_actor_name' in logs

def test_execute_func_actor(headers):
    # toy function and list to send as a message:
    def f(a, b, c=1):
        return a+b+c
    l = [5, 7]
    message = cloudpickle.dumps({'func': f, 'args': l, 'kwargs': {'c': 5}})
    headers['Content-Type'] = 'application/octet-stream'
    actor_id = get_actor_id(headers, name='abaco_test_suite_func')
    result = execute_actor(headers, actor_id, binary=message)
    exec_id = result['id']
    headers.pop('Content-Type')
    url = f'{base_url}/actors/{actor_id}/executions/{exec_id}/results'
    rsp = requests.get(url, headers=headers)
    result = cloudpickle.loads(rsp.content)
    assert result == 17

def test_execute_and_delete_sleep_loop_actor(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_sleep_loop')
    url = f'{base_url}/actors/{actor_id}/messages'
    # send the sleep loop actor a very long execution so that we can delete it before it ends.
    data = {"sleep": 1, "iterations": 999}

    rsp = requests.post(url, json=data, headers=headers)
    # get the execution id -
    rsp_data = json.loads(rsp.content.decode('utf-8'))
    result = rsp_data['result']
    if case == 'snake':
        assert result.get('execution_id')
        exc_id = result.get('execution_id')
    else:
        assert result.get('executionId')
        exc_id = result.get('executionId')
    # wait for a worker to take the execution -
    url = f'{base_url}/actors/{actor_id}/executions/{exc_id}'
    worker_id = None
    idx = 0
    while not worker_id:
        rsp = requests.get(url, headers=headers).json().get('result')
        if case == 'snake':
            worker_id = rsp.get('worker_id')
        else:
            worker_id = rsp.get('workerId')
        idx += 1
        time.sleep(1)
        if idx > 15:
            print("worker never got sleep_loop execution. "
                  f"actor: {actor_id}; execution: {exc_id}; idx:{idx}")
            assert False
    # now let's kill the execution -
    time.sleep(1)
    url = f'{base_url}/actors/{actor_id}/executions/{exc_id}'
    rsp = requests.delete(url, headers=headers)
    assert rsp.status_code in [200, 201, 202, 203, 204]
    assert 'Issued force quit command for execution' in rsp.json().get('message')
    # make sure execution is stopped in a timely manner
    i = 0
    stopped = False
    status = None
    while i < 20:
        rsp = requests.get(url, headers=headers)
        rsp_data = json.loads(rsp.content.decode('utf-8'))
        status = rsp_data['result']['status']
        if status == 'COMPLETE':
            stopped = True
            break
        time.sleep(1)
        i += 1
    print("Execution never stopped. Last status of execution: {status}; "
          "actor_id: {actor_id}; execution_id: {exc_id}")
    assert stopped

def test_list_execution_details(headers):
    actor_id = get_actor_id(headers)
    # get execution id
    url = f'{base_url}/actors/{actor_id}/executions'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    exec_id = result.get('executions')[0].get('id')
    url = f'{base_url}/actors/{actor_id}/executions/{exec_id}'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    if case == 'snake':
        assert 'actor_id' in result
        assert result['actor_id'] == actor_id
    else:
        assert 'actorId' in result
        assert result['actorId'] == actor_id
    assert 'cpu' in result
    assert 'executor' in result
    assert 'id' in result
    assert 'io' in result
    assert 'runtime' in result
    assert 'status' in result
    assert result['status'] == 'COMPLETE'
    assert result['id'] == exec_id

def test_invalid_method_get_execution(headers):
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/{actor_id}/executions'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    exec_id = result.get('executions')[0].get('id')
    url = f'{base_url}/actors/{actor_id}/executions/{exec_id}'
    rsp = requests.post(url, headers=headers)
    assert rsp.status_code == 405
    response_format(rsp)

def test_invalid_method_get_execution_logs(headers):
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/{actor_id}/executions'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    exec_id = result.get('executions')[0].get('id')
    url = f'{base_url}/actors/{actor_id}/executions/{exec_id}/logs'
    rsp = requests.post(url, headers=headers)
    assert rsp.status_code == 405
    response_format(rsp)

def test_list_execution_logs(headers):
    actor_id = get_actor_id(headers)
    # get execution id
    url = f'{base_url}/actors/{actor_id}/executions'
    rsp = requests.get(url, headers=headers)
    # we don't check tenant because it could (and often does) appear in the logs
    result = basic_response_checks(rsp, check_tenant=False)
    exec_id = result.get('executions')[0].get('id')
    url = f'{base_url}/actors/{actor_id}/executions/{exec_id}/logs'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp, check_tenant=False)
    assert 'Contents of MSG: testing execution' in result['logs']
    assert 'PATH' in result['logs']
    assert '_abaco_actor_id' in result['logs']
    assert '_abaco_api_server' in result['logs']
    assert '_abaco_actor_state' in result['logs']
    assert '_abaco_username' in result['logs']
    assert '_abaco_execution_id' in result['logs']
    assert '_abaco_Content_Type' in result['logs']

def test_execute_actor_json(headers):
    actor_id = get_actor_id(headers)
    data = {'key1': 'value1', 'key2': 'value2'}
    execute_actor(headers, actor_id=actor_id, json_data=data)

def test_execute_basic_actor_synchronous(headers):
    actor_id = get_actor_id(headers)
    data = {'message': 'testing execution'}
    execute_actor(headers, actor_id, data=data, synchronous=True)



# Clean up
def test_delete_actors(headers):
    delete_actors(headers)