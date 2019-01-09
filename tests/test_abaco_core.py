# Functional test suite for abaco.
# This test suite now runs in its own docker container. To build the image, run
#     docker build -f Dockerfile-test -t jstubbs/abaco_testsuite .
# from within the tests directory.
#
# To run the tests execute, first start the development stack using:
#  1. export abaco_path=$(pwd)
#  2. docker-compose -f docker-compose-local-db.yml up -d (from within the root directory)
#  3. docker-compose -f docker-compose-local.yml up -d (from within the root directory)
# Then, also from the root directory, execute:
#     docker run -e base_url=http://172.17.0.1:8000 -e case=camel -v $(pwd)/local-dev.conf:/etc/service.conf -it --rm abaco/testsuite$TAG
# Change the -e case=camel to -e case=snake depending on the functionality you want to test.

#
# # --- Original notes for running natively ------
# Start the local development abaco stack (docker-compose-local.yml) and run these tests with py.test from the cwd.
#     $ py.test test_abaco_core.py
#
# Notes:
# 1. Running the tests against the docker-compose-local.yml instance (using local-dev.conf) will use an access_control
#    of none and the tenant configured in local-dev.conf (dev_staging) for all requests (essentially ignore headers).
#
# 2. With access control of type 'none'. abaco reads the tenant from a header "tenant" if present. If not present, it
#    uses the default tenant configured in the abaco.conf file.
#
# 3. most tests appear twice, e.g. "test_list_actors" and "test_tenant_list_actors": The first test uses the default
#    tenant by not setting the tenant header, while the second one sets tenant: abaco_test_suite_tenant; this enables
#    the suite to test tenancy bleed-over.
#
import ast
import os
import sys

# these paths allow for importing modules from the actors package both in the docker container and native when the test
# suite is launched from the command line.
sys.path.append(os.path.split(os.getcwd())[0])
sys.path.append('/actors')
import time

import cloudpickle
import pytest
import requests
import json

from actors import health, models, codes, stores

from util import headers, base_url, case, \
    response_format, basic_response_checks, get_actor_id, check_execution_details, \
    execute_actor, get_tenant, priv_headers, limited_headers


# #################
# registration API
# #################

def test_dict_to_camel():
    dic = {"_links": {"messages": "http://localhost:8000/actors/v2/ca39fac2-60a7-11e6-af60-0242ac110009-059/messages",
                      "owner": "http://localhost:8000/profiles/v2/anonymous",
                      "self": "http://localhost:8000/actors/v2/ca39fac2-60a7-11e6-af60-0242ac110009-059/executions/458ab16c-60a8-11e6-8547-0242ac110008-053"
    },
           "execution_id": "458ab16c-60a8-11e6-8547-0242ac110008-053",
           "msg": "test"
    }
    dcamel = models.dict_to_camel(dic)
    assert 'executionId' in dcamel
    assert dcamel['executionId'] == "458ab16c-60a8-11e6-8547-0242ac110008-053"

def test_permission_NONE_READ():
    assert codes.NONE < codes.READ

def test_permission_NONE_EXECUTE():
    assert codes.NONE < codes.EXECUTE

def test_permission_NONE_UPDATE():
    assert codes.NONE < codes.UPDATE

def test_permission_READ_EXECUTE():
    assert codes.READ < codes.EXECUTE

def test_permission_READ_UPDATE():
    assert codes.READ < codes.UPDATE

def test_permission_EXECUTE_UPDATE():
    assert codes.EXECUTE < codes.UPDATE


def test_list_actors(headers):
    url = '{}/{}'.format(base_url, '/actors')
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result) == 0

def test_invalid_method_list_actors(headers):
    url = '{}/{}'.format(base_url, '/actors')
    rsp = requests.put(url, headers=headers)
    assert rsp.status_code == 405
    response_format(rsp)

def test_list_nonexistent_actor(headers):
    url = '{}/{}'.format(base_url, '/actors/bad_actor_id')
    rsp = requests.get(url, headers=headers)
    assert rsp.status_code == 404
    data = json.loads(rsp.content.decode('utf-8'))
    assert data['status'] == 'error'

def test_cors_list_actors(headers):
    url = '{}/{}'.format(base_url, '/actors')
    headers['Origin'] = 'http://example.com'
    rsp = requests.get(url, headers=headers)
    basic_response_checks(rsp)
    assert 'Access-Control-Allow-Origin' in rsp.headers

def test_cors_options_list_actors(headers):
    url = '{}/{}'.format(base_url, '/actors')
    headers['Origin'] = 'http://example.com'
    headers['Access-Control-Request-Method'] = 'POST'
    headers['Access-Control-Request-Headers'] = 'X-Requested-With'
    rsp = requests.options(url, headers=headers)
    assert rsp.status_code == 200
    assert 'Access-Control-Allow-Origin' in rsp.headers
    assert 'Access-Control-Allow-Methods' in rsp.headers
    assert 'Access-Control-Allow-Headers' in rsp.headers

def test_register_actor(headers):
    url = '{}/{}'.format(base_url, '/actors')
    data = {'image': 'jstubbs/abaco_test', 'name': 'abaco_test_suite', 'stateless': False}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert 'description' in result
    assert 'owner' in result
    assert result['owner'] == 'testuser'
    assert result['image'] == 'jstubbs/abaco_test'
    assert result['name'] == 'abaco_test_suite'
    assert result['id'] is not None

def test_register_alias_actor(headers):
    url = '{}/{}'.format(base_url, '/actors')
    data = {'image': 'jstubbs/abaco_test', 'name': 'abaco_test_suite_alias'}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert 'description' in result
    assert 'owner' in result
    assert result['owner'] == 'testuser'
    assert result['image'] == 'jstubbs/abaco_test'
    assert result['name'] == 'abaco_test_suite_alias'
    assert result['id'] is not None

def test_register_stateless_actor(headers):
    url = '{}/{}'.format(base_url, '/actors')
    # stateless actors are the default now, so stateless tests should pass without specifying "stateless": True
    data = {'image': 'jstubbs/abaco_test', 'name': 'abaco_test_suite_statelesss'}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert 'description' in result
    assert 'owner' in result
    assert result['owner'] == 'testuser'
    assert result['image'] == 'jstubbs/abaco_test'
    assert result['name'] == 'abaco_test_suite_statelesss'
    assert result['id'] is not None

def test_register_actor_default_env(headers):
    url = '{}/{}'.format(base_url, '/actors')
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
    assert result['owner'] == 'testuser'
    assert result['image'] == 'abacosamples/test'
    assert result['name'] == 'abaco_test_suite_default_env'
    assert result['id'] is not None

def test_register_actor_func(headers):
    url = '{}/{}'.format(base_url, '/actors')
    data = {'image': 'abacosamples/py3_func', 'name': 'abaco_test_suite_func'}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert 'description' in result
    assert 'owner' in result
    assert result['owner'] == 'testuser'
    assert result['image'] == 'abacosamples/py3_func'
    assert result['name'] == 'abaco_test_suite_func'
    assert result['id'] is not None

def test_invalid_method_get_actor(headers):
    actor_id = get_actor_id(headers)
    url = '{}/actors/{}'.format(base_url, actor_id)
    rsp = requests.post(url, headers=headers)
    assert rsp.status_code == 405
    response_format(rsp)


def test_list_actor(headers):
    actor_id = get_actor_id(headers)
    url = '{}/actors/{}'.format(base_url, actor_id)
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
    url = '{}/actors/{}/state'.format(base_url, actor_id)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert 'state' in result

def test_update_actor_state_string(headers):
    actor_id = get_actor_id(headers)
    url = '{}/actors/{}/state'.format(base_url, actor_id)
    rsp = requests.post(url, headers=headers, json='abc')
    result = basic_response_checks(rsp)
    assert 'state' in result
    assert result['state'] == 'abc'

def test_update_actor_state_dict(headers):
    actor_id = get_actor_id(headers)
    url = '{}/actors/{}/state'.format(base_url, actor_id)
    # update the state
    rsp = requests.post(url, headers=headers, json={'foo': 'abc', 'bar': 1, 'baz': True})
    result = basic_response_checks(rsp)
    # retrieve the actor's state:
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert 'state' in result
    assert result['state'] == {'foo': 'abc', 'bar': 1, 'baz': True}

# invalid requests
def test_register_without_image(headers):
    url = '{}/actors'.format(base_url)
    rsp = requests.post(url, headers=headers, data={})
    response_format(rsp)
    assert rsp.status_code not in range(1, 399)
    data = json.loads(rsp.content.decode('utf-8'))
    message = data['message']
    assert 'image' in message

def test_register_with_invalid_stateless(headers):
    url = '{}/{}'.format(base_url, '/actors')
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
    url = '{}/{}'.format(base_url, '/actors')
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
    url = '{}/{}'.format(base_url, '/actors')
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
    url = '{}/{}'.format(base_url, '/actors')
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
    url = '{}/actors'.format(base_url)
    rsp = requests.put(url, headers=headers, data={'image': 'abacosamples/test'})
    response_format(rsp)
    assert rsp.status_code not in range(1, 399)

def test_cant_update_stateless_actor_state(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_statelesss')
    url = '{}/actors/{}/state'.format(base_url, actor_id)
    rsp = requests.post(url, headers=headers, data={'state': 'abc'})
    response_format(rsp)
    assert rsp.status_code not in range(1, 399)

# invalid check having to do with authorization
def test_cant_set_max_workers_limited(headers):
    url = '{}/{}'.format(base_url, '/actors')
    field = 'max_workers'
    if case == 'camel':
        field = 'maxWorkers'
    data = {'image': 'abacosamples/test',
            'name': 'abaco_test_suite_invalid',
            field: 3,
            }
    rsp = requests.post(url, json=data, headers=limited_headers())
    response_format(rsp)
    assert rsp.status_code not in range(1, 399)

def test_cant_set_max_cpus_limited(headers):
    url = '{}/{}'.format(base_url, '/actors')
    field = 'max_cpus'
    if case == 'camel':
        field = 'maxCpus'
    data = {'image': 'abacosamples/test',
            'name': 'abaco_test_suite_invalid',
            field: 3000000000,
            }
    rsp = requests.post(url, json=data, headers=limited_headers())
    response_format(rsp)
    assert rsp.status_code not in range(1, 399)

def test_cant_set_mem_limit_limited(headers):
    url = '{}/{}'.format(base_url, '/actors')
    field = 'mem_limit'
    if case == 'camel':
        field = 'memLimit'
    data = {'image': 'abacosamples/test',
            'name': 'abaco_test_suite_invalid',
            field: '3g',
            }
    rsp = requests.post(url, json=data, headers=limited_headers())
    response_format(rsp)
    assert rsp.status_code not in range(1, 399)


# check actors are ready ---

def check_actor_is_ready(headers, actor_id=None):
    count = 0
    if not actor_id:
        actor_id = get_actor_id(headers)
    while count < 10:
        url = '{}/actors/{}'.format(base_url, actor_id)
        rsp = requests.get(url, headers=headers)
        result = basic_response_checks(rsp)
        if result['status'] == 'READY':
            return
        time.sleep(3)
        count += 1
    assert False

def test_basic_actor_is_ready(headers):
    check_actor_is_ready(headers)

def test_alias_actor_is_ready(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_alias')
    check_actor_is_ready(headers, actor_id)

def test_stateless_actor_is_ready(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_statelesss')
    check_actor_is_ready(headers, actor_id)

def test_default_env_actor_is_ready(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_default_env')
    check_actor_is_ready(headers, actor_id)

def test_func_actor_is_ready(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_func')
    check_actor_is_ready(headers, actor_id)

def test_executions_empty_list(headers):
    actor_id = get_actor_id(headers)
    url = '{}/actors/{}/executions'.format(base_url, actor_id)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert 'executions' in result
    assert len(result['executions']) == 0


# ###################
# executions and logs
# ###################

def test_list_executions(headers):
    actor_id = get_actor_id(headers)
    url = '{}/actors/{}/executions'.format(base_url, actor_id)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result.get('executions')) == 0

def test_invalid_method_list_executions(headers):
    actor_id = get_actor_id(headers)
    url = '{}/actors/{}/executions'.format(base_url, actor_id)
    rsp = requests.put(url, headers=headers)
    assert rsp.status_code == 405
    response_format(rsp)

def test_list_messages(headers):
    actor_id = get_actor_id(headers)
    url = '{}/actors/{}/messages'.format(base_url, actor_id)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert result.get('messages') == 0

def test_invalid_method_list_messages(headers):
    actor_id = get_actor_id(headers)
    url = '{}/actors/{}/messages'.format(base_url, actor_id)
    rsp = requests.put(url, headers=headers)
    assert rsp.status_code == 405
    response_format(rsp)

def test_cors_list_messages(headers):
    actor_id = get_actor_id(headers)
    url = '{}/actors/{}/messages'.format(base_url, actor_id)
    headers['Origin'] = 'http://example.com'
    rsp = requests.get(url, headers=headers)
    basic_response_checks(rsp)
    assert 'Access-Control-Allow-Origin' in rsp.headers

def test_cors_options_list_messages(headers):
    actor_id = get_actor_id(headers)
    url = '{}/actors/{}/messages'.format(base_url, actor_id)
    headers['Origin'] = 'http://example.com'
    headers['Access-Control-Request-Method'] = 'POST'
    headers['Access-Control-Request-Headers'] = 'X-Requested-With'
    rsp = requests.options(url, headers=headers)
    assert rsp.status_code == 200
    assert 'Access-Control-Allow-Origin' in rsp.headers
    assert 'Access-Control-Allow-Methods' in rsp.headers
    assert 'Access-Control-Allow-Headers' in rsp.headers

def check_execution_details(result, actor_id, exc_id):
    if case == 'snake':
        assert result.get('actor_id') == actor_id
        assert 'worker_id' in result
        assert 'exit_code' in result
        assert 'final_state' in result
        assert 'message_received_time' in result
        assert 'start_time' in result
    else:
        assert result.get('actorId') == actor_id
        assert 'workerId' in result
        assert 'exitCode' in result
        assert 'finalState' in result
        assert 'messageReceivedTime' in result
        assert 'startTime' in result

    assert result.get('id') == exc_id
    # note: it is possible for io to be 0 in which case an `assert result['io']` will fail.
    assert 'io' in result
    assert 'runtime' in result


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
    url = '{}/actors/{}/executions/{}/logs'.format(base_url, actor_id, exec_id)
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
    url = '{}/actors/{}/executions/{}/results'.format(base_url, actor_id, exec_id)
    rsp = requests.get(url, headers=headers)
    result = cloudpickle.loads(rsp.content)
    assert result == 17

def test_list_execution_details(headers):
    actor_id = get_actor_id(headers)
    # get execution id
    url = '{}/actors/{}/executions'.format(base_url, actor_id)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    exec_id = result.get('executions')[0].get('id')
    url = '{}/actors/{}/executions/{}'.format(base_url, actor_id, exec_id)
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
    url = '{}/actors/{}/executions'.format(base_url, actor_id)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    exec_id = result.get('executions')[0].get('id')
    url = '{}/actors/{}/executions/{}'.format(base_url, actor_id, exec_id)
    rsp = requests.post(url, headers=headers)
    assert rsp.status_code == 405
    response_format(rsp)


def test_invalid_method_get_execution_logs(headers):
    actor_id = get_actor_id(headers)
    url = '{}/actors/{}/executions'.format(base_url, actor_id)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    exec_id = result.get('executions')[0].get('id')
    url = '{}/actors/{}/executions/{}/logs'.format(base_url, actor_id, exec_id)
    rsp = requests.post(url, headers=headers)
    assert rsp.status_code == 405
    response_format(rsp)


def test_list_execution_logs(headers):
    actor_id = get_actor_id(headers)
    # get execution id
    url = '{}/actors/{}/executions'.format(base_url, actor_id)
    rsp = requests.get(url, headers=headers)
    # we don't check tenant because it could (and often does) appear in the logs
    result = basic_response_checks(rsp, check_tenant=False)
    exec_id = result.get('executions')[0].get('id')
    url = '{}/actors/{}/executions/{}/logs'.format(base_url, actor_id, exec_id)
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


def test_update_actor(headers):
    actor_id = get_actor_id(headers)
    url = '{}/actors/{}'.format(base_url, actor_id)
    data = {'image': 'jstubbs/abaco_test2', 'stateless': False}
    rsp = requests.put(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert 'description' in result
    assert result['image'] == 'jstubbs/abaco_test2'
    assert result['name'] == 'abaco_test_suite'
    assert result['id'] is not None

def test_update_actor_other_user(headers):
    actor_id = get_actor_id(headers)
    url = '{}/actors/{}'.format(base_url, actor_id)
    rsp = requests.get(url, headers=headers)
    orig_actor = basic_response_checks(rsp)

    # give other user UPDATE access
    user = 'testshareuser'
    url = '{}/actors/{}/permissions'.format(base_url, actor_id)
    data = {'user': user, 'level': 'UPDATE'}
    rsp = requests.post(url, data=data, headers=headers)
    basic_response_checks(rsp)

    # now, update the actor with another user:
    data = {'image': 'jstubbs/abaco_test2', 'stateless': False}
    url = '{}/actors/{}'.format(base_url, actor_id)
    rsp = requests.put(url, data=data, headers=priv_headers())
    result = basic_response_checks(rsp)
    assert 'description' in result
    assert result['image'] == 'jstubbs/abaco_test2'
    assert result['name'] == 'abaco_test_suite'
    assert result['id'] is not None
    # make sure owner has not changed
    assert result['owner'] == orig_actor['owner']
    # make sure update time has changed
    if case == 'snake':
        assert not result['last_update_time'] == orig_actor['last_update_time']
    else:
        assert not result['lastUpdateTime'] == orig_actor['lastUpdateTime']


# ##########
# alias API
# ##########

ALIAS_1 = 'jane'
ALIAS_2 = 'doe'

def test_add_alias(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_alias')
    url = '{}/actors/aliases'.format(base_url)
    field = 'actor_id'
    if case == 'camel':
        field = 'actorId'
    data = {'alias': ALIAS_1,
            field: actor_id}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert result['alias'] == ALIAS_1
    assert result[field] == actor_id

def test_add_second_alias(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_alias')
    url = '{}/actors/aliases'.format(base_url)
    field = 'actor_id'
    if case == 'camel':
        field = 'actorId'
    # it's OK to have two aliases to the same actor
    data = {'alias': ALIAS_2,
            field: actor_id}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert result['alias'] == ALIAS_2
    assert result[field] == actor_id

def test_cant_add_same_alias(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_alias')
    url = '{}/actors/aliases'.format(base_url)
    field = 'actor_id'
    if case == 'camel':
        field = 'actorId'
    data = {'alias': ALIAS_1,
            field: actor_id}
    rsp = requests.post(url, data=data, headers=headers)
    assert rsp.status_code == 400
    data = response_format(rsp)
    assert 'already exists' in data['message']

def test_list_aliases(headers):
    url = '{}/actors/aliases'.format(base_url)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    field = 'actor_id'
    if case == 'camel':
        field = 'actorId'
    for alias in result:
        assert 'alias' in alias
        assert field in alias

def test_list_alias(headers):
    url = '{}/actors/aliases/{}'.format(base_url, ALIAS_1)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    actor_id = get_actor_id(headers, name='abaco_test_suite_alias')
    field = 'actor_id'
    if case == 'camel':
        field = 'actorId'
    assert field in result
    assert result[field] == actor_id
    assert result['alias'] == ALIAS_1

def test_list_alias_permission(headers):
    # first, get the alias to determine the owner
    url = '{}/actors/aliases/{}'.format(base_url, ALIAS_1)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    owner = result['owner']

    # now check that owner has an update permission -
    url = '{}/actors/aliases/{}/permissions'.format(base_url, ALIAS_1)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert owner in result
    assert result[owner] == 'UPDATE'

def test_other_user_cant_list_alias(headers):
    url = '{}/actors/aliases/{}'.format(base_url, ALIAS_1)
    rsp = requests.get(url, headers=priv_headers())
    data = response_format(rsp)
    assert rsp.status_code == 400
    assert 'you do not have access to this alias' in data['message']

def test_add_alias_permission(headers):
    user = 'testshareuser'
    data = {'user': user, 'level': 'UPDATE'}
    url = '{}/actors/aliases/{}/permissions'.format(base_url, ALIAS_1)
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert user in result
    assert result[user] == 'UPDATE'

def test_other_user_can_now_list_alias(headers):
    url = '{}/actors/aliases/{}'.format(base_url, ALIAS_1)
    rsp = requests.get(url, headers=priv_headers())
    result = basic_response_checks(rsp)
    assert 'alias' in result

def test_other_user_still_cant_list_actor(headers):
    # alias permissions do not confer access to the actor itself -
    url = '{}/actors/{}'.format(base_url, ALIAS_1)
    rsp = requests.get(url, headers=priv_headers())
    assert rsp.status_code == 400
    data = response_format(rsp)
    assert 'you do not have access to this actor' in data['message']

def test_get_actor_with_alias(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_alias')
    url = '{}/actors/{}'.format(base_url, ALIAS_1)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert result['id'] == actor_id

def test_get_actor_messages_with_alias(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_alias')
    url = '{}/actors/{}/messages'.format(base_url, ALIAS_1)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert actor_id in result['_links']['self']
    assert 'messages' in result

def test_get_actor_executions_with_alias(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_alias')
    url = '{}/actors/{}/executions'.format(base_url, ALIAS_1)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert actor_id in result['_links']['self']
    assert 'executions' in result

def test_owner_can_delete_alias(headers):
    url = '{}/actors/aliases/{}'.format(base_url, ALIAS_2)
    rsp = requests.delete(url, headers=headers)
    result = basic_response_checks(rsp)

    # list aliases and make sure it is gone -
    url = '{}/actors/aliases'.format(base_url)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    for alias in result:
        assert not alias['alias'] == ALIAS_2

def test_other_user_can_delete_shared_alias(headers):
    url = '{}/actors/aliases/{}'.format(base_url, ALIAS_1)
    rsp = requests.delete(url, headers=priv_headers())
    basic_response_checks(rsp)

    # list aliases and make sure it is gone -
    url = '{}/actors/aliases'.format(base_url)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    for alias in result:
        assert not alias['alias'] == ALIAS_1


# ################
# nonce API
# ################

def check_nonce_fields(nonce, actor_id=None, nonce_id=None,
                       current_uses=None, max_uses=None, remaining_uses=None, level=None, owner=None):
    """Basic checks of the nonce object returned from the API."""
    nid = nonce.get('id')
    # check that nonce id has a valid tenant:
    assert nid
    assert nid.rsplit('_', 1)[0]
    if nonce_id:
        assert nonce.get('id') == nonce_id
    assert nonce.get('owner')
    if owner:
        assert nonce.get('owner') == owner
    assert nonce.get('level')
    if level:
        assert nonce.get('level') == level
    assert nonce.get('roles')

    # case-specific checks:
    if case == 'snake':
        assert nonce.get('actor_id')
        if actor_id:
            assert nonce.get('actor_id') == actor_id
        assert nonce.get('api_server')
        assert nonce.get('create_time')
        assert 'current_uses' in nonce
        if current_uses:
            assert nonce.get('current_uses') == current_uses
        assert nonce.get('last_use_time')
        assert nonce.get('max_uses')
        if max_uses:
            assert nonce.get('max_uses') == max_uses
        assert 'remaining_uses' in nonce
        if remaining_uses:
            assert nonce.get('remaining_uses') == remaining_uses
    else:
        assert nonce.get('actorId')
        if actor_id:
            assert nonce.get('actorId') == actor_id
        assert nonce.get('apiServer')
        assert nonce.get('createTime')
        assert 'currentUses'in nonce
        if current_uses:
            assert nonce.get('currentUses') == current_uses
        assert nonce.get('lastUseTime')
        assert nonce.get('maxUses')
        if max_uses:
            assert nonce.get('maxUses') == max_uses
        assert 'remainingUses' in nonce
        if remaining_uses:
            assert nonce.get('remainingUses') == remaining_uses

def test_list_empty_nonce(headers):
    actor_id = get_actor_id(headers)
    url = '{}/actors/{}/nonces'.format(base_url, actor_id)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    # initially, no nonces
    assert len(result) == 0

def test_create_unlimited_nonce(headers):
    actor_id = get_actor_id(headers)
    url = '{}/actors/{}/nonces'.format(base_url, actor_id)
    # passing no data to the POST should use the defaults for a nonce:
    # unlimited uses and EXECUTE level
    rsp = requests.post(url, headers=headers)
    result = basic_response_checks(rsp)
    check_nonce_fields(result, level='EXECUTE', max_uses=-1, current_uses=0, remaining_uses=-1)

def test_create_limited_nonce(headers):
    actor_id = get_actor_id(headers)
    url = '{}/actors/{}/nonces'.format(base_url, actor_id)
    if case == 'snake':
        data = {'max_uses': 3, 'level': 'READ'}
    else:
        data = {'maxUses': 3, 'level': 'READ'}
    # unlimited uses and EXECUTE level
    rsp = requests.post(url, headers=headers, data=data)
    result = basic_response_checks(rsp)
    check_nonce_fields(result, actor_id=actor_id, level='READ',
                       max_uses=3, current_uses=0, remaining_uses=3)

def test_list_nonces(headers):
    actor_id = get_actor_id(headers)
    url = '{}/actors/{}/nonces'.format(base_url, actor_id)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    # should now have 2 nonces
    assert len(result) == 2

def test_invalid_method_list_nonces(headers):
    actor_id = get_actor_id(headers)
    url = '{}/actors/{}/nonces'.format(base_url, actor_id)
    rsp = requests.put(url, headers=headers)
    assert rsp.status_code == 405
    response_format(rsp)

def test_redeem_unlimited_nonce(headers):
    actor_id = get_actor_id(headers)
    # first, get the nonce id:
    nonce_id = None
    url = '{}/actors/{}/nonces'.format(base_url, actor_id)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    for nonce in result:
        if case == 'snake':
            if nonce.get('max_uses') == -1:
                nonce_id = nonce.get('id')
        else:
            if nonce.get('maxUses') == -1:
                nonce_id = nonce.get('id')

    # if we didn't find an unlimited nonce, there's a problem:
    assert nonce_id
    # redeem the unlimited nonce for reading:
    url = '{}/actors/{}?x-nonce={}'.format(base_url, actor_id, nonce_id)
    # no JWT header -- we're using the nonce
    rsp = requests.get(url)
    basic_response_checks(rsp)
    url = '{}/actors/{}/executions?x-nonce={}'.format(base_url, actor_id, nonce_id)
    # no JWT header -- we're using the nonce
    rsp = requests.get(url)
    basic_response_checks(rsp)
    url = '{}/actors/{}/messages?x-nonce={}'.format(base_url, actor_id, nonce_id)
    # no JWT header -- we're using the nonce
    rsp = requests.get(url)
    basic_response_checks(rsp)
    # check that we have 3 uses and unlimited remaining uses:
    url = '{}/actors/{}/nonces/{}'.format(base_url, actor_id, nonce_id)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    check_nonce_fields(result, actor_id=actor_id, level='EXECUTE',
                       max_uses=-1, current_uses=3, remaining_uses=-1)
    # redeem the unlimited nonce for executing:
    url = '{}/actors/{}/messages?x-nonce={}'.format(base_url, actor_id, nonce_id)
    rsp = requests.post(url, data={'message': 'test'})
    basic_response_checks(rsp)
    # check that we now have 4 uses and unlimited remaining uses:
    url = '{}/actors/{}/nonces/{}'.format(base_url, actor_id, nonce_id)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    check_nonce_fields(result, actor_id=actor_id, level='EXECUTE',
                       max_uses=-1, current_uses=4, remaining_uses=-1)

def test_redeem_limited_nonce(headers):
    actor_id = get_actor_id(headers)
    # first, get the nonce id:
    nonce_id = None
    url = '{}/actors/{}/nonces'.format(base_url, actor_id)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    for nonce in result:
        if case == 'snake':
            if nonce.get('max_uses') == 3:
                nonce_id = nonce.get('id')
        else:
            if nonce.get('maxUses') == 3:
                nonce_id = nonce.get('id')
    # if we didn't find the limited nonce, there's a problem:
    assert nonce_id
    # redeem the limited nonce for reading:
    url = '{}/actors/{}?x-nonce={}'.format(base_url, actor_id, nonce_id)
    # no JWT header -- we're using the nonce
    rsp = requests.get(url)
    basic_response_checks(rsp)
    # check that we have 1 use and 2 remaining uses:
    url = '{}/actors/{}/nonces/{}'.format(base_url, actor_id, nonce_id)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    check_nonce_fields(result, actor_id=actor_id, level='READ',
                       max_uses=3, current_uses=1, remaining_uses=2)
    # check that attempting to redeem the limited nonce for executing fails:
    url = '{}/actors/{}/messages?x-nonce={}'.format(base_url, actor_id, nonce_id)
    rsp = requests.post(url, data={'message': 'test'})
    assert rsp.status_code not in range(1, 399)
    # try redeeming 3 more times; first two should work, third should fail:
    url = '{}/actors/{}?x-nonce={}'.format(base_url, actor_id, nonce_id)
    # use #2
    rsp = requests.get(url)
    basic_response_checks(rsp)
    # use #3
    rsp = requests.get(url)
    basic_response_checks(rsp)
    # use #4 -- should fail
    rsp = requests.get(url)
    assert rsp.status_code not in range(1, 399)
    # finally, check that nonce has no remaining uses:
    url = '{}/actors/{}/nonces/{}'.format(base_url, actor_id, nonce_id)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    check_nonce_fields(result, actor_id=actor_id, level='READ',
                       max_uses=3, current_uses=3, remaining_uses=0)

def test_invalid_method_get_nonce(headers):
    actor_id = get_actor_id(headers)
    url = '{}/actors/{}/nonces'.format(base_url, actor_id)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    nonce_id = result[0].get('id')
    url = '{}/actors/{}/nonces/{}'.format(base_url, actor_id, nonce_id)
    rsp = requests.post(url, headers=headers)
    assert rsp.status_code == 405
    response_format(rsp)



# ################
# admin API
# ################

def check_worker_fields(worker):
    assert worker.get('image') == 'jstubbs/abaco_test'
    assert worker.get('status') in ['READY', 'BUSY']
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


def test_list_workers(headers):
    actor_id = get_actor_id(headers)
    url = '{}/actors/{}/workers'.format(base_url, actor_id)
    rsp = requests.get(url, headers=headers)
    # workers collection returns the tenant_id since it is an admin api
    result = basic_response_checks(rsp, check_tenant=False)
    assert len(result) > 0
    # get the first worker
    worker = result[0]
    check_worker_fields(worker)

def test_invalid_method_list_workers(headers):
    actor_id = get_actor_id(headers)
    url = '{}/actors/{}/workers'.format(base_url, actor_id)
    rsp = requests.put(url, headers=headers)
    assert rsp.status_code == 405
    response_format(rsp)

def test_cors_list_workers(headers):
    actor_id = get_actor_id(headers)
    url = '{}/actors/{}/workers'.format(base_url, actor_id)
    headers['Origin'] = 'http://example.com'
    rsp = requests.get(url, headers=headers)
    basic_response_checks(rsp)
    assert 'Access-Control-Allow-Origin' in rsp.headers

def test_cors_options_list_workers(headers):
    actor_id = get_actor_id(headers)
    url = '{}/actors/{}/workers'.format(base_url, actor_id)
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
    url = '{}/actors/{}/workers'.format(base_url, actor_id)
    rsp = requests.post(url, headers=headers)
    # workers collection returns the tenant_id since it is an admin api
    assert rsp.status_code in [200, 201]
    time.sleep(8)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp, check_tenant=False)
    assert len(result) == 1

def test_ensure_two_worker(headers):
    actor_id = get_actor_id(headers)
    url = '{}/actors/{}/workers'.format(base_url, actor_id)
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
    url = '{}/actors/{}/workers'.format(base_url, actor_id)
    rsp = requests.get(url, headers=headers)
    # workers collection returns the tenant_id since it is an admin api
    result = basic_response_checks(rsp, check_tenant=False)

    # delete the first one
    id = result[0].get('id')
    url = '{}/actors/{}/workers/{}'.format(base_url, actor_id, id)
    rsp = requests.delete(url, headers=headers)
    result = basic_response_checks(rsp, check_tenant=False)
    time.sleep(4)

    # get the update list of workers
    url = '{}/actors/{}/workers'.format(base_url, actor_id)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp, check_tenant=False)
    assert len(result) == 1

    # check the workers store:
    dbid = models.Actor.get_dbid(get_tenant(headers), actor_id)
    workers = stores.workers_store.get(dbid)
    for k,v in workers.items():
        assert not k == id

def test_list_permissions(headers):
    actor_id = get_actor_id(headers)
    url = '{}/actors/{}/permissions'.format(base_url, actor_id)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result) == 2

def test_invalid_method_list_permissions(headers):
    actor_id = get_actor_id(headers)
    url = '{}/actors/{}/permissions'.format(base_url, actor_id)
    rsp = requests.put(url, headers=headers)
    assert rsp.status_code == 405
    response_format(rsp)

def test_add_permissions(headers):
    actor_id = get_actor_id(headers)
    url = '{}/actors/{}/permissions'.format(base_url, actor_id)
    data = {'user': 'tester', 'level': 'UPDATE'}
    rsp = requests.post(url, data=data, headers=headers)
    basic_response_checks(rsp)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result) == 3

def test_modify_user_perissions(headers):
    actor_id = get_actor_id(headers)
    url = '{}/actors/{}/permissions'.format(base_url, actor_id)
    data = {'user': 'tester', 'level': 'READ'}
    rsp = requests.post(url, data=data, headers=headers)
    basic_response_checks(rsp)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    # should still only have 2 results; previous call should have
    # modified the user's permission to READ
    assert len(result) == 3



# #########################
# role based access control
# #########################
# The above tests were done with an admin user. In the following tests, we check RBAC with users with different Abaco
# roles. The following defines the role types we check. These strings need to much the sufixes on the jwt files in this
# tests directory.
ROLE_TYPES = ['limited', 'privileged', 'user']

def get_role_headers(role_type):
    """
    Return headers with a JWT representing a user with a specific Abaco role. Each role type is represented by a
    *different* user. The valid role_type values are listed above.
     """
    with open('/tests/jwt-abaco_{}'.format(role_type), 'r') as f:
        jwt = f.read()
    jwt_header = os.environ.get('jwt_header', 'X-Jwt-Assertion-AGAVE-PROD')
    return {jwt_header: jwt}

def test_other_users_can_create_basic_actor():
    for r_type in ROLE_TYPES:
        headers = get_role_headers(r_type)
        url = '{}/{}'.format(base_url, '/actors')
        data = {'image': 'jstubbs/abaco_test', 'name': 'abaco_test_suite_{}'.format(r_type)}
        rsp = requests.post(url, data=data, headers=headers)
        result = basic_response_checks(rsp)

def test_other_users_actor_list():
    for r_type in ROLE_TYPES:
        headers = get_role_headers(r_type)
        url = '{}/{}'.format(base_url, '/actors')
        rsp = requests.get(url, headers=headers)
        result = basic_response_checks(rsp)
        # this list should only include the actors for this user.
        assert len(result) == 1

def test_other_users_get_actor():
    for r_type in ROLE_TYPES:
        headers = get_role_headers(r_type)
        actor_id = get_actor_id(headers, 'abaco_test_suite_{}'.format(r_type))
        url = '{}/actors/{}'.format(base_url, actor_id)
        rsp = requests.get(url, headers=headers)
        basic_response_checks(rsp)

def test_other_users_can_delete_basic_actor():
    for r_type in ROLE_TYPES:
        headers = get_role_headers(r_type)
        actor_id = get_actor_id(headers, 'abaco_test_suite_{}'.format(r_type))
        url = '{}/actors/{}'.format(base_url, actor_id)
        rsp = requests.delete(url, headers=headers)
        basic_response_checks(rsp)

# limited role:
def test_limited_user_cannot_create_priv_actor():
    headers = get_role_headers('limited')
    url = '{}/{}'.format(base_url, '/actors')
    data = {'image': 'jstubbs/abaco_test', 'name': 'abaco_test_suite', 'privileged': True}
    rsp = requests.post(url, data=data, headers=headers)
    assert rsp.status_code not in range(1, 399)

# privileged role:
def test_priv_user_can_create_priv_actor():
    headers = get_role_headers('privileged')
    url = '{}/{}'.format(base_url, '/actors')
    data = {'image': 'jstubbs/abaco_test', 'name': 'abaco_test_suite_priv_delete', 'privileged': True}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    actor_id = result.get('id')
    url = '{}/{}/{}'.format(base_url, '/actors', actor_id)
    rsp = requests.delete(url, headers=headers)
    basic_response_checks(rsp)

# ##############################
# tenancy - tests for bleed over
# ##############################

def switch_tenant_in_header(headers):
    jwt = headers.get('X-Jwt-Assertion-DEV-DEVELOP')
    return {'X-Jwt-Assertion-DEV-STAGING': jwt}


def test_tenant_list_actors(headers):
    # passing another tenant should result in 0 actors.
    headers = switch_tenant_in_header(headers)
    url = '{}/{}'.format(base_url, '/actors')
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result) == 0

def test_tenant_register_actor(headers):
    headers = switch_tenant_in_header(headers)
    url = '{}/{}'.format(base_url, '/actors')
    data = {'image': 'jstubbs/abaco_test', 'name': 'abaco_test_suite_other_tenant'}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert 'description' in result
    assert result['image'] == 'jstubbs/abaco_test'
    assert result['name'] == 'abaco_test_suite_other_tenant'
    assert result['id'] is not None

def test_tenant_actor_is_ready(headers):
    headers = switch_tenant_in_header(headers)
    count = 0
    actor_id = get_actor_id(headers, name='abaco_test_suite_other_tenant')
    while count < 10:
        url = '{}/actors/{}'.format(base_url, actor_id)
        rsp = requests.get(url, headers=headers)
        result = basic_response_checks(rsp)
        if result['status'] == 'READY':
            return
        time.sleep(3)
        count += 1
    assert False

def test_tenant_list_registered_actors(headers):
    # passing another tenant should result in 1 actor.
    headers = switch_tenant_in_header(headers)
    url = '{}/{}'.format(base_url, '/actors')
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result) == 1

def test_tenant_list_actor(headers):
    headers = switch_tenant_in_header(headers)
    actor_id = get_actor_id(headers, name='abaco_test_suite_other_tenant')
    url = '{}/actors/{}'.format(base_url, actor_id)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert 'description' in result
    assert result['image'] == 'jstubbs/abaco_test'
    assert result['name'] == 'abaco_test_suite_other_tenant'
    assert result['id'] is not None

def test_tenant_list_executions(headers):
    headers = switch_tenant_in_header(headers)
    actor_id = get_actor_id(headers, name='abaco_test_suite_other_tenant')
    url = '{}/actors/{}/executions'.format(base_url, actor_id)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result.get('executions')) == 0

def test_tenant_list_messages(headers):
    headers = switch_tenant_in_header(headers)
    actor_id = get_actor_id(headers, name='abaco_test_suite_other_tenant')
    url = '{}/actors/{}/messages'.format(base_url, actor_id)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert result.get('messages') == 0

def test_tenant_list_workers(headers):
    headers = switch_tenant_in_header(headers)
    actor_id = get_actor_id(headers, name='abaco_test_suite_other_tenant')
    url = '{}/actors/{}/workers'.format(base_url, actor_id)
    rsp = requests.get(url, headers=headers)
    # workers collection returns the tenant_id since it is an admin api
    result = basic_response_checks(rsp, check_tenant=False)
    assert len(result) > 0
    # get the first worker
    worker = result[0]
    check_worker_fields(worker)


##############
# Clean up
##############

def test_remove_final_actors(headers):
    url = '{}/actors'.format(base_url)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    for act in result:
        url = '{}/actors/{}'.format(base_url, act.get('id'))
        rsp = requests.delete(url, headers=headers)
        result = basic_response_checks(rsp)

def test_tenant_remove_final_actors(headers):
    headers = switch_tenant_in_header(headers)
    url = '{}/actors'.format(base_url)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    for act in result:
        url = '{}/actors/{}'.format(base_url, act.get('id'))
        rsp = requests.delete(url, headers=headers)
        result = basic_response_checks(rsp)

def test_clean_up_ipc_dirs():
    health.clean_up_ipc_dirs()
