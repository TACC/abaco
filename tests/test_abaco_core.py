# Functional test suite for abaco.
# This test suite now runs in its own docker container. To build the image, run
#     docker build -f Dockerfile-test -t abaco/testsuite .
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
#
# Test Suite Outline
# I_ non-http tests
# II) actor registrations
# III) invlaid http endpoint tests
# IV) check actors are ready
# V) execution tests
# VI) update actor tests
# VII) custom queue tests (WIP)
# VIII) alias tests
# IX) nonce tests
# X) workers tests
# XI) roles and authorization tests
# XII) search and search authorization tests
# XIII) tenancy tests
# XV) events tests

# Design Notes
# 1. The *headers() functions in the util.py module provides the JWTs used for the tests. The plain headers() returns a
#    JWT for a user with the Abaco admin role (username='testuser'), but there is also limited_headers() and
#    priv_headers()
#    for getting JWTs for other users.
# 2. The get_actor_id() function by default returns the abaco_test_suite actor_id but takes an optional name parameter
#     for getting the id of a different actor.
#
# 3. Actors registered and owned by testuser:
# abaco_test_suite -- a stateful actor
# abaco_test_suite_alias  -- add "jane" and "doe" aliases to this actor; same user.
# abaco_test_suite_statelesss
# abaco_test_suite_hints
# abaco_test_suite_default_env
# abaco_test_suite_func
# abaco_test_suite_sleep_loop
#
# 4. Actors registered and owned by testotheruser user (limited):
# abaco_test_suite_limited_user
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
import pytest

from actors import controllers, health, models, codes, stores, spawner
from channels import ActorMsgChannel, CommandChannel
from util import headers, base_url, case, \
    response_format, basic_response_checks, get_actor_id, check_execution_details, \
    execute_actor, get_tenant, priv_headers, limited_headers


# #################
# registration API
# #################

@pytest.mark.regapi
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


@pytest.mark.regapi
def test_permission_NONE_READ():
    assert codes.NONE < codes.READ

@pytest.mark.regapi
def test_permission_NONE_EXECUTE():
    assert codes.NONE < codes.EXECUTE

@pytest.mark.regapi
def test_permission_NONE_UPDATE():
    assert codes.NONE < codes.UPDATE


@pytest.mark.regapi
def test_permission_READ_EXECUTE():
    assert codes.READ < codes.EXECUTE


@pytest.mark.regapi
def test_permission_READ_UPDATE():
    assert codes.READ < codes.UPDATE


@pytest.mark.regapi
def test_permission_EXECUTE_UPDATE():
    assert codes.EXECUTE < codes.UPDATE


@pytest.mark.regapi
def test_list_actors(headers):
    url = '{}/{}'.format(base_url, '/actors')
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result) == 0


@pytest.mark.regapi
def test_invalid_method_list_actors(headers):
    url = '{}/{}'.format(base_url, '/actors')
    rsp = requests.put(url, headers=headers)
    assert rsp.status_code == 405
    response_format(rsp)


@pytest.mark.regapi
def test_list_nonexistent_actor(headers):
    url = '{}/{}'.format(base_url, '/actors/bad_actor_id')
    rsp = requests.get(url, headers=headers)
    assert rsp.status_code == 404
    data = json.loads(rsp.content.decode('utf-8'))
    assert data['status'] == 'error'


@pytest.mark.regapi
def test_cors_list_actors(headers):
    url = '{}/{}'.format(base_url, '/actors')
    headers['Origin'] = 'http://example.com'
    rsp = requests.get(url, headers=headers)
    basic_response_checks(rsp)
    assert 'Access-Control-Allow-Origin' in rsp.headers


@pytest.mark.regapi
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


@pytest.mark.regapi
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


@pytest.mark.aliastest
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


@pytest.mark.regapi
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

@pytest.mark.regapi
def test_register_hints_actor(headers):
    url = '{}/{}'.format(base_url, '/actors')
    data = {'image': 'abacosamples/wc', 'name': 'abaco_test_suite_hints', 'hints': ['sync', 'test', 'hint_1']}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert 'description' in result
    assert 'owner' in result
    assert result['owner'] == 'testuser'
    assert result['image'] == 'abacosamples/wc'
    assert result['name'] == 'abaco_test_suite_hints'
    assert result['id'] is not None


@pytest.mark.regapi
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


@pytest.mark.regapi
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

@pytest.mark.regapi
def test_register_actor_limited_user(headers):
    url = '{}/{}'.format(base_url, '/actors')
    data = {'image': 'abacosamples/test', 'name': 'abaco_test_suite_limited_user'}
    rsp = requests.post(url, data=data, headers=limited_headers())
    result = basic_response_checks(rsp)
    assert 'description' in result
    assert 'owner' in result
    assert result['owner'] == 'testotheruser'
    assert result['image'] == 'abacosamples/test'
    assert result['name'] == 'abaco_test_suite_limited_user'
    assert result['id'] is not None


@pytest.mark.regapi
def test_register_actor_sleep_loop(headers):
    url = '{}/{}'.format(base_url, '/actors')
    data = {'image': 'abacosamples/sleep_loop', 'name': 'abaco_test_suite_sleep_loop'}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert 'description' in result
    assert 'owner' in result
    assert result['owner'] == 'testuser'
    assert result['image'] == 'abacosamples/sleep_loop'
    assert result['name'] == 'abaco_test_suite_sleep_loop'
    assert result['id'] is not None

@pytest.mark.regapi
def test_invalid_method_get_actor(headers):
    actor_id = get_actor_id(headers)
    url = '{}/actors/{}'.format(base_url, actor_id)
    rsp = requests.post(url, headers=headers)
    assert rsp.status_code == 405
    response_format(rsp)


@pytest.mark.regapi
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


@pytest.mark.regapi
def test_list_actor_state(headers):
    actor_id = get_actor_id(headers)
    url = '{}/actors/{}/state'.format(base_url, actor_id)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert 'state' in result


@pytest.mark.regapi
def test_update_actor_state_string(headers):
    actor_id = get_actor_id(headers)
    url = '{}/actors/{}/state'.format(base_url, actor_id)
    rsp = requests.post(url, headers=headers, json='abc')
    result = basic_response_checks(rsp)
    assert 'state' in result
    assert result['state'] == 'abc'


@pytest.mark.regapi
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
@pytest.mark.regapi
def test_register_without_image(headers):
    url = '{}/actors'.format(base_url)
    rsp = requests.post(url, headers=headers, data={})
    response_format(rsp)
    assert rsp.status_code not in range(1, 399)
    data = json.loads(rsp.content.decode('utf-8'))
    message = data['message']
    assert 'image' in message


@pytest.mark.regapi
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


@pytest.mark.regapi
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

@pytest.mark.regapi
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


@pytest.mark.regapi
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


@pytest.mark.regapi
def test_register_with_put(headers):
    url = '{}/actors'.format(base_url)
    rsp = requests.put(url, headers=headers, data={'image': 'abacosamples/test'})
    response_format(rsp)
    assert rsp.status_code not in range(1, 399)


@pytest.mark.regapi
def test_cant_update_stateless_actor_state(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_statelesss')
    url = '{}/actors/{}/state'.format(base_url, actor_id)
    rsp = requests.post(url, headers=headers, data={'state': 'abc'})
    response_format(rsp)
    assert rsp.status_code not in range(1, 399)

# invalid check having to do with authorization
@pytest.mark.regapi
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

@pytest.mark.regapi
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


@pytest.mark.regapi
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


@pytest.mark.regapi
def test_basic_actor_is_ready(headers):
    check_actor_is_ready(headers)

@pytest.mark.aliastest
def test_alias_actor_is_ready(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_alias')
    check_actor_is_ready(headers, actor_id)

@pytest.mark.regapi
def test_hints_actor_is_ready(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_hints')
    check_actor_is_ready(headers, actor_id)

@pytest.mark.regapi
def test_stateless_actor_is_ready(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_statelesss')
    check_actor_is_ready(headers, actor_id)

@pytest.mark.regapi
def test_default_env_actor_is_ready(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_default_env')
    check_actor_is_ready(headers, actor_id)

@pytest.mark.regapi
def test_func_actor_is_ready(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_func')
    check_actor_is_ready(headers, actor_id)

@pytest.mark.regapi
def test_sleep_loop_actor_is_ready(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_sleep_loop')
    check_actor_is_ready(headers, actor_id)

@pytest.mark.regapi
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

def test_execute_and_delete_sleep_loop_actor(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_sleep_loop')
    url = '{}/actors/{}/messages'.format(base_url, actor_id)
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
    url = '{}/actors/{}/executions/{}'.format(base_url, actor_id, exc_id)
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
                  "actor: {}; execution: {}; idx:{}".format(actor_id, exc_id, idx))
            assert False
    # now let's kill the execution -
    time.sleep(1)
    url = '{}/actors/{}/executions/{}'.format(base_url, actor_id, exc_id)
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
    print("Execution never stopped. Last status of execution: {}; "
          "actor_id: {}; execution_id: {}".format(status, actor_id, exc_id))
    assert stopped

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

def test_execute_basic_actor_synchronous(headers):
    actor_id = get_actor_id(headers)
    data = {'message': 'testing execution'}
    execute_actor(headers, actor_id, data=data, synchronous=True)


# ##################
# updates to actors
# ##################

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


###############
# actor queue
#     tests
###############

CH_NAME_1 = 'special'
CH_NAME_2 = 'default'

@pytest.mark.queuetest
def test_create_actor_with_custom_queue_name(headers):
    url = '{}/actors'.format(base_url)
    data = {
        'image': 'jstubbs/abaco_test',
        'name': 'abaco_test_suite_queue1_actor1',
        'stateless': False,
        'queue': CH_NAME_1
    }
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert result['queue'] == CH_NAME_1

@pytest.mark.xfail
@pytest.mark.queuetest
def test_actor_uses_custom_queue(headers):
    url = '{}/actors'.format(base_url)
    # get the actor id of an actor registered on the default queue:
    default_queue_actor_id = get_actor_id(headers, name='abaco_test_suite_statelesss')
    # and the actor id for the actor on the special queue:
    special_queue_actor_id = get_actor_id(headers, name='abaco_test_suite_queue1_actor1')

    # send a request to start a bunch of workers for that actor; this should keep the default
    # spawner busy for some time:
    url = '{}/actors/{}/workers'.format(base_url, default_queue_actor_id)
    data = {'num': '5'}
    rsp = requests.post(url, data=data, headers=headers)

    # now, try to start a second worker for the abaco_test_suite_queue1_actor1 actor.
    url = '{}/actors/{}/workers'.format(base_url, special_queue_actor_id)
    data = {'num': '2'}
    rsp = requests.post(url, data=data, headers=headers)
    basic_response_checks(rsp)
    # ensure that worker is started within a small time window:
    ch = CommandChannel(name=CH_NAME_1)
    i = 0
    while True:
        time.sleep(2)
        if len(ch._queue._queue) == 0:
            break
        i = i + 1
        if i > 10:
            assert False
    # wait for workers to be ready and the shut them down
    url = '{}/actors/{}/workers'.format(base_url, default_queue_actor_id)
    check = True
    i = 0
    while check and i < 10:
        time.sleep(5)
        rsp = requests.get(url, headers=headers)
        result = basic_response_checks(rsp)
        for w in result:
            if w['status'] == 'REQUESTED':
                i = i + 1
                continue
        check = False
    # remove all workers -
    rsp = requests.get(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    for w in result:
        url = '{}/actors/{}/workers/{}'.format(base_url, default_queue_actor_id, w['id'])
        rsp = requests.delete(url, headers=headers)
        basic_response_checks(rsp)
    # check that workers are gone -
    url = '{}/actors/{}/workers'.format(base_url, default_queue_actor_id)
    check = True
    i = 0
    while check and i < 10:
        time.sleep(5)
        rsp = requests.get(url, headers=headers)
        result = basic_response_checks(rsp)
        try:
            if len(result) == 0:
                check = False
            else:
                i = i + 1
        except:
            check = False

# @pytest.mark.queuetest
# def test_custom_actor_queue_with_autoscaling(headers):
#     url = '{}/{}'.format(base_url, '/actors')
#     data = {
#         'image': 'jstubbs/abaco_test',
#         'name': 'abaco_test_queue3',
#         'stateless': False,
#         'queue': CH_NAME_1
#     }
#     rsp = requests.post(url, data=data, headers=headers)
#     result = basic_response_checks(rsp)
#     assert result['queue'] == CH_NAME_1
#
#     actor_id = get_actor_id(headers, name='abaco_test_queue2')
#     data = {'message': 'testing execution'}
#     url = '{}/actors/{}/messages'.format(base_url, actor_id)
#
#     for i in range(50):
#         rsp = requests.post(url, data=data, headers=headers)
#
#     url = '{}/actors/{}/workers'.format(base_url, actor_id)
#     rsp = requests.get(url, headers=headers)
#     # workers collection returns the tenant_id since it is an admin api
#     result = basic_response_checks(rsp, check_tenant=False)
#     assert len(result) > 1
#     # get the first worker
#     # worker = result[0]


@pytest.mark.queuetest
def test_actor_with_default_queue(headers):
    pass


@pytest.mark.queuetest
def test_2_actors_with_different_queues(headers):
    pass


# ##########
# alias API
# ##########

ALIAS_1 = 'jane'
ALIAS_2 = 'doe'


@pytest.mark.aliastest
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


@pytest.mark.aliastest
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


@pytest.mark.aliastest
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


@pytest.mark.aliastest
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


@pytest.mark.aliastest
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


@pytest.mark.aliastest
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


@pytest.mark.aliastest
def test_update_alias(headers):
    # alias UPDATE permissions alone are not sufficient to change the definition of the alias to an actor -
    # the user must have UPDATE access to the underlying actor_id as well.
    url = '{}/actors/aliases/{}'.format(base_url, ALIAS_1)
    # change the alias to point to the "abaco_test_suite" actor:
    actor_id = get_actor_id(headers)
    field = 'actor_id'
    if case == 'camel':
        field = 'actorId'
    data = {field: actor_id}
    rsp = requests.put(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert field in result
    assert result[field] == actor_id
    # now, change the alias back to point to the original "abaco_test_suite_alias" actor:
    actor_id = get_actor_id(headers, name='abaco_test_suite_alias')
    data = {field: actor_id}
    rsp = requests.put(url, data=data, headers=headers)
    result = basic_response_checks(rsp)


@pytest.mark.aliastest
def test_other_user_cant_list_alias(headers):
    url = '{}/actors/aliases/{}'.format(base_url, ALIAS_1)
    rsp = requests.get(url, headers=priv_headers())
    data = response_format(rsp)
    assert rsp.status_code == 400
    assert 'you do not have access to this alias' in data['message']


@pytest.mark.aliastest
def test_add_alias_permission(headers):
    user = 'testshareuser'
    data = {'user': user, 'level': 'UPDATE'}
    url = '{}/actors/aliases/{}/permissions'.format(base_url, ALIAS_1)
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert user in result
    assert result[user] == 'UPDATE'


@pytest.mark.aliastest
def test_other_user_can_now_list_alias(headers):
    url = '{}/actors/aliases/{}'.format(base_url, ALIAS_1)
    rsp = requests.get(url, headers=priv_headers())
    result = basic_response_checks(rsp)
    assert 'alias' in result


@pytest.mark.aliastest
def test_other_user_still_cant_list_actor(headers):
    # alias permissions do not confer access to the actor itself -
    url = '{}/actors/{}'.format(base_url, ALIAS_1)
    rsp = requests.get(url, headers=priv_headers())
    assert rsp.status_code == 400
    data = response_format(rsp)
    assert 'you do not have access to this actor' in data['message']

@pytest.mark.aliastest
def test_other_user_still_cant_update_alias_wo_actor(headers):
    # alias UPDATE permissions alone are not sufficient to change the definition of the alias to an actor -
    # the user must have UPDATE access to the underlying actor_id as well.
    url = '{}/actors/aliases/{}'.format(base_url, ALIAS_1)
    # priv user does not have access to the abaco_test_suite_alias actor
    field = 'actor_id'
    if case == 'camel':
        field = 'actorId'
    data = {field: get_actor_id(headers, name="abaco_test_suite_alias")}

    rsp = requests.put(url, data=data, headers=priv_headers())
    assert rsp.status_code == 400
    data = response_format(rsp)
    assert 'ou do not have UPDATE access to the actor you want to associate with this alias' in data['message']

@pytest.mark.aliastest
def test_other_user_still_cant_create_alias_nonce(headers):
    # alias permissions do not confer access to the actor itself, and alias nonces require BOTH
    # permissions on the alias AND on the actor
    url = '{}/actors/aliases/{}/nonces'.format(base_url, ALIAS_1)
    rsp = requests.get(url, headers=priv_headers())
    assert rsp.status_code == 400
    data = response_format(rsp)
    assert 'you do not have access to this alias and actor' in data['message']


@pytest.mark.aliastest
def test_get_actor_with_alias(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_alias')
    url = '{}/actors/{}'.format(base_url, ALIAS_1)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert result['id'] == actor_id


@pytest.mark.aliastest
def test_get_actor_messages_with_alias(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_alias')
    url = '{}/actors/{}/messages'.format(base_url, ALIAS_1)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert actor_id in result['_links']['self']
    assert 'messages' in result


@pytest.mark.aliastest
def test_get_actor_executions_with_alias(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_alias')
    url = '{}/actors/{}/executions'.format(base_url, ALIAS_1)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert actor_id in result['_links']['self']
    assert 'executions' in result

@pytest.mark.aliastest
def test_create_unlimited_alias_nonce(headers):
    url = '{}/actors/aliases/{}/nonces'.format(base_url, ALIAS_1)
    # passing no data to the POST should use the defaults for a nonce:
    # unlimited uses and EXECUTE level
    rsp = requests.post(url, headers=headers)
    result = basic_response_checks(rsp)
    check_nonce_fields(result, alias=ALIAS_1, level='EXECUTE', max_uses=-1, current_uses=0, remaining_uses=-1)

@pytest.mark.aliastest
def test_redeem_unlimited_alias_nonce(headers):
    # first, get the nonce id:
    url = '{}/actors/aliases/{}/nonces'.format(base_url, ALIAS_1)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    nonce_id = result[0].get('id')
    # sanity check that alias can be used to get the actor
    url = '{}/actors/{}'.format(base_url, ALIAS_1)
    rsp = requests.get(url, headers=headers)
    basic_response_checks(rsp)
    # use the nonce-id and the alias to list the actor
    url = '{}/actors/{}?x-nonce={}'.format(base_url, ALIAS_1, nonce_id)
    # no JWT header -- we're using the nonce
    rsp = requests.get(url)
    basic_response_checks(rsp)

@pytest.mark.aliastest
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


@pytest.mark.aliastest
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

def check_nonce_fields(nonce, actor_id=None, alias=None, nonce_id=None,
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
    if alias:
        assert nonce.get('alias') == alias

    # case-specific checks:
    if case == 'snake':
        if actor_id:
            assert nonce.get('actor_id')
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
        if actor_id:
            assert nonce.get('actorId')
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
    workers = stores.workers_store.items({'actor_id': dbid})
    for worker in workers:
        assert not worker['id'] == id

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


# #####################################
# search and search authorization tests
# #####################################
# Checking that search is performing correctly for each database and returning correct projections.
# Also checking that permissions work correctly; one user can't read anothers information and 
# admins can read everything. These tests do no work without prior tests.

def test_search_logs_details(headers):
    url = '{}/actors/search/logs'.format(base_url)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    if case == 'snake':
        assert result['_metadata']['count_returned'] == 7
        assert result['_metadata']['record_limit'] == 100
        assert result['_metadata']['records_skipped'] == 0
        assert result['_metadata']['total_count'] == 7
        assert len(result['search']) == result['_metadata']['count_returned']
    else:
        assert result['_metadata']['countReturned'] == 7
        assert result['_metadata']['recordLimit'] == 100
        assert result['_metadata']['recordsSkipped'] == 0
        assert result['_metadata']['totalCount'] == 7
        assert len(result['search']) == result['_metadata']['countReturned']
    assert '_links' in result['search'][0]
    assert 'logs' in result['search'][0]
    assert not '_id' in result['search'][0]
    assert not 'permissions' in result['search'][0]
    assert not 'tenant' in result['search'][0]
    assert not 'exp' in result['search'][0]

def test_search_executions_details(headers):
    url = '{}/actors/search/executions'.format(base_url)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    if case == 'snake':
        assert result['_metadata']['count_returned'] == 7
        assert result['_metadata']['record_limit'] == 100
        assert result['_metadata']['records_skipped'] == 0
        assert result['_metadata']['total_count'] == 7
        assert len(result['search']) == result['_metadata']['count_returned']
    else:
        assert result['_metadata']['countReturned'] == 7
        assert result['_metadata']['recordLimit'] == 100
        assert result['_metadata']['recordsSkipped'] == 0
        assert result['_metadata']['totalCount'] == 7
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
    url = '{}/actors/search/actors'.format(base_url)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    if case == 'snake':
        assert result['_metadata']['count_returned'] == 8
        assert result['_metadata']['record_limit'] == 100
        assert result['_metadata']['records_skipped'] == 0
        assert result['_metadata']['total_count'] == 8
        assert len(result['search']) == result['_metadata']['count_returned']
    else:
        assert result['_metadata']['countReturned'] == 8
        assert result['_metadata']['recordLimit'] == 100
        assert result['_metadata']['recordsSkipped'] == 0
        assert result['_metadata']['totalCount'] == 8
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
    url = '{}/actors/search/workers'.format(base_url)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    if case == 'snake':
        assert result['_metadata']['count_returned'] == 13
        assert result['_metadata']['record_limit'] == 100
        assert result['_metadata']['records_skipped'] == 0
        assert result['_metadata']['total_count'] == 13
        assert len(result['search']) == result['_metadata']['count_returned']
    else:
        assert result['_metadata']['countReturned'] == 13
        assert result['_metadata']['recordLimit'] == 100
        assert result['_metadata']['recordsSkipped'] == 0
        assert result['_metadata']['totalCount'] == 13
        assert len(result['search']) == result['_metadata']['countReturned']
    assert 'status' in result['search'][0]
    assert 'id' in result['search'][0]
    assert not '_id' in result['search'][0]
    assert not 'permissions' in result['search'][0]
    assert not 'tenant' in result['search'][0]

# privileged role
def test_search_permissions_priv(headers):
    # Logs
    url = '{}/actors/search/logs'.format(base_url)
    rsp = requests.get(url, headers=priv_headers())
    result = basic_response_checks(rsp)
    print(result['_metadata'])
    if case == 'snake':
        assert result['_metadata']['count_returned'] == 4
        assert result['_metadata']['record_limit'] == 100
        assert result['_metadata']['records_skipped'] == 0
        assert result['_metadata']['total_count'] == 4
    else:
        assert result['_metadata']['countReturned'] == 4
        assert result['_metadata']['recordLimit'] == 100
        assert result['_metadata']['recordsSkipped'] == 0
        assert result['_metadata']['totalCount'] == 4
    
    # Executions
    url = '{}/actors/search/executions'.format(base_url)
    rsp = requests.get(url, headers=priv_headers())
    result = basic_response_checks(rsp)
    print(result['_metadata'])
    if case == 'snake':
        assert result['_metadata']['count_returned'] == 4
        assert result['_metadata']['record_limit'] == 100
        assert result['_metadata']['records_skipped'] == 0
        assert result['_metadata']['total_count'] == 4
    else:
        assert result['_metadata']['countReturned'] == 4
        assert result['_metadata']['recordLimit'] == 100
        assert result['_metadata']['recordsSkipped'] == 0
        assert result['_metadata']['totalCount'] == 4

    # Actors
    url = '{}/actors/search/actors'.format(base_url)
    rsp = requests.get(url, headers=priv_headers())
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
    url = '{}/actors/search/workers'.format(base_url)
    rsp = requests.get(url, headers=priv_headers())
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

# limited role
def test_search_permissions_limited(headers):
    # Logs
    url = '{}/actors/search/logs'.format(base_url)
    rsp = requests.get(url, headers=limited_headers())
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
    url = '{}/actors/search/executions'.format(base_url)
    rsp = requests.get(url, headers=limited_headers())
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
    url = '{}/actors/search/actors'.format(base_url)
    rsp = requests.get(url, headers=limited_headers())
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
    url = '{}/actors/search/workers'.format(base_url)
    rsp = requests.get(url, headers=limited_headers())
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

def test_search_datetime(headers):
    url = '{}/actors/search/executions?final_state.StartedAt.gt=2000-05:00'.format(base_url)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    print(result)
    assert len(result['search']) == 7

    url = '{}/actors/search/executions?final_state.StartedAt.gt=2000-01-01T01:00:00.000Z'.format(base_url)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result['search']) == 7

    url = '{}/actors/search/executions?final_state.StartedAt.lt=2000-01-01T01:00'.format(base_url)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result['search']) == 0

    url = '{}/actors/search/executions?message_received_time.between=2000-01-01T01:00,2200-12-30T23:59:59.999Z'.format(base_url)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result['search']) == 7

    url = '{}/actors/search/actors?create_time.between=2000-01-01,2200-01-01'.format(base_url)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result['search']) == 8

def test_search_exactsearch_search(headers):
    url = '{}/actors/search/actors?exactsearch=abacosamples/sleep_loop'.format(base_url)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result['search']) == 1

    url = '{}/actors/search/actors?search=sleep_loop'.format(base_url)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert result['search'][0]['image'] == 'abacosamples/sleep_loop'

    url = '{}/actors/search/actors?exactsearch=NOTATHINGWORD'.format(base_url)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result['search']) == 0

def test_search_in_nin(headers):
    url = '{}/actors/search/executions?status.in=["COMPLETE", "READY"]'.format(base_url)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result['search']) == 7

    url = '{}/actors/search/executions?status.nin=["COMPLETE", "READY"]'.format(base_url)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result['search']) == 0

def test_search_eq_neq(headers):
    url = '{}/actors/search/actors?image=abacosamples/py3_func'.format(base_url)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result['search']) == 1

    url = '{}/actors/search/actors?image.neq=abacosamples/py3_func'.format(base_url)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result['search']) == 7

def test_search_like_nlike(headers):
    url = '{}/actors/search/actors?image.like=py3_func'.format(base_url)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result['search']) == 1
    
    url = '{}/actors/search/actors?image.nlike=py3_func'.format(base_url)
    rsp = requests.get(url, headers=headers)
    print(rsp.content)
    print(rsp)
    result = basic_response_checks(rsp)
    assert len(result['search']) == 7

def test_search_skip_limit(headers):
    url = '{}/actors/search/actors?skip=4&limit=23'.format(base_url)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    if case == 'snake':
        assert result['_metadata']['count_returned'] == 4
        assert result['_metadata']['record_limit'] == 23
        assert result['_metadata']['records_skipped'] == 4
        assert result['_metadata']['total_count'] == 8
    else:
        assert result['_metadata']['countReturned'] == 4
        assert result['_metadata']['recordLimit'] == 23
        assert result['_metadata']['recordsSkipped'] == 4
        assert result['_metadata']['totalCount'] == 8


# ##############################
# tenancy - tests for bleed over
# ##############################

def switch_tenant_in_header(headers):
    jwt = headers.get('X-Jwt-Assertion-DEV-DEVELOP')
    return {'X-Jwt-Assertion-DEV-STAGING': jwt}

@pytest.mark.tenant
def test_tenant_list_actors(headers):
    # passing another tenant should result in 0 actors.
    headers = switch_tenant_in_header(headers)
    url = '{}/{}'.format(base_url, '/actors')
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result) == 0

@pytest.mark.tenant
def test_tenant_register_actor(headers):
    headers = switch_tenant_in_header(headers)
    url = '{}/{}'.format(base_url, '/actors')
    data = {'image': 'jstubbs/abaco_test', 'name': 'abaco_test_suite_other_tenant', 'stateless': False}
    rsp = requests.post(url, json=data, headers=headers)
    result = basic_response_checks(rsp)
    assert 'description' in result
    assert result['image'] == 'jstubbs/abaco_test'
    assert result['name'] == 'abaco_test_suite_other_tenant'
    assert result['id'] is not None

@pytest.mark.tenant
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

@pytest.mark.tenant
def test_tenant_list_registered_actors(headers):
    # passing another tenant should result in 1 actor.
    headers = switch_tenant_in_header(headers)
    url = '{}/{}'.format(base_url, '/actors')
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result) == 1

@pytest.mark.tenant
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

@pytest.mark.tenant
def test_tenant_list_executions(headers):
    headers = switch_tenant_in_header(headers)
    actor_id = get_actor_id(headers, name='abaco_test_suite_other_tenant')
    url = '{}/actors/{}/executions'.format(base_url, actor_id)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result.get('executions')) == 0

@pytest.mark.tenant
def test_tenant_list_messages(headers):
    headers = switch_tenant_in_header(headers)
    actor_id = get_actor_id(headers, name='abaco_test_suite_other_tenant')
    url = '{}/actors/{}/messages'.format(base_url, actor_id)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert result.get('messages') == 0

@pytest.mark.tenant
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
# events tests
##############

REQUEST_BIN_URL = 'https://enqjwyug892gl.x.pipedream.net'


def check_event_logs(logs):
    assert 'event_time_utc' in logs
    assert 'event_time_display' in logs
    assert 'actor_id' in logs

def test_has_cycles_1():
    links = {'A': 'B',
             'B': 'C',
             'C': 'D'}
    assert not controllers.has_cycles(links)

def test_has_cycles_2():
    links = {'A': 'B',
             'B': 'A',
             'C': 'D'}
    assert controllers.has_cycles(links)

def test_has_cycles_3():
    links = {'A': 'B',
             'B': 'C',
             'C': 'D',
             'D': 'E',
             'E': 'H',
             'H': 'B'}
    assert controllers.has_cycles(links)

def test_has_cycles_4():
    links = {'A': 'B',
             'B': 'C',
             'D': 'E',
             'E': 'H',
             'H': 'J',
             'I': 'J',
             'K': 'J',
             'L': 'M',
             'M': 'J'}
    assert not controllers.has_cycles(links)

def test_has_cycles_5():
    links = {'A': 'B',
             'B': 'C',
             'C': 'C'}
    assert controllers.has_cycles(links)

def test_create_event_link_actor(headers):
    url = '{}/{}'.format(base_url, '/actors')
    data = {'image': 'jstubbs/abaco_test', 'name': 'abaco_test_suite_event-link', 'stateless': False}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)

def test_create_actor_with_link(headers):
    # first, get the actor id of the event_link actor:
    link_actor_id = get_actor_id(headers, name='abaco_test_suite_event-link')
    # register a new actor with link to event_link actor
    url = '{}/{}'.format(base_url, '/actors')
    data = {'image': 'jstubbs/abaco_test',
            'name': 'abaco_test_suite_event',
            'link': link_actor_id}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)

def test_create_actor_with_webhook(headers):
    # register an actor to serve as the webhook target
    url = '{}/{}'.format(base_url, '/actors')
    data = {'image': 'jstubbs/abaco_test', 'name': 'abaco_test_suite_event-webhook', 'stateless': False}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    aid = get_actor_id(headers, name='abaco_test_suite_event-webhook')
    # create a nonce for this actor
    url = '{}/actors/{}/nonces'.format(base_url, aid)
    rsp = requests.post(url, headers=headers)
    result = basic_response_checks(rsp)
    nonce = result['id']
    # in practice, no one should ever do this - the built in link property should be used instead;
    # but this illustrates the use of the webhook feature without relying on external APIs.
    webhook = '{}/actors/{}/messages?x-nonce={}'.format(base_url, aid, nonce)
    # make a new actor with a webhook property that points to the above messages endpoint -
    url = '{}/actors'.format(base_url)
    data = {'image': 'jstubbs/abaco_test',
            'name': 'abaco_test_suite_event-2',
            'webhook': webhook}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    event_aid = result['id']
    # once the new actor is READY, the webhook actor should have gotten a message to
    url = '{}/actors/{}/executions'.format(base_url, aid)
    webhook_ready_ex_id = None
    idx = 0
    while not webhook_ready_ex_id and idx < 35:
        rsp = requests.get(url, headers=headers)
        ex_data = rsp.json().get('result').get('executions')
        if ex_data and len(ex_data) > 0:
            webhook_ready_ex_id = ex_data[0]['id']
            break
        else:
            idx = idx + 1
            time.sleep(1)
    if not webhook_ready_ex_id:
        print("webhook actor never executed. actor_id: {}; webhook_actor_id: {}".format(event_aid, aid))
        assert False
        # wait for linked execution to complete and get logs
    idx = 0
    done = False
    while not done and idx < 20:
        # get executions for linked actor and check status of each
        rsp = requests.get(url, headers=headers)
        ex_data = rsp.json().get('result').get('executions')
        if ex_data[0].get('status') == 'COMPLETE':
            done = True
            break
        else:
            time.sleep(1)
            idx = idx + 1
    if not done:
        print("webhook actor executions never completed. actor: {}; "
              "actor: {}; Final execution data: {}".format(event_aid, aid, ex_data))
        assert False
    # now check the logs from the two executions --
    # first one should be the actor READY message:
    url = '{}/actors/{}/executions/{}/logs'.format(base_url, aid, webhook_ready_ex_id)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    logs = result.get('logs')
    assert "'event_type': 'ACTOR_READY'" in logs
    check_event_logs(logs)


def test_execute_event_actor(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_event')
    data = {'message': 'testing events execution'}
    result = execute_actor(headers, actor_id, data=data)
    exec_id = result['id']
    # now that this execution has completed, check that the linked actor also executed:
    idx = 0
    link_execution_ex_id = None
    link_actor_id = get_actor_id(headers, name='abaco_test_suite_event-link')
    url = '{}/actors/{}/executions'.format(base_url, link_actor_id)
    # the linked actor should get 2 messages - one for the original actor initially being set to READY
    # and a second when the execution sent above completes.
    while not link_execution_ex_id and idx < 35:
        rsp = requests.get(url, headers=headers)
        ex_data = rsp.json().get('result').get('executions')
        if ex_data and len(ex_data) > 1:
            link_ready_ex_id = ex_data[0]['id']
            link_execution_ex_id = ex_data[1]['id']
            break
        else:
            idx = idx + 1
            time.sleep(1)
    if not link_execution_ex_id:
        print("linked actor never executed. actor_id: {}; link_actor_id: {}".format(actor_id, link_actor_id))
        assert False
    # wait for linked execution to complete and get logs
    idx = 0
    done = False
    while not done and idx < 20:
        # get executions for linked actor and check status of each
        rsp = requests.get(url, headers=headers)
        ex_data = rsp.json().get('result').get('executions')
        if ex_data[0].get('status') == 'COMPLETE' and ex_data[1].get('status') == 'COMPLETE':
            done = True
            break
        else:
            time.sleep(1)
            idx = idx + 1
    if not done:
        print("linked actor executions never completed. actor: {}; "
              "linked_actor: {}; Final execution data: {}".format(actor_id, link_actor_id, ex_data))
        assert False
    # now check the logs from the two executions --
    # first one should be the actor READY message:
    url = '{}/actors/{}/executions/{}/logs'.format(base_url, link_actor_id, link_ready_ex_id)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    logs = result.get('logs')
    assert "'event_type': 'ACTOR_READY'" in logs
    check_event_logs(logs)

    # second one should be the actor execution COMPLETE message:
    url = '{}/actors/{}/executions/{}/logs'.format(base_url, link_actor_id, link_execution_ex_id)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    logs = result.get('logs')
    assert "'event_type': 'EXECUTION_COMPLETE'" in logs
    assert 'execution_id' in logs
    check_event_logs(logs)

def test_cant_create_link_with_cycle(headers):
    # this test checks that adding a link to an actor that did not have one that creates a cycle
    # is not allowed.
    # register a new actor with no link
    url = '{}/{}'.format(base_url, '/actors')
    data = {'image': 'jstubbs/abaco_test',
            'name': 'abaco_test_suite_create_link',}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    new_actor_id = result['id']
    # create 5 new actors, each with a link to the one created previously:
    new_actor_ids = []
    for i in range(5):
        data['link'] = new_actor_id
        rsp = requests.post(url, data=data, headers=headers)
        result = basic_response_checks(rsp)
        new_actor_id = result['id']
        new_actor_ids.append(new_actor_id)
    # now, update the first created actor with a link that would create a cycle
    first_aid = new_actor_ids[0]
    data['link'] = new_actor_ids[4]
    url = '{}/actors/{}'.format(base_url, first_aid)
    print("url: {}; data: {}".format(url, data))
    rsp = requests.put(url, data=data, headers=headers)
    assert rsp.status_code == 400
    assert 'this update would result in a cycle of linked actors' in rsp.json().get('message')

def test_cant_update_link_with_cycle(headers):
    # this test checks that an update to a link that would create a cycle is not allowed
    link_actor_id = get_actor_id(headers, name='abaco_test_suite_event-link')
    # register a new actor with link to event_link actor
    url = '{}/{}'.format(base_url, '/actors')
    data = {'image': 'jstubbs/abaco_test',
            'name': 'abaco_test_suite_event',
            'link': link_actor_id}
    # create 5 new actors, each with a link to the one created previously:
    new_actor_ids = []
    for i in range(5):
        rsp = requests.post(url, data=data, headers=headers)
        result = basic_response_checks(rsp)
        new_actor_id = result['id']
        data['link'] = new_actor_id
        new_actor_ids.append(new_actor_id)
    # now, update the first created actor with a link that would great a cycle
    first_aid = new_actor_ids[0]
    data['link'] = new_actor_ids[4]
    url = '{}/actors/{}'.format(base_url, first_aid)
    print("url: {}; data: {}".format(url, data))

    rsp = requests.put(url, data=data, headers=headers)
    assert rsp.status_code == 400
    assert 'this update would result in a cycle of linked actors' in rsp.json().get('message')


##############
# Clean up
##############

def test_remove_final_actors(headers):
    time.sleep(60)
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

def test_limited_remove_final_actors(headers):
    headers = limited_headers()
    #headers = switch_tenant_in_header(headers)
    url = '{}/actors'.format(base_url)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    for act in result:
        url = '{}/actors/{}'.format(base_url, act.get('id'))
        rsp = requests.delete(url, headers=headers)
        result = basic_response_checks(rsp)

def test_clean_up_ipc_dirs():
    health.clean_up_ipc_dirs()