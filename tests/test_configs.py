import os
import sys

# these paths allow for importing modules from the actors package both in the docker container and native when the test
# suite is launched from the command line.
sys.path.append(os.path.split(os.getcwd())[0])
sys.path.append('/actors')

import requests

from util import headers, base_url, case, \
    response_format, basic_response_checks, get_actor_id, check_execution_details, \
    execute_actor, get_tenant, privileged_headers, regular_headers, \
    get_tapis_token_headers, alternative_tenant_headers, delete_actors


# Setup
def test_register_actor(headers):
    url = f'{base_url}/actors'
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
    url = f'{base_url}/actors'
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


# Testing
def test_register_config(headers):
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/configs'
    data = {"image": "abacosamples/test",
            "name": "the_config",
            "value": "my value",
            "actors": actor_id}
    if case == 'snake':
        data['is_secret'] = False
    else:
        data['isSecret'] = False
    rsp = requests.post(url, json=data, headers=headers)
    result = basic_response_checks(rsp)
    if case == 'snake':
        assert result['is_secret'] == False
    else:
        assert result['isSecret'] == False
    assert result['name'] == 'the_config'

def test_register_secret_config(headers):
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/configs'
    data = {"name": "another_config", 
            "value": "my value", 
            "actors": actor_id}
    if case == 'snake':
        data['is_secret'] = True
    else:
        data['isSecret'] = True
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    if case == 'snake':
        assert result['is_secret'] == True
    else:
        assert result['isSecret'] == True
    assert result['name'] is not 'Another config'

def test_register_config_multiple_actors(headers):
    actor_id = get_actor_id(headers)
    actor_id2 = get_actor_id(headers, name='abaco_test_suite_default_env')
    url = f'{base_url}/actors/configs'
    data = {"name": "a_multi_actor_config",
            "value": "my value", 
            "actors": f"{actor_id}, {actor_id2}"}
    if case == 'snake':
        data['is_secret'] = True
    else:
        data['isSecret'] = True
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert result['actors'] == f"{actor_id}, {actor_id2}"

def test_register_with_nonexistent_actor(headers):
    url = f'{base_url}/actors/configs'
    data = {"name": "another_config", 
            "value": "my value", 
            "actors": "henry"}
    if case == 'snake':
        data['is_secret'] = True
    else:
        data['isSecret'] = True
    rsp = requests.post(url, data=data, headers=headers)
    assert rsp.status_code == 404

def test_get_configs(headers):
    url = f'{base_url}/actors/configs'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)

def test_get_specific_config(headers):
    url = f'{base_url}/actors/configs/the_config'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    if case == 'snake':
        assert result['is_secret'] == False
    else:
        assert result['isSecret'] == False
    assert result['name'] == 'the_config'

def test_register_config_regular_headers(headers, regular_headers):
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/configs'
    data = {"name": "limited_config", 
            "value": "my value", 
            "actors": actor_id}
    if case == 'snake':
        data['is_secret'] = True
    else:
        data['isSecret'] = True
    rsp = requests.post(url, data=data, headers=regular_headers)
    result = basic_response_checks(rsp)
    
## Update config, add actors to config, change with diff permissions, can we assign nonexistent actors?
def test_update_config(headers):
    actor_id = get_actor_id(headers)
    actor_id2 = get_actor_id(headers, name='abaco_test_suite_default_env')
    url = f'{base_url}/actors/configs/the_config'
    data = {"name": "the_config",
            "value": "my value",
            "actors": f"{actor_id}, {actor_id2}"}
    if case == 'snake':
        data['is_secret'] = True
    else:
        data['isSecret'] = True
    rsp = requests.put(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    if case == 'snake':
        assert result['is_secret'] == True
    else:
        assert result['isSecret'] == True

def test_update_config_regular_headers(headers, regular_headers):
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/configs/the_config'
    data = {"name": "the_config", 
            "value": "my value",
            "actors": actor_id}
    if case == 'snake':
        data['is_secret'] = True
    else:
        data['isSecret'] = True
    rsp = requests.put(url, data=data, headers=regular_headers)
    if not rsp.status_code == 400:
        print(rsp.content)
    assert rsp.status_code == 400

# Clean up
def test_delete_actors(headers):
    delete_actors(headers)
