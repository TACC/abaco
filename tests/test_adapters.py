import os
import sys

# these paths allow for importing modules from the actors package both in the docker container and native when the test
# suite is launched from the command line.
sys.path.append(os.path.split(os.getcwd())[0])
sys.path.append('/actors')
import time

import requests
import json

from util import get_adapter_id, headers, base_url, case, \
    response_format, basic_response_checks, get_actor_id, check_execution_details, \
    execute_actor, get_tenant, privileged_headers, regular_headers, \
    get_tapis_token_headers, alternative_tenant_headers, delete_actors, create_test_roles, wait_for_rabbit



# Testing listing the adapters
def test_list_adapters(headers):
    url = f"{base_url}/adapters"
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result) == 0

def test_create_adapter(headers):
    url = f"{base_url}/adapters"
    data = {'image': 'nshresth/flask-helloworld', 'name': 'abaco_test_suite'}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert 'owner' in result
    assert result['owner'] == '_abaco_testuser_admin'
    assert result['image'] == 'nshresth/flask-helloworld'
    assert result['name'] == 'abaco_test_suite'
    assert result['id'] is not None

def test_send_message(headers):
    reeeee= get_adapter_id(headers)
    url = f"{base_url}/adapters/{reeeee}"
    idx=0
    success=False
    while idx<20 and not success:
        try:
            rsp = requests.get(f'{url}/data', headers=headers)
            result = basic_response_checks(rsp)
            success=True
        except:
            time.sleep(1)
            idx = idx + 1
    assert result == "<p>Hello, World!</p>"


def test_check_permission(headers):
    reeeee= get_adapter_id(headers)
    rsp = requests.get(f'{base_url}/adapters/{reeeee}/permissions', headers=headers)
    result = basic_response_checks(rsp)
    assert result["_abaco_testuser_admin"] == "UPDATE"

# Invalid Requests - Ensuring proper response when sending invalid requests
def test_invalid_method_list_adapters(headers):
    url = f"{base_url}/adapters"
    rsp = requests.put(url, headers=headers)
    assert rsp.status_code == 405
    response_format(rsp)

def test_list_nonexistent_adapter(headers):
    url = f"{base_url}/adapter/bad_actor_id"
    rsp = requests.get(url, headers=headers)
    assert rsp.status_code == 404
    data = json.loads(rsp.content.decode('utf-8'))
    assert data['status'] == 'error'


def test_register_without_image(headers):
    url = f'{base_url}/adapters'
    rsp = requests.post(url, headers=headers, data={})
    response_format(rsp)
    assert rsp.status_code not in range(1, 399)
    data = json.loads(rsp.content.decode('utf-8'))
    message = data['message']
    assert 'image' in message


def test_delete(headers):
    reeeee= get_adapter_id(headers)
    rsp = requests.delete(f'{base_url}/adapters/{reeeee}', headers=headers)
    assert rsp.status_code in [200, 201]
    response_format(rsp)
    data = json.loads(rsp.content.decode('utf-8'))
    assert 'message' in data.keys()
    assert data['message'] == "adapter deleted successfully."


