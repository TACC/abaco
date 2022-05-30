import os
import sys

# these paths allow for importing modules from the actors package both in the docker container and native when the test
# suite is launched from the command line.
sys.path.append(os.path.split(os.getcwd())[0])
sys.path.append('/actors')
import time

import requests
import json

from util import headers, base_url_ad, case, \
    response_format, basic_response_checks, get_actor_id, check_execution_details, \
    execute_actor, get_tenant, privileged_headers, regular_headers, \
    get_tapis_token_headers, alternative_tenant_headers, delete_actors, create_test_roles, wait_for_rabbit



# Testing listing the adapters
def test_list_adapters(headers):
    url = f"{base_url_ad}/adapters"
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert len(result) == 0

def test_create_adapter(headers):
    url = f"{base_url_ad}/actors"
    data = {'image': 'nshresth/flask-helloworld', 'name': 'abaco_test_suite'}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert 'owner' in result
    assert result['owner'] == '_abaco_testuser_admin'
    assert result['image'] == 'nshresth/flask-helloworld'
    assert result['name'] == 'abaco_test_suite'
    assert result['id'] is not None

def test_send_message(headers):
    id=get_adapter_id
    rsp = requests.get(f'http://localhost:5000/adapters/{reeeee}/data', headers=headers)
    result = basic_response_checks(rsp)
    assert result['result'] == 'Hellow World'

#delete


