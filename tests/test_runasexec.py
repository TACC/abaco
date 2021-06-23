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

testheader1 = get_tapis_token_headers('nshresth')
testheader2 = get_tapis_token_headers('jstubbs')

def test_runAsexecuteor_actor(testheader1, testheader2):
    url = f'{base_url}/actors'
    data = {'image': 'nshresth/abacotest:1.0', 'runAsExecutor': True}
    rsp = requests.post(url, data=data, headers=testheader1)
    result = basic_response_checks(rsp)
    assert 'description' in result
    assert 'owner' in result
    assert result['owner'] == 'testuser'
    assert result['image'] == 'nshresth/abacotest:1.0'
    assert result['name'] == 'abaco_test_suite'
    assert result['id'] is not None
    assert result['run_as_executor'] == True
	
    actorid = result['id']
    url2 = f'{base_url}/actors/{actorid}/permissions'
    data2 = {'user':'jstubbs', 'level':'EXECUTE'}
    headers2 = testheader1
    rsp2 = requests.get(url2, data=data2, headers=headers2)
    result2 = basic_response_checks(rsp2)
    assert result2["nshresth"] == "EXECUTE"
    
    url3 = f'{base_url}/actors/{actorid}/messages'
    data3 = "message='hello'"
    headers3 = testheader2
    rsp3 = requests.post(url3, data=data3, headers=headers3)
    result3 = basic_response_checks(rsp3)
    assert result3['execution_id'] is not None
    
    execid = result3['execution_id']
    url4 = f'{base_url}/actors/{actorid}/executions/{execid}/logs'
    headers4 = testheader1
    rsp4 = requests.get(url4, headers=headers4)
    result4 = basic_response_checks(rsp4)
    k = result4["logs"].split("***")
    assert k[-1] == '\nuid=811324 gid=811324\n'

#check if useContaineruid and runAsexecutor can both simultaneously be turned on.
def test_runAsexecuteor_actor(testheader2):
    url = f'{base_url}/actors'
    data = {'image': 'nshresth/abacotest:1.0', 'runAsExecutor': True, 'useContainerUid': True}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    data = json.loads(rsp.content.decode('utf-8'))
    assert data['message'] == "Cannot set both useContainerUid and runAsExecutor as true"

#check if the user is in TAS
def test_runAsexecuteor_actor(headers):
    url = f'{base_url}/actors'
    data = {'image': 'nshresth/abacotest:1.0', 'runAsExecutor': True}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    data = json.loads(rsp.content.decode('utf-8'))
    assert data['message'] == "Run_as_executor isn't supported for your tenant"