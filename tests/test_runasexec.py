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
    get_tapis_token_headers, alternative_tenant_headers, delete_actors, nshresth_header, jstubbs_header

def test_runAsexecuteor_actor(nshresth_header, jstubbs_header):
    #registering the actor
    url = f'{base_url}/actors'
    data = {'image': 'nshresth/abacotest:1.0', 'runAsExecutor': True}
    rsp = requests.post(url, data=data, headers=nshresth_header)
    result = basic_response_checks(rsp)
    assert 'description' in result
    assert 'owner' in result
    assert result['owner'] == 'nshresth'
    assert result['image'] == 'nshresth/abacotest:1.0'
    assert result['id'] is not None
    if case == 'snake':
        assert result['run_as_executor'] == True
    else:
        assert result['runAsExecutor'] == True
	
    #updating the permission for the actor
    actorid = result['id']
    url2 = f'{base_url}/actors/{actorid}/permissions'
    data2 = {"user":"jstubbs", "level":"EXECUTE"}
    headers2 = nshresth_header
    rsp2 = requests.post(url2, data=data2, headers=headers2)
    result2 = basic_response_checks(rsp2)
    assert result2["jstubbs"] == "EXECUTE"
    
    #sending a message as jstubbs
    url3 = f'{base_url}/actors/{actorid}/messages'
    data3 = {"message":"hello"}
    headers3 = jstubbs_header
    rsp3 = requests.post(url3, data=data3, headers=headers3)
    result3 = basic_response_checks(rsp3)
    if case == 'snake':
        assert result3['execution_id'] is not None
        execid = result3['execution_id']
    else:
        assert result3['executionId'] is not None
        execid = result3['executionId']
    
    #we have to wait a bit until the execution is finished
    result4 = {'status' : 'SUBMITTED'}
    while result4['status'] != 'COMPLETE':
        url4 = f'{base_url}/actors/{actorid}/executions/{execid}'
        headers4 = nshresth_header
        rsp4 = requests.get(url4, headers=headers4)
        result4 = basic_response_checks(rsp4)

    #getting the execution logs
    url5 = f'{base_url}/actors/{actorid}/executions/{execid}/logs'
    headers5 = nshresth_header
    rsp5 = requests.get(url5, headers=headers5)
    result5 = basic_response_checks(rsp5)
    k = result5["logs"].split("***")        #the uid and gid is spelled out on the end
    assert k[-1] == '\nuid=811324 gid=811324\n'

#check if useContaineruid and runAsexecutor can both simultaneously be turned on.
def test_use_and_run(privileged_headers):
    url = f'{base_url}/actors'
    data = {'image': 'nshresth/abacotest:1.0', 'runAsExecutor': True, 'useContainerUid': True}
    rsp = requests.post(url, data=data, headers=privileged_headers)
    data = json.loads(rsp.content.decode('utf-8'))
    if case == 'snake':
        assert data['message'] == "Cannot set both use_container_uid and run_as_executor as true"
    else:
        assert data['message'] == "Cannot set both useContainerUid and runAsExecutor as true"

#check if the user is in TAS
def test_run_as_not_tas(headers):
   url = f'{base_url}/actors'
   data = {'image': 'nshresth/abacotest:1.0', 'runAsExecutor': True}
   rsp = requests.post(url, data=data, headers=headers)
   data = json.loads(rsp.content.decode('utf-8'))
   assert data['message'] == "Run_as_executor isn't supported for your tenant"

# Clean up
def test_delete_actors(headers):
    delete_actors(headers)