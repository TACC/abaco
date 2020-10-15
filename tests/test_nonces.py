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



# Testing
def test_list_empty_nonce(headers):
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/{actor_id}/nonces'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    # initially, no nonces
    assert len(result) == 0

def test_create_unlimited_nonce(headers):
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/{actor_id}/nonces'
    # passing no data to the POST should use the defaults for a nonce:
    # unlimited uses and EXECUTE level
    rsp = requests.post(url, headers=headers)
    result = basic_response_checks(rsp)
    check_nonce_fields(result, level='EXECUTE', max_uses=-1, current_uses=0, remaining_uses=-1)

def test_create_limited_nonce(headers):
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/{actor_id}/nonces'
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
    url = f'{base_url}/actors/{actor_id}/nonces'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    # should now have 2 nonces
    assert len(result) == 2

def test_invalid_method_list_nonces(headers):
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/{actor_id}/nonces'
    rsp = requests.put(url, headers=headers)
    assert rsp.status_code == 405
    response_format(rsp)

def test_redeem_unlimited_nonce(headers):
    actor_id = get_actor_id(headers)
    # first, get the nonce id:
    nonce_id = None
    url = f'{base_url}/actors/{actor_id}/nonces'
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
    url = f'{base_url}/actors/{actor_id}?x-nonce={nonce_id}'
    # no JWT header -- we're using the nonce
    rsp = requests.get(url)
    basic_response_checks(rsp)
    url = f'{base_url}/actors/{actor_id}/executions?x-nonce={nonce_id}'
    # no JWT header -- we're using the nonce
    rsp = requests.get(url)
    basic_response_checks(rsp)
    url = f'{base_url}/actors/{actor_id}/messages?x-nonce={nonce_id}'
    # no JWT header -- we're using the nonce
    rsp = requests.get(url)
    basic_response_checks(rsp)
    # check that we have 3 uses and unlimited remaining uses:
    url = f'{base_url}/actors/{actor_id}/nonces/{nonce_id}'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    check_nonce_fields(result, actor_id=actor_id, level='EXECUTE',
                       max_uses=-1, current_uses=3, remaining_uses=-1)
    # redeem the unlimited nonce for executing:
    url = f'{base_url}/actors/{actor_id}/messages?x-nonce={nonce_id}'
    rsp = requests.post(url, data={'message': 'test'})
    basic_response_checks(rsp)
    # check that we now have 4 uses and unlimited remaining uses:
    url = f'{base_url}/actors/{actor_id}/nonces/{nonce_id}'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    check_nonce_fields(result, actor_id=actor_id, level='EXECUTE',
                       max_uses=-1, current_uses=4, remaining_uses=-1)

def test_redeem_limited_nonce(headers):
    actor_id = get_actor_id(headers)
    # first, get the nonce id:
    nonce_id = None
    url = f'{base_url}/actors/{actor_id}/nonces'
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
    url = f'{base_url}/actors/{actor_id}?x-nonce={nonce_id}'
    # no JWT header -- we're using the nonce
    rsp = requests.get(url)
    basic_response_checks(rsp)
    # check that we have 1 use and 2 remaining uses:
    url = f'{base_url}/actors/{actor_id}/nonces/{nonce_id}'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    check_nonce_fields(result, actor_id=actor_id, level='READ',
                       max_uses=3, current_uses=1, remaining_uses=2)
    # check that attempting to redeem the limited nonce for executing fails:
    url = f'{base_url}/actors/{actor_id}/messages?x-nonce={nonce_id}'
    rsp = requests.post(url, data={'message': 'test'})
    assert rsp.status_code not in range(1, 399)
    # try redeeming 3 more times; first two should work, third should fail:
    url = f'{base_url}/actors/{actor_id}?x-nonce={nonce_id}'
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
    url = f'{base_url}/actors/{actor_id}/nonces/{nonce_id}'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    check_nonce_fields(result, actor_id=actor_id, level='READ',
                       max_uses=3, current_uses=3, remaining_uses=0)

def test_invalid_method_get_nonce(headers):
    actor_id = get_actor_id(headers)
    url = f'{base_url}/actors/{actor_id}/nonces'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    nonce_id = result[0].get('id')
    url = f'{base_url}/actors/{actor_id}/nonces/{nonce_id}'
    rsp = requests.post(url, headers=headers)
    assert rsp.status_code == 405
    response_format(rsp)



# Clean up
def test_delete_actors(headers):
    delete_actors(headers)