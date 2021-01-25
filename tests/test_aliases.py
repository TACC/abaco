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
    get_tapis_token_headers, alternative_tenant_headers, delete_actors, check_nonce_fields



# Initialization
ALIAS_1 = 'jane'
ALIAS_2 = 'doe'
ALIAS_3 = 'dots.in.my.name.alias' # Testing to make sure dots are okay in aliases.

def test_register_alias_actor(headers):
    url = f"{base_url}/actors"
    data = {'image': 'jstubbs/abaco_test', 'name': 'abaco_test_suite_alias'}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert 'description' in result
    assert 'owner' in result
    assert result['owner'] == '_abaco_testuser_admin'
    assert result['image'] == 'jstubbs/abaco_test'
    assert result['name'] == 'abaco_test_suite_alias'
    assert result['id'] is not None



# Testing
def test_add_alias(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_alias')
    url = f'{base_url}/actors/aliases'
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
    url = f'{base_url}/actors/aliases'
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
    url = f'{base_url}/actors/aliases'
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
    url = f'{base_url}/actors/aliases'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    field = 'actor_id'
    if case == 'camel':
        field = 'actorId'
    for alias in result:
        assert 'alias' in alias
        assert field in alias

def test_list_alias(headers):
    url = f'{base_url}/actors/aliases/{ALIAS_1}'
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
    url = f'{base_url}/actors/aliases/{ALIAS_1}'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    owner = result['owner']

    # now check that owner has an update permission -
    url = f'{base_url}/actors/aliases/{ALIAS_1}/permissions'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert owner in result
    assert result[owner] == 'UPDATE'

def test_update_alias(headers):
    # alias UPDATE permissions alone are not sufficient to change the definition of actor an alias points at -
    # the user must have UPDATE access to the underlying actor_id as well.
    url = f'{base_url}/actors/aliases/{ALIAS_1}'
    # change the alias to point to the "abaco_test_suite" actor:
    actor_id = get_actor_id(headers, name='abaco_test_suite_alias')
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

def test_other_user_cant_list_alias(privileged_headers):
    # ensuring privileged user cannot list other users alias
    url = f'{base_url}/actors/aliases/{ALIAS_1}'
    rsp = requests.get(url, headers=privileged_headers)
    data = response_format(rsp)
    assert rsp.status_code == 400
    assert 'you do not have access to this alias' in data['message']

def test_add_alias_permission(headers):
    # adding privileged user access to alias permissions
    user = '_abaco_testuser_privileged'
    data = {'user': user, 'level': 'UPDATE'}
    url = f'{base_url}/actors/aliases/{ALIAS_1}/permissions'
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert user in result
    assert result[user] == 'UPDATE'

def test_other_user_can_now_list_alias(privileged_headers):
    # privileged user should now be able to list alias
    url = f'{base_url}/actors/aliases/{ALIAS_1}'
    rsp = requests.get(url, headers=privileged_headers)
    result = basic_response_checks(rsp)
    assert 'alias' in result

def test_other_user_still_cant_list_actor(privileged_headers):
    # alias permissions do not confer access to the actor itself -
    url = f'{base_url}/actors/{ALIAS_1}'
    rsp = requests.get(url, headers=privileged_headers)
    assert rsp.status_code == 400
    data = response_format(rsp)
    assert 'you do not have access to this actor' in data['message']

def test_other_user_still_cant_update_alias_wo_actor(headers, privileged_headers):
    # alias UPDATE permissions alone are not sufficient to change the definition of the alias to an actor -
    # the user must have UPDATE access to the underlying actor_id as well.
    url = f'{base_url}/actors/aliases/{ALIAS_1}'
    # priv user does not have access to the abaco_test_suite_alias actor
    field = 'actor_id'
    if case == 'camel':
        field = 'actorId'
    data = {field: get_actor_id(headers, name="abaco_test_suite_alias")}

    rsp = requests.put(url, data=data, headers=privileged_headers)
    assert rsp.status_code == 400
    data = response_format(rsp)
    print(f'data: {data}')
    assert 'you do not have UPDATE access to the actor you want to associate with this alias' in data['message']

def test_other_user_still_cant_create_alias_nonce(privileged_headers):
    # alias permissions do not confer access to the actor itself, and alias nonces require BOTH
    # permissions on the alias AND on the actor
    url = f'{base_url}/actors/aliases/{ALIAS_1}/nonces'
    rsp = requests.get(url, headers=privileged_headers)
    assert rsp.status_code == 400
    data = response_format(rsp)
    assert 'you do not have access to this alias and actor' in data['message']

def test_get_actor_with_alias(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_alias')
    url = f'{base_url}/actors/{ALIAS_1}'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert result['id'] == actor_id

def test_get_actor_messages_with_alias(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_alias')
    url = f'{base_url}/actors/{ALIAS_1}/messages'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert actor_id in result['_links']['self']
    assert 'messages' in result

def test_get_actor_executions_with_alias(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_alias')
    url = f'{base_url}/actors/{ALIAS_1}/executions'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert actor_id in result['_links']['self']
    assert 'executions' in result

def test_create_unlimited_alias_nonce(headers):
    url = f'{base_url}/actors/aliases/{ALIAS_1}/nonces'
    # passing no data to the POST should use the defaults for a nonce:
    # unlimited uses and EXECUTE level
    rsp = requests.post(url, headers=headers)
    result = basic_response_checks(rsp)
    check_nonce_fields(result, alias=ALIAS_1, level='EXECUTE', max_uses=-1, current_uses=0, remaining_uses=-1)

def test_redeem_unlimited_alias_nonce(headers):
    # first, get the nonce id:
    url = f'{base_url}/actors/aliases/{ALIAS_1}/nonces'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    nonce_id = result[0].get('id')
    # sanity check that alias can be used to get the actor
    url = f'{base_url}/actors/{ALIAS_1}'
    rsp = requests.get(url, headers=headers)
    basic_response_checks(rsp)
    # use the nonce-id and the alias to list the actor
    url = f'{base_url}/actors/{ALIAS_1}?x-nonce={nonce_id}'
    # no JWT header -- we're using the nonce
    rsp = requests.get(url)
    basic_response_checks(rsp)

def test_owner_can_delete_alias(headers):
    url = f'{base_url}/actors/aliases/{ALIAS_2}'
    rsp = requests.delete(url, headers=headers)
    result = basic_response_checks(rsp)

    # list aliases and make sure it is gone -
    url = f'{base_url}/actors/aliases'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    for alias in result:
        assert not alias['alias'] == ALIAS_2

def test_other_user_can_delete_shared_alias(privileged_headers):
    url = f'{base_url}/actors/aliases/{ALIAS_1}'
    rsp = requests.delete(url, headers=privileged_headers)
    basic_response_checks(rsp)

    # list aliases and make sure it is gone -
    url = f'{base_url}/actors/aliases'
    rsp = requests.get(url, headers=privileged_headers)
    result = basic_response_checks(rsp)
    for alias in result:
        assert not alias['alias'] == ALIAS_1

def test_add_alias_with_dots_in_name(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_alias')
    url = '{}/actors/aliases'.format(base_url)
    field = 'actor_id'
    if case == 'camel':
        field = 'actorId'
    data = {'alias': ALIAS_3,
            field: actor_id}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    assert result['alias'] == ALIAS_3
    assert result[field] == actor_id

def test_list_alias_with_dots_in_name(headers):
    url = '{}/actors/aliases/{}'.format(base_url, ALIAS_3)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    actor_id = get_actor_id(headers, name='abaco_test_suite_alias')
    field = 'actor_id'
    if case == 'camel':
        field = 'actorId'
    assert field in result
    assert result[field] == actor_id
    assert result['alias'] == ALIAS_3

def test_get_actor_with_alias_with_dots_in_name(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_alias')
    url = '{}/actors/{}'.format(base_url, ALIAS_3)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert result['id'] == actor_id

def test_get_actor_messages_with_alias_with_dots_in_name(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_alias')
    url = '{}/actors/{}/messages'.format(base_url, ALIAS_3)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert actor_id in result['_links']['self']
    assert 'messages' in result

def test_get_actor_executions_with_alias_with_dots_in_name(headers):
    actor_id = get_actor_id(headers, name='abaco_test_suite_alias')
    url = '{}/actors/{}/executions'.format(base_url, ALIAS_3)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    assert actor_id in result['_links']['self']
    assert 'executions' in result

def test_create_unlimited_alias_nonce_with_dots_in_name(headers):
    url = '{}/actors/aliases/{}/nonces'.format(base_url, ALIAS_3)
    # passing no data to the POST should use the defaults for a nonce:
    # unlimited uses and EXECUTE level
    rsp = requests.post(url, headers=headers)
    result = basic_response_checks(rsp)
    check_nonce_fields(result, alias=ALIAS_3, level='EXECUTE', max_uses=-1, current_uses=0, remaining_uses=-1)

def test_redeem_unlimited_alias_nonce_with_dots_in_name(headers):
    # first, get the nonce id:
    url = '{}/actors/aliases/{}/nonces'.format(base_url, ALIAS_3)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    nonce_id = result[0].get('id')
    # sanity check that alias can be used to get the actor
    url = '{}/actors/{}'.format(base_url, ALIAS_3)
    rsp = requests.get(url, headers=headers)
    basic_response_checks(rsp)
    # use the nonce-id and the alias to list the actor
    url = '{}/actors/{}?x-nonce={}'.format(base_url, ALIAS_3, nonce_id)
    # no JWT header -- we're using the nonce
    rsp = requests.get(url)
    basic_response_checks(rsp)

def test_owner_can_delete_alias_with_dots_in_name(headers):
    url = '{}/actors/aliases/{}'.format(base_url, ALIAS_3)
    rsp = requests.delete(url, headers=headers)
    result = basic_response_checks(rsp)

    # list aliases and make sure it is gone -
    url = '{}/actors/aliases'.format(base_url)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    for alias in result:
        assert not alias['alias'] == ALIAS_3



# Clean up
def test_delete_actors(headers, privileged_headers):
    delete_actors(headers)
    delete_actors(privileged_headers)