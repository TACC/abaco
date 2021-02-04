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
    get_tapis_token_headers, alternative_tenant_headers, delete_actors, has_cycles


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
    assert not has_cycles(links)

def test_has_cycles_2():
    links = {'A': 'B',
             'B': 'A',
             'C': 'D'}
    assert has_cycles(links)

def test_has_cycles_3():
    links = {'A': 'B',
             'B': 'C',
             'C': 'D',
             'D': 'E',
             'E': 'H',
             'H': 'B'}
    assert has_cycles(links)

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
    assert not has_cycles(links)

def test_has_cycles_5():
    links = {'A': 'B',
             'B': 'C',
             'C': 'C'}
    assert has_cycles(links)

def test_create_event_link_actor(headers):
    url = f"{base_url}/actors"
    data = {'image': 'jstubbs/abaco_test', 'name': 'abaco_test_suite_event-link', 'stateless': False}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)

def test_create_actor_with_link(headers):
    # first, get the actor id of the event_link actor:
    link_actor_id = get_actor_id(headers, name='abaco_test_suite_event-link')
    # register a new actor with link to event_link actor
    url = f"{base_url}/actors"
    data = {'image': 'jstubbs/abaco_test',
            'name': 'abaco_test_suite_event',
            'link': link_actor_id}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)

def test_create_actor_with_webhook(headers):
    # register an actor to serve as the webhook target
    url = f"{base_url}/actors"
    data = {'image': 'jstubbs/abaco_test', 'name': 'abaco_test_suite_event-webhook', 'stateless': False}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    aid = get_actor_id(headers, name='abaco_test_suite_event-webhook')
    # create a nonce for this actor
    url = f'{base_url}/actors/{aid}/nonces'
    rsp = requests.post(url, headers=headers)
    result = basic_response_checks(rsp)
    nonce = result['id']
    # in practice, no one should ever do this - the built in link property should be used instead;
    # but this illustrates the use of the webhook feature without relying on external APIs.
    webhook = f'{base_url}/actors/{aid}/messages?x-nonce={nonce}'
    # make a new actor with a webhook property that points to the above messages endpoint -
    url = f'{base_url}/actors'
    data = {'image': 'jstubbs/abaco_test',
            'name': 'abaco_test_suite_event-2',
            'webhook': webhook}
    rsp = requests.post(url, data=data, headers=headers)
    result = basic_response_checks(rsp)
    event_aid = result['id']
    # once the new actor is READY, the webhook actor should have gotten a message to
    url = f'{base_url}/actors/{aid}/executions'
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
        print(f"webhook actor never executed. actor_id: {event_aid}; webhook_actor_id: {aid}")
        assert False
        # wait for linked execution to complete and get logs
    idx = 0
    done = False
    while not done and idx < 30:
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
        print("webhook actor executions never completed. actor: {event_aid}; "
              "actor: {aid}; Final execution data: {ex_data}")
        assert False
    # now check the logs from the two executions --
    # first one should be the actor READY message:
    url = f'{base_url}/actors/{aid}/executions/{webhook_ready_ex_id}/logs'
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
    url = f'{base_url}/actors/{link_actor_id}/executions'
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
        print(f"linked actor never executed. actor_id: {actor_id}; link_actor_id: {link_actor_id}")
        assert False
    # wait for linked execution to complete and get logs
    idx = 0
    done = False
    while not done and idx < 30:
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
        print("linked actor executions never completed. actor: {actor_id}; "
              "linked_actor: {link_actor_id}; Final execution data: {ex_data}")
        assert False
    # now check the logs from the two executions --
    # first one should be the actor READY message:
    url = f'{base_url}/actors/{link_actor_id}/executions/{link_ready_ex_id}/logs'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    logs = result.get('logs')
    assert "'event_type': 'ACTOR_READY'" in logs
    check_event_logs(logs)

    # second one should be the actor execution COMPLETE message:
    url = f'{base_url}/actors/{link_actor_id}/executions/{link_execution_ex_id}/logs'
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
    url = f"{base_url}/actors"
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
    url = f'{base_url}/actors/{first_aid}'
    print(f"url: {url}; data: {data}")
    rsp = requests.put(url, data=data, headers=headers)
    assert rsp.status_code == 400
    assert 'this update would result in a cycle of linked actors' in rsp.json().get('message')

def test_cant_update_link_with_cycle(headers):
    # this test checks that an update to a link that would create a cycle is not allowed
    link_actor_id = get_actor_id(headers, name='abaco_test_suite_event-link')
    # register a new actor with link to event_link actor
    url = f"{base_url}/actors"
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
    url = f'{base_url}/actors/{first_aid}'
    print(f"url: {url}; data: {data}")
    rsp = requests.put(url, data=data, headers=headers)
    assert rsp.status_code == 400
    assert 'this update would result in a cycle of linked actors' in rsp.json().get('message')



# Clean up
def test_delete_actors(headers):
    delete_actors(headers)