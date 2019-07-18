# run with
# docker run -v /:/host -v /var/run/docker.sock:/var/run/docker.sock -it -e case=camel -e base_url=http://172.17.0.1:8000 -v $(pwd)/local-dev.conf:/etc/service.conf --rm --entrypoint=bash abaco/testsuite:$TAG

import json
import os
import requests
import time
from util import base_url, basic_response_checks


with open('jwt-abaco_admin', 'r') as f:
    jwt_default = f.read()

headers = {'X-Jwt-Assertion-DEV': jwt_default}


def register_actor(idx):
    IMAGE = os.environ.get('IMAGE', 'abacosamples/test')
    data = {'image': IMAGE, 'name': 'abaco_load_test_{}'.format(idx)}
    rsp = requests.post('{}/actors'.format(base_url), data=data, headers=headers)
    result = basic_response_checks(rsp)
    return result['id']

def check_for_ready(actor_ids):
    """
    Check for READY status of each actor in the actor_ids list.
    :param actor_ids:
    :return:
    """
    for aid in actor_ids:
        ready = False
        idx = 0
        while not ready and idx < 10:
            url = '{}/actors/{}'.format(base_url, aid)
            rsp = requests.get(url, headers=headers)
            result = basic_response_checks(rsp)
            if result['status'] == 'READY':
                ready = True
                break
            idx = idx + 1
            time.sleep(1)
        if not ready:
            print("ERROR - actor {} never entered READY status.")
            raise Exception()

def get_actors():
    """
    Helper script that can return that actor_ids associated with this test suite.
    :return:
    """
    aids = []
    url = '{}/actors'.format(base_url)
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    for actor in result:
        if 'abaco_load_test_' in actor.get('name'):
            aids.append(actor.get('id'))
    return aids

def get_executions(actor_ids):
    ex_ids = {}
    for a in actor_ids:
        ex_ids[a] = []
        url = '{}/actors/{}/executions'.format(base_url, a)
        rsp = requests.get(url, headers=headers)
        result = basic_response_checks(rsp)
        for ex in result['executions']:
            ex_ids[a].append(ex['id'])
    return ex_ids


def send_messages(actor_ids, num_messages_per_actor):
    execution_ids = {}
    for idx, aid in enumerate(actor_ids):
        execution_ids[aid] = []
        url = '{}/actors/{}/messages'.format(base_url, aid)
        for j in range(num_messages_per_actor):
            rsp = requests.post(url, data={'message': 'test_{}_{}'.format(idx, j)}, headers=headers)
            result = basic_response_checks(rsp)
            ex_id = result['executionId']
            print("Send actor id {} message {}_{}; execution id: {}".format(aid, idx, j, ex_id))
            execution_ids[aid].append(ex_id)
    return execution_ids

def check_for_complete(actor_ids, execution_ids):
    for idx, aid in enumerate(actor_ids):
        print('checking executions for actor id {}, {} out of {} total actors.'.format(aid, idx+1, len(actor_ids)))
        for i, ex_id in enumerate(execution_ids[aid]):
            print("checking execution {} (number {} of {} for actor {})".format(ex_id, i+1, len(execution_ids[aid]), aid))
            done = False
            count = 0
            while not done and count < 25:
                url = '{}/actors/{}/executions/{}'.format(base_url, aid, ex_id)
                rsp = requests.get(url, headers=headers)
                result = basic_response_checks(rsp)
                if result['status'] == 'COMPLETE':
                    print("execution (ex_id {}) COMPLETE".format(ex_id))
                    done = True
                    continue
                else:
                    print("status was: {}. sleeping {}/20...".format(result['status'], count))
                    count = count + 1
                    time.sleep(1)
                if count > 20:
                    print("ERROR - execution never completed; actor_id: {}; "
                          "execution id: {}; count: {}".format(aid, ex_id, count))

def main():
    NUM_ACTORS = int(os.environ.get('NUM_ACTORS', 60))
    actor_ids = []
    for i in range(NUM_ACTORS):
        aid = register_actor(i)
        actor_ids.append(aid)
        print("registered actor # {}; id: {}".format(i, aid))

    # wait for actors to reach READY status
    check_for_ready(actor_ids)

    # send actors messages -
    NUM_MESSAGES_PER_ACTOR = int(os.environ.get('NUM_MESSAGES_PER_ACTOR', 1))
    execution_ids = send_messages(actor_ids, NUM_MESSAGES_PER_ACTOR)

    # check for executions to complete
    check_for_complete(actor_ids, execution_ids)



if __name__ == '__main__':
    main()
