# run with
# docker run -v /:/host -v /var/run/docker.sock:/var/run/docker.sock -it -e case=camel -e base_url=http://172.17.0.1:8000 -v $(pwd)/local-dev.conf:/etc/service.conf --rm --entrypoint=bash abaco/testsuite:$TAG
# Once inside the container:
# cd tests
# Export the following variables to configure the behavior
# BASE= the base URL for Abaco.
# NUM_ACTORS = total number of actors
# NUM_WORKERS = total number of workers per actor
# NUM_MESSAGES_PER_ACTOR = total number of messages per actor
# then run:
# python3 load_tests.py
import json
import multiprocessing
import os
import requests
import time
import timeit
from util import base_url, basic_response_checks


with open('jwt-abaco_admin', 'r') as f:
    jwt_default = f.read()

headers = {'X-Jwt-Assertion-DEV': jwt_default}

base = os.environ.get('BASE', base_url)

def register_actor(idx):
    IMAGE = os.environ.get('IMAGE', 'abacosamples/test')
    data = {'image': IMAGE, 'name': 'abaco_load_test_{}'.format(idx)}
    rsp = requests.post('{}/actors'.format(base), data=data, headers=headers)
    result = basic_response_checks(rsp)
    return result['id']

def start_workers(aid, num_workers):
    data = {'num': num_workers}
    rsp = requests.post(f'{base}/actors/{aid}/workers', data=data, headers=headers)
    result = basic_response_checks(rsp)

def check_for_ready(actor_ids):
    """
    Check for READY status of each actor in the actor_ids list.
    :param actor_ids:
    :return:
    """
    for aid in actor_ids:
        # check for workers to be ready
        idx = 0
        ready = False
        while not ready and idx < 60:
            url = '{}/actors/{}/workers'.format(base, aid)
            rsp = requests.get(url, headers=headers)
            result = basic_response_checks(rsp)
            for worker in result:
                if not worker['status'] == 'READY':
                    idx = idx + 1
                    time.sleep(2)
                    continue
                ready = True
        if not ready:
            print("ERROR - workers for actor {} never entered READY status.")
            raise Exception()
        # now check that the actor itself is ready -
        ready = False
        idx = 0
        while not ready and idx < 10:
            url = '{}/actors/{}'.format(base, aid)
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
        print(f"workers and actor ready for actor {aid}")

def get_actors():
    """
    Helper script that can return that actor_ids associated with this test suite.
    :return:
    """
    aids = []
    url = '{}/actors'.format(base)
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
        url = '{}/actors/{}/executions'.format(base, a)
        rsp = requests.get(url, headers=headers)
        result = basic_response_checks(rsp)
        for ex in result['executions']:
            ex_ids[a].append(ex['id'])
    return ex_ids


def send_one_message(actor_ids):
    execution_ids = {}
    for idx, aid in enumerate(actor_ids):
        print("sending one primer message to actor: {}".format(aid))
        execution_ids[aid] = []
        url = '{}/actors/{}/messages'.format(base, aid)
        rsp = requests.post(url, data={'message': 'test1_{}'.format(idx)}, headers=headers)
        result = basic_response_checks(rsp)
        ex_id = result['executionId']
        print("Send actor id {} message {}; execution id: {}".format(aid, idx, ex_id))
        execution_ids[aid].append(ex_id)
    print("All primer messages sent.")
    return execution_ids

def send_messages(actor_ids, num_messages_per_actor):
    execution_ids = {}
    for idx, aid in enumerate(actor_ids):
        execution_ids[aid] = []
        url = '{}/actors/{}/messages'.format(base, aid)
        for j in range(num_messages_per_actor):
            rsp = requests.post(url, data={'message': 'test_{}_{}'.format(idx, j)}, headers=headers)
            result = basic_response_checks(rsp)
            ex_id = result['executionId']
            print("Send actor id {} message {}_{}; execution id: {}".format(aid, idx, j, ex_id))
            execution_ids[aid].append(ex_id)
    return execution_ids

def _thread_send_actor_message(url):
    result = []
    data = {'message': 'test'}
    try:
        rsp = requests.post(url, headers=headers, data=data)
    except Exception as e:
        print(f"got exception trying to send message {i}; exception: {e}")
    try:
        result.append(rsp.json()['result']['executionId'])
    except Exception as e:
        print(f"got exception trying to append result of message; rsp: {rsp}; exception: {e}")
    return result

def send_messages_threaded(actor_ids, num_messages_per_actor):
    execution_ids = []
    try:
        POOL_SIZE = int(os.environ['POOL_SIZE'])
    except:
        POOL_SIZE = 8
    print(f"using pool of size: {POOL_SIZE}")
    multiprocessing.freeze_support()
    pool = multiprocessing.Pool(processes=POOL_SIZE)
    total_messages = len(actor_ids) * num_messages_per_actor
    messages_per_process = int(total_messages / POOL_SIZE)
    # this is a list with a URL for every message we want to send
    urls = [f'{base}/actors/{aid}/messages' for aid in actor_ids] * num_messages_per_actor
    start = time.time()
    print(f'Sending {num_messages_per_actor} messages now.')
    results = pool.map(_thread_send_actor_message, urls)
    print(f'Messages sent in {start - time.time()}s.')
    execution_ids = results
    # for r in results:
        # each result is a list of execution id's, so we extend by the result, r
        # execution_ids[aid].extend(r)
    return execution_ids

def check_for_complete(actor_ids):
    done_actors = []
    count = 0
    while not len(done_actors) == len(actor_ids):
        count = count + 1
        messages = []
        for idx, aid in enumerate(actor_ids):
            if aid in done_actors:
                continue
            url = f'{base}/actors/{aid}/executions'
            try:
                rsp = requests.get(url, headers=headers)
                result = basic_response_checks(rsp)
            except Exception as e:
                print(f"got exception trying to check executions for actor {aid}, exception: {e}")
                continue
            tot_done = 0
            tot = 0
            for e in result['executions']:
                tot = tot + 1
                if e['status'] == 'COMPLETE':
                    tot_done = tot_done + 1
            if tot == tot_done:
                done_actors.append(aid)
            else:
                messages.append(f"{tot_done}/{tot} for actor {aid}.")
        if count % 100 == 0:
            print(f"{len(done_actors)}/{len(actor_ids)} actors completed executions.")
            for m in messages:
                print(m)
            # sleep more at the beginning -
            if len(done_actors)/len(actor_ids) < 0.5:
                time.sleep(4)
            elif len(done_actors)/len(actor_ids) < 0.75:
                time.sleep(3)
            elif len(done_actors)/len(actor_ids) < 0.9:
                time.sleep(2)
            else:
                time.sleep(1)
    print("All executions complete.")


def main():
    NUM_ACTORS = int(os.environ.get('NUM_ACTORS', 1))
    actor_ids = []
    start_t = timeit.default_timer()
    for i in range(NUM_ACTORS):
        aid = register_actor(i)
        actor_ids.append(aid)
        print("registered actor # {}; id: {}".format(i, aid))
    reg_t = timeit.default_timer()
    # start up workers
    NUM_WORKERS = int(os.environ.get('NUM_WORKERS', 3))
    for aid in actor_ids:
        start_workers(aid, NUM_WORKERS)
    work_t = timeit.default_timer()
    # wait for actors and workers to reach READY status
    check_for_ready(actor_ids)
    ready_t = timeit.default_timer()
    # send a "primer" message -
    send_one_message(actor_ids)
    # send actors messages -
    NUM_MESSAGES_PER_ACTOR = int(os.environ.get('NUM_MESSAGES_PER_ACTOR', 1500))
    THREADED = os.environ.get('THREADED_MESSAGES', 'TRUE')
    if THREADED == 'TRUE':
        execution_ids = send_messages_threaded(actor_ids, NUM_MESSAGES_PER_ACTOR)
    else:
        execution_ids = send_messages(actor_ids, NUM_MESSAGES_PER_ACTOR)
    send_t = timeit.default_timer()
    # check for executions to complete -- TODO
    check_for_complete(actor_ids)
    end_t = timeit.default_timer()
    print(f"Final times -- ")
    print(f"complete run: {end_t - start_t}")
    print(f"Register: {reg_t - start_t}")
    print(f"Start up workers: {work_t - reg_t}")
    print(f"Workers ready: {ready_t - reg_t}")
    print(f"Send messages: {send_t - ready_t}")
    print(f"Complete executions: {end_t - send_t}")



if __name__ == '__main__':
    main()
