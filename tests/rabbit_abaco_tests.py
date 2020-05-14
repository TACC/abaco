"""
Module to test implementation of cleanup code in Abaco attempting to clean up RabbitMQ channel and connection
objects.

Usage:
1. Start a testsuite container in interactive mode:
$ docker run -it -e case=camel -e base_url=http://172.17.0.1:8000 -v $(pwd)/local-dev.conf:/etc/service.conf --rm --entrypoint=bash abaco/testsuite:dev

2. Start the test program
bash-4.3# python3 /tests/rabbit_abaco_tests.py

3. Watch the http://localhost:15672/#/queues and http://localhost:15672/#/connections
"""
import os
import sys
import time
import requests

from util import base_url, case, headers, basic_response_checks

sys.path.append('/')
sys.path.append('/actors')
from actors import channels, codes, stores, models


# whether to first delete the workers before deleting the actor.
FIRST_DELETE_WORKERS = os.environ.get('FIRST_DELETE_WORKERS', "True")

print("rabbit_abaco_tests running with: {}. {}. {}".format(base_url, case, FIRST_DELETE_WORKERS))

def create_actor_object():
    args = {'image': 'abacosamples/test',
            'tenant': 'TACC-PROD',
            'owner': 'jstubbs',
            'api_server': 'https://api.tacc.utexas.edu',
            'mounts': []
    }
    actor = models.Actor(**args)
    stores.actors_store[actor.db_id] = actor.to_db()
    print("Stored new actor in actors_store. actor_id: {}".format(actor['id']))
    return actor

def create_execution_object(actor):
    return models.Execution.add_execution(actor.dbid, {'cpu': 0,
                                                       'io': 0,
                                                       'runtime': 0,
                                                       'status': codes.SUBMITTED,
                                                       'executor': 'jstubbs'})


def minimal_test():
    """
    This test uses the channels
    :return:
    """
    # create an actor object in the actors_store
    actor = create_actor_object()
    aid = actor.db_id

    # send spawner a message to start a worker for a new actor
    worker_id = models.Worker.ensure_one_worker(aid, actor.tenant)
    ch = channels.CommandChannel()
    ch.put_cmd(actor_id=aid,
               worker_id=worker_id,
               image=actor.image,
               tenant=actor.tenant,
               stop_existing=False)

    # send a message to the actor's inbox
    eid = create_execution_object(actor)
    ch = channels.ActorMsgChannel(actor=aid)
    d = {}
    d['_abaco_username'] = 'jstubbs'
    d['_abaco_api_server'] = actor.api_server
    d['_abaco_execution_id'] = eid
    d['_abaco_Content_Type'] = 'str'
    ch.put_msg(message='test', d=d)
    ch.close()

    # wait for execution to complete:
    wait_for_execution(aid, eid)


def wait_for_execution(aid, eid):
    # wait for execution to complete:
    count = 0
    while count < 10:
        time.sleep(1)
        url = '{}/actors/{}/executions/{}'.format(base_url, aid, eid)
        rsp = requests.get(url, headers=headers())
        result = basic_response_checks(rsp)
        status = result.get('status')
        if status == 'COMPLETE':
            print("execution complete")
            break
    else:
        print("execution didn't complete. exiting")
        sys.exit()


def basic_test():
    """This test uses the http API to create, execute and delete an actor."""
    # create an actor
    url = '{}/actors'.format(base_url)
    data = {'image': 'abacosamples/test'}
    rsp = requests.post(url, data=data, headers=headers())
    result = basic_response_checks(rsp)
    aid = result['id']
    print("created actor {}".format(aid))

    # send a message
    url = '{}/actors/{}/messages'.format(base_url, aid)
    data = {'message': 'test'}
    rsp = requests.post(url, data=data, headers=headers())
    result = basic_response_checks(rsp)
    if case == 'snake':
        eid = result.get('execution_id')
    else:
        eid = result.get('executionId')
    print("got execution: {}".format(eid))

    # wait for execution to complete:
    wait_for_execution(aid, eid)

    # delete workers first, if configured:
    if FIRST_DELETE_WORKERS == 'True':
        print("deleting worker.")
        url = '{}/actors/{}/workers'.format(base_url, aid)
        rsp = requests.get(url, headers=headers())
        result = basic_response_checks(rsp)
        wid = result[0]['id']
        url = '{}/actors/{}/workers/{}'.format(base_url, aid, wid)
        rsp = requests.delete(url, headers=headers())
        basic_response_checks(rsp)
        print("worker {} deleted.".format(wid))
        time.sleep(1.5)
    else:
        print("not deleting worker first.")
    # delete actor
    print("deleting actor {}.".format(aid))
    url = '{}/actors/{}'.format(base_url, aid)
    rsp = requests.delete(url, headers=headers())
    basic_response_checks(rsp)
    print("deleted actor: {}.".format(aid))


if __name__ == '__main__':
    tot = 1
    while True:
        print("launching actor: {}".format(tot))
        basic_test()
        time.sleep(1)
        tot += 1