"""
Locust test suite for core Abaco functionality. To execute this test suite, build the testsuite image and then run
with an entrypoint of bash and mount a config file to /tests/locust_credentials.json; e.g.,
 
  $ docker run --rm -it -p 8089:8089 -v $(pwd)/locust_credentials_local.json:/tests/locust_credentials.json --entrypoint=locust  abaco/testsuite:dev -f /tests/locust_tests.py  
  
then, navigate to localhost:8089 and start up some locusts.

Notes:
  1. when using access_token auth, be sure to set url_suffix: "/v2"
  2. Each simulated user registers a single actor at start up; thus, the number of of users hatched on a given run should
     not exceed the max number of workers for a host specified in the abaco.conf file.
"""


import json
import os
import random
import uuid

from locust import HttpLocust, TaskSet, task, events
import util

HERE = os.path.dirname(os.path.abspath(__file__))

# configuration --

TEST_MODE = os.environ.get('TEST_MODE', 'simple')
DELETE_ACTORS = os.environ.get('DELETE_ACTORS', 'false')

ACTORS_PER_USER = int(os.environ.get('ACTORS_PER_USER', '5'))

SEND_NUMPY_MSG_WEIGHT = int(os.environ.get('SEND_NUMPY_MSG_WEIGHT', 5))
NUMPY_SIZE = os.environ.get('NUMPY_SIZE', '1024')
NUMPY_STD_DEV = os.environ.get('NUMPY_STD_DEV', '50')

SEND_WC_MSG_WEIGHT = int(os.environ.get('SEND_WC_MSG_WEIGHT', 3))
WC_MESSAGE = os.environ.get('WC_MESSAGE', 'how many words can a words count actor count')

SIMPLE_MSG_COUNT_WEIGHT = int(os.environ.get('SIMPLE_MSG_COUNT_WEIGHT', 100))

def get_credentials():
    config = json.load(open(
        os.path.join(HERE, 'locust_credentials.json')))
    config['headers'] = {}
    config['url_suffix'] = ''
    if config.get('use_jwt'):
        config['headers'] = util.get_jwt_headers()
    else:
        if config.get('access_token'):
            config['headers']['Authorization'] = 'Bearer {}'.format(config.get('access_token'))
            config['url_suffix'] = '/v2'
        else:
            print("Must provide either use_jwt or an access_token.")
            raise Exception()
    return config

credentials = get_credentials()


class BasicAbacoTasks(TaskSet):
    message_count = 0

    def __init__(self, *args, **kwargs):
        self.numpy_ex_ids = {}
        self.wc_ex_ids = {}
        self.simple_ex_ids = {}
        self.actor_ids = {'simple': [],
                 'numpy': [],
                 'wc': [],
                 'sleep': []}
        super().__init__(*args, **kwargs)

    def get_random_aid(self, typ='simple'):
        if len(self.actor_ids[typ]) <= 0:
            return None
        return self.actor_ids[typ][random.randint(0, len(self.actor_ids[typ]) - 1)]

    def register_actor(self, image='abacosamples/test', typ='simple'):
        rsp = self.client.post('/actors{}'.format(credentials['url_suffix']),
                               headers=credentials['headers'],
                               json={'image': image})
        rsp.raise_for_status()
        try:
            data = rsp.json()
        except Exception as e:
            print("Unexpected response from POST /actors, response: {}; content: {} exception: {}".format(rsp, rsp.content, e))
            print("Response request: {}".format(rsp.request))
            raise Exception()
        try:
            aid = data.get('result').get('id')
        except Exception as e:
            print("Unexpected response from POST /actors, response: {}; content: {} exception: {}".format(rsp, rsp.content, e))
            print("Response request: {}".format(rsp.request))
            raise Exception("Unexpected response from POST /actors, response: {}; content: {} exception: {}".format(rsp, rsp.content, e))
        try:
            self.actor_ids[typ].append(aid)
        except Exception as e:
            raise Exception("Unexpected response from POST /actors, response: {}; content: {} exception: {}".format(rsp, rsp.content, e))

    def register_multiple_actors(self):
        # every user registers one numpy actor, one sleep loop, and one wc actor:
        self.register_actor(image='abacosamples/numpy_mult', typ='numpy')
        # self.register_actor(image='abacosamples/sleep_loop', typ='sleep_loop')
        self.register_actor(image='abacosamples/wc', typ='wc')
        for i in range(ACTORS_PER_USER):
            self.register_actor()

    def delete_actor(self, typ='simple'):
        aid = self.actor_ids[typ].pop()
        self.client.delete('/actors{}/{}'.format(credentials['url_suffix'], aid),
                           headers=credentials['headers'])

    def send_message(self, aid=None, typ='simple'):
        if not aid:
            aid = self.get_random_aid(typ=typ)
        if typ == 'simple':
            data = {'message': 'test {}'.format(self.message_count)}
            rsp = self.client.post('/actors{}/{}/messages'.format(credentials['url_suffix'], aid),
                                   headers=credentials['headers'],
                                   data=data,
                                   name='/actors/{}/messages'.format(typ))
        if typ == 'numpy':
            data = {'size': NUMPY_SIZE,
                    'std_dev': NUMPY_STD_DEV}
            rsp = self.client.post('/actors{}/{}/messages'.format(credentials['url_suffix'], aid),
                                   headers=credentials['headers'],
                                   json=data,
                                   name='/actors/{}/messages'.format(typ))
        if typ == 'wc':
            data = {'message': WC_MESSAGE}
            rsp = self.client.post('/actors{}/{}/messages'.format(credentials['url_suffix'], aid),
                                   headers=credentials['headers'],
                                   data=data,
                                   name='/actors/{}/messages'.format(typ))
        try:
            return aid, rsp.json()['result']['executionId']
        except:
            raise Exception("Did not get execution id for actor id: {} type: {}".format(aid, typ))

    def on_start(self):
        if TEST_MODE.lower() == 'simple':
            self.register_actor()
        else:
            self.register_multiple_actors()
        self.uid = str(uuid.uuid1()).split('-')[0]
        with open('/tmp/actors_report_{}.json'.format(self.uid), 'w') as f:
            json.dump(self.actor_ids, f)

    def on_stop(self):
        if DELETE_ACTORS.lower == 'true':
            self.delete_actor()
        with open('/tmp/numpy_executions_report_{}.json'.format(self.uid), 'w') as f:
            json.dump(self.numpy_ex_ids, f)
        with open('/tmp/wc_executions_report_{}.json'.format(self.uid), 'w') as f:
            json.dump(self.wc_ex_ids, f)
        with open('/tmp/simple_executions_report_{}.json'.format(self.uid), 'w') as f:
            json.dump(self.simple_ex_ids, f)

    @task(10)
    def list_actors(self):
        self.client.get('/actors{}'.format(credentials['url_suffix']),
                        headers=credentials['headers'])

    @task(10)
    def get_actor(self):
        aid = self.get_random_aid()
        self.client.get('/actors{}/{}'.format(credentials['url_suffix'], aid),
                        headers=credentials['headers'])

    def check_ex_status(self, aid, eid, typ):
        rsp = self.client.get('/actors{}/{}/executions/{}'.format(credentials['url_suffix'], aid, eid),
                              headers=credentials['headers'],
                              name='/actors/{}/messages'.format(typ))
        if rsp.json()['result']['status'] == 'COMPLETE':
            return True
        return False

    @task(7)
    def get_actor_executions(self):
        # check all numpy --
        for aid in self.numpy_ex_ids.keys():
            done = False
            while len(self.numpy_ex_ids[aid]) > 0 and not done:
                eid = self.numpy_ex_ids[aid][0]
                if self.check_ex_status(aid, eid, 'numpy'):
                    self.numpy_ex_ids[aid].pop(0)
                else:
                    done = True

        # check all wc --
        for aid in self.wc_ex_ids.keys():
            done = False
            while len(self.wc_ex_ids[aid]) > 0 and not done:
                eid = self.wc_ex_ids[aid][0]
                if self.check_ex_status(aid, eid, 'wc'):
                    self.wc_ex_ids[aid].pop(0)
                else:
                    done = True

        # check all simple --
        for aid in self.simple_ex_ids.keys():
            done = False
            while len(self.simple_ex_ids[aid]) > 0 and not done:
                eid = self.simple_ex_ids[aid][0]
                if self.check_ex_status(aid, eid, 'simple'):
                    self.simple_ex_ids[aid].pop(0)
                else:
                    done = True

    @task(2)
    def send_actor_message(self):
        # each time called there is a 1/SEND_NUMPY_MSG_WEIGHT chance a numpy message is sent
        if not TEST_MODE == 'simple':
            send_numpy_message = random.randint(1, SEND_NUMPY_MSG_WEIGHT) <= 1
            if send_numpy_message:
                aid, ex_id = self.send_message(typ='numpy')
                if aid not in self.numpy_ex_ids.keys():
                    self.numpy_ex_ids[aid] = [ex_id]
                else:
                    self.numpy_ex_ids[aid].append(ex_id)
                self.message_count += 1

            # each time called there is a 1/SEND_WC_MSG_WEIGHT chance a wc message is sent
            send_wc_message = random.randint(1, SEND_WC_MSG_WEIGHT) <= 1
            if send_wc_message:
                aid, ex_id = self.send_message(typ='wc')
                if aid not in self.wc_ex_ids.keys():
                    self.wc_ex_ids[aid] = [ex_id]
                else:
                    self.wc_ex_ids[aid].append(ex_id)
                self.message_count += 1
        weight = random.randint(1, SIMPLE_MSG_COUNT_WEIGHT)
        aid = self.get_random_aid(typ='simple')
        if weight < 70:
            num_simple_messages = 1
        elif weight < 80:
            num_simple_messages = 2
        elif weight < 90:
            num_simple_messages = 3
        elif weight < 97:
            num_simple_messages = 4
        elif weight < 99:
            num_simple_messages = 10
        else:
            num_simple_messages = 100
        for i in range(num_simple_messages):
            aid, ex_id = self.send_message(aid=aid, typ='simple')
            if aid not in self.simple_ex_ids.keys():
                self.simple_ex_ids[aid] = [ex_id]
            else:
                self.simple_ex_ids[aid].append(ex_id)
            self.message_count += 1

class AbacoApiUser(HttpLocust):
    host = credentials.get('api_server')
    task_set = BasicAbacoTasks
    wait_function = lambda self: random.expovariate(1) * 500