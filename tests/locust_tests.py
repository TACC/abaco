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

from locust import HttpLocust, TaskSet, task, events
import util

HERE = os.path.dirname(os.path.abspath(__file__))

def get_credentials():
    config = json.load(open(
        os.path.join(HERE, 'locust_credentials.json')))
    config['headers'] = {}
    config['url_suffix'] = ''
    if config.get('use_jwt'):
        config['headers'] = util.get_jwt_headers()
    else:
        if config.get('access_token'):
            config['headers']['Authorizationn'] = 'Bearer {}'.format(config.get('access_token'))
            config['url_suffix'] = '/v2'
        else:
            print("Must provide either use_jwt or an access_token.")
            raise Exception()
    return config

credentials = get_credentials()


class BasicAbacoTasks(TaskSet):

    actor_ids = []
    message_count = 0

    def get_random_aid(self):
        if len(self.actor_ids) <= 0:
            return
        return self.actor_ids[random.randint(0, len(self.actor_ids)-1)]

    def register_simple_actor(self):
        rsp = self.client.post('/actors{}'.format(credentials['url_suffix']),
                               headers=credentials['headers'],
                               json={'image': 'abacosamples/test'})
        self.actor_ids.append(rsp.json().get('result').get('id'))

    def delete_actor(self):
        aid = self.actor_ids.pop()
        self.client.delete('/actors{}/{}'.format(credentials['url_suffix'], aid),
                           headers=credentials['headers'])

    def on_start(self):
        self.register_simple_actor()

    def on_stop(self):
        self.delete_actor()

    @task(10)
    def list_actors(self):
        self.client.get('/actors{}'.format(credentials['url_suffix']),
                        headers=credentials['headers'])

    @task(10)
    def get_actor(self):
        aid = self.get_random_aid()
        self.client.get('/actors{}/{}'.format(credentials['url_suffix'], aid),
                        headers=credentials['headers'])

    @task(5)
    def get_actor_executions(self):
        aid = self.get_random_aid()
        self.client.get('/actors{}/{}/executions'.format(credentials['url_suffix'], aid),
                        headers=credentials['headers'])

    @task(1)
    def send_actor_message(self):
        aid = self.get_random_aid()
        rsp = self.client.post('/actors{}/{}/messages'.format(credentials['url_suffix'], aid),
                               headers=credentials['headers'],
                               data={'message': 'test {}'.format(self.message_count)})
        self.message_count += 1



class AbacoApiUser(HttpLocust):
    host = credentials.get('api_server')
    task_set = BasicAbacoTasks
    wait_function = lambda self: random.expovariate(1) * 500