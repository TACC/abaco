import json
import os
import random
import requests
import unittest

from locust import HttpLocust, TaskSet, task, events
import util_copy as util


base_url = os.environ.get('base_url', 'http://172.17.0.1:8000')
HERE = os.path.dirname(os.path.abspath(__file__))

def get_credentials():
    config = json.load(open(
        os.path.join(HERE, 'locust_credentials_local.json')))
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


class BasicAbacoTasks(object):

    def __init__(self):
        self.actor_ids = []
        self.message_count = 0

    def get_headers(self):
        with open('jwt-abaco_admin', 'r') as f:
            jwt_default = f.read()
        headers = {'X-Jwt-Assertion-AGAVE-PROD': jwt_default}
        return headers

    def create_actor(self, max_workers):
        headers = self.get_headers()
        data = {'image': 'jstubbs/abaco_test',
                'name': 'abaco_test_suite_python',
                'maxWorkers':max_workers
                }
        rsp = requests.post('{}/actors'.format(base_url), data=data, headers=headers)
        result = util.basic_response_checks(rsp)
        aid = result.get('id')
        self.actor_ids.append(aid)
        print("Created actor: {}".format(aid))
        return aid

    def send_actor_message(self, aid):
        headers = self.get_headers()
        url = '{}/actors/{}/messages'.format(base_url, aid)
        data = {'message': 'testing execution'}
        rsp = requests.post(url, data=data, headers=headers)
        result = util.basic_response_checks(rsp)

        # rsp = self.client.post('/actors{}/{}/messages'.format(credentials['url_suffix'], aid),
        #                        headers=credentials['headers'],
        #                        data={'message': '{"sleep": 1, "iterations": 3}'})
        self.message_count += 1

    def get_random_aid(self):
        if len(self.actor_ids) <= 0:
            return
        return self.actor_ids[random.randint(0, len(self.actor_ids)-1)]

    def register_simple_actor(self):
        rsp = self.client.post('/actors{}'.format(credentials['url_suffix']),
                               headers=credentials['headers'],
                               json={'image': 'abacosamples/sleep_loop'})
        self.actor_ids.append(rsp.json().get('result').get('id'))

    def delete_actor(self):
        aid = self.actor_ids.pop()
        self.client.delete('/actors{}/{}'.format(credentials['url_suffix'], aid),
                           headers=credentials['headers'])

    def on_start(self):
        self.register_simple_actor()

    def on_stop(self):
        self.delete_actor()

    # def send_actor_message(self):
    #     aid = self.get_random_aid()
    #     rsp = self.client.post('/actors{}/{}/messages'.format(credentials['url_suffix'], aid),
    #                            headers=credentials['headers'],
    #                            data={'message': '{"sleep": 1, "iterations": 3}'})
    #     self.message_count += 1

    def check_actor_msg_count(self, aid):
        msg_count = 0
        # prom query to get actor message count
        return msg_count


def main():
    a = BasicAbacoTasks()
    # a.register_simple_actor()
    actor_1 = a.create_actor(2)
    actor_2 = a.create_actor(100)
    for i in range(100):
        a.send_actor_message(actor_1)
        a.send_actor_message(actor_2)




if __name__ == '__main__':
    main()
