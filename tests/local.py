"""
A python module to aid interacting with a local development stack
"""

import requests

import json
import os
import pytest
import requests
import time

base_url = os.environ.get('base_url', 'http://172.17.0.1:8000')
case = os.environ.get('case', 'snake')

def headers():
    with open('/tests/jwt-abaco_admin', 'r') as f:
        jwt_default = f.read()
    jwt = os.environ.get('jwt', jwt_default)
    if jwt:
        jwt_header = os.environ.get('jwt_header', 'X-Jwt-Assertion-DEV-DEVELOP')
        headers = {jwt_header: jwt}
    else:
        token = os.environ.get('token', '')
        headers = {'Authorization': 'Bearer {}'.format(token)}
    return headers

def create_actor(image):
    data = {'image': image}
    url = '{}/{}'.format(base_url, '/actors')
    rsp = requests.post(url, data=data, headers=headers())
    return rsp.json()

def send_message(actor_id, message='test'):
    data = {'message': message}
    url = '{}/actors/{}/messages'.format(base_url, actor_id)
    rsp = requests.post(url, data=data, headers=headers())
    return rsp.json()

def get_execution_status(actor_id, ex_id):
    url = '{}/actors/{}/executions/{}'.format(base_url, actor_id, ex_id)
    rsp = requests.get(url, headers=headers())
    return rsp.json()['result']['status']

def send_message_and_block(actor_id, message='test'):
    rsp = send_message(actor_id, message)
    ex_id = rsp.get('result').get('executionId')
    status = ''
    while not status == 'COMPLETE':
        status = get_execution_status(actor_id, ex_id)
    return status


# example use:
# from local import *
# aid = create_actor(image='abacosamples/fast_lookup')['result']['id']
# tots = []
# for i in range(20):
#     start = time.time(); send_message_and_block(aid); end=time.time()
#     tots.append(end-start)
#     print("finished: {}".format(i))

