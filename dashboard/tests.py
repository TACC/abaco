# To run tests: docker-compose -f docker-compose-dashboard.yml run django python manage.py test tests
# to build dashboard image: docker build -t abaco/dashboard -f Dockerfile-dashboard .

import unittest
from django.test import TestCase, Client
from util import base_url

import requests
import os
import tempfile
import json

import pytest







class DashboardClassCase(TestCase):
    def setup(self):
        self.client = Client()

    def login(self, username, password):
        return self.client.post('/login', data=dict(
            username=username,
            password=password,
        ), follow_redirects=True)



    def logout(self, client):
        return self.client.get('/logout', follow_redirects=True)

    def test_login(self):
        rsp = self.login(username='testshareuser', password='testshareuser')
        self.assertNotIn("Invalid username or password", rsp.content)
        self.assertNotIn("You do not have Admin privileges.", rsp.content)
        self.logout(client=Client)

    def test_no_login_if_not_admin(self):
        rsp = self.login(username='testuser', password='testuser')
        self.assertIn("You do not have Admin privileges.", rsp.content)
        self.logout(client=Client)

    def test_no_login_if_invalid(self):
        rsp = self.login(username='hshs', password='jsjsjs')
        self.assertIn("Invalid username or password", rsp.content)
        self.logout(client=Client)

    def test_actors_tab_no_session(self):
        rsp = self.client.get('/actors')
        self.assertEquals(rsp.status_code, 302)
        self. assertIn(rsp.content, "there was an error")

    def test_workers_tab_no_session(self):
        rsp = self.client.get('/workers')
        self.assertEquals(rsp.status_code, 302)
        self.assertIn(rsp.content, "there was an error")

    def test_executions_tab_no_session(self):
        rsp = self.client.get('/executions')
        self.assertEquals(rsp.status_code, 302)
        self.assertIn(rsp.content, "there was an error")

    def test_logout(self):
        rsp = self.login(username='testshareuser', password='testshareuser')
        self.assertEquals(rsp.status_code, 302)
        self.logout(client=Client)
        rsp = self.client.get('/workers')
        self.assertEquals(rsp.status_code, 302)
        self.assertIn(rsp.content, "there was an error")
        rspa = self.client.get('/actors')
        self.assertEquals(rspa.status_code, 302)
        self.assertIn(rspa.content, "there was an error")
        rspe = self.client.get('/executions')
        self.assertEquals(rspe.status_code, 302)
        self.assertIn(rspe.content, "there was an error")



    def test_api_deletes_actors_from_dashboard(self):
        url = '{}/{}'.format(base_url, 'actors')
        print(base_url)
        data = {
            'image': 'jstubbs/abaco_test',
            'name': 'abaco_test_default',
            'stateless': False,
        }
        file_path = 'jwt-abaco-admin'
        with open(file_path, 'r') as f:
            jwt_default = f.read()
        jwt = os.environ.get('jwt', jwt_default)
        jwt_header = os.environ.get('jwt_header', 'X-Jwt-Assertion-DEV-DEVELOP')
        headers = {jwt_header: jwt}
        rsp = requests.post(url, data=data, headers=headers)
        data = json.loads(rsp.content.decode('utf-8'))
        result = data['result']
        actor_id = result['id']
        print(actor_id)
        url_del = '{}/{}/v2/{}'.format(base_url, 'actors', actor_id)
        self.assertIn(actor_id, rsp.content)
        del_rsp = requests.delete(url_del, headers=headers)
        fin_rsp = self.client.get('/actors')
        self.assertNotIn(actor_id, fin_rsp.content)


    def test_delete_button_deletes_workers_from_api(self):
        pass
    # pass until caching has been implemented
    ###########
    #     url = '{}/{}'.format(base_url, '/actors')
    #     data = {
    #         'image': 'jstubbs/abaco_test',
    #         'name': 'abaco_test_default',
    #         'stateless': False,
    #     }
    #     rsp = requests.post(url, data=data, headers=headers)
    #     actor_id = rsp['id']
    #     print(actor_id)
    #     drsp = requests.delete(self.url, headers=headers, actorId=self.actor_id)
    #     # frsp = self.client.get('/actors') this one needs to go to API
    #     self.assertNotIn(frsp.conent, actor_id)


