from django.test import TestCase

rom rest_framework.authtoken.models import Token
from rest_framework.test import APIClient

from dashboard.local_secrets import *



class DashboardClassCase(TestCase):
    def setup(self):

        token_tsu = Token.objects.get(user__username='testshareuser')

        self.client = APIClient()

    def test_no_login_if_not_admin(self):
        token_tu = Token.objects.get(user__username='testuser')
        self.client.credentials(HTTP_AUTHORIZATION='Token ' + token_tu.key)
        print



    def test_flushes_session_if_not_admin(self):
        # self.client.login(username='testuser', password='{PW_TU}')
        pass

    def test_actors_tab_no_session(self):
        pass

    def test_workers_tab_no_session(self):
        pass

    def test_executions_tab_no_session(self):
        pass




