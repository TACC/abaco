from django.test import TestCase

# from rest_framework.authtoken.models import Token
# from rest_framework.test import APIClient

from local_secrets import *



class DashboardClassCase(TestCase):
    def setup(self):
        pass

    def test_no_login_if_not_admin(self):
        pass


    def test_flushes_session_if_not_admin(self):
        pass

    def test_actors_tab_no_session(self):
        pass

    def test_workers_tab_no_session(self):
        pass

    def test_executions_tab_no_session(self):
        pass




