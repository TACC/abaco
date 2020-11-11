import os
import sys

# these paths allow for importing modules from the actors package both in the docker container and native when the test
# suite is launched from the command line.
sys.path.append(os.path.split(os.getcwd())[0])
sys.path.append('/actors')
import time

import requests
import json

from actors import codes
from util import headers, base_url, case, \
    response_format, basic_response_checks, get_actor_id, check_execution_details, \
    execute_actor, get_tenant, privileged_headers, regular_headers, \
    get_tapis_token_headers, alternative_tenant_headers, dict_to_camel


def test_dict_to_camel(): 
    dic = {"_links": {"messages": "http://localhost:8000/v3/actors/ca39fac2-60a7-11e6-af60-0242ac110009-059/messages",
                      "owner": "http://localhost:8000/v3/oauth2/profiles/anonymous",
                      "self": "http://localhost:8000/v3/actors/ca39fac2-60a7-11e6-af60-0242ac110009-059/executions/458ab16c-60a8-11e6-8547-0242ac110008-053"
    },
           "execution_id": "458ab16c-60a8-11e6-8547-0242ac110008-053",
           "msg": "test"
    }
    dcamel = dict_to_camel(dic)
    assert 'executionId' in dcamel
    assert dcamel['executionId'] == "458ab16c-60a8-11e6-8547-0242ac110008-053"


def test_permission_NONE_READ():
    assert codes.NONE < codes.READ

def test_permission_NONE_EXECUTE():
    assert codes.NONE < codes.EXECUTE

def test_permission_NONE_UPDATE():
    assert codes.NONE < codes.UPDATE

def test_permission_READ_EXECUTE():
    assert codes.READ < codes.EXECUTE

def test_permission_READ_UPDATE():
    assert codes.READ < codes.UPDATE

def test_permission_EXECUTE_UPDATE():
    assert codes.EXECUTE < codes.UPDATE