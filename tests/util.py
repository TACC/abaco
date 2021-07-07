# Utilities shared across testing modules.
import json
import os
import pytest
import requests
import time
from tapipy.tapis import Tapis
from common.auth import Tenants
import subprocess

# Need base_url as it's where we direct calls. But also need SK url for tapipy.
base_url = os.environ.get('base_url', 'http://172.17.0.1:8000')
case = os.environ.get('case', 'snake')
testuser_tenant = os.environ.get('tenant', 'dev')

def get_service_tapis_client():
    with open('config-local.json') as json_file:
        conf = json.load(json_file)
    sk_url = os.environ.get('sk_url', conf['primary_site_admin_tenant_base_url'])
    tenant_id = os.environ.get('tenant', 'admin')
    service_password = os.environ.get('service_password', conf['service_password'])
    jwt = os.environ.get('jwt', None)
    resource_set = os.environ.get('resource_set', 'local')
    custom_spec_dict = os.environ.get('custom_spec_dict', None)
    download_latest_specs = os.environ.get('download_latest_specs', False)
    # if there is no tenant_id, use the service_tenant_id and primary_site_admin_tenant_base_url configured for the service:
    t = Tapis(base_url=sk_url or base_url,
              tenant_id=tenant_id,
              username='abaco',
              account_type='service',
              service_password=service_password,
              jwt=jwt,
              resource_set=resource_set,
              custom_spec_dict=custom_spec_dict,
              download_latest_specs=download_latest_specs,
              is_tapis_service=True,
              tenants=Tenants())
    if not jwt:
        t.get_tokens()
    return t

t = get_service_tapis_client()

# In dev:
# service account owns abaco_admin and abaco_privileged roles
# _abaco_testuser_admin is granted abaco_admin role
# _abaco_testuser_privileged is granted abaco_privileged role
# _abaco_testuser_regular is granted nothing
@pytest.fixture(scope='session', autouse=True)
def create_test_roles():
    # Using Tapipy to ensure each abaco environment has proper roles and testusers created before starting
    all_role_names = t.sk.getRoleNames(tenant=testuser_tenant)
    if not 'abaco_admin' in all_role_names.names:
        print('Creating role: abaco_admin')
        t.sk.createRole(roleTenant=testuser_tenant, roleName='abaco_admin', description='Admin role in Abaco.')
    if not 'abaco_privileged' in all_role_names.names:
        print('Creating role: abaco_privileged')
        t.sk.createRole(roleTenant=testuser_tenant, roleName='abaco_privileged', description='Privileged role in Abaco.')
    t.sk.grantRole(tenant=testuser_tenant, user='_abaco_testuser_admin', roleName='abaco_admin')
    t.sk.grantRole(tenant=testuser_tenant, user='_abaco_testuser_privileged', roleName='abaco_privileged')

@pytest.fixture(scope='session', autouse=True)
def headers():
    return get_tapis_token_headers('_abaco_testuser_admin', None)

@pytest.fixture(scope='session', autouse=True)
def nshresth_header():
    return get_tapis_token_headers('nshresth', 'tacc')

@pytest.fixture(scope='session', autouse=True)
def jstubbs_header():
    return get_tapis_token_headers('jstubbs', 'tacc')

@pytest.fixture(scope='session', autouse=True)
def privileged_headers():
    return get_tapis_token_headers('_abaco_testuser_privileged', None)

@pytest.fixture(scope='session', autouse=True)
def regular_headers():
    return get_tapis_token_headers('_abaco_testuser_regular', None)

@pytest.fixture(scope='session', autouse=True)
def limited_headers():
    return get_tapis_token_headers('_abaco_testuser_limited', None)

@pytest.fixture(scope='session', autouse=True)
def alternative_tenant_headers():
    # Find an alternative tenant than the one currently being tested, usually
    # "dev", if "dev" is used, "tacc" will be used. Or otherwise specified.
    alt_tenant = 'dev'
    alt_alt_tenant = 'tacc'
    curr_tenant = get_tenant()
    if curr_tenant == alt_tenant:
        alt_tenant = alt_alt_tenant
    return get_tapis_token_headers('_abaco_testuser_regular', alt_tenant)

@pytest.fixture(scope='session', autouse=True)
def cycling_headers(regular_headers, privileged_headers):
    return {'regular': regular_headers,
            'privileged': privileged_headers}

def get_tapis_token_headers(user, alt_tenant=None):
    # Use alternative tenant if provided.
    if alt_tenant:
        testuser_tenant = alt_tenant
    token_res = t.tokens.create_token(account_type='user', 
                                      token_tenant_id=testuser_tenant,
                                      token_username=user,
                                      access_token_ttl=999999,
                                      generate_refresh_token=False,
                                      use_basic_auth=False)
    if not token_res.access_token or not token_res.access_token.access_token:
        raise KeyError(f"Did not get access token; token response: {token_res}")
    header_dat = {"X-Tapis-Token": token_res.access_token.access_token}
    return header_dat

@pytest.fixture(scope='session', autouse=True)
def wait_for_rabbit():
    with open('config-local.json') as json_file:
        conf = json.load(json_file)

    rabbit_dash_host = conf['rabbit_dash_host']

    fn_call = f'/home/tapis/rabbitmqadmin -H {rabbit_dash_host} '

    # Get admin credentials from rabbit_uri. Add auth to fn_call if it exists.
    admin_user = conf['admin_rabbitmq_user'] or None
    admin_pass = conf['admin_rabbitmq_pass'] or None

    if admin_user and admin_pass:
        fn_call += (f'-u {admin_user} ')
        fn_call += (f'-p {admin_pass} ')
    else:
        fn_call += (f'-u {admin_user} ')

    # We poll to check rabbitmq is operational. Done by trying to list vhosts, arbitrary command.
    # Exit code 0 means rabbitmq is running. Need access to rabbitmq dash/management panel.
    i = 5
    while i:
        result = subprocess.run(fn_call + f'list vhosts', shell=True)
        if result.returncode == 0:
            break
        else:
            time.sleep(3)
        i -= 1
    time.sleep(7)

def get_tenant():
    """ Get the tenant_id associated with the test suite requests."""
    return t.tenant_id

def get_jwt_headers(file_path='/home/tapis/tests/jwt-abaco_admin'):
    with open(file_path, 'r') as f:
        jwt_default = f.read()
    jwt = os.environ.get('jwt', jwt_default)
    if jwt:
        jwt_header = os.environ.get('jwt_header', 'X-Jwt-Assertion-DEV-DEVELOP')
        headers = {jwt_header: jwt}
    else:
        token = os.environ.get('token', '')
        headers = {'Authorization': f'Bearer {token}'}
    return headers

def get_actor_id(headers, name='abaco_test_suite'):
    url = f'{base_url}/actors'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    for k in result:
        if k.get('name') == name:
            return k.get('id')
    # didn't find the test actor
    assert False

def delete_actors(headers):
    url = f'{base_url}/actors'
    rsp = requests.get(url, headers=headers)
    result = basic_response_checks(rsp)
    for act in result:
        url = f'{base_url}/actors/{act.get("id")}'
        rsp = requests.delete(url, headers=headers)
        basic_response_checks(rsp)

def response_format(rsp):
    assert 'application/json' in rsp.headers['content-type']
    data = json.loads(rsp.content.decode('utf-8'))
    assert 'message' in data.keys()
    assert 'status' in data.keys()
    assert 'version' in data.keys()
    return data

def basic_response_checks(rsp, check_tenant=True):
    if not rsp.status_code in [200, 201]:
        print(str(rsp.content)[:400])
        pytest.fail(f'Status code: {rsp.status_code} not in [200, 201].')
    response_format(rsp)
    data = json.loads(rsp.content.decode('utf-8'))
    assert 'result' in data.keys()
    result = data['result']
    if check_tenant:
        if result is not None:
            assert 'tenant' not in result
    return result

def check_execution_details(result, actor_id, exc_id):
    if case == 'snake':
        assert result.get('actor_id') == actor_id
        assert 'worker_id' in result
        assert 'exit_code' in result
        assert 'final_state' in result
        assert 'message_received_time' in result
        assert 'start_time' in result
    else:
        assert result.get('actorId') == actor_id
        assert 'workerId' in result
        assert 'exitCode' in result
        assert 'finalState' in result
        assert 'messageReceivedTime' in result
        assert 'startTime' in result

    assert result.get('id') == exc_id
    # note: it is possible for io to be 0 in which case an `assert result['io']` will fail.
    assert 'io' in result
    assert 'runtime' in result

def check_worker_fields(worker):
    assert worker.get('status') in ['READY', 'BUSY']
    assert worker.get('image') == 'jstubbs/abaco_test' or worker.get('image') == 'jstubbs/abaco_test2'
    assert worker.get('location')
    assert worker.get('cid')
    assert worker.get('tenant')
    if case == 'snake':
        assert worker.get('ch_name')
        assert 'last_execution_time' in worker
        assert 'last_health_check_time' in worker
    else:
        assert worker.get('chName')
        assert 'lastExecutionTime' in worker
        assert 'lastHealthCheckTime' in worker

def check_nonce_fields(nonce, actor_id=None, alias=None, nonce_id=None,
                       current_uses=None, max_uses=None, remaining_uses=None, level=None, owner=None):
    """Basic checks of the nonce object returned from the API."""
    nid = nonce.get('id')
    # check that nonce id has a valid tenant:
    assert nid
    assert nid.rsplit('_', 1)[0]
    if nonce_id:
        assert nonce.get('id') == nonce_id
    assert nonce.get('owner')
    if owner:
        assert nonce.get('owner') == owner
    assert nonce.get('level')
    if level:
        assert nonce.get('level') == level
    assert nonce.get('roles')
    if alias:
        assert nonce.get('alias') == alias

    # case-specific checks:
    if case == 'snake':
        if actor_id:
            assert nonce.get('actor_id')
            assert nonce.get('actor_id') == actor_id
        assert nonce.get('api_server')
        assert nonce.get('create_time')
        assert 'current_uses' in nonce
        if current_uses:
            assert nonce.get('current_uses') == current_uses
        assert nonce.get('last_use_time')
        assert nonce.get('max_uses')
        if max_uses:
            assert nonce.get('max_uses') == max_uses
        assert 'remaining_uses' in nonce
        if remaining_uses:
            assert nonce.get('remaining_uses') == remaining_uses
    else:
        if actor_id:
            assert nonce.get('actorId')
            assert nonce.get('actorId') == actor_id
        assert nonce.get('apiServer')
        assert nonce.get('createTime')
        assert 'currentUses'in nonce
        if current_uses:
            assert nonce.get('currentUses') == current_uses
        assert nonce.get('lastUseTime')
        assert nonce.get('maxUses')
        if max_uses:
            assert nonce.get('maxUses') == max_uses
        assert 'remainingUses' in nonce
        if remaining_uses:
            assert nonce.get('remainingUses') == remaining_uses

def execute_actor(headers, actor_id, data=None, json_data=None, binary=None, synchronous=False):
    url = f'{base_url}/actors/{actor_id}/messages'
    params = {}
    if synchronous:
        # url += '?_abaco_synchronous=true'
        params = {'_abaco_synchronous': 'true'}
    if data:
        rsp = requests.post(url, data=data, headers=headers, params=params)
    elif json_data:
        rsp = requests.post(url, json=json_data, headers=headers, params=params)
    elif binary:
        rsp = requests.post(url, data=binary, headers=headers, params=params)
    else:
        raise Exception # invalid
    # in the synchronous case, the result should be the actual execution result logs
    if synchronous:
        assert rsp.status_code in [200]
        logs = rsp.content.decode()
        assert logs is not None
        print(f"synchronous logs: {logs}")
        assert 'Contents of MSG' in logs
        return None
    # asynchronous case -----
    result = basic_response_checks(rsp)
    if data:
        assert data.get('message') in result.get('msg')
    if case == 'snake':
        assert result.get('execution_id')
        exc_id = result.get('execution_id')
    else:
        assert result.get('executionId')
        exc_id = result.get('executionId')
    # check for the execution to complete
    count = 0
    while count < 10:
        time.sleep(3)
        url = f'{base_url}/actors/{actor_id}/executions'
        rsp = requests.get(url, headers=headers)
        result = basic_response_checks(rsp)
        ids = result.get('ids')
        if ids:
            assert exc_id in ids
        url = f'{base_url}/actors/{actor_id}/executions/{exc_id}'
        rsp = requests.get(url, headers=headers)
        result = basic_response_checks(rsp)
        status = result.get('status')
        assert status
        if status == 'COMPLETE':
            check_execution_details(result, actor_id, exc_id)
            return result
        count += 1
    assert False

def create_delete_actor():
    with open('jwt-abaco_admin', 'r') as f:
        jwt_default = f.read()
    headers = {'X-Jwt-Assertion-AGAVE-PROD': jwt_default}
    data = {'image': 'jstubbs/abaco_test', 'name': 'abaco_test_suite_python'}
    rsp = requests.post(f'{base_url}/actors', data=data, headers=headers)
    result = basic_response_checks(rsp)
    aid = result.get('id')
    print(f"Created actor: {aid}")
    try:
        requests.delete(f'{base_url}/actors/{aid}', headers=headers)
        print("deleted actor")
    except Exception as e:
        print(f"Got exception tring to delete actor: {e.response.content}")

def has_cycles(links):
    """
    FROM actors/controllers.py. Taken for modularization.
    Checks whether the `links` dictionary contains a cycle.
    :param links: dictionary of form d[k]=v where k->v is a link
    :return: 
    """
    # consider each link entry as the starting node:
    for k, v in links.items():
        # list of visited nodes on this iteration; starts with the two links.
        # if we visit a node twice, we have a cycle.
        visited = [k, v]
        # current node we are on
        current = v
        while current:
            # look up current to see if it has a link:
            current = links.get(current)
            # if it had a link, check if it was alread in visited:
            if current and current in visited:
                return True
            visited.append(current)
    return False

def under_to_camel(value):
    def camel_case():
        yield type(value).lower
        while True:
            yield type(value).capitalize
    c = camel_case()
    return "".join(c.__next__()(x) if x else '_' for x in value.split("_"))


def dict_to_camel(d):
    """
    FROM actors/models.py. Take for modularization
    Convert all keys in a dictionary to camel case.
    """
    d2 = {}
    for k,v in d.items():
        d2[under_to_camel(k)] = v
    return d2
