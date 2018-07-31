import base64
import datetime
import inspect
import json
import os


from Crypto.PublicKey import RSA
import jwt

import logging
import requests

# Get an instance of a logger
logger = logging.getLogger(__name__)

from agavepy.agave import Agave
from django.shortcuts import render, redirect, reverse
from django.conf import settings
from django.core.mail import send_mail
from django.http import HttpResponse, JsonResponse

import errors

def get_request():
    """Walk up the stack, return the nearest first argument named "request"."""
    frame = None
    try:
        for f in inspect.stack()[1:]:
            frame = f[0]
            code = frame.f_code
            if code.co_varnames and code.co_varnames[0] == "request":
                request = frame.f_locals['request']
    finally:
        del frame
    return request

def update_session_tokens(**kwargs):
    """Update the request's session with the latest tokens since the client may have
    automatically refreshed them."""

    request = get_request()
    request.session['access_token'] = kwargs['access_token']
    request.session['refresh_token'] = kwargs['refresh_token']

def get_current_utc_time():
    """Return string representation of current time in UTC."""
    utcnow = datetime.datetime.utcnow()
    return str(utcnow.timestamp())

def display_time(t):
    """ Convert a string representation of a UTC timestamp to a display string."""
    if not t:
        return "None"
    try:
        time_f = float(t)
        dt = datetime.datetime.fromtimestamp(time_f)
    except ValueError as e:
        logger.error("Invalid time data. Could not cast {} to float. Exception: {}".format(t, e))
        raise errors.DAOError("Error retrieving time data.")
    except TypeError as e:
        logger.error("Invalid time data. Could not convert float to datetime. t: {}. Exception: {}".format(t, e))
        raise errors.DAOError("Error retrieving time data.")
    return str(dt)

def get_agave_exception_content(e):
    """Check if an Agave exception has content"""
    try:
        return e.response.content
    except Exception:
        return ""

def get_service_client():
    """Returns an agave client representing the service account. This client can be used to access
    the authorized endpoints such as the abaco endpoint."""
    if not settings.CALL_ACTOR:
        logger.debug("Skipping call to actor since settings.CALL_ACTOR was False.")
    service_token = os.environ.get('AGAVE_SERVICE_TOKEN')
    if not service_token:
        raise Exception("Missing SERVICE_TOKEN configuration.")
    base_url = os.environ.get('AGAVE_BASE_URL', "https://api.tacc.utexas.edu")
    return Agave(api_server=base_url, token=service_token)

def check_for_tokens(request):
    access_token = request.session.get("access_token")
    if access_token:
        return True
    return False

def get_agave_client(username, password):
    client_key = os.environ.get('AGAVE_CLIENT_KEY')
    client_secret = os.environ.get('AGAVE_CLIENT_SECRET')
    base_url = os.environ.get('AGAVE_BASE_URL', "https://api.tacc.utexas.edu")
    if not client_key or not client_secret:
        raise Exception("Missing OAuth client credentials.")
    return Agave(api_server=base_url, username=username, password=password, client_name="ipt", api_key=client_key,
                 api_secret=client_secret, token_callback=update_session_tokens)

def get_agave_client_tokens(access_token, refresh_token):
    client_key = os.environ.get('AGAVE_CLIENT_KEY')
    client_secret = os.environ.get('AGAVE_CLIENT_SECRET')
    base_url = os.environ.get('AGAVE_BASE_URL', "https://api.tacc.utexas.edu")
    if not client_key:
        raise Exception("Missing OAuth client key.")
    if not client_secret:
        raise Exception("Missing OAuth client secret.")
    return Agave(api_server=base_url, token=access_token, refresh_token=refresh_token, client_name="ipt",
                 api_key=client_key, api_secret=client_secret, token_callback=update_session_tokens)

def get_agave_client_session(request):
    """Return an instantiated Agave client using data from an authenticated session."""
    # grab tokens for session authorization
    access_token = request.session.get("access_token")
    refresh_token = request.session.get("refresh_token")
    # token_exp = ag.token.token_info['expires_at']
    return get_agave_client_tokens(access_token, refresh_token)

def get_pub_key():
    pub_key = os.environ.get('apim_public_key', 'MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCUp/oV1vWc8/TkQSiAvTousMzOM4asB2iltr2QKozni5aVFu818MpOLZIr8LMnTzWllJvvaA5RAAdpbECb+48FjbBe0hseUdN5HpwvnH/DW8ZccGvk53I6Orq7hLCv1ZHtuOCokghz/ATrhyPq+QktMfXnRS4HrKGJTzxaCcU7OQIDAQAB')
    return RSA.importKey(base64.b64decode(pub_key))

def get_roles(access_token, request):
    """Return the list of roles occupied by a user represented by an access_token."""
    # call the headers API to get the JWT represented by this user:
    if 'roles' in request.session:
       return request.session['roles']
    headers = {'Authorization': 'Bearer {}'.format(access_token)}
    url = '{}/headers'.format(os.environ.get('AGAVE_BASE_URL', "https://api.tacc.utexas.edu"))
    try:
        rsp = requests.get(url, headers=headers)
        data = rsp.json()
        jwt_header_name = os.environ.get('JWT_HEADER')
        if jwt_header_name:
            jwt_header = data['headers'][jwt_header_name]
        else:
            jwt_header = data['headers'].get('X-Jwt-Assertion-Tacc-Prod')
            if not jwt_header:
                jwt_header = data['headers']['X-Jwt-Assertion-Sd2E']
    except Exception as e:
        msg = "Error retrieving user roles. Contact system administrator."
        logger.error('{}. Exception: {}'.format(msg, e))
        raise e
    try:
        PUB_KEY = get_pub_key()
        decoded = jwt.decode(jwt_header, PUB_KEY)
    except (jwt.DecodeError, KeyError):
        logger.warning("Invalid JWT")
        raise errors.PermissionsError(msg='Invalid JWT.')
    try:
        roles_str = decoded['http://wso2.org/claims/role']
        roles = roles_str.split(',')
    except Exception as e:
        msg = "Unable to determine roles. Contact system administrator."
        logger.error('{}. Exception: {}'.format(msg, e))
        raise PermissionError(msg)
    request.session['roles'] = roles
    return roles

def is_admin(request):
    if 'roles' in request.session:
       roles = request.session['roles']
    else:
        roles = get_roles(request.session.get("access_token"), request)
    return 'Internal/abaco-admin' in roles

def is_abaco_user(request):
   # if 'roles' in request.session:
      # roles = request.session['roles']
  # else:
       # roles = get_roles(request.session.get("access_token"), request)
  #  return 'Internal/abaco-user' in roles or 'Internal/abaco-limited' in roles or 'Internal/abaco-admin' in roles
    return True

# VIEWS

def admin(request):
    """List all current actors registered in the system."""
    if os.environ.get('JWT_HEADER') and os.environ.get('JWT_VALUE') and os.environ.get('AGAVE_BASE_URL'):
        jwt_header = os.environ.get('JWT_HEADER')
        jwt_value = os.environ.get('JWT_VALUE')
        base_url = os.environ.get('AGAVE_BASE_URL')
        headers = {jwt_header: jwt_value}
        url = '{}/actors/admin'.format(base_url)
    else:
        if not check_for_tokens(request):
            return redirect(reverse("login"))
        if not is_admin(request):
            return HttpResponse('Unauthorized', status=401)
        access_token = request.session.get("access_token")
        headers = {'Authorization': 'Bearer {}'.format(access_token)}
        url = '{}/actors/v2/admin'.format(os.environ.get('AGAVE_BASE_URL', "https://api.tacc.utexas.edu"))
    context = {"admin": is_admin(request), "active_tab":"admin"}
    actors = []
    error = None
    try:
        rsp = requests.get(url, headers=headers)
    except Exception as e:
        msg = "Error retrieving admin endpoint."
        logger.error('{}. Exception: {}'.format(msg, e))
        raise e
    if rsp.status_code not in [200, 201]:
        logger.error("Did not get 200 from /actors/v2/admin. Status: {}. content: {}".format(
            rsp.status_code, rsp.content))
        if "message" in rsp:
            msg = rsp.get("message")
        else:
            msg = rsp.content
            error = "Unable to retrieve actors. Error was: {}".format(msg)
    else:
        logger.info("Request to /actors successful.")
        data = json.loads(rsp.content.decode('utf-8'))
        actors_data = data.get("result")
        if not actors_data and request.method == 'POST':
            error = "No actors found."
        else:

            for a in actors_data:
                tot_workers = 0
                a['workerStatus'] = ''
                a['workerLastHealthCheckTime'] = ''
                a['workerLastExecutionTime'] = ''
                a['workerId'] = ''
                if a.get("workers"):
                  for w, worker_data in a.get("workers").items():
                      tot_workers += 1
                      a['workerId'] += '{}, '.format(worker_data.get('id'))
                      a['workerStatus'] += '{}, '.format(worker_data.get('status'))
                      try:
                          a['workerLastHealthCheckTime'] += '{}, '.format(display_time(worker_data.get('last_health_check_time')))
                          a['workerLastExecutionTime'] += '{}, '.format(display_time(worker_data.get('last_execution_time')))
                      except KeyError as e:
                          logger.error("Error pulling worker data from admin api. Exception: {}".format(e))
                elif a.get('worker'):
                    tot_workers = 1
                    a['workerId'] == a['worker'].get('id')
                    a['workerStatus'] = a['worker']['status']
                    try:
                        a['workerLastHealthCheckTime'] = display_time(a['worker'].get('last_health_check_time'))
                        a['workerLastExecutionTime'] = display_time(a['worker'].get('last_execution_time'))
                    except KeyError as e:
                        logger.error("Error pulling worker data from admin api. Exception: {}".format(e))
                logger.info("Adding actor data after converting to camel: {}".format(a))
                a['totWorkers'] = tot_workers
                a['createTime'] = display_time(a.get('createTime'))
                a['lastUpdateTime'] = display_time(a.get('lastUpdateTime'))
                actors.append(a)
    context['actors'] = actors
    context['error'] = error
    return render(request, 'abaco/admin.html', context, content_type='text/html')

def worker(request):
    """
    This view generates the workers page.
    """
    if not check_for_tokens(request):
        return redirect(reverse("login"))
    if not is_abaco_user(request):
        auth_error = "Insufficient roles. Current roles are: {}".format(
            get_roles(request.session['access_token'], request))
        # return HttpResponse('Unauthorized', status=401)
        context = {"admin": False, 'auth_error': auth_error}
        return render(request, 'abaco/worker.html', context, content_type='text/html')



    if os.environ.get('JWT_HEADER') and os.environ.get('JWT_VALUE') and os.environ.get('AGAVE_BASE_URL'):
        jwt_header = os.environ.get('JWT_HEADER')
        jwt_value = os.environ.get('JWT_VALUE')
        base_url = os.environ.get('AGAVE_BASE_URL')
        headers = {jwt_header: jwt_value}
        url = '{}/actors/admin/workers'.format(base_url)
    else:
        if not check_for_tokens(request):
            return redirect(reverse("login"))
        if not is_admin(request):
            return HttpResponse('Unauthorized', status=401)
        access_token = request.session.get("access_token")
        headers = {'Authorization': 'Bearer {}'.format(access_token)}
        url = '{}/actors/v2/admin/workers'.format(os.environ.get('AGAVE_BASE_URL', "https://api.tacc.utexas.edu"))
    context = {"admin": is_admin(request), "active_tab":"worker"}
    workers = []
    error = None

    """This part retrieves the information from the actors"""


    try:
        rsp = requests.get(url, headers=headers)
    except Exception as e:
        msg = "Error retrieving admin endpoint."
        logger.error('{}. Exception: {}'.format(msg, e))
        raise e
    if rsp.status_code not in [200, 201]:
        logger.error("Did not get 200 from /actors/v2/admin/worker. Status: {}. content: {}".format(
            rsp.status_code, rsp.content))
        if "message" in rsp:
            msg = rsp.get("message")
        else:
            msg = rsp.content
            error = "Unable to retrieve actors. Error was: {}".format(msg)
    else:
        logger.info("Request to /actors successful.")
        data = json.loads(rsp.content.decode('utf-8'))
        all_data = data.get("result")
        workers_data = all_data.get("workers")
        summary_data = all_data.get("summary")
        if not workers_data and request.method == 'POST':
            error = "No actors found."


        else:

            for worker in workers_data:

                if worker.get("workers"):
                    for w, workers_data in worker.get("workers").items():

                        worker['actorId'] += '{}, '.format(workers_data.get('actorId'))
                        worker['status'] += '{}, '.format(workers_data.get('status'))
                        worker['id'] += '{}, '.format(workers_data.get('id'))
                        worker['actorName'] += '{}, '.format(workers_data.get('actorName'))
                        try:
                            worker['lastHExecutionTime'] += '{}, '.format(
                                display_time(workers_data.get('last_execution_time')))
                            worker['lastHealthCheck'] += '{}, '.format(
                                display_time(workers_data.get('last_health_check')))
                        except KeyError as e:
                            logger.error("Error pulling worker data from admin api. Exception: {}".format(e))
                elif worker.get('worker'):
                    worker['status'] = worker['status']
                    worker['id'] = worker['id']
                    try:
                        worker['lastExecutionTime'] = display_time(worker['worker'].get('last_execution_time'))
                        worker['lastHealthCheck'] = display_time(worker['worker'].get('last_health_check_time'))
                    except KeyError as e:
                        logger.error("Error pulling worker data from admin api. Exception: {}".format(e))
                logger.info("Adding actor data after converting to camel: {}".format(worker))

                workers.append(worker)
                context['summary'] = summary_data

    context['workers'] = workers
    context['error'] = error

    return render(request, 'abaco/worker.html', context, content_type='text/html')

def execution(request):
    """
    This view generates the execution page.
    """
    if not check_for_tokens(request):
        return redirect(reverse("login"))
    if not is_abaco_user(request):
        auth_error = "Insufficient roles. Current roles are: {}".format(
            get_roles(request.session['access_token'], request))
        # return HttpResponse('Unauthorized', status=401)
        context = {"admin": False, 'auth_error': auth_error}
        return render(request, 'abaco/execution.html', context, content_type='text/html')

    if os.environ.get('JWT_HEADER') and os.environ.get('JWT_VALUE') and os.environ.get('AGAVE_BASE_URL'):
        jwt_header = os.environ.get('JWT_HEADER')
        jwt_value = os.environ.get('JWT_VALUE')
        base_url = os.environ.get('AGAVE_BASE_URL')
        headers = {jwt_header: jwt_value}
        url = '{}/actors/admin/executions'.format(base_url)
    else:
        if not check_for_tokens(request):
            return redirect(reverse("login"))
        if not is_admin(request):
            return HttpResponse('Unauthorized', status=401)
        access_token = request.session.get("access_token")
        headers = {'Authorization': 'Bearer {}'.format(access_token)}
        url = '{}/actors/v2/admin/executions'.format(os.environ.get('AGAVE_BASE_URL', "https://api.tacc.utexas.edu"))
    context = {"admin": is_admin(request), "active_tab":"execution"}
    actors = []
    error = None

    """This part retrieves the information from the actors"""

    try:
        rsp = requests.get(url, headers=headers)
    except Exception as e:
        msg = "Error retrieving admin endpoint."
        logger.error('{}. Exception: {}'.format(msg, e))
        raise e
    if rsp.status_code not in [200, 201]:
        logger.error("Did not get 200 from /actors/v2/admin/execution. Status: {}. content: {}".format(
            rsp.status_code, rsp.content))
        if "message" in rsp:
            msg = rsp.get("message")
        else:
            msg = rsp.content
            error = "Unable to retrieve actors. Error was: {}".format(msg)
    else:
        logger.info("Request to /actors successful.")
        data = json.loads(rsp.content.decode('utf-8'))
        all_data = data.get("result")
        execution_data = all_data.get("actors")
        summary_data = all_data.get("summary")
        if not execution_data and request.method == 'POST':
            error = "No actors found."


        else:

            for execution in execution_data:



                if execution.get("actors"):
                    for e, execution_data in execution.get("actors").items():

                        execution['actor_Id'] += '{}, '.format(execution_data.get('actor_Id'))
                        execution['image'] += '{}, '.format(execution_data.get('image'))
                        execution['owner'] += '{}, '.format(execution_data.get('owner'))
                        execution['total_execution_cpu'] += '{}, '.format(execution_data.get('totalExecutionCpu'))
                        execution['tot_execution_io'] += '{}, '.format(execution_data.get('totalExecutionIo'))
                        execution['total_execution_runtime'] += '{}, '.format(execution_data.get('totalExecutionRuntime'))
                        execution['total_executions'] += '{}, '.format(execution_data.get('totalExecutions'))

                context['summary'] = summary_data
                actors.append(execution)

    context['actors'] = actors
    context['error'] = error
    return render(request, 'abaco/execution.html', context, content_type='text/html')


def register(request):
    """
    This view generates the register page.
    """
    if not check_for_tokens(request):
        return redirect(reverse("login"))
    if not is_abaco_user(request):
        auth_error = "Insufficient roles. Current roles are: {}".format(get_roles(request.session['access_token'], request))
        # return HttpResponse('Unauthorized', status=401)
        context = {"admin": False, 'auth_error': auth_error}
        return render(request, 'abaco/register.html', context, content_type='text/html')

    context = {"admin": is_admin(request), "active_tab":"register",
               "image_placeholder": "Enter Docker image, ex. abacosamples/py_test",
               "name_placeholder": "Enter a name for this actor.",
               "default_environment_placeholder": "Enter a JSON string of Key/Value Pairs, ex. {\"VAR1\": \"abcd\", \"VAR2\": \"123\"} ",
               "description_placeholder": "Enter a description of this actor."}
    if request.method == 'POST':
        ag = get_agave_client_session(request)
        submit = True
        image = request.POST.get("image")
        if not image:
            context['error'] = "Invalid Reactor description; see specific error(s) below."
            context['image_error'] = "A Docker image publicly accessible from the Docker Hub is required."
            submit = False
        name = request.POST.get('name')
        description = request.POST.get('description')
        default_environment = request.POST.get('default_environment')
        if default_environment:
            try:
                d = json.loads(default_environment)
                if not type(d) == dict:
                    context['error'] = "Invalid Reactor description; see specific error(s) below."
                    context['default_environment_error'] = 'The Default Environment string must be a JSON dictionary. Type was: {}'.format(type(d))
                    submit = False
            except ValueError as e:
                context['error'] = "Invalid Reactor description; see specific error(s) below."
                context['default_environment_error'] = 'The Default Environment string must be valid JSON: {}'.format(e)
                submit = False
        stateless = request.POST.get('stateless')
        if submit:
            body = {'image': image, 'stateless': stateless}
            if name:
                body['name'] = name
            if description:
                body['description'] = description
            if default_environment:
                body['default_environment'] = json.loads(default_environment)
            try:
                rsp = ag.actors.add(body=body)
                context['success'] = "Reactor registered successfully. {}".format(rsp)
            except Exception as e:
                if hasattr(e, "response") and hasattr(e.response, "content"):
                    msg = e.response.content
                else:
                    msg = e
                context['error'] = "error trying to register the reactor. Msg: {}".format(msg)
        # if there was an error, update the placeholder texts with the user's input
        if 'error' in context:
            if image:
                context['image_placeholder'] = image
            if name:
                context['name_placeholder'] = name
            if description:
                context['description_placeholder'] = description
            if default_environment:
                context['default_environment_placeholder'] = default_environment


    return render(request, 'abaco/register.html', context=context, content_type='text/html')


def run(request):
    """
    This view generates the Run page.
    """
    if not check_for_tokens(request):
        return redirect(reverse("login"))
    if not is_abaco_user(request):
        auth_error = "Insufficient roles. Current roles are: {}".format(get_roles(request.session['access_token'], request))
        # return HttpResponse('Unauthorized', status=401)
        context = {"admin": False, 'auth_error': auth_error}
        return render(request, 'abaco/run.html', context, content_type='text/html')

    context = {"admin": is_admin(request), "active_tab":"run"}
    ag = get_agave_client_session(request)
    if request.method == 'POST':
        submit = True
        actor_id = request.POST.get("actor_id")
        if not actor_id:
            context['error'] = "Could not run reactor; see specific error(s) below."
            context['actor_id_error'] = "A reactor id is required."
            submit = False
        else:
            actor_id_val = actor_id
        msg = request.POST.get('message')
        if submit:
            try:
                rsp = ag.actors.sendMessage(actorId=actor_id, body={'message': msg})
                context['success'] = "Reactor executed successfully. {}".format(rsp)
            except Exception as e:
                if hasattr(e, "response") and hasattr(e.response, "content"):
                    msg = e.response.content
                else:
                    msg = e
                context['error'] = "There was an error trying to run the reactor. Msg: {}".format(msg)
        # either way, for POST requests, check if there is an execution_id in the form:
        execution_id_val = request.POST.get("execution_id")
    if request.method == 'GET':
        actor_id_val = request.GET.get("reactor_id")
        execution_id_val = request.GET.get("execution_id")

    # pull actor details for either GET or POST method:
    if actor_id_val:
        try:
            actor = ag.actors.get(actorId=actor_id_val)
            executions = ag.actors.listExecutions(actorId=actor_id_val)
            if not hasattr(actor, 'name') or not actor.name:
                actor.name = None
            if not hasattr(actor, 'defaultEnvironment') or not actor.defaultEnvironment:
                actor.defaultEnvironment = None
            if not hasattr(actor, 'description') or not actor.description:
                actor.description = None
            context['actor'] = actor
            context['executions'] = executions
        except Exception as e:
            if hasattr(e, "response") and hasattr(e.response, "content"):
                msg = e.response.content
            else:
                msg = e
            context['details_error'] = "error trying to retrieve reactor details. Msg: {}".format(msg)

        context['actor_id_val'] = actor_id_val
    if actor_id_val and execution_id_val:
        try:
            execution = ag.actors.getExecution(actorId=actor_id_val, executionId=execution_id_val)
            execution_logs = ag.actors.getExecutionLogs(actorId=actor_id_val, executionId=execution_id_val)
            context['execution'] = execution
            context['execution_logs'] = execution_logs
        except Exception as e:
            if hasattr(e, "response") and hasattr(e.response, "content"):
                msg = e.response.content
            else:
                msg = e
            context['details_error'] = "error trying to retrieve reactor details. Msg: {}".format(msg)

        context['actor_id_val'] = actor_id_val

    return render(request, 'abaco/run.html', context, content_type='text/html')


def request_access(request):
    """
    This view generates the request_access page.
    """
    context = {}
    if request.method == 'POST':
        context.update(request.POST)
        context['name'] = request.POST.get('name')
        context['email_address'] = request.POST.get('email_address')
        context['username'] = request.POST.get('username')
        context['affiliation'] = request.POST.get('affiliation')
        context['reason'] = request.POST.get('reason')

        if not request.POST.get('name'):
            context['error'] = 'name is required'
        elif not request.POST.get('username'):
            context['error']  = 'username is required'
        elif not request.POST.get('email_address'):
            context['error'] = 'email address is required.'
        elif not request.POST.get('reason'):
            context['error'] = 'A reason is required'
        if not context.get('error'):
            msg = """User Requesting Access to Reactors Service\n
            Name:{}
            username: {}
            email address: {}
            affiliation: {}
            reason: {}""".format(context['name'],
                                context['username'],
                                context['email_address'],
                                context.get('affiliation'),
                                context['reason'])
            # write the request to a local file as backup:
            with open(os.path.join(settings.REACTORS_REQUESTS_DIR, context['username']), 'w+') as f:
                f.write(msg)
            # attempt to notify
            try:
                send_mail(msg,
                          "do-not-reply@reactors.tacc.cloud",
                          settings.REACTORS_EMAIL_LIST)
            except Exception as e:
                print("Error sending email for context: {}".format(context))
            context['success'] = "You request has been submitted! You will hear back from TACC stuff soon."
    return render(request, 'abaco/request_access.html', context, content_type='text/html')


def help(request):
    """
    This view generates the Help page.
    """
    if request.method == 'GET':
        return render(request, 'abaco/help.html', content_type='text/html')

# @check_for_tokens
def login(request):
    """
    This view generates the User Login page.
    """
    # check if user already has a valid auth session just redirect them to actors page
    if check_for_tokens(request):
        return redirect(reverse("actors"))

    if request.method == 'POST':
        username = request.POST.get('username')
        password = request.POST.get('password')

        if not username:
            context = {"error": "Username cannot be blank"}
            return render(request, 'abaco/login.html', context, content_type='text/html')

        if not password:
            context = {"error": "Password cannot be blank"}
            return render(request, 'abaco/login.html', context, content_type='text/html')

        try:
            ag = get_agave_client(username, password)
        except Exception as e:
            # render login template with an error
            context = {"error": "Invalid username or password: {}".format(e)}
            return render(request, 'abaco/login.html', context, content_type='text/html')
        # if we are here, we successfully generated an Agave client, so get the token data:
        access_token = ag.token.token_info['access_token']
        refresh_token = ag.token.token_info['refresh_token']
        token_exp = ag.token.token_info['expires_at']
        roles = get_roles(access_token, request)
        request.session['username'] = username
        request.session['access_token'] = access_token
        request.session['refresh_token'] = refresh_token
        return redirect(reverse("actors"))

    elif request.method == 'GET':
        return render(request, 'abaco/login.html', content_type='text/html')

    return render(request, 'abaco/login.html', content_type='text/html')


def logout(request):
    """
    This view allows user to logout after logging in
    """
    # log user out, end session
    # display success message ?
    # Dispatch the signal before the user is logged out so the receivers have a
    # chance to find out *who* logged out.
    user = getattr(request, 'user', None)
    if hasattr(user, 'is_authenticated') and not user.is_authenticated:
        user = None
    # user_logged_out.send(sender=user.__class__, request=request, user=user)

    # remember language choice saved to session
    # language = request.session.get(LANGUAGE_SESSION_KEY)

    request.session.flush()

    # if language is not None:
    # request.session[LANGUAGE_SESSION_KEY] = language

    if hasattr(request, 'user'):
        from django.contrib.auth.models import AnonymousUser

        request.user = AnonymousUser()

    # try:
    # 	del request.session['member_id']
    # except KeyError:
    # 	pass
    # return HttpResponse("You're logged out.")
    return redirect(reverse("login"))
    # while the user is logged in, give user option to logout (button)
    # button should be displayed at the top of the page in a nav bar on the right hand side, allowing the user to click
    # their "Account" or username and a drop down option should appear with "Log out"

    # logout button once clicked should display some message to the user saying that they are successfully logged out? Show this
    # on the login page, a simple small modal
    # or simply redirect the user back to the login page, ending their session and removing their tokens, POST method


def actors(request):
    """
    This view generates the Actors page.
    """
    if not check_for_tokens(request):
        return redirect(reverse("login"))
    if not is_abaco_user(request):
        auth_error = "Insufficient roles. Current roles are: {}".format(get_roles(request.session['access_token'], request))
        # return HttpResponse('Unauthorized', status=401)
        context = {"admin": False, 'auth_error': auth_error}
        return render(request, 'abaco/actors.html', context, content_type='text/html')

    context = {'admin': is_admin(request), "active_tab":"reactors"}
    error = None
    ag = get_agave_client_session(request)
    if request.method == 'POST':
        if request.POST.get('command') == 'delete':
            try:
                ag.actors.delete(actorId=request.POST.get('actor_id'))
            except Exception as e:
                error = "error trying to delete the reactor. Msg: {}".format(e)
    actors = ag.actors.list()
    for actor in actors:
        if not hasattr(actor, 'name') or not actor.name:
            actor.name = None
        if not hasattr(actor, 'defaultEnvironment') or not actor.defaultEnvironment:
            actor.defaultEnvironment = None
        if not hasattr(actor, 'description') or not actor.description:
            actor.description = None

    context['actors'] = actors
    context['error'] = error
    return render(request, 'abaco/actors.html', context, content_type='text/html')

