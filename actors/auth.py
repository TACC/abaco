# Utilities for authn/z
import base64
import os
import re
import timeit

from Crypto.Signature import PKCS1_v1_5
from Crypto.PublicKey import RSA
from Crypto.Hash import SHA256
from flask import g, request
import jwt
import requests

from __init__ import t, Tenants
from tapisservice.tapisflask.auth import authn_and_authz as flaskbase_az
from tapisservice.logs import get_logger
logger = get_logger(__name__)

from tapisservice.config import conf
import codes
from models import Actor, Alias, ActorConfig, get_permissions, is_hashid, Nonce, get_config_permissions, permission_process

from errors import ClientException, ResourceError, PermissionsException

def get_api_server(tenant_name):
    # todo - lookup tenant in tenants table
    if tenant_name.upper() == '3DEM':
        return 'https://api.3dem.org'
    if tenant_name.upper() == 'A2CPS':
        return 'https://api.a2cps.org'
    if tenant_name.upper() == 'AGAVE-PROD':
        return 'https://public.agaveapi.co'
    if tenant_name.upper() == 'ARAPORT-ORG':
        return 'https://api.araport.org'
    if tenant_name.upper() == 'DESIGNSAFE':
        return 'https://agave.designsafe-ci.org'
    if tenant_name.upper() == 'DEV-STAGING':
        return 'https://dev.tenants.aloestaging.tacc.cloud'
    if tenant_name.upper() == 'DEV':
        return 'https://dev.tenants.aloestaging.tacc.cloud'
    if tenant_name.upper() == 'DEV-DEVELOP':
        return 'https://dev.tenants.aloedev.tacc.cloud'
    if tenant_name.upper() == 'IPLANTC-ORG':
        return 'https://agave.iplantc.org'
    if tenant_name.upper() == 'IREC':
        return 'https://irec.tenants.prod.tacc.cloud'
    if tenant_name.upper() == 'PORTALS':
        return 'https://portals-api.tacc.utexas.edu'
    if tenant_name.upper() == 'TACC-PROD':
        return 'https://api.tacc.utexas.edu'
    if tenant_name.upper() == 'SD2E':
        return 'https://api.sd2e.org'
    if tenant_name.upper() == 'SGCI':
        return 'https://agave.sgci.org'
    if tenant_name.upper() == 'VDJSERVER-ORG':
        return 'https://vdj-agave-api.tacc.utexas.edu'
    return 'http://172.17.0.1:8000'

def get_pub_key():
    pub_key = conf.web_apim_public_key
    return RSA.importKey(base64.b64decode(pub_key))


PUB_KEY = get_pub_key()

TOKEN_RE = re.compile('Bearer (.+)')

WORLD_USER = 'ABACO_WORLD'


def authn_and_authz():
    """All-in-one convenience function for implementing the basic abaco authentication
    and authorization on a flask app. Use as follows:

    import auth

    my_app = Flask(__name__)
    @my_app.before_request
    def authnz_for_my_app():
        auth.authn_and_authz()

    """
    if conf.web_accept_nonce:
        logger.debug("Config allows nonces, using nonces.")
        flaskbase_az(Tenants, check_nonce, authorization)
    else:
        # we use the flaskbase authn_and_authz function, passing in our authorization callback.
        logger.debug("Config does now allow nonces, not using nonces.")
        flaskbase_az(Tenants, authorization)

def required_level(request):
    """Returns the required permission level for the request."""
    if request.method == 'OPTIONS':
        return codes.NONE
    elif request.method == 'GET':
        return codes.READ
    elif request.method == 'POST' and 'messages' in request.url_rule.rule:
        return codes.EXECUTE
    return codes.UPDATE


def check_nonce():
    """
    This function is an flaskbase authentication callback used to process the existence of a query parameter,
    x-nonce, an alternative authentication mechanism to JWT.
    
    When an x-nonce query parameter is provided, the request context is updated with the identity of the user owning
    the actor to which the nonce belongs. Note that the roles of said user will not be calculated so, in particular, 
    any privileged action cannot be taken via a nonce. 
    """
    logger.debug("top of check_nonce")
    # first check whether the request is even valid -
    if hasattr(request, 'url_rule'):
        logger.debug(f"request.url_rule: {request.url_rule}")
        if hasattr(request.url_rule, 'rule'):
            logger.debug(f"url_rule.rule: {request.url_rule.rule}")
        else:
            logger.info("url_rule has no rule.")
            raise ResourceError(
                "Invalid request: the API endpoint does not exist or the provided HTTP method is not allowed.", 405)
    else:
        logger.info("Request has no url_rule")
        raise ResourceError(
            "Invalid request: the API endpoint does not exist or the provided HTTP method is not allowed.", 405)
    try:
        nonce_id = request.args['x-nonce']
    except KeyError:
        raise PermissionsException("No JWT or nonce provided.")
    logger.debug(f"checking nonce with id: {nonce_id}")
    # the nonce encodes the tenant in its id:
    g.request_tenant_id = Nonce.get_tenant_from_nonce_id(nonce_id)
    g.api_server = get_api_server(g.request_tenant_id)
    logger.debug(f"tenant associated with nonce: {g.request_tenant_id}; api_server assoicated with nonce: {g.api_server}")
    # get the actor_id base on the request path
    actor_id, actor_identifier = get_db_id()
    logger.debug(f"db_id: {actor_id}; actor_identifier: {actor_identifier}")
    level = required_level(request)

    # if the actor_identifier is an alias, then the nonce must be attached to that, so we must pass that in the
    # nonce check:
    if is_hashid(actor_identifier):
        Nonce.check_and_redeem_nonce(actor_id=actor_id, alias=None, nonce_id=nonce_id, level=level)
    else:
        alias_id = Alias.generate_alias_id(tenant=g.request_tenant_id, alias=actor_identifier)
        Nonce.check_and_redeem_nonce(actor_id=None, alias=alias_id, nonce_id=nonce_id, level=level)
    # if we were able to redeem the nonce, update auth context with the actor owner data:
    logger.debug("nonce valid and redeemed.")
    if is_hashid(actor_identifier):
        nonce = Nonce.get_nonce(actor_id=actor_id, alias=None, nonce_id=nonce_id)
    else:
        nonce = Nonce.get_nonce(actor_id=None, alias=alias_id, nonce_id=nonce_id)
    g.request_username = nonce.owner
    # update roles data with that stored on the nonce:
    g.roles = [nonce.roles]
    logger.debug(f"setting g.request_tenant_id: {g.request_tenant_id}; g.request_username: {g.request_username}")


def get_user_sk_roles():
    """
    """
    logger.debug(f"Getting SK roles on tenant {g.request_tenant_id} and user {g.request_username}")
    start_timer = timeit.default_timer()
    try:
        roles_obj = t.sk.getUserRoles(tenant=g.request_tenant_id, user=g.request_username, _tapis_set_x_headers_from_service=True)
    except Exception as e:
        end_timer = timeit.default_timer()
        total = (end_timer - start_timer) * 1000
        if total > 4000:
            logger.critical(f"t.sk.getUserRoles took {total} to run for user {g.request_username}, tenant: {g.request_tenant_id}")
        raise e
    end_timer = timeit.default_timer()
    total = (end_timer - start_timer) * 1000
    if total > 4000:
        logger.critical(f"t.sk.getUserRoles took {total} to run for user {g.request_username}, tenant: {g.request_tenant_id}")
    roles_list = roles_obj.names

    # we now take all roles nad filter to abaco specific roles so we don't spam logs.
    abaco_roles_list = []
    for role in roles_list:
        if "abaco" in role.lower():
            abaco_roles_list.append(role)
    logger.debug(f"Roles received: {abaco_roles_list}")
    g.roles = abaco_roles_list


def get_user_site_id():
    user_tenant_obj = t.tenant_cache.get_tenant_config(tenant_id=g.request_tenant_id)
    user_site_obj = user_tenant_obj.site
    g.site_id = user_site_obj.site_id


def authorization():
    """
    This is the flaskbase authorization callback and implements the main Abaco authorization
    logic. This function is called by flaskbase after all authentication processing and initial
    authorization logic has run.
    """
    # first check whether the request is even valid -
    if hasattr(request, 'url_rule'):
        logger.debug(f"request.url_rule: {request.url_rule}")
        if hasattr(request.url_rule, 'rule'):
            logger.debug(f"url_rule.rule: {request.url_rule.rule}")
        else:
            logger.info("url_rule has no rule.")
            raise ResourceError(
                "Invalid request: the API endpoint does not exist or the provided HTTP method is not allowed.", 405)
    else:
        logger.info("Request has no url_rule")
        raise ResourceError(
            "Invalid request: the API endpoint does not exist or the provided HTTP method is not allowed.", 405)

    # get the actor db_id from a possible identifier once and for all -
    # these routes do not have an actor id in them:
    if request.url_rule.rule == '/actors' \
        or request.url_rule.rule == '/actors/' \
        or '/actors/admin' in request.url_rule.rule \
        or '/actors/aliases' in request.url_rule.rule \
        or '/actors/configs' in request.url_rule.rule \
        or '/actors/utilization' in request.url_rule.rule \
        or '/actors/search/' in request.url_rule.rule:
        db_id = None
        logger.debug(f"setting db_id to None; rule: {request.url_rule.rule}")
    else:
        # every other route should have an actor identifier
        logger.debug(f"fetching db_id; rule: {request.url_rule.rule}")
        db_id, _ = get_db_id()
    g.db_id = db_id
    logger.debug(f"db_id: {db_id}")

    # Generally request.url returns `https://tapis.io/actors`, we get rid of the actors bit.
    g.api_server = request.url_root.replace('http://', 'https://')

    g.admin = False
    if request.method == 'OPTIONS':
        # allow all users to make OPTIONS requests
        logger.info("Allowing request because of OPTIONS method.")
        return True

    get_user_site_id()

    get_user_sk_roles()

    # there is a bug in wso2 that causes the roles claim to sometimes be missing; this should never happen:
    #if not g.roles:
    #    g.roles = ['Internal/everyone']

    # all other requests require some kind of abaco role:
    # THIS IS NO LONGER TRUE. Only rules are admin and privileged.

    has_pem = False
    
    logger.debug(f"request.path: {request.path}")

    # the admin role when JWT auth is configured:
    if codes.ADMIN_ROLE in g.roles:
        g.admin = True
        logger.info("Allowing request because of ADMIN_ROLE.")
        return True

    # the admin API requires the admin role:
    if 'admin' in request.path or '/actors/admin' in request.url_rule.rule or '/actors/admin/' in request.url_rule.rule:
        if g.admin:
            return True
        else:
            raise PermissionsException("Abaco Admin role required.")

    # the utilization endpoint is available to every authenticated user
    if '/actors/utilization' == request.url_rule.rule or '/actors/utilization/' == request.url_rule.rule:
        return True

    if '/actors/search/<string:search_type>' == request.url_rule.rule:
        return True

    # there are special rules on the actors root collection:
    if '/actors' == request.url_rule.rule or '/actors/' == request.url_rule.rule:
        logger.debug("Checking permissions on root collection.")
        # first, only admins can create/update actors to be privileged, so check that:
        if request.method == 'POST':
            check_privileged()
        # if we are here, it is either a GET or a new actor, so the request is allowed:
        logger.debug("new actor or GET on root connection. allowing request.")
        return True

    # aliases root collection has special rules as well -
    if '/actors/configs' == request.url_rule.rule or '/actors/configs/' == request.url_rule.rule:
        # anyone can GET their actor configs and anyone can create an actor config
        return True

    if '/actors/configs' in request.url_rule.rule:
        logger.debug('auth.py /actors/configs if statement')
        config_name = get_config_name()
        config_id = ActorConfig.get_config_db_key(tenant_id=g.request_tenant_id, name=config_name)
        if request.method == 'GET':
            # GET requests require READ access
            has_pem = check_config_permissions(user=g.request_username, config_id=config_id, level=codes.READ)
            # all other requests require UPDATE access
        elif request.method in ['DELETE', 'POST', 'PUT']:
            has_pem = check_config_permissions(user=g.request_username, config_id=config_id, level=codes.UPDATE)
        if not has_pem:
            raise PermissionsException("You do not have sufficient access to this actor config.")

    # aliases root collection has special rules as well -
    if '/actors/aliases' == request.url_rule.rule or '/actors/aliases/' == request.url_rule.rule:
        return True

    # request to a specific alias needs to check aliases permissions
    if '/actors/aliases' in request.url_rule.rule:
        alias_id = get_alias_id()
        noun = 'alias'
        # we need to compute the db_id since it is not computed in the general case for
        # alias endpoints
        db_id, _ = get_db_id()
        # reading/creating/updating nonces for an alias requires permissions for both the
        # alias itself and the underlying actor
        if 'nonce' in request.url_rule.rule:
            noun = 'alias and actor'
            # logger.debug("checking user {} has permissions for "
            #              "alias: {} and actor: {}".format(g.request_username, alias_id, db_id))
            if request.method == 'GET':
                # GET requests require READ access

                has_pem = check_permissions(user=g.request_username, identifier=alias_id, level=codes.READ)
                has_pem = has_pem and check_permissions(user=g.request_username, identifier=db_id, level=codes.READ)
            elif request.method in ['DELETE', 'POST', 'PUT']:
                has_pem = check_permissions(user=g.request_username, identifier=alias_id, level=codes.UPDATE)
                has_pem = has_pem and check_permissions(user=g.request_username, identifier=db_id, level=codes.UPDATE)

        # otherwise, this is a request to manage the alias itself; only requires permissions on the alias
        else:
            if request.method == 'GET':
                # GET requests require READ access
                has_pem = check_permissions(user=g.request_username, identifier=alias_id, level=codes.READ)
                # all other requests require UPDATE access
            elif request.method in ['DELETE', 'POST', 'PUT']:
                has_pem = check_permissions(user=g.request_username, identifier=alias_id, level=codes.UPDATE)
    else:
        # all other checks are based on actor-id:
        noun = 'actor'
        if request.method == 'GET':
            # GET requests require READ access
            has_pem = check_permissions(user=g.request_username, identifier=db_id, level=codes.READ)
        elif request.method == 'DELETE':
            has_pem = check_permissions(user=g.request_username, identifier=db_id, level=codes.UPDATE)
        else:
            logger.debug(f"URL rule in request: {request.url_rule.rule}")
            # first, only admins can create/update actors to be privileged, so check that:
            if request.method == 'POST' or request.method == 'PUT':
                check_privileged()
                # only admins have access to the workers endpoint, and if we are here, the user is not an admin:
                if 'workers' in request.url_rule.rule:
                    raise PermissionsException("Not authorized -- only admins are authorized to update workers.")
                try:
                    # POST to the messages endpoint requires EXECUTE
                    if 'messages' in request.url_rule.rule:
                        has_pem = check_permissions(user=g.request_username, identifier=db_id, level=codes.EXECUTE)
                    # otherwise, we require UPDATE
                    else:
                        has_pem = check_permissions(user=g.request_username, identifier=db_id, level=codes.UPDATE)
                except Exception as e:
                    msg = f"error checking permissions {e}"
                    logger.critical(msg)
                    raise Exception(msg)

    if not has_pem:
        logger.info("NOT allowing request.")
        msg = f"Not authorized -- you do not have access to this {noun}."
        logger.info(msg)
        raise PermissionsException(msg)


def check_privileged():
    """Check if request is trying to make an actor privileged."""
    logger.debug("top of check_privileged")
    # admins have access to all actors:
    if g.admin:
        return True
    data = request.get_json(silent=True)
    if not data:
        data = request.form
    # various APIs (e.g., the state api) allow an arbitrary JSON serializable objects which won't have a get method:
    if not hasattr(data, 'get'):
        return True
    if not codes.PRIVILEGED_ROLE in g.roles:
        logger.info("User does not have privileged role.")
        # if we're here, user isn't an admin so must have privileged role:
        if data.get('privileged'):
            logger.debug("User is trying to set privileged")
            raise PermissionsException("Not authorized -- only admins and privileged users can make privileged actors.")
        if data.get('max_workers') or data.get('maxWorkers'):
            logger.debug("User is trying to set max_workers")
            raise PermissionsException("Not authorized -- only admins and privileged users can set max workers.")
        if data.get('max_cpus') or data.get('maxCpus'):
            logger.debug("User is trying to set max CPUs")
            raise PermissionsException("Not authorized -- only admins and privileged users can set max CPUs.")
        if data.get('mem_limit') or data.get('memLimit'):
            logger.debug("User is trying to set mem limit")
            raise PermissionsException("Not authorized -- only admins and privileged users can set mem limit.")
        if data.get('queue'):
            logger.debug("User is trying to set queue")
            raise PermissionsException("Not authorized -- only admins and privileged users can set queue.")
    else:
        logger.debug("user allowed to set privileged.")

    # when using the UID associated with the user in TAS, admins can still register actors
    # to use the UID built in the container using the use_container_uid flag:
    if conf.global_tenant_object.get('use_tas_uid'):
        if data.get('use_container_uid') or data.get('useContainerUid'):
            logger.debug("User is trying to use_container_uid")
            # if we're here, user isn't an admin so must have privileged role:
            if not codes.PRIVILEGED_ROLE in g.roles:
                logger.info("User does not have privileged role.")
                raise PermissionsException("Not authorized -- only admins and privileged users can use container uid.")
            else:
                logger.debug("user allowed to use container uid.")
    else:
        logger.debug("not trying to use privileged options.")
        return True

def check_permissions(user, identifier, level, roles=None):
    """Check the permissions store for user and level. Here, `identifier` is a unique id in the
    permissions_store; e.g., actor db_id or alias_id.
    """
    logger.debug(f"Checking user: {user} permissions for identifier: {identifier}")
    # first, if roles were passed, check for admin role -
    if roles:
        if codes.ADMIN_ROLE in roles:
            return True
    # get all permissions for this actor -
    try:
        permissions = get_permissions(identifier)
    except PermissionsException:
        # There's a chance that a permission doc does not exist, but an actor still does
        # In this case, no one should have access to it, but we're not just going to delete it
        pass
    for p_user, p_name in permissions.items():
        # if the actor has been shared with the WORLD_USER anyone can use it
        if p_user == WORLD_USER:
            logger.info(f"Allowing request - {identifier} has been shared with the WORLD_USER.")
            return True
        # otherwise, check if the permission belongs to this user and has the necessary level
        if p_user == user:
            p_pem = codes.PermissionLevel(p_name)
            if p_pem >= level:
                logger.info(f"Allowing request - user has appropriate permission with {identifier}.")
                return True
            else:
                # we found the permission for the user but it was insufficient; return False right away
                logger.info(f"Found permission {level} for {identifier}, rejecting request.")
                return False
    # didn't find the user or world_user, return False
    logger.info(f"user had no permissions for {identifier}. Permissions found: {permissions}")
    permissions = get_permissions(identifier)
    if permission_process(permissions, user, level, identifier):
        return True
    # didn't find the user or world_user, return False
    logger.info("user had no permissions for {}. Permissions found: {}".format(identifier, permissions))
    return False


def check_config_permissions(user, config_id, level, roles=None):
    """
    Check if a given `user` has permissions at level `level` for config with id `config_id`. The optional `roles`
    attribute can be passed in to consider roles as well.
    """
    logger.debug(f"top of check_config_permissions; user: {user}; config: {config_id}; level: {level}; roles: {roles}")
    # first, if roles were passed, check for admin role -
    if roles:
        if codes.ADMIN_ROLE in roles:
            return True
    # get all permissions for this config -
    permissions = get_config_permissions(config_id)
    if permission_process(permissions, user, level, config_id):
        return True
    # didn't find the user or world_user, return False
    return False


def get_db_id():
    """Get the db_id and actor_identifier from the request path."""
    # the location of the actor identifier is different for aliases vs actor_id's.
    # for actors, it is in index 2:
    #     /actors/<actor_id>
    # for aliases, it is in index 3:
    #     /actors/aliases/<alias_id>
    idx = 2
    if 'aliases' in request.path:
        idx = 3
    path_split = request.path.split("/")
    if len(path_split) < 3:
        logger.error(f"Unrecognized request -- could not find the actor id. path_split: {path_split}")
        raise PermissionsException("Not authorized.")
    logger.debug(f"path_split: {path_split}")
    try:
        actor_identifier = path_split[idx]
    except IndexError:
        raise ResourceError("Unable to parse actor identifier: is it missing from the URL?", 404)
    logger.debug(f"actor_identifier: {actor_identifier}; tenant: {g.request_tenant_id}")
    if actor_identifier == 'search':
        raise ResourceError("'x-nonce' query parameter on the '/actors/search/{database}' endpoint does not resolve.", 404)
    try:
        actor_id = Actor.get_actor_id(g.request_tenant_id, actor_identifier)
    except KeyError:
        logger.info(f"Unrecognized actor_identifier: {actor_identifier}. Actor not found")
        raise ResourceError(f"Actor with identifier '{actor_identifier}' not found", 404)
    except Exception as e:
        msg = "Unrecognized exception trying to resolve actor identifier: {}; " \
              "exception: {}".format(actor_identifier, e)
        logger.error(msg)
        raise ResourceError(msg)
    logger.debug(f"actor_id: {actor_id}")
    return Actor.get_dbid(g.request_tenant_id, actor_id), actor_identifier

def get_alias_id():
    """Get the alias from the request path."""
    path_split = request.path.split("/")
    if len(path_split) < 4:
        logger.error(f"Unrecognized request -- could not find the alias. path_split: {path_split}")
        raise PermissionsException("Not authorized.")
    alias = path_split[3]
    logger.debug(f"alias: {alias}")
    return Alias.generate_alias_id(g.request_tenant_id, alias)

def get_config_name():
    """Get the config name from the request path."""
    logger.debug("top of auth.get_config_id()")
    path_split = request.path.split("/")
    if len(path_split) < 4:
        logger.error(f"Unrecognized request -- could not find the config. path_split: {path_split}")
        raise PermissionsException("Not authorized.")
    config_name = path_split[3]
    logger.debug(f"returning config_name from path: {config_name}")
    return config_name

def get_tenant_verify(tenant):
    """Return whether to turn on SSL verification."""
    # sandboxes and the develop instance have a self-signed certs
    if 'SANDBOX' in tenant.upper():
        return False
    if tenant.upper() == 'DEV-DEVELOP':
        return False
    return True

def get_tenant_userstore_prefix(tenant):
    """
    Returnst he WSO2 userstore prefix associated with a tenant; this prefix is needed to generating tokens using
    the impersonation grant
    :param tenant:
    :return:
    """
    if tenant == 'SD2E':
        return 'SD2E'
    if tenant == 'TACC':
        return 'TACC'
    if tenant == 'A2CPS':
        return 'A2CPS'
    if tenant == 'DESIGNSAFE':
        return 'TACC'
    if tenant == 'IPLANTC-ORG':
        return 'IPLANTC'
    if tenant == 'VDJ':
        return 'VDJ'
    if tenant == '3DEM':
        return '3DEM'
    if tenant == 'IREC':
        return 'IREC'
    if tenant == 'SGCI':
        return 'SGCI-PROD'
    if tenant == 'DEV' or tenant == 'DEV-DEVELOP' or tenant == 'DEV-STAGING':
        return 'agavedev'
    return 'TACC'

def get_tenants():
    """Return a list of tenants"""
    return ['3DEM',
            'A2CPS',
            'AGAVE-PROD',
            'ARAPORT-ORG',
            'DESIGNSAFE',
            'DEV',
            'DEV-DEVELOP',
            'DEV-STAGING',
            'IPLANTC-ORG',
            'IREC',
            'PORTALS',
            'SD2E',
            'SGCI',
            'TACC-PROD',
            'VDJSERVER-ORG']

def tenant_can_use_tas(tenant):
    """Return whether a tenant can use TAS for uid/gid resolution. This is equivalent to whether the tenant uses
    the TACC IdP"""
    if tenant.upper() in ['DESIGNSAFE', 'SD2E', 'TACC', 'A2CPS']:
        return True
    # all other tenants use some other IdP so username will not be a TAS account:
    return False

# TAS configuration:
# base URL for TAS API.
TAS_URL_BASE = os.environ.get('TAS_URL_BASE', 'https://tas.tacc.utexas.edu/api/v1')
TAS_ROLE_ACCT = os.environ.get('TAS_ROLE_ACCT', 'tas-jetstream')
TAS_ROLE_PASS = os.environ.get('TAS_ROLE_PASS')

def get_tas_data(username, tenant):
    """Get the TACC uid, gid and homedir for this user from the TAS API."""
    logger.debug(f"Top of get_tas_data for username: {username}; tenant: {tenant}")
    if not TAS_ROLE_ACCT:
        logger.error("No TAS_ROLE_ACCT configured. Aborting.")
        return None, None, None
    if not TAS_ROLE_PASS:
        logger.error("No TAS_ROLE_PASS configured. Aborting.")
        return None, None, None
    if not tenant_can_use_tas(tenant):
        logger.debug(f"Tenant {tenant} cannot use TAS")
        return None, None, None
    url = f'{TAS_URL_BASE}/users/username/{username}'
    headers = {'Content-type': 'application/json',
               'Accept': 'application/json'}
    try:
        rsp = requests.get(url,
                           headers=headers,
                           auth=(TAS_ROLE_ACCT, TAS_ROLE_PASS))
    except Exception as e:
        logger.error("Got an exception from TAS API. "
                     f"Exception: {e}. url: {url}. TAS_ROLE_ACCT: {TAS_ROLE_ACCT}")
        return None, None, None
    try:
        data = rsp.json()
    except Exception as e:
        logger.error("Did not get JSON from TAS API. rsp: {}"
                     f"Exception: {e}. url: {url}. TAS_ROLE_ACCT: {TAS_ROLE_ACCT}")
        return None, None, None
    try:
        tas_uid = data['result']['uid']
        tas_homedir = data['result']['homeDirectory']
    except Exception as e:
        logger.error("Did not get attributes from TAS API. rsp: {}"
                     f"Exception: {e}. url: {url}. TAS_ROLE_ACCT: {TAS_ROLE_ACCT}")
        return None, None, None

    # first look for an "extended profile" record in Tapis v3 metadata. such a record might have the
    # gid to use for this user. to do this search we need a service client for the tenant:
    tas_gid = None
    
    
    # Not currently using extended profiles because they both use meta v2 which use agavepy, and
    # none of the service_tokens necessary to test are on SD2E or Tacc tenant, so I assume that they
    # weren't used. To implement again, just add in the code to get the profiles from meta v2 and search for "gid"


    # if we are here, we didn't get a TAS_GID from the extended profile.
    logger.debug("did not get an extended profile.")
    # if the instance has a configured TAS_GID to use we will use that; otherwise,
    # we fall back on using the user's uid as the gid, which is (almost) always safe)
    tas_gid = os.environ.get('TAS_GID', tas_uid)
    logger.info("Setting the following TAS data: uid:{} gid:{} homedir:{}".format(tas_uid,
                                                                                  tas_gid,
                                                                                  tas_homedir))
    return tas_uid, tas_gid, tas_homedir


def get_token_default():
    """
    Returns the default token attribute based on the tenant and instance configs.
    """

    users_tenant_object = conf.get(f"{g.request_tenant_id}_tenant_object") or {}
    default_token = users_tenant_object.get("default_token") or conf.global_tenant_object.get("default_token")
    logger.debug(f"got default_token: {default_token}. Either for {g.request_tenant_id} or global.")
    ## We have to stringify the boolean as it's listed with results and it would require a database change.
    if default_token:
        default_token = 'true'
    else:
        default_token = 'false'
    return default_token

def get_uid_gid_homedir(actor, user, tenant):
    """
    Determines the uid and gid that should be used to run an actor's container. This function does
    not need to be called if the user is a privileged user
    :param actor:
    :param tenant:
    :return:
    """
    logger.debug(f"Top of get_uid_gid_homedir for user: {user} and tenant: {tenant}")
    users_tenant_object = conf.get(f"{tenant}_tenant_object") or {}

    # first, check for tas usage for tenant or globally:
    use_tas = users_tenant_object.get("use_tas_uid") or False
    if use_tas and tenant_can_use_tas(tenant):
        return get_tas_data(user, tenant)

    # next, look for a tenant-specific uid and gid:
    uid = users_tenant_object.get("actor_uid") or None
    gid = users_tenant_object.get("actor_gid") or None
    if uid and gid:
        home_dir = users_tenant_object.get("actor_homedir") or None
        return uid, gid, home_dir

    # next, look for a global use_tas config
    use_tas = conf.global_tenant_object.get("use_tas_uid") or False
    if use_tas and tenant_can_use_tas(tenant):
        return get_tas_data(user, tenant)

    # finally, look for a global uid and gid:
    uid = conf.global_tenant_object.get("actor_uid") or None
    gid = conf.global_tenant_object.get("actor_gid") or None
    if uid and gid:
        home_dir = conf.global_tenant_object.get("actor_homedir") or None
        return uid, gid, home_dir

    # otherwise, run using the uid and gid set in the container
    return None, None, None
