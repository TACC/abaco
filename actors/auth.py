# Utilities for authn/z
import base64
import os
import re

from Crypto.Signature import PKCS1_v1_5
from Crypto.PublicKey import RSA
from Crypto.Hash import SHA256
import configparser
from flask import g, request
import jwt
import requests

from agaveflask.auth import authn_and_authz as agaveflask_az, get_api_server
from agaveflask.logs import get_logger
logger = get_logger(__name__)

from agavepy.agave import Agave
from config import Config
import codes
from models import Actor, Alias, get_permissions, is_hashid, Nonce

from errors import ClientException, ResourceError, PermissionsException


jwt.verify_methods['SHA256WITHRSA'] = (
    lambda msg, key, sig: PKCS1_v1_5.new(key).verify(SHA256.new(msg), sig))
jwt.prepare_key_methods['SHA256WITHRSA'] = jwt.prepare_RS_key


def get_pub_key():
    pub_key = Config.get('web', 'apim_public_key')
    return RSA.importKey(base64.b64decode(pub_key))


PUB_KEY = get_pub_key()

TOKEN_RE = re.compile('Bearer (.+)')

WORLD_USER = 'ABACO_WORLD'

def get_pub_key():
    pub_key = Config.get('web', 'apim_public_key')
    return RSA.importKey(base64.b64decode(pub_key))


def authn_and_authz():
    """All-in-one convenience function for implementing the basic abaco authentication
    and authorization on a flask app. Use as follows:

    import auth

    my_app = Flask(__name__)
    @my_app.before_request
    def authnz_for_my_app():
        auth.authn_and_authz()

    """
    accept_nonce = Config.get('web', 'accept_nonce')
    if accept_nonce:
        agaveflask_az(check_nonce, authorization)
    else:
        # we use the agaveflask authn_and_authz function, passing in our authorization callback.
        agaveflask_az(authorization)

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
    This function is an agaveflask authentication callback used to process the existence of a query parameter,
    x-nonce, an alternative authentication mechanism to JWT.
    
    When an x-nonce query parameter is provided, the request context is updated with the identity of the user owning
    the actor to which the nonce belongs. Note that the roles of said user will not be calculated so, in particular, 
    any privileged action cannot be taken via a nonce. 
    """
    logger.debug("top of check_nonce")
    # first check whether the request is even valid -
    if hasattr(request, 'url_rule'):
        logger.debug("request.url_rule: {}".format(request.url_rule))
        if hasattr(request.url_rule, 'rule'):
            logger.debug("url_rule.rule: {}".format(request.url_rule.rule))
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
    logger.debug("checking nonce with id: {}".format(nonce_id))
    # the nonce encodes the tenant in its id:
    g.tenant = Nonce.get_tenant_from_nonce_id(nonce_id)
    g.api_server = get_api_server(g.tenant)
    logger.debug("tenant associated with nonce: {}; api_server assoicated with nonce: {}".format(g.tenant, g.api_server))
    # get the actor_id base on the request path
    actor_id, actor_identifier = get_db_id()
    logger.debug("db_id: {}; actor_identifier: {}".format(actor_id, actor_identifier))
    level = required_level(request)

    # if the actor_identifier is an alias, then the nonce must be attached to that, so we must pass that in the
    # nonce check:
    if is_hashid(actor_identifier):
        Nonce.check_and_redeem_nonce(actor_id=actor_id, alias=None, nonce_id=nonce_id, level=level)
    else:
        alias_id = Alias.generate_alias_id(tenant=g.tenant, alias=actor_identifier)
        Nonce.check_and_redeem_nonce(actor_id=None, alias=alias_id, nonce_id=nonce_id, level=level)
    # if we were able to redeem the nonce, update auth context with the actor owner data:
    logger.debug("nonce valid and redeemed.")
    if is_hashid(actor_identifier):
        nonce = Nonce.get_nonce(actor_id=actor_id, alias=None, nonce_id=nonce_id)
    else:
        nonce = Nonce.get_nonce(actor_id=None, alias=alias_id, nonce_id=nonce_id)
    g.user = nonce.owner
    # update roles data with that stored on the nonce:
    g.roles = nonce.roles
    # now, manually call our authorization function:
    authorization()

def authorization():
    """This is the agaveflask authorization callback and implements the main Abaco authorization
    logic. This function is called by agaveflask after all authentication processing and initial
    authorization logic has run.
    """
    # first check whether the request is even valid -
    if hasattr(request, 'url_rule'):
        logger.debug("request.url_rule: {}".format(request.url_rule))
        if hasattr(request.url_rule, 'rule'):
            logger.debug("url_rule.rule: {}".format(request.url_rule.rule))
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
        or '/actors/utilization' in request.url_rule.rule \
        or '/actors/search/' in request.url_rule.rule:
        db_id = None
        logger.debug("setting db_id to None; rule: {}".format(request.url_rule.rule))
    else:
        # every other route should have an actor identifier
        logger.debug("fetching db_id; rule: {}".format(request.url_rule.rule))
        db_id, _ = get_db_id()
    g.db_id = db_id
    logger.debug("db_id: {}".format(db_id))

    g.admin = False
    if request.method == 'OPTIONS':
        # allow all users to make OPTIONS requests
        logger.info("Allowing request because of OPTIONS method.")
        return True

    # the 'ALL' role is a role set by agaveflask in case the access_control_type is None
    if codes.ALL_ROLE in g.roles:
        g.admin = True
        logger.info("Allowing request because of ALL role.")
        return True

    # there is a bug in wso2 that causes the roles claim to sometimes be missing; this should never happen:
    if not g.roles:
        g.roles = ['Internal/everyone']

    # all other requests require some kind of abaco role:
    if set(g.roles).isdisjoint(codes.roles):
        logger.info("NOT allowing request - user has no abaco role.")
        raise PermissionsException("Not authorized -- missing required role.")
    else:
        logger.debug("User has an abaco role.")
    logger.debug("request.path: {}".format(request.path))

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
            #              "alias: {} and actor: {}".format(g.user, alias_id, db_id))
            if request.method == 'GET':
                # GET requests require READ access

                has_pem = check_permissions(user=g.user, identifier=alias_id, level=codes.READ)
                has_pem = has_pem and check_permissions(user=g.user, identifier=db_id, level=codes.READ)
            elif request.method in ['DELETE', 'POST', 'PUT']:
                has_pem = check_permissions(user=g.user, identifier=alias_id, level=codes.UPDATE)
                has_pem = has_pem and check_permissions(user=g.user, identifier=db_id, level=codes.UPDATE)

        # otherwise, this is a request to manage the alias itself; only requires permissions on the alias
        else:
            if request.method == 'GET':
                # GET requests require READ access
                has_pem = check_permissions(user=g.user, identifier=alias_id, level=codes.READ)
                # all other requests require UPDATE access
            elif request.method in ['DELETE', 'POST', 'PUT']:
                has_pem = check_permissions(user=g.user, identifier=alias_id, level=codes.UPDATE)
    else:
        # all other checks are based on actor-id:
        noun = 'actor'
        if request.method == 'GET':
            # GET requests require READ access
            has_pem = check_permissions(user=g.user, identifier=db_id, level=codes.READ)
        elif request.method == 'DELETE':
            has_pem = check_permissions(user=g.user, identifier=db_id, level=codes.UPDATE)
        else:
            logger.debug("URL rule in request: {}".format(request.url_rule.rule))
            # first, only admins can create/update actors to be privileged, so check that:
            if request.method == 'POST' or request.method == 'PUT':
                check_privileged()
                # only admins have access to the workers endpoint, and if we are here, the user is not an admin:
                if 'workers' in request.url_rule.rule:
                    raise PermissionsException("Not authorized -- only admins are authorized to update workers.")
                # POST to the messages endpoint requires EXECUTE
                if 'messages' in request.url_rule.rule:
                    has_pem = check_permissions(user=g.user, identifier=db_id, level=codes.EXECUTE)
                # otherwise, we require UPDATE
                else:
                    has_pem = check_permissions(user=g.user, identifier=db_id, level=codes.UPDATE)
    if not has_pem:
        logger.info("NOT allowing request.")
        raise PermissionsException("Not authorized -- you do not have access to this {}.".format(noun))


def check_privileged():
    """Check if request is trying to make an actor privileged."""
    logger.debug("top of check_privileged")
    # admins have access to all actors:
    if g.admin:
        return True
    data = request.get_json()
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
    if Config.get('workers', 'use_tas_uid'):
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
    logger.debug("Checking user: {} permissions for identifier: {}".format(user, identifier))
    # first, if roles were passed, check for admin role -
    if roles:
        if codes.ADMIN_ROLE in roles:
            return True
    # get all permissions for this actor -
    permissions = get_permissions(identifier)
    for p_user, p_name in permissions.items():
        # if the actor has been shared with the WORLD_USER anyone can use it
        if p_user == WORLD_USER:
            logger.info("Allowing request - {} has been shared with the WORLD_USER.".format(identifier))
            return True
        # otherwise, check if the permission belongs to this user and has the necessary level
        if p_user == user:
            p_pem = codes.PermissionLevel(p_name)
            if p_pem >= level:
                logger.info("Allowing request - user has appropriate permission with {}.".format(identifier))
                return True
            else:
                # we found the permission for the user but it was insufficient; return False right away
                logger.info("Found permission {} for {}, rejecting request.".format(level, identifier))
                return False
    # didn't find the user or world_user, return False
    logger.info("user had no permissions for {}. Permissions found: {}".format(identifier, permissions))
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
        logger.error("Unrecognized request -- could not find the actor id. path_split: {}".format(path_split))
        raise PermissionsException("Not authorized.")
    logger.debug("path_split: {}".format(path_split))
    try:
        actor_identifier = path_split[idx]
    except IndexError:
        raise ResourceError("Unable to parse actor identifier: is it missing from the URL?", 404)
    logger.debug("actor_identifier: {}; tenant: {}".format(actor_identifier, g.tenant))
    try:
        actor_id = Actor.get_actor_id(g.tenant, actor_identifier)
    except KeyError:
        logger.info("Unrecognized actor_identifier: {}. Actor not found".format(actor_identifier))
        raise ResourceError("Actor with identifier '{}' not found".format(actor_identifier), 404)
    except Exception as e:
        msg = "Unrecognized exception trying to resolve actor identifier: {}; " \
              "exception: {}".format(actor_identifier, e)
        logger.error(msg)
        raise ResourceError(msg)
    logger.debug("actor_id: {}".format(actor_id))
    return Actor.get_dbid(g.tenant, actor_id), actor_identifier

def get_alias_id():
    """Get the alias from the request path."""
    path_split = request.path.split("/")
    if len(path_split) < 4:
        logger.error("Unrecognized request -- could not find the alias. path_split: {}".format(path_split))
        raise PermissionsException("Not authorized.")
    alias = path_split[3]
    logger.debug("alias: {}".format(alias))
    return Alias.generate_alias_id(g.tenant, alias)

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
            'AGAVE-PROD',
            'ARAPORT-ORG',
            'DESIGNSAFE',
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
    if tenant == 'DESIGNSAFE' or \
       tenant == 'SD2E' or \
       tenant == 'TACC-PROD':
        return True
    # all other tenants use some other IdP so username will not be a TAS account:
    return False

# TAS configuration:
# base URL for TAS API.
TAS_URL_BASE = os.environ.get('TAS_URL_BASE', 'https://tas.tacc.utexas.edu/api/v1')
TAS_ROLE_ACCT = os.environ.get('TAS_ROLE_ACCT', 'tas-jetstream')
TAS_ROLE_PASS = os.environ.get('TAS_ROLE_PASS')

def get_service_client(tenant):
    """Returns the service client for a specific tenant."""
    service_token = os.environ.get('_abaco_{}_service_token'.format(tenant))
    if not service_token:
        raise ClientException("No service token configured for tenant: {}".format(tenant))
    api_server = get_api_server(tenant)
    verify = get_tenant_verify(tenant)
    # generate an Agave client with the service token
    logger.info("Attempting to generate an agave client.")
    return Agave(api_server=api_server,
                 token=service_token,
                 verify=verify)

def get_tas_data(username, tenant):
    """Get the TACC uid, gid and homedir for this user from the TAS API."""
    logger.debug("Top of get_tas_data for username: {}; tenant: {}".format(username, tenant))
    if not TAS_ROLE_ACCT:
        logger.error("No TAS_ROLE_ACCT configured. Aborting.")
        return None, None, None
    if not TAS_ROLE_PASS:
        logger.error("No TAS_ROLE_PASS configured. Aborting.")
        return None, None, None
    if not tenant_can_use_tas(tenant):
        logger.debug("Tenant {} cannot use TAS".format(tenant))
        return None, None, None
    url = '{}/users/username/{}'.format(TAS_URL_BASE, username)
    headers = {'Content-type': 'application/json',
               'Accept': 'application/json'
               }
    try:
        rsp = requests.get(url,
                           headers=headers,
                           auth=requests.auth.HTTPBasicAuth(TAS_ROLE_ACCT, TAS_ROLE_PASS))
    except Exception as e:
        logger.error("Got an exception from TAS API. "
                       "Exception: {}. url: {}. TAS_ROLE_ACCT: {}".format(e, url, TAS_ROLE_ACCT))
        return None, None, None
    try:
        data = rsp.json()
    except Exception as e:
        logger.error("Did not get JSON from TAS API. rsp: {}"
                       "Exception: {}. url: {}. TAS_ROLE_ACCT: {}".format(rsp, e, url, TAS_ROLE_ACCT))
        return None, None, None
    try:
        tas_uid = data['result']['uid']
        tas_homedir = data['result']['homeDirectory']
    except Exception as e:
        logger.error("Did not get attributes from TAS API. rsp: {}"
                       "Exception: {}. url: {}. TAS_ROLE_ACCT: {}".format(rsp, e, url, TAS_ROLE_ACCT))
        return None, None, None

    # first look for an "extended profile" record in agave metadata. such a record might have the
    # gid to use for this user. to do this search we need a service client for the tenant:
    ag = None
    tas_gid = None
    try:
        ag = get_service_client(tenant)
    except ClientException as e:
        logger.info("got ClientException trying to generate the service client; e: {}".format(e))
    except Exception as e:
        logger.error("Unexpected exception trying to generate service client; e: {}".format(e))
    # if we get a service client, try to look up extended profile:
    if ag:
        meta_name = 'profile.{}.{}'.format(tenant.lower(), username)
        q = "{'name': '" + meta_name + "'}"
        logger.debug("using query: {}".format(q))
        try:
            rsp = ag.meta.listMetadata(q=q)
        except Exception as e:
            logger.error("Got an exception trying to retrieve the extended profile. Exception: {}".format(e))
        try:
            tas_gid = rsp[0].value['posix_gid']
        except IndexError:
            logger.info("Got an index error - returning None. response: {}".format(rsp))
            tas_gid = None
        except Exception as e:
            logger.error("Got an exception trying to retrieve the gid from the extended profile. Exception: {}".format(e))
        if tas_gid:
            logger.debug("Got a tas gid from the extended profile.")
            logger.info("Setting the following TAS data: uid:{} gid:{} homedir:{}".format(tas_uid,
                                                                                          tas_gid,
                                                                                          tas_homedir))
            return tas_uid, tas_gid, tas_homedir
        else:
            logger.error("got a valid response but did not get a tas_gid. Full rsp: {}".format(rsp))
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
    token_default = False
    try:
        token_default = Config.get('web', f'{g.tenant}_default_token')
        logger.debug(f"got tenant token_default: {token_default} for {g.tenant}")
    except:
        # if there isn't a tenant config, check for a global config:
        try:
            token_default = Config.get('web', 'default_token')
            logger.debug(f"got global token default: {token_default}")
        except:
            logger.debug("did not find any token default config. Using False")
            token_default = False
    return token_default

def get_uid_gid_homedir(actor, user, tenant):
    """
    Determines the uid and gid that should be used to run an actor's container. This function does
    not need to be called if the user is a privileges user
    :param actor:
    :param tenant:
    :return:
    """
    # first, determine if this tenant is using tas:
    try:
        use_tas = Config.get('workers', '{}_use_tas_uid'.format(tenant))
    except configparser.NoOptionError:
        logger.debug("no {}_use_tas_uid config.".format(tenant))
        try:
            use_tas = Config.get('workers', 'use_tas_uid')
        except configparser.NoOptionError:
            logger.debug("no use_tas_uid config.".format(tenant))
            use_tas = 'false'
    if hasattr(use_tas, 'lower'):
        use_tas = use_tas.lower() == 'true'
    else:
        logger.error("use_tas_uid configured but not as a string. use_tas_uid: {}".format(use_tas))
    if use_tas and tenant_can_use_tas(tenant):
        return get_tas_data(user, tenant)

    # next, look for a tenant-specific uid and gid:
    try:
        uid = Config.get('workers', '{}_actor_uid'.format(tenant))
        gid = Config.get('workers', '{}_actor_gid'.format(tenant))
        found_config = True
        # the homr_dir is optional
        try:
            home_dir = Config.get('workers', '{}_actor_homedir'.format(tenant))
        except:
            home_dir = None
        return uid, gid, home_dir
    except configparser.NoOptionError:
        logger.debug("no tenant uid or gid config.")

    # next, look for a global use_tas config
    try:
        use_tas = Config.get('workers', 'use_tas_uid')
        found_config = True
    except configparser.NoOptionError:
        logger.debug("no use_tas_uid config.")
        use_tas = False
    if use_tas and tenant_can_use_tas(tenant):
        return get_tas_data(user, tenant)

    # finally, look for a global uid and gid:
    try:
        uid = Config.get('workers', 'actor_uid'.format(tenant))
        gid = Config.get('workers', 'actor_gid'.format(tenant))
        found_config = True
        # the homr_dir is optional
        try:
            home_dir = Config.get('workers', 'actor_homedir'.format(tenant))
        except:
            home_dir = None
        return uid, gid, home_dir
    except configparser.NoOptionError:
        logger.debug("no global uid or gid config.")

    # otherwise, run using the uid and gid set in the container
    return None, None, None
