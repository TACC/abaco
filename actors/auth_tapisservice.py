import base64
import json
import os
import re
import threading
import time
from Crypto.PublicKey import RSA
from Crypto.Hash import SHA256
import datetime
import jwt
from tapipy.tapis import Tapis

# serializes service-token refresh across request threads; racing a (single-use) refresh token
# is how a service ends up cached with a dead token until restart.
_service_token_refresh_lock = threading.Lock()
# set while this thread is inside the refresh/re-acquisition critical section: a tapipy call made
# from there must never try to take the (non-reentrant) lock again -- that was a 30s self-stall.
_service_token_refresh_tls = threading.local()
# tenant -> time.monotonic() before which refresh/re-acquisition is not retried, set when
# both failed. Bounds how often a dead auth host is hammered (and how long request threads
# wait on the lock); applies whether or not the current token has expired.
_service_token_refresh_backoff_until = {}
SERVICE_TOKEN_REFRESH_COOLDOWN_S = int(os.environ.get('service_token_refresh_cooldown') or 30)
# refresh this long before expiry (capped per token at a quarter of its TTL). Parsed once here: a
# bad value used to raise inside the per-call try below and silently disable refresh.
SERVICE_TOKEN_REFRESH_MARGIN_S = int(os.environ.get('service_token_refresh_margin') or 3600)
# how long a request thread whose token has already EXPIRED waits for another thread's refresh.
SERVICE_TOKEN_REFRESH_WAIT_S = 30

from tapisservice import errors
from tapisservice.tenants import tenant_cache
from tapisservice.config import conf
from tapisservice.logs import get_logger
logger = get_logger(__name__)


def _load_jwt_leeway_seconds():
    raw = conf.get('jwt_leeway_seconds', 0)
    try:
        leeway = int(raw or 0)
        if leeway < 0:
            raise ValueError(raw)
    except (TypeError, ValueError):
        raise errors.ServiceConfigError(f"jwt_leeway_seconds must be a non-negative integer (seconds); got {raw!r}")
    return leeway

# cast once at load: a string here made every jwt.decode raise, i.e. a 401 on every request.
JWT_LEEWAY_S = _load_jwt_leeway_seconds()


def _redact_headers(headers):
    """
    Request headers safe to log or put in an exception: Authorization (HTTP basic auth, i.e. the
    base64 service password), X-Tapis-Token (the service JWT) and anything else naming a token or
    auth are masked.
    """
    if not headers:
        return headers
    return {k: ('<redacted>' if ('auth' in k.lower() or 'token' in k.lower()) else v) for k, v in headers.items()}


def _is_service_token_acquisition(operation, kwargs):
    """
    True for the service acquiring ITS OWN tokens (get_service_tokens -> tokens.create_token with
    token_username == the client's username). That call must authenticate with the service password
    (tapipy adds basic auth only when no X-Tapis-Token is set), exactly as upstream, where
    get_service_tokens emptied service_tokens before calling create_token. create_token for other
    subjects (e.g. the authenticator minting user tokens) still goes out with the service JWT.
    """
    return (operation.resource_name == 'tokens' and operation.operation_id == 'create_token'
            and kwargs.get('use_basic_auth', True)
            and kwargs.get('token_username') == getattr(operation.tapis_client, 'username', None))


def get_service_tapis_client(tenant_id=None,
                             base_url=None,
                             jwt=None,
                             resource_set='tapipy', #todo -- change back to resource_set='tapipy'
                             custom_spec_dict=None,
                             download_latest_specs=False,
                             tenants=None,
                             access_token_ttl=None,
                             generate_tokens=True,
                             debug_prints=False,
                             spec_dir=None):
    """
    Returns a Tapis client for the service using the service's configuration. If tenant_id is not passed, uses the first
    tenant in the service's tenants configuration.
    :param tenant_id: (str) The tenant_id associated with the tenant to configure the client with.
    :param base_url: (str) The base URL for the tenant to configure the client with.
    :return: (tapipy.tapis.Tapis) A Tapipy client object.
    """
    # if there is no base_url the primary_site_admin_tenant_base_url configured for the service:
    if not base_url:
        base_url = conf.primary_site_admin_tenant_base_url
    if not tenant_id:
        tenant_id = conf.service_tenant_id
    if not tenants:
        # the following would work to reference this module's tenants object, but we'll choose to raise
        # an error instead; it could be that
        # tenants = sys.modules[__name__].tenants
        raise errors.BaseTapisError("As a Tapis service, passing in the appropriate tenants manager object"
                                    "is required.")
    t = Tapis(base_url=base_url,
              tenant_id=tenant_id,
              username=conf.service_name,
              account_type='service',
              service_password=conf.service_password,
              jwt=jwt,
              resource_set=resource_set,
              custom_spec_dict=custom_spec_dict,
              spec_dir=spec_dir,
              debug_prints=debug_prints,
              download_latest_specs=download_latest_specs,
              tenants=tenants,
              plugins=["tapisservice"],
              is_tapis_service=True)
    if generate_tokens:
        logger.debug("tapis service client constructed, now getting tokens.")
        if access_token_ttl:
            t.get_tokens(access_token_ttl=access_token_ttl)
        else:
            t.get_tokens()
    logger.debug("got tokens, returning tapipy client.")
    return t


def get_service_tokens(self, **kwargs):
    """
    Calls the Tapis Tokens API to get access and refresh tokens for a service and set them on the client.
    :return:
    """
    if not 'username' in kwargs:
        username = self.username
    else:
        username = kwargs['username']
    # tapis services manage a set ot tokens, one for each of the tenants for which we need to interact with.
    # Build the new set locally and publish it in ONE assignment at the end. This function
    # used to clobber self.service_tokens with EMPTY placeholders at entry, so (a) a failed
    # acquisition mid-outage left {tenant: {}} poison behind, wedging the service until
    # restart (reproduced live 2026-08-19), and (b) every other thread reading
    # self.service_tokens during the network calls saw an empty dict.
    _existing_tokens = dict(getattr(self, 'service_tokens', None) or {})
    # if the caller passed a tenant_id explicitly, we just use that
    if 'tenant_id' in kwargs:
        target_tenants = [kwargs['tenant_id']]
    # otherwise, we compute all the tenant's that this service could need to interact with.
    else:
        try:
            target_tenants = list(self.tenant_cache.get_site_admin_tenants_for_service())
        except AttributeError:
            raise errors.BaseTapisError("Unable to retrieve target tenants for a service. Are you really a Tapis "
                                        "service? Did you pass in your Tenants manager instance?")
    if not hasattr(self, 'service_tokens'):
        self.service_tokens = {}
    if 'access_token_ttl' not in kwargs:
        # default to a 24 hour access token; the service_token_ttl env var overrides (for testing
        # the refresh/re-acquisition machinery with short-lived tokens).
        access_token_ttl = int(os.environ.get('service_token_ttl') or 86400)
    else:
        access_token_ttl = kwargs['access_token_ttl']
    if 'refresh_token_ttl' not in kwargs:
        # default to 1 year refresh token -
        refresh_token_ttl = 3153600000
    else:
        refresh_token_ttl = kwargs['refresh_token_ttl']
    new_tokens = {}
    for tenant_id in target_tenants:
        logger.debug(f"attempting to generate token for tenant {tenant_id}")
        try:
            target_site_id = self.tenant_cache.get_tenant_config(tenant_id=tenant_id).site_id
        except Exception as e:
            raise errors.BaseTapisError(f"Got exception computing target site id; e:{e}")
        try:
            tokens = self.tokens.create_token(token_username=username,
                                              token_tenant_id=self.tenant_id,
                                              account_type=self.account_type,
                                              access_token_ttl=access_token_ttl,
                                              generate_refresh_token=True,
                                              refresh_token_ttl=refresh_token_ttl,
                                              target_site_id=target_site_id,
                                              _tapis_set_x_headers_from_service=True)
        except Exception as e:
            # nothing has been published yet: self.service_tokens is exactly what it was,
            # so a failed acquisition never leaves the client with LESS than it started with.
            req = getattr(e, 'request', None)
            raise errors.BaseTapisError(f"Could not generate service tokens for service: {username};\n"
                                           f"exception: {e};\n"
                                           f"function args:\n"
                                           f"token_username: {self.username};\n "
                                           f"account_type: {self.account_type};\n "
                                           f"target_site_id: {target_site_id};\n "
                                           f"request url: {getattr(req, 'url', None)}; \n "
                                           f"headers: {_redact_headers(getattr(req, 'headers', None))}")
        logger.debug(f"generate token for tenant {tenant_id} successfully.")
        new_tokens[tenant_id] = {'access_token': self.add_claims_to_token(tokens.access_token),
                                 'refresh_token': tokens.refresh_token}
    # single-tenant acquisition must not drop other tenants' tokens: merge, then publish.
    merged = {t: tok for t, tok in _existing_tokens.items() if tok}
    merged.update(new_tokens)
    self.service_tokens = merged


def refresh_service_tokens(self, tenant_id):
    """
    Use the refresh token operation for tokens of type "service".
    """
    refresh_token = self.service_tokens[tenant_id]['refresh_token'].refresh_token
    tokens = self.tokens.refresh_token(refresh_token=refresh_token, _tapis_set_x_headers_from_service=True)
    self.service_tokens[tenant_id]['access_token'] = self.add_claims_to_token(tokens.access_token)
    self.service_tokens[tenant_id]['refresh_token'] = tokens.refresh_token
    return tokens.access_token


def set_refresh_token(self, token):
    """
    Set the refresh token to be used in this session.
    :param token: (TapisResult) A TapisResult object returned using the t.tokens.create_token() method.
    :return:
    """

    def _expires_in():
        return self.refresh_token.expires_at - datetime.datetime.now(datetime.timezone.utc)

    self.refresh_token = token
    try:
        self.refresh_token.claims = self.validate_token(self.refresh_token.refresh_token)
        self.refresh_token.original_ttl = self.refresh_token.expires_in
        self.refresh_token.expires_in = _expires_in
        self.refresh_token.expires_at = datetime.datetime.fromtimestamp(self.refresh_token.claims['exp'],
                                                                        datetime.timezone.utc)
    except:
        pass


def preprocess_service_request(operation, prepared_request, **kwargs):
    """
    This function is called whenever a tapis.<resource>.<operation> is invoked (i.e, when the __call__ method executes) 
    before tapipy sends the request to the Tapis API server.
    
    operation: the Operation object being invoked.
    prepared_request: the requests.PreparedRequest object created by the tapipy library
    kwargs: the kwargs passed to the __call__ function.

    This function makes the following updates to the prepared_request object:
    1. It sets the base URL for the request. 
    2. It sets the X-Tapis-Token header to the appropriate service token.
    3. It sets the X-Tapis-Tenant and X-Tapis-User headers.
    """
    logger.debug(f"top of preprocess_service_request for operation: {operation.http_method.upper()}: {operation.path_name}")
    # for service requests, we must determine which site to use for the request. there are 3 cases.
    # first, the caller can explicitly set the _tapis_set_x_headers_from_service variable. this instructs the library
    # to use the service's site and name for setting the X-Tapis-Tenant and User headers when making the request.
    if '_tapis_set_x_headers_from_service' in kwargs and kwargs['_tapis_set_x_headers_from_service']:
        # in this case, we assume the tenant for the request is the admin tenant of the site the service belongs to.
        request_tenant_id = conf.service_tenant_id
        request_x_tapis_user = conf.service_name
        site_id, base_url = operation.tapis_client.tenant_cache.get_site_and_base_url_for_service_request(request_tenant_id, 
                                                                                                    operation.resource_name)
    # otherwise, if the _x_tapis_tenant and _x_tapis_user have been explicitly set, we use those to determine the site.
    elif '_x_tapis_tenant' in kwargs and '_x_tapis_user' in kwargs:
        request_tenant_id = kwargs['_x_tapis_tenant']
        request_x_tapis_user = kwargs['_x_tapis_user']
        site_id, base_url = operation.tapis_client.tenant_cache.get_site_and_base_url_for_service_request(request_tenant_id, 
        operation.resource_name)
    # next we'll look for "defaults" on the client.
    elif hasattr(operation.tapis_client, 'x_tenant_id') and hasattr(operation.tapis_client, 'x_username'):
        request_tenant_id = operation.tapis_client.x_tenant_id
        request_x_tapis_user = operation.tapis_client.x_username
        site_id, base_url = operation.tapis_client.tenant_cache.get_site_and_base_url_for_service_request(request_tenant_id, 
        operation.resource_name)        
    # finally, we look for a request object in the request_thread_local that could have these 
    else:
        if conf.python_framework_type == 'flask':
            logger.debug("looking on the flask thread local to determine the X-Tapis-* headers ")
            
            from tapisservice.tapisflask import request_thread_local
            try:
                request_tenant_id = request_thread_local.request_tenant_id
            except RuntimeError:
                msg = "Couldn't determine how to set the X-Tapis-Tenant and User headers. This is probably a flask service running "\
                "outside of the request context. Consider setting _tapis_set_x_headers_from_service or set the "\
                    "_x_tapis_tenant and _x_tapis_user parameters."
                raise errors.BaseTapisError(msg)
            # always use x_tapis_user, if set:
            if hasattr(request_thread_local, 'x_tapis_user') and request_thread_local.x_tapis_user:
                request_x_tapis_user = request_thread_local.x_tapis_user
            # if x_tapis_user was not set, then we look for the `username` attr on the request_thread_local. this attr 
            # gets set based on the access token.
            elif hasattr(request_thread_local, 'username') and request_thread_local.username:
                request_x_tapis_user = request_thread_local.username
            # finally, if we don't have an x_tapis_user header OR a token, we look for a kwarg _x_tapis_user
            elif '_x_tapis_user' in kwargs.keys():
                request_x_tapis_user = kwargs.get('_x_tapis_user')
            else:
                request_x_tapis_user = conf.service_name
            site_id, base_url = operation.tapis_client.tenant_cache.get_site_and_base_url_for_service_request(request_tenant_id, 
                                                                                                    operation.resource_name)

        elif conf.python_framework_type == 'django':
            msg = """Did not find `_tapis_set_x_headers_from_service` or `_x_tapis_tenant` and `_x_tapis_user` in kwargs; 
            Automatic derivation of these properties is currently only supported for APIs written in flask. 
            If your API is written in flask, be sure to set conf.python_framework_type == flask."""
            raise errors.BaseTapisError(msg)
        else:
            msg = """Did not find `_tapis_set_x_headers_from_service` or `_x_tapis_tenant` and `_x_tapis_user` in kwargs; 
            Automatic derivation of these properties is currently only supported for APIs written in flask. 
            If your API is written in flask, be sure to set conf.python_framework_type == flask."""
            raise errors.BaseTapisError(msg)
    
    # modify the request base url --
    # the original base URL is the URL up to the '/v3/'
    orig_base_url = prepared_request.url.split('/v3')[0]
    prepared_request.url = prepared_request.url.replace(orig_base_url, base_url)
    logger.debug(f"final URL: {prepared_request.url}")

    # modify the X-Tapis-Tenant and X-Tapis-User request headers ---
    prepared_request.headers['X-Tapis-Tenant'] = request_tenant_id
    prepared_request.headers['X-Tapis-User'] = request_x_tapis_user

    # set the X-Tapis-Token header -----
    # the tenant_id for the request could be a user tenant (e.g., "tacc" or "dev") but the
    # service tokens are stored by admin tenant, so we need to get the admin tenant for the
    # owning site of the tenant.
    request_site_admin_tenant_id = None
    for tn in operation.tapis_client.tenant_cache.tenants.values(): # tenant_cache is a dict
        if tn.site.site_id == site_id:
            request_site_admin_tenant_id = tn.site.site_admin_tenant_id
    if request_site_admin_tenant_id is None:
        # previously this fell through to an UnboundLocalError on every outbound call
        raise errors.BaseTapisError(f"Could not compute the site admin tenant for site {site_id}; "
                                    f"the tenant cache may be empty or stale.")
    logger.debug(f"site admin tenant for the request: {request_site_admin_tenant_id}")
    # the service's own token acquisition goes out with basic auth: never the cached JWT (it may be
    # the expired one being replaced), and never a refresh from inside it.
    if _is_service_token_acquisition(operation, kwargs):
        prepared_request.headers.pop('X-Tapis-Token', None)
        logger.debug("service token acquisition; using basic auth, not the cached service token.")
        return
    # service_tokens may be defined but still be empty dictionaries... this __call__ could be to get
    # the service's first set of tokens.
    if request_site_admin_tenant_id in operation.tapis_client.service_tokens.keys() \
            and 'access_token' in operation.tapis_client.service_tokens[request_site_admin_tenant_id].keys():
        try:
            access_token = operation.tapis_client.service_tokens[request_site_admin_tenant_id]['access_token']
            jwt_str = access_token.access_token
            prepared_request.headers['X-Tapis-Token']= jwt_str
            # also remove the basic auth header; we shouldn't send both
            prepared_request.headers.pop('Authorization', None)
        except (KeyError, TypeError):
            raise errors.BaseTapisError(f"Did not find service tokens for "
                                        f"tenant {request_site_admin_tenant_id};")
        
        # The remaining code is to check whether the token has expired and refresh if necessary
        # if the access token doesn't have an expires_in attribute, there isn't much we can do.
        if not hasattr(access_token, "expires_in"):
            #  tokens created by and for the Tokens API itself do not have an expired_in attr; they are
            #  service.models.TapisAccessToken type
            try: 
                if operation.tapis_client.username == 'tokens':
                    return
            except:
                pass
            # it is expected that some tokens, including will be raw string types or 
            if not type(access_token) == str:
                logger.warn(f"The access token didn't have an expired_in attr and was not a string type. type was: {type(access_token)}")
            logger.debug("returning from preprocess_service_request without checking for the need to refresh.")
            return
        # check the time remaining on the access token ---        
        
        logger.debug("checking to see if we need to refresh the service token...")
        expired = False
        try:
            time_remaining = access_token.expires_in()
            # refresh well before expiry (SERVICE_TOKEN_REFRESH_MARGIN_S, default 1 hour) so a transient
            # refresh failure gets many retry opportunities before the token actually dies. the old margin
            # was 5 seconds, which meant one bad window at hour 24 could kill the service. Capped at a
            # quarter of the token's TTL: a margin >= the TTL would refresh on every outbound call.
            refresh_margin_s = SERVICE_TOKEN_REFRESH_MARGIN_S
            ttl = getattr(access_token, 'original_ttl', None)
            if isinstance(ttl, (int, float)) and ttl > 0:
                refresh_margin_s = min(refresh_margin_s, ttl / 4)
            refresh_margin = datetime.timedelta(seconds=refresh_margin_s)
            if refresh_margin > time_remaining:
                if (operation.resource_name == 'tokens' and operation.operation_id == 'refresh_token')\
                        or (operation.resource_name == 'authenticator' and operation.operation_id == 'create_token')\
                        or getattr(_service_token_refresh_tls, 'active', False):
                    pass
                else:
                    expired = time_remaining <= datetime.timedelta(0)

                    def _in_cooldown():
                        # a refresh AND a re-acquisition just failed; do not make the next N seconds of
                        # requests each pay two timed-out calls (or queue on the lock) for nothing.
                        return time.monotonic() < _service_token_refresh_backoff_until.get(request_site_admin_tenant_id, 0)
                    # Inside the margin the current token is still valid, so a thread that finds
                    # another refresh in progress just uses it (non-blocking acquire); only an
                    # already-expired token is worth waiting for. Refresh used to hold the lock
                    # across two network calls (up to ~70s with tapipy timeouts), so during an
                    # auth outage every outbound call queued behind it, one at a time. The cooldown
                    # is checked before the lock, expired or not (an expired token fails fast below).
                    if _in_cooldown():
                        acquired = False
                        logger.warning(f"service-token refresh for {request_site_admin_tenant_id} is in cooldown "
                                       f"after a failure; using the current token.")
                    elif expired:
                        acquired = _service_token_refresh_lock.acquire(timeout=SERVICE_TOKEN_REFRESH_WAIT_S)
                    else:
                        acquired = _service_token_refresh_lock.acquire(blocking=False)
                    if acquired:
                        _service_token_refresh_tls.active = True
                        try:
                            current = operation.tapis_client.service_tokens[request_site_admin_tenant_id]['access_token']
                            # another thread may have refreshed -- or failed and started a cooldown --
                            # while we waited for the lock
                            if refresh_margin <= current.expires_in() or _in_cooldown():
                                access_token = current
                            else:
                                logger.info("service token near expiry; attempting to refresh service tokens.")
                                try:
                                    try:
                                        access_token = operation.tapis_client.refresh_service_tokens(tenant_id=request_site_admin_tenant_id)
                                        logger.info("service tokens refreshed successfully.")
                                    except Exception as e:
                                        # the stored refresh token may be dead (e.g., rotated but the response
                                        # was lost). fall back to acquiring a brand-new token pair with the
                                        # service password -- the difference between a blip and a permanent
                                        # outage. get_tokens (get_service_tokens, bound in tapisservice/__init__.py)
                                        # merges into the existing dict, so other tenants' tokens survive.
                                        logger.error(f"Service-token refresh failed; attempting full re-acquisition. exception: {e}")
                                        operation.tapis_client.get_tokens(tenant_id=request_site_admin_tenant_id)
                                        access_token = operation.tapis_client.service_tokens[request_site_admin_tenant_id]['access_token']
                                        logger.info("service tokens re-acquired successfully after failed refresh.")
                                    _service_token_refresh_backoff_until.pop(request_site_admin_tenant_id, None)
                                except Exception:
                                    _service_token_refresh_backoff_until[request_site_admin_tenant_id] = \
                                        time.monotonic() + SERVICE_TOKEN_REFRESH_COOLDOWN_S
                                    raise
                        finally:
                            _service_token_refresh_tls.active = False
                            _service_token_refresh_lock.release()
                    else:
                        logger.debug("another thread is refreshing the service token (or refresh is in cooldown); using the current one.")
                    if access_token.expires_in() <= datetime.timedelta(0):
                        raise errors.BaseTapisError(f"The service token for {request_site_admin_tenant_id} has expired "
                                                    f"and is not being refreshed right now (cooldown after a failure, or "
                                                    f"another thread's refresh timed out).", 503)
                    prepared_request.headers['X-Tapis-Token']= access_token.access_token
                    # also remove the basic auth header; we shouldn't send both
                    prepared_request.headers.pop('Authorization', None)
        except Exception as e:
            if expired:
                # nothing valid to fall back on: sending the expired token only turns this into a
                # misleading 401 downstream, so fail fast with a 503.
                if isinstance(e, errors.BaseTapisError) and getattr(e, 'code', None) == 503:
                    raise
                raise errors.BaseTapisError(f"The service token for {request_site_admin_tenant_id} has expired and "
                                            f"could not be refreshed or re-acquired; exception: {e}", 503)
            # both refresh and re-acquisition failed; proceed with the existing token (still valid
            # while inside the refresh margin). every subsequent request retries the whole ladder.
            logger.error(f"Could not refresh or re-acquire the service token; will proceed with the existing token. exception: {e}")
    else:
        # not having a token is not an issue if this is a request to generate a token --
        if '/v3/tokens' not in prepared_request.url:
            logger.warning(f"Not able to set the access token; service token keys: {operation.tapis_client.service_tokens.keys()}")
            # SELF-HEAL: an empty/poisoned entry used to be a dead end -- the whole
            # refresh/re-acquire ladder lives in the have-a-token branch, so the
            # service stayed wedged (500s) until a human restarted it. Re-acquire
            # under the same lock; every request retries until the auth host is back,
            # except during the post-failure cooldown, when requests fail fast instead
            # of queueing behind a lock holder waiting on a dead auth host.
            try:
                now = time.monotonic()
                backoff_until = _service_token_refresh_backoff_until.get(request_site_admin_tenant_id, 0)
                if getattr(_service_token_refresh_tls, 'active', False):
                    entry = {}  # called from inside a refresh on this thread: never re-enter it
                elif now < backoff_until:
                    logger.warning(f"service token for {request_site_admin_tenant_id} is missing and re-acquisition is "
                                   f"in cooldown for another {backoff_until - now:.0f}s; request proceeds without a token.")
                    entry = {}
                elif _service_token_refresh_lock.acquire(timeout=SERVICE_TOKEN_REFRESH_WAIT_S):
                    _service_token_refresh_tls.active = True
                    try:
                        # another thread may have re-acquired -- or failed and started a cooldown --
                        # while we waited; only the first of a queue of waiters pays for get_tokens.
                        entry = operation.tapis_client.service_tokens.get(request_site_admin_tenant_id) or {}
                        if 'access_token' not in entry \
                                and time.monotonic() >= _service_token_refresh_backoff_until.get(request_site_admin_tenant_id, 0):
                            logger.error(f"service token entry for {request_site_admin_tenant_id} is empty; "
                                         f"attempting full re-acquisition (self-heal).")
                            try:
                                operation.tapis_client.get_tokens(tenant_id=request_site_admin_tenant_id)
                            except Exception:
                                _service_token_refresh_backoff_until[request_site_admin_tenant_id] = \
                                    time.monotonic() + SERVICE_TOKEN_REFRESH_COOLDOWN_S
                                raise
                            entry = operation.tapis_client.service_tokens.get(request_site_admin_tenant_id) or {}
                    finally:
                        _service_token_refresh_tls.active = False
                        _service_token_refresh_lock.release()
                else:
                    logger.warning(f"timed out waiting for another thread's re-acquisition for {request_site_admin_tenant_id}; "
                                   f"request proceeds without a token.")
                    entry = {}
                if 'access_token' in entry:
                    prepared_request.headers['X-Tapis-Token'] = entry['access_token'].access_token
                    prepared_request.headers.pop('Authorization', None)
                    logger.info(f"self-heal succeeded; token restored for {request_site_admin_tenant_id}.")
            except Exception as e:
                logger.error(f"self-heal re-acquisition failed; request proceeds without a token. e: {e}")
    logger.debug("returning from preprocess_service_request")
            

def add_headers(request_thread_local, request):
    """
    Adds the standard Tapis headers to the request thread local.

    Requires the library to pass a `request` object with the `headers` attribute; i.e., `request.headers` should be the 
    headers sent on the request.

    :return:
    """
    # the actual access token -
    request_thread_local.x_tapis_token = request.headers.get('X-Tapis-Token')

    # the tenant associated with the subject of the request; used, for instance, when the subject is different
    # from the subject in the actual access_token (for example, when the access_token represents a service account).
    request_thread_local.x_tapis_tenant = request.headers.get('X-Tapis-Tenant')

    # the user associated with the subject of the request. Similar to x_tapis_tenant, this is used, for instance, when
    # the subject is different from the subject in the actual access_token (for example, when the access_token
    # represents a service account).
    request_thread_local.x_tapis_user = request.headers.get('X-Tapis-User')

    # a hash of the original user's access token. this can be used, for instance, to check if the original user's
    # access token has been revoked.
    request_thread_local.x_tapis_user_token_hash = request.headers.get('X-Tapis-User-Token-Hash')


def resolve_tenant_id_for_request(request_thread_local, request, tenant_cache=tenant_cache):
    """
    Resolves the tenant associated with the request and sets it on the request_thread_local.request_tenant_id variable.
    Additionally, sets the request_thread_local.request_tenant_base_url variable in the process. Returns the
    request_tenant_id (string).
    
    request should be the request object for the incoming request. at a minimum it should have the following:
      * request.headers - the headers of the request, as a dictionary-like object.
      * request.base_url - the base URL for the request which should include the domain (e.g., localhost or dev.tapis.io).
    request_base_url should be the base URL on the request object. for example, in the flask framework, it is the
    flask.request.base_url attribute. We assume it includes the protocol ('http://' or 'https://') as well as the port
    (e.g., ':5000') if included in the actual request.

    The high-level algorithm is as follows:

    1) If the X-Tapis-Tenant header is set in the request, this is the tenant_id for the request;
    If the X-Tapis-Token is provided in this case, it must be a service token. The Tapis service should validate
    aspects of service token usage by calling the validate_request_token() function.
    2) If the X-Tapis-Tenant header is not set, then the base_url for this request dictates the tenant_id. There
    are two sub-cases:
      2a) The base_url is the tenant's base URL, in which case the base_url will be in the tenant list,
      2b) The base_url is the tenant's primary-site URL (this is the case when the associate site has forwarded
          a request to the primary site) in which case the base_url will be of the form <tenant_id>.tapis.io
      cf., https://confluence.tacc.utexas.edu/display/CIC/Authentication+Subsystem
    :return:
    """
    logger.debug("top of resolve_tenant_id_for_request")
    add_headers(request_thread_local, request)
    # if the x_tapis_tenant header was set, then this must be a request from a service account. in this case, the
    # request_tenant_id will in general not match the tapis/tenant_id claim in the service token.
    if request_thread_local.x_tapis_tenant:
        # if a token was also set, we need to do additional checks
        if request_thread_local.x_tapis_token:
            logger.debug("found x_tapis_tenant and x_tapis_token on the request_thread_local object.")
            # need to check token is a service token
            if not hasattr(request_thread_local, 'token_claims'):
                logger.error(f"Did not find token_claims attribute on request_thread_local; attrs: {dir(request_thread_local)}")
            if not request_thread_local.token_claims.get('tapis/account_type') == 'service':
                raise errors.PermissionsError('Setting X-Tapis-Tenant header and X-Tapis-Token requires a service token.')
        
        # validation has passed, so set the request tenant_id to the x_tapis_tenant:
        request_thread_local.request_tenant_id = request_thread_local.x_tapis_tenant
        request_tenant = tenant_cache.get_tenant_config(tenant_id=request_thread_local.request_tenant_id)
        request_thread_local.request_tenant_base_url = request_tenant.base_url
        # todo -- compute and set g.request_site_id
        return request_thread_local.request_tenant_id
    # in all other cases, the request's tenant_id is based on the base URL of the request:
    logger.debug(f"computing base_url based on the URL of the request: {request.base_url}")
    # the request_baseurl includes the protocol, port (if present) and contains the url path; examples:
    #  http://localhost:5000/v3/oauth2/tenant;
    #  https://dev.develop.tapis.io/v3/oauth2/tenant
    # in the local development case, the base URL (e.g., localhost:5000, 172.17.0.1, dev_base_url conf var)
    # cannot be used to resolve the tenant id so instead we use the tenant_id claim within the x-tapis-token:
    dev_request_url = conf.get("dev_request_url", "dev://request_url")
    if 'http://172.17.0.1:' in request.base_url or 'http://localhost:' in request.base_url or dev_request_url in request.base_url:
        logger.debug(f"found 172.17.0.1, localhost, or {dev_request_url} in base_url")
        # some services, such as authenticator, have endpoints that do not receive tokens. in the local development
        # case for these endpoints, we don't have a lot of good options -- we can't use the base URL or the token
        # to determine the tenant, so we just set it to the "dev" tenant.
        if not hasattr(request_thread_local, 'token_claims'):
            logger.warn("did not find a token_claims attribute in local development case. Can't use the URL, can't"
                        "use the token. We have no option but to set the tenant to dev!!")
            request_thread_local.request_tenant_id = 'dev'
            request_thread_local.request_tenant_base_url = 'http://dev.develop.tapis.io'
            return request_thread_local.request_tenant_id
        request_tenant = tenant_cache.get_tenant_config(tenant_id=request_thread_local.token_claims.get('tapis/tenant_id'))
        request_thread_local.request_tenant_id = request_tenant.tenant_id
        request_thread_local.request_tenant_base_url = request_tenant.base_url
        # todo -- compute and set request_thread_local.request_site_id
        return request_thread_local.request_tenant_id
    # otherwise we are not in the local development case, so use the request's base URL to determine the tenant id
    # and make sure that tenant_id matches the tenant_id claim in the token.
    request_tenant = tenant_cache.get_tenant_config(url=request.base_url)
    request_thread_local.request_tenant_id = request_tenant.tenant_id
    request_thread_local.request_tenant_base_url = request_tenant.base_url
    # todo -- compute and set request_thread_local.request_site_id
    # we need to check that the request's tenant_id matches the tenant_id in the token:
    if request_thread_local.x_tapis_token:
        logger.debug("found x_tapis_token on g; making sure tenant claim inside token matches that of the base URL.")
        if not hasattr(request_thread_local, "token_claims"):
            logger.error(f"request_thread_local missing token_claims! attrs: {dir(request_thread_local)}")
        token_tenant_id = request_thread_local.token_claims.get('tapis/tenant_id')
        if not token_tenant_id == request_thread_local.request_tenant_id:
            raise errors.PermissionsError(f'The tenant_id claim in the token, '
                                          f'{token_tenant_id} does not match the URL tenant, {request_thread_local.request_tenant_id}.')
    logger.debug(f"resolve_tenant_id_for_request returning {request_thread_local.request_tenant_id}")
    return request_thread_local.request_tenant_id


def validate_request_token(request_thread_local, tenant_cache=tenant_cache, expected_aud=[]):
    """
    Attempts to validate the Tapis access token in the request based on the public key and signature in the JWT.
    This function raises
        - NoTokenError - if no token is present in the request.
        - AuthenticationError - if validation is not successful.
    :param tenants: The service's tenants object.
    :param expected_aud: allows developers to change the expected audience of the token.
    :return:
    """
    logger.debug(f"top of validate_request_token; thread_local attrs: {dir(request_thread_local)}")
    if not hasattr(request_thread_local, 'x_tapis_token'):
        raise errors.NoTokenError("No access token found in the request.")
    claims = validate_token(request_thread_local.x_tapis_token, tenant_cache, expected_aud)
    # set basic variables on the flask thread-local
    request_thread_local.token_claims = claims
    request_thread_local.username = claims.get('tapis/username')
    # We initialize request_username to look at token username. We overwrite this if it's a service account
    # later by setting the var equal to request_thread_local.x_tapis_user. Services should use this variable
    # to process request with correct username.
    request_thread_local.request_username = claims.get('tapis/username')
    request_thread_local.tenant_id = claims.get('tapis/tenant_id')
    request_thread_local.account_type = claims.get('tapis/account_type')
    request_thread_local.delegation = claims.get('tapis/delegation')
    # service tokens have some extra checks:
    if claims.get('tapis/account_type') == 'service':
        request_thread_local.site_id = claims.get('tapis/target_site_id')
        service_token_checks(request_thread_local, claims, tenant_cache)
        # Overwrite request_username with x_tapis_user which is set using `_x_tapis_user` headers by service
        request_thread_local.request_username = request_thread_local.x_tapis_user
    # user tokens must *not* set the X-Tapis-Tenant and X-Tapis_user headers
    else:
        try:
            if request_thread_local.x_tapis_tenant or request_thread_local.x_tapis_user:
                raise errors.AuthenticationError("Invalid request; cannot set OBO headers with a user token.")
        except AttributeError:
            pass


def insecure_decode_jwt_to_claims(token):
    """
    Returns the claims associated with a token WITHOUT checking the signature.
    """
    # JWT is 3 parts delineated by '.' character
    parts = token.split('.')
    # should be three parts
    if not len(parts) == 3:
        raise errors.BaseTapisError(f"Invalid JWT format; did not get 3 parts got: {len(parts)}.")
    # convert part 1 to bytes and add additional padding; see this issue: 
    # https://stackoverflow.com/questions/2941995/python-ignore-incorrect-padding-error-when-base64-decoding
    part_bytes = parts[1].encode()
    part_bytes = part_bytes + b'=='
    # try to base64 decode the middle part after adding padding; the decode returns `data` as bytes
    try:
        data = base64.b64decode(part_bytes)
    except Exception as e:
        raise errors.BaseTapisError(f"could not b64 decode the data; exception: {e}")
    try:
        claims = json.loads(data)
    except Exception as e:
        raise errors.BaseTapisError(f"Could not serialize the bytes data; exception: {e}")
    return claims


def validate_token(token, tenant_cache=tenant_cache, expected_aud=[]):
    """
    Stand-alone function to validate a Tapis token. 
    :param token: The token to validate
    :param tenant_cache: The service's tenant_cache object with tenant configs; Should be an instance of TenantCache.
    :param expected_aud: allows developers to change the expected audience of the token.
    Tapis doesn't set/care about aud. But OIDC clients like it on tokens. Meaning OIDC clients might check /oauth2/userinfo/oidc
    with a token using aud. For this case we need to be able to set what to expect. If none, any aud is rejected. Info in comments
    :return: 
    """
    # first, decode the token data to determine the tenant associated with the token. We are not able to
    # check the signature until we know which tenant, and thus, which public key, to use for validation.
    logger.debug("top of validate_token")
    if not token:
        raise errors.NoTokenError("No Tapis access token found in the request.")
    try:
        data = insecure_decode_jwt_to_claims(token)
    except Exception as e:
        logger.debug(f"got exception trying to parse data from the access_token jwt; exception: {e}")
        raise errors.AuthenticationError("Could not parse the Tapis access token.")
    logger.debug(f"got data from token: {data}")
    # get the tenant out of the jwt payload and get associated public key
    try:
        token_tenant_id = data['tapis/tenant_id']
    except KeyError:
        raise errors.AuthenticationError("Unable to process Tapis token; could not parse the tenant_id. It is possible "
                                         "the token is in a format no longer supported by the platform.")
    try:
        token_tenant = tenant_cache.get_tenant_config(tenant_id=token_tenant_id)
        public_key_str = token_tenant.public_key
    except errors.BaseTapisError:
        logger.error(f"Did not find the public key for tenant_id {token_tenant_id} in the tenant configs.")
        raise errors.AuthenticationError("Unable to process Tapis token; unexpected tenant_id.")
    except AttributeError:
        raise errors.AuthenticationError("Unable to process Tapis token; no public key associated with the "
                                         "tenant_id.")
    if not public_key_str:
        raise errors.AuthenticationError("Could not find the public key for the tenant_id associated with the tenant.")
    # allow a small configurable clock skew between systems; default remains strict (0).
    jwt_leeway = JWT_LEEWAY_S
    # check signature and decode
    tries = 0
    while tries < 2:
        tries = tries + 1
        try:
            # expected_aud - https://github.com/jpadilla/pyjwt/blob/master/docs/usage.rst#audience-claim-aud
            # by default, if oidc token with aud is input, jwt.decode rejects it. if no aud at all, it'll accept
            # oidc clients generally want aud==client_id. expected_aud allows us to configure validation
            # by default this implementation rejects all aud, set expected to change that
            if "*" in expected_aud:
                claims = jwt.decode(token, public_key_str, algorithms=["RS256"], options={"verify_aud": False}, leeway=jwt_leeway)
            elif expected_aud:
                claims = jwt.decode(token, public_key_str, algorithms=["RS256"], audience=expected_aud, leeway=jwt_leeway)
            else:
                claims = jwt.decode(token, public_key_str, algorithms=["RS256"], leeway=jwt_leeway)

        except jwt.exceptions.ExpiredSignatureError as e:
            # token signature has expired; no point refreshing the tenant cache, fail immediately.
            logger.debug(f"Got exception trying to decode token; token has expired. exception: {e}")
            raise errors.AuthenticationError("Tapis token has expired.")
        except Exception as e:
            # if we get an exception decoding it could be that the tenant's public key has changed (i.e., that
            # the public key in out tenant_cache is stale. if we haven't updated the tenant_cache in the last
            # update_tenant_cache_timedelta then go ahead and update and try the decode again.
            if ( (datetime.datetime.now() > tenant_cache.last_tenants_cache_update + tenant_cache.update_tenant_cache_timedelta)
                    and tries == 1):
                try:
                    # reload_tenants() stores the fresh list; get_tenants() only RETURNED it,
                    # so the re-read below kept finding the stale key after a rotation.
                    tenant_cache.reload_tenants()
                    # update key reference in case the tenant key rotated.
                    token_tenant = tenant_cache.get_tenant_config(tenant_id=token_tenant_id)
                    if hasattr(token_tenant, 'public_key') and token_tenant.public_key:
                        public_key_str = token_tenant.public_key
                except Exception as refresh_err:
                    logger.error(f"Unable to refresh tenant cache while validating token signature. "
                                 f"original decode exception: {e}; refresh exception: {refresh_err}")
                    # 503, not 401: the token may well be valid; it is our side that cannot check it,
                    # and a 401 tells clients to throw a good token away.
                    raise errors.BaseTapisError("Unable to validate Tapis token right now; the tenants "
                                                "service could not be reached to refresh signing keys.", 503)
                continue
            # otherwise, we were using a recent public key, so just fail out.
            logger.debug(f"Got exception trying to decode token; exception: {e}")
            raise errors.AuthenticationError("Invalid Tapis token.")
    # if the token is a service token (i.e., this is a service to service request), do additional checks:
    return claims


def get_pub_rsa_key(pub_key):
    """
    Return the RSA public key object associated with the string `pub_key`.
    :param pub_key:
    :return:
    """
    return RSA.importKey(pub_key)

def service_token_checks(request_thread_local, claims, tenant_cache):
    """
    This function does additional checks when a service token is used to make a Tapis request.

    """
    logger.debug(f"top of service_token_checks; claims: {claims}")
    # first check that the target_site claim in the token matches this service's site_id --
    target_site_id = claims.get('tapis/target_site')
    try:
        service_site_id = conf.service_site_id
    except AttributeError:
        msg = "service configured without a site_id. Aborting."
        logger.error(msg)
        raise errors.BaseTapisError(msg)
    if not target_site_id == service_site_id:
        msg = f"token's target_site ({target_site_id}) does not match service's site_id ({service_site_id}."
        logger.info(msg)
        raise errors.AuthenticationError("Invalid service token; "
                                         "target_site claim does not match this service's site_id.")
    # check that this service should be fulfilling this request based on its site_id config --
    # the X-Tapis-* (OBO) headers are required for service requests; if it is not set, raise an error.
    if not request_thread_local.x_tapis_tenant:
        raise errors.AuthenticationError("Invalid service request; X-Tapis-Tenant header missing.")
    if not request_thread_local.x_tapis_user:
        raise errors.AuthenticationError("Invalid service request; X-Tapis-User header missing.")
    request_tenant = tenant_cache.get_tenant_config(tenant_id=request_thread_local.x_tapis_tenant)
    site_id_for_request = request_tenant.site_id
    # if the service's site_id is the same as the site for the request, the request is always allowed:
    if service_site_id == site_id_for_request:
        logger.debug("request is for the same site as the service; allowing request.")
        return True
    # otherwise, we only allow the primary site to handle requests for other sites, and only if the service is NOT
    # on the site's list of services that it runs.
    if not tenant_cache.service_running_at_primary_site:
        raise errors.AuthenticationError("Cross-site service requests are only allowed to the primary site.")
    logger.debug("this service is running at the primary site.")
    # make sure this service is not on the list of services deployed at the associate site --
    if conf.service_name in request_tenant.site.services:
        raise errors.AuthenticationError(f"The primary site does not handle requests to service {conf.service}")
    logger.debug("this service is NOT in the JWT tenant's owning site's set of services. allowing the request.")