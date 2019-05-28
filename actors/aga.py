"""
Python3 compatible agave binding. WARNING -- only the token and clients service are supported.
NOTE -- This is a temporary module!
        once python3 support is properly added to agavepy this module will be removed.
"""

from urllib.parse import urlparse, urljoin
import requests
import time

from agaveflask.logs import get_logger
logger = get_logger(__name__)

class AgaveError(Exception):
    pass


class Token(object):

    def __init__(self,
                 username, password,
                 api_server, api_key, api_secret, verify,
                 parent, _token=None, _refresh_token=None, token_username=None):
        self.username = username
        self.password = password
        self.api_server = api_server
        self.api_key = api_key
        self.api_secret = api_secret
        self.token_username = token_username
        # Agave object that created this token
        self.parent = parent
        self.verify = verify
        if _token and _refresh_token:
            self.token_info = {'access_token': _token,
                               'refresh_token': _refresh_token}
            self.parent._token = _token

        self.token_url = urljoin(self.api_server, 'token')

    def _token(self, data):
        logger.debug("top of _token")
        auth = requests.auth.HTTPBasicAuth(self.api_key, self.api_secret)
        logger.debug("about to make POST request for token; URL: {}; "
                     "data: {}; auth: {}:{}".format(self.token_url, data, self.api_key, self.api_secret))
        resp = requests.post(self.token_url, data=data, auth=auth,
                             verify=self.verify)
        logger.debug("made request for token; rsp: {}".format(resp))
        resp.raise_for_status()
        self.token_info = resp.json()
        try:
            expires_in = int(self.token_info.get('expires_in'))
        except ValueError:
            expires_in = 3600
        created_at = int(time.time())
        self.token_info['created_at'] = created_at
        self.token_info['expiration'] = created_at + expires_in
        self.token_info['expires_at'] = time.ctime(created_at + expires_in)
        token = self.token_info['access_token']
        # Notify parent that a token was created
        self.parent._token = token
        return token

    def create(self):
        logger.debug("top of token.create for username: {}; password: ****".format(self.username))
        data = {'grant_type': 'password',
                'username': self.username,
                'password': self.password,
                'scope': 'PRODUCTION'}
        if self.token_username:
            data['grant_type'] = 'admin_password'
            data['token_username'] = self.token_username
        return self._token(data)

    def refresh(self):
        data = {'grant_type': 'refresh_token',
                'scope': 'PRODUCTION',
                'refresh_token': self.token_info['refresh_token']}
        return self._token(data)


class Agave(object):
    PARAMS = [
        # param name, mandatory?, attr_name, default
        ('username', False, 'username', None),
        ('password', False, 'password', None),
        ('token_username', False, 'token_username', None),
        ('jwt', False, 'jwt', None),
        ('jwt_header_name', False, 'header_name', None),
        ('api_server', True, 'api_server', None),
        ('client_name', False, 'client_name', None),
        ('api_key', False, 'api_key', None),
        ('api_secret', False, 'api_secret', None),
        ('token', False, '_token', None),
        ('refresh_token', False, '_refresh_token', None),
        ('verify', False, 'verify', True),
    ]

    def __init__(self, **kwargs):
        for param, mandatory, attr, default in self.PARAMS:
            try:
                value = (kwargs[param] if mandatory
                         else kwargs.get(param, default))
            except KeyError:
                raise AgaveError(
                    'parameter "{}" is mandatory'.format(param))
            setattr(self, attr, value)
        # If we are passed a JWT directly, we can bypass all OAuth-related tasks
        if self.jwt:
            if not self.header_name:
                raise AgaveError("The jwt header name is required to use the jwt authenticator.")
        self.token = None
        if self.api_key is not None and self.api_secret is not None and self.jwt is None:
            self.set_client(self.api_key, self.api_secret)
        # set the clients object to the AgaveClientsService
        self.clients = AgaveClientsService(self)

    def set_client(self, key, secret):
        """

        :type key: str
        :type secret: str
        :rtype: None
        """
        logger.debug("top of set_client")
        self.api_key = key
        self.api_secret = secret
        self.token = Token(
            self.username, self.password,
            self.api_server, self.api_key, self.api_secret,
            self.verify,
            self, self._token, self._refresh_token, self.token_username)
        if self._token:
            pass
        else:
            logger.debug("calling token.create()")
            self.token.create()


class AgaveClientsService(object):
    """Class for interacting with the Agave OAuth2 clients service."""

    def __init__(self, parent):
        # maintain pointer to parent Agave client
        self.parent = parent

    def create(self, body):
        """Create a new Agave OAuth client. `body` should be a dictionary with `clientName` parameter."""
        if not body or not hasattr(body, 'get'):
            raise AgaveError('body dictionary required.')
        auth = requests.auth.HTTPBasicAuth(self.parent.username, self.parent.password)
        try:
            rsp = requests.post(url='{}/clients/v2'.format(self.parent.api_server),
                                auth=auth,
                                data={'clientName': body.get('clientName')},
                                verify=self.parent.verify)
            result = rsp.json().get('result')
            logger.debug("response from POST to create client: {}; content: {}".format(rsp, rsp.content))
            logger.debug("result from POST to create client: {}".format(result))
            self.parent.set_client(result['consumerKey'], result['consumerSecret'])
            logger.debug("set_client in parent, returning result.")
            return result
        except Exception as e:
            raise AgaveError('Error creating client: {}'.format(e))

    def test(self, arg):
        print(self)
        print(self.parent)
        print('Here is a URL: {}/clients/v2'.format(self.parent.api_server))
        print(arg)

    def delete(self, clientName):
        """Delete an Agave OAuth2 client."""
        auth = requests.auth.HTTPBasicAuth(self.parent.username, self.parent.password)
        try:
            rsp = requests.delete(url='{}/clients/v2/{}'.format(self.parent.api_server, clientName),
                                  auth=auth)
            rsp.raise_for_status()
            return {}
        except Exception as e:
            raise AgaveError('Error creating client: {}'.format(e))
