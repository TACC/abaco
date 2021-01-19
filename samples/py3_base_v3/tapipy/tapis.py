from base64 import b64encode
from collections.abc import Sequence
import datetime
import json
import jwt
import os
import requests
from openapi_core import create_spec
from openapi_core.schema.parameters.enums import ParameterLocation
import yaml
from . import errors
import pickle
import shutil
import multiprocessing
import copy
from typing import Dict, NewType, Mapping, Optional
from atomicwrites import atomic_write
import openapi_core

import tapipy.errors

# Type declarations
ResourceName = NewType('ResourceName', str)
ResourceUrl = NewType('ResourceUrl', str)
SpecPath = NewType('SpecPath', str)
OpenApiSpec = NewType('OpenApiSpec', openapi_core.schema.specs.models.Spec)
Resources = Dict[ResourceName, ResourceUrl]
Specs = Dict[ResourceName, OpenApiSpec]
ResourceInfo = Mapping[ResourceName, SpecPath]


def _seq_but_not_str(obj: object) -> bool:
    """
    Determine if an object is a Sequence, i.e., has an iteratable type, but not a string, bytearray, etc.
    :param obj: Any python object.
    :return:
    """
    return isinstance(obj, Sequence) and not isinstance(obj, (str, bytes, bytearray))


# currently the files spec is missing operationId's for some of its operations.
RESOURCES = {
    'local':{
        'actors': f"local: {os.path.join(os.path.dirname(__file__), 'resources')}/openapi_v3-actors.yml",
        'authenticator': f"local: {os.path.join(os.path.dirname(__file__), 'resources')}/openapi_v3-authenticator.yml",
        'meta': f"local: {os.path.join(os.path.dirname(__file__), 'resources')}/openapi_v3-meta.yml",
        'files': f"local: {os.path.join(os.path.dirname(__file__), 'resources')}/openapi_v3-files.yml",
        'sk': f"local: {os.path.join(os.path.dirname(__file__), 'resources')}/openapi_v3-sk.yml",
        'streams': f"local: {os.path.join(os.path.dirname(__file__), 'resources')}/openapi_v3-streams.yml",
        'systems': f"local: {os.path.join(os.path.dirname(__file__), 'resources')}/openapi_v3-systems.yml",
        'tenants': f"local: {os.path.join(os.path.dirname(__file__), 'resources')}/openapi_v3-tenants.yml",
        'tokens': f"local: {os.path.join(os.path.dirname(__file__), 'resources')}/openapi_v3-tokens.yml",
    },
    'tapipy':{
        'actors': 'https://raw.githubusercontent.com/tapis-project/tapipy/prod/tapipy/resources/openapi_v3-actors.yml',
        'authenticator': 'https://raw.githubusercontent.com/tapis-project/tapipy/prod/tapipy/resources/openapi_v3-authenticator.yml',
        'meta': 'https://raw.githubusercontent.com/tapis-project/tapipy/prod/tapipy/resources/openapi_v3-meta.yml',
        'files': 'https://raw.githubusercontent.com/tapis-project/tapipy/prod/tapipy/resources/openapi_v3-files.yml',
        'sk': 'https://raw.githubusercontent.com/tapis-project/tapipy/prod/tapipy/resources/openapi_v3-sk.yml',
        'streams': 'https://raw.githubusercontent.com/tapis-project/tapipy/prod/tapipy/resources/openapi_v3-streams.yml',
        'systems': 'https://raw.githubusercontent.com/tapis-project/tapipy/prod/tapipy/resources/openapi_v3-systems.yml',
        'tenants': 'https://raw.githubusercontent.com/tapis-project/tapipy/prod/tapipy/resources/openapi_v3-tenants.yml',
        'tokens': 'https://raw.githubusercontent.com/tapis-project/tapipy/prod/tapipy/resources/openapi_v3-tokens.yml'
    },
    'prod': {
        'actors': 'https://raw.githubusercontent.com/TACC/abaco/dev-v3/docs/specs/openapi_v3.yml',               
        'authenticator': 'https://raw.githubusercontent.com/tapis-project/authenticator/prod/service/resources/openapi_v3.yml',
        'meta': 'https://raw.githubusercontent.com/tapis-project/tapis-client-java/prod/meta-client/src/main/resources/metav3-openapi.yaml',
        'files': 'https://raw.githubusercontent.com/tapis-project/tapis-files/prod/api/src/main/resources/openapi.yaml',
        'sk': 'https://raw.githubusercontent.com/tapis-project/tapis-client-java/prod/security-client/src/main/resources/SKAuthorizationAPI.yaml',
        'streams': 'https://raw.githubusercontent.com/tapis-project/streams-api/prod/service/resources/openapi_v3.yml',
        'systems': 'https://raw.githubusercontent.com/tapis-project/tapis-client-java/prod/systems-client/SystemsAPI.yaml',
        'tenants': 'https://raw.githubusercontent.com/tapis-project/tenants-api/prod/service/resources/openapi_v3.yml',
        'tokens': 'https://raw.githubusercontent.com/tapis-project/tokens-api/prod/service/resources/openapi_v3.yml'
    },
    'dev': {
        'actors': 'https://raw.githubusercontent.com/TACC/abaco/dev-v3/docs/specs/openapi_v3.yml',
        'authenticator': 'https://raw.githubusercontent.com/tapis-project/authenticator/dev/service/resources/openapi_v3.yml',
        'meta': 'https://raw.githubusercontent.com/tapis-project/tapis-client-java/dev/meta-client/src/main/resources/metav3-openapi.yaml',
        'files': 'https://raw.githubusercontent.com/tapis-project/tapis-files/dev/api/src/main/resources/openapi.yaml',
        'sk': 'https://raw.githubusercontent.com/tapis-project/tapis-client-java/dev/security-client/src/main/resources/SKAuthorizationAPI.yaml',
        'streams': 'https://raw.githubusercontent.com/tapis-project/streams-api/dev/service/resources/openapi_v3.yml',
        'systems': 'https://raw.githubusercontent.com/tapis-project/tapis-client-java/dev/systems-client/SystemsAPI.yaml',
        'tenants': 'https://raw.githubusercontent.com/tapis-project/tenants-api/dev/service/resources/openapi_v3.yml',
        'tokens': 'https://raw.githubusercontent.com/tapis-project/tokens-api/dev/service/resources/openapi_v3.yml'
    }
}


def _get_specs(resources: Resources, spec_dir: str = None, download_latest_specs: bool = False) -> Specs:
    """
    Gets specs requested in resources.
    Will download any spec that is not already downloaded.
    """
    spec_dir = get_spec_dir(spec_dir)
    # Download and save specs if neccessary
    download_and_pickle_spec_dicts(resources, spec_dir=spec_dir, download_latest_specs=download_latest_specs)
    # Load, unpickle, and create specs
    specs = unpickle_and_create_specs(resources, spec_dir=spec_dir)
    return specs


def download_and_pickle_spec_dicts(resources: Resources, spec_dir: str, download_latest_specs: bool) -> None:
    """
    Function that calls threads to download and pickle specs.
    Cuts wait time for 9 specs from 4s to 0.7s
    """
    # Get list of specs and check which we need to download
    # We download if the file name requested does not exist
    urls_to_download = []
    for resource_name, url in resources.items():
        if "local:" in url:
            continue 
        _, full_spec_name, spec_path = get_file_info_from_url(url, spec_dir)
        if download_latest_specs:
            urls_to_download.append([resource_name, url, spec_path])
        else:
            if not full_spec_name in os.listdir(spec_dir):
                urls_to_download.append([resource_name, url, spec_path])
    
    # Set off some parallel processes cause it's quick.
    POOL_SIZE = os.environ.get('POOL_SIZE', 16)
    pool = multiprocessing.Pool(processes=POOL_SIZE)
    pool.map(_thread_download_spec_dict, urls_to_download)
    pool.close()
    pool.join()


def _thread_download_spec_dict(resource_info: ResourceInfo) -> None:
    """
    Function that multiprocessing pool calls to download and store pickled
    spec dicts. Gets spec dict, if it's valid, stores it at 'spec_path',
    else it does nothing.
    """
    resource_name, resource_url, spec_path = resource_info
    # Attempt to get spec from url
    response = requests.get(resource_url)
    if response.status_code == 200:
        try:
            # Loads yaml into Python dictionary
            spec_dict = yaml.load(response.content, Loader=yaml.FullLoader)
        except Exception as e:
            print(f'Got exception when attempting to load yaml for '
                  f'"{resource_name}" resource; exception: {e}')
            return
        try:
            # Attempts to create spec from dict to ensure the spec is valid
            # We do a deepcopy as create_spec for some reason adds fields
            # to the dictionary that's given
            test_spec_dict = copy.deepcopy(spec_dict)
            create_spec(test_spec_dict)
        except Exception as e:
            print(f'Got exception when test creating spec for "{resource_name}" '
                  f'resource; Spec probably not verifying; exception: {e}')
            return
        try:
            # Pickles and saves the spec dict to the spec_path atomically
            with atomic_write(f'{spec_path}', overwrite=True, mode='wb') as spec_file:
                pickle.dump(spec_dict, spec_file, protocol=4)
        except Exception as e:
            print(f'Got exception when attempting to pickle spec_dict for '
                  f'"{resource_name}" resource; exception: {e}')
            return
    else:
        raise KeyError(f'Error getting "{resource_name}" resource. URL: "{resource_url}".'
                       f'Did not get 200, got the following back:\n{response.text}')


def unpickle_and_create_specs(resources: Resources, spec_dir: str) -> Specs:
    """
    Pickles loads a specifed spec_path and creates said spec.
    Can't be threaded, map doesn't allow spec object to be sent back.
    """
    specs = {}
    # Get resource path to point the unpickling at.
    for resource_name, url in resources.items():
        if "local:" in url:
            try:
                spec_path = url.replace('local:', '').strip()
                with open(spec_path, 'rb') as spec_file:
                    spec_dict = yaml.load(spec_file, Loader=yaml.FullLoader)
                specs.update({resource_name: create_spec(spec_dict)})
            except Exception as e:
                print(f'Error reading local "{resource_name}" resource. '
                      f'Ensure path is absolute. e:{e}')
            continue
        _, _, spec_path = get_file_info_from_url(url, spec_dir)
        try:
            # Unpickle and create_spec
            with open(spec_path, 'rb') as spec_file:
                spec_dict = pickle.load(spec_file)
            specs.update({resource_name: create_spec(spec_dict)})
        except Exception as e:
            print(f'Got exception trying to unpickle and create spec for '
                  f'spec_path: "{resource_name}"; exception: {e}')
            # Using resource name to look if the resource exists in the
            # tapipy prod resources. If it is found, we fall back and use that!
            # If it isn't we just skip it.
            print(f'Falling back to prod spec for "{resource_name}" resource')
            if resource_name in RESOURCES['tapipy']:
                url = RESOURCES['tapipy'][resource_name]
                _, _, spec_path = get_file_info_from_url(url, spec_dir)
                try:
                    # Unpickle and create_spec
                    with open(spec_path, 'rb') as spec_file:
                        spec_dict = pickle.load(spec_file)
                    specs.update({resource_name: create_spec(spec_dict)})
                except Exception as e:
                    print('Error opening tapipy prod spec. This is bad.')
            else:
                print(f'No "{resource_name}" spec was found to fallback on')
    return specs


def update_spec_cache(resources: Resources = None, spec_dir: str = None) -> None:
    """
    Allows users to update specified specs in cache.
    If nothing specified, all urls in "RESOURCES" are updated
    in the Tapipy folder.
    If a folder is specified, all urls specified are updated there.
    """
    if not resources:
        # Get base resources from RESOURCES if resoruces not inputted
        url_list = []
        for resource_set in RESOURCES:
            url_list.extend(list(RESOURCES[resource_set].values()))
    else:
        # Get just the URL's from the resources given
        url_list = resources.values()

    spec_dir = get_spec_dir(spec_dir)
    download_and_pickle_spec_dicts(url_list, spec_dir=spec_dir, download_latest_specs=True)


def get_file_info_from_url(url: str, spec_dir: str):
    """
    Using a url string we create a file name to store said url contents.
    """
    # Parse a url to create a filename
    spec_name = url.replace('https://raw.githubusercontent.com/', '')\
                   .replace('.yml', '')\
                   .replace('.yaml', '')\
                   .replace('/', '-')\
                   .lower()
    # Get directory and full name for spec file
    full_spec_name = f'{spec_name}.pickle'
    spec_path = f'{spec_dir}/{spec_name}.pickle'
    return spec_name, full_spec_name, spec_path


def get_spec_dir(spec_dir: str):
    """
    Create a new directory for specs by copy Tapipy's to new dir
    Useful in cases where you don't want to overwrite base specs
    """
    if spec_dir:
        # Create folder if it doesn't exist
        if not os.path.exists(spec_dir):
            try:
                # Copy over base specs so we don't need to redownload them
                shutil.copytree(os.path.join(os.path.dirname(__file__), 'specs'), spec_dir)
            except PermissionError:
                raise PermissionError(f"You do not have permission to create/write"
                                      f"to your specifed spec_dir at '{spec_dir}.'")
    else:
        # Fallback on the spec folder from the Tapipy package directory
        spec_dir = os.path.join(os.path.dirname(__file__), 'specs')
    return spec_dir
    

RESOURCE_SPECS = _get_specs(RESOURCES['tapipy'])


def get_basic_auth_header(username: str, password: str) -> str:
    """
    Convenience function with will return a properly formatted Authorization
    header from a username and password.
    """
    user_pass = bytes(f"{username}:{password}", 'utf-8')
    return 'Basic {}'.format(b64encode(user_pass).decode())


class Tenants(object):
    """
    Basic tenancy manager for Tapipy clients.
    """

    def __init__(self, tapi_client):
        self.tapi_client = tapi_client
        self.tenants = {}

    def reload_tenants(self):
        try:
            sites = self.tapi_client.tenants.list_sites()
            tenants = self.tapi_client.tenants.list_tenants()
        except Exception as e:
            raise errors.BaseTapyException(f"Unable to retrieve sites and tenants from the Tenants API. e: {e}")
        for t in tenants:
            for s in sites:
                if s.site_id == t.site_id:
                    t.site = s

        self.tenants = {tn.tenant_id: tn for tn in tenants}

    def get_tenant_config(self, tenant_id):
        try:
            return self.tenants[tenant_id]
        except KeyError:
            # update the cache and try again, but let the KeyError bubble up this time:
            self.reload_tenants()
            try:
                return self.tenants[tenant_id]
            except KeyError:
                raise errors.BaseTapyException("Tenant not found.")


class Tapis(object):
    """
    A client for the Tapis API.
    """

    def __init__(self,
                 base_url: Optional[str]=None,
                 username: Optional[str]=None,
                 password: Optional[str]=None,
                 tenant_id: Optional[str]=None,
                 account_type: Optional[str]=None,
                 access_token: Optional[str]=None,
                 refresh_token: Optional[str]=None,
                 jwt: Optional[str]=None,
                 x_tenant_id: Optional[str]=None,
                 x_username: Optional[str]=None,
                 verify: Optional[bool]=True,
                 service_password: Optional[str]=None,
                 client_id: Optional[str]=None,
                 client_key: Optional[str]=None,
                 resource_set: str = 'tapipy',
                 custom_spec_dict: Resources = None,
                 download_latest_specs: bool = False,
                 spec_dir: str = None,
                 tenants: Tenants = None,
                 is_tapis_service: bool = False
                 ):
        # the base_url for the server this Tapis client should interact with
        self.base_url = base_url

        # the username associated with this Tapis client
        self.username = username

        # the password associated with a user account; use service_password for service accounts.
        self.password = password

        # the tenant id associated with this Tapis client
        self.tenant_id = tenant_id

        # the account_type ("user" or "service") associated with this Tapis client
        self.account_type = account_type
        if not self.account_type:
            self.account_type = 'user'

        # the access token to use -- should be an honest TapisResult access token.
        self.access_token = access_token

        # the refresh token to use -- should be an honest TapisResult refresh token.
        self.refresh_token = refresh_token

        # pass in a "raw" JWT directly. This is only used if the access_token is not set.
        self.jwt = jwt

        # whether to verify the TLS certificate at the base_url
        self.verify = verify

        # the service password, for service accounts to retrieve a token with.
        self.service_password = service_password

        # the client id of an OAuth2 client to use for generating tokens
        self.client_id = client_id

        # the client key of an OAuth2 client to use for generating tokens
        self.client_key = client_key

        # the requests.Session object this client will use to prepare requests
        self.requests_session = requests.Session()

        # use the following two parameters to set headers to make requests on behalf of a different
        # tenant_id and username.
        self.x_tenant_id = x_tenant_id
        self.x_username = x_username

        # Allows a user to specify which set of resources to pull from.
        # Only used when download_lastest_specs is used.
        self.resource_set = resource_set
        if resource_set == 'prod':
            self.resource_set = 'tapipy'
        if self.resource_set == 'master':
            raise errors.TapyClientConfigurationError("the master branch has been removed from tapis-project "
                                                      "repositories; use 'prod' or 'tapipy' instead.")
        if not self.resource_set in RESOURCES.keys():
            raise errors.TapyClientConfigurationError(f"'resource_set' must be one of {RESOURCES.keys()}, "
                                                      f"not {self.resource_set}.")

        # If a custom spec dict is provided then the RESOURCES dict gets updated with it.
        # If any repeated fields are used, the RESOURCES fields are overwritten.
        # Only works when download_latest_specs is True
        self.custom_spec_dict = custom_spec_dict

        # Type checking dictionary interior, it'll be cool to do this with typing, but that's
        # not available or available on a high python version.
        if self.custom_spec_dict:
            for spec_name, spec_val in self.custom_spec_dict.items():
                if isinstance(spec_name, str) and isinstance(spec_val, str):
                    RESOURCES[self.resource_set].update({spec_name: spec_val})
                else: 
                    raise KeyError(f"Custom spec should be a dict of key: str and val: str, got {spec_name} and {spec_val}.")

        # download_latest_specs sets whether to download the latest OpenAPI v3 specs for the service. This could
        # result in "live updates" to your code without warning. It also adds significant overhead to this method.
        # Use it at your own risk!
        self.download_latest_specs = download_latest_specs

        # Allows users to relocate the folder their OpenAPI v3 specs are located on the host.
        # Valuable so users don't overwrite their base specs.
        self.spec_dir = spec_dir

        # Uses module instantiated RESOURCE_SPECS if there are no changes to the specs. 
        if self.custom_spec_dict or self.spec_dir or self.download_latest_specs or not self.resource_set == 'tapipy':
            resource_specs = _get_specs(RESOURCES[resource_set], spec_dir=self.spec_dir, download_latest_specs=self.download_latest_specs)
        else:
            resource_specs = RESOURCE_SPECS

        # create resources for each API defined above. In the future we could make this more dynamic in multiple ways.
        for resource_name, spec in resource_specs.items():
            # each API is a top-level attribute on the DynaTapy object, a Resource object constructed as follows:
            setattr(self, resource_name, Resource(resource_name, spec.paths, self))

        # whether or not this tapis client is running inside a Tapis service.
        self.is_tapis_service = is_tapis_service

        # we lazy-load the tenant_cache to prevent making a call to the Tenants API when not needed.
        if tenants:
            self.tenant_cache = tenants
        else:
            self.tenant_cache = Tenants(self)
        # if the user passed just base_url, try to get the list of tenants and derive the tenant_id from it.
        if not self.is_tapis_service and base_url and not tenant_id:
            self.tenant_cache.reload_tenants()
            for tid, t in self.tenant_cache.tenants.items():
                if t.base_url == base_url:
                    self.tenant_id = t.tenant_id

        # it the caller did not explicitly set the x_tenant_id and x_username headers, and this is a service token
        # set them for the caller.
        if not self.x_tenant_id and not self.x_username:
            if self.account_type == 'service':
                self.x_tenant_id = self.tenant_id
                self.x_username = self.username


    def update_spec_cache(self):
        """
        Updates the spec cache by using the Tapis object's settings.
        So, if object has custom dict, those will be updated, etc.
        """
        # Still doing error catching in case modifications have been made.
        if not self.resource_set in RESOURCES.keys():
            raise KeyError(f"'resource_set' must be one of {RESOURCES.keys()}, not {self.resource_set}.")
        if self.custom_spec_dict:
            for spec_name, spec_val in self.custom_spec_dict.items():
                if isinstance(spec_name, str) and isinstance(spec_val, str):
                    RESOURCES[self.resource_set].update({spec_name: spec_val})
                else:
                    raise KeyError(f"Custom spec should be a dict of key: str and val: str, got {spec_name} and {spec_val}.")
        update_spec_cache(RESOURCES[self.resource_set], self.spec_dir)

    def get_tokens(self, **kwargs):
        """
        Convenience wrapper to get either service tokens (tokengen Tokens API) or user tokens (Authenticator/OAuth2 API)
        based on the account_type on this client instance.
        """
        if self.account_type == 'service':
            return self.get_service_tokens(**kwargs)
        return self.get_user_tokens(**kwargs)

    def get_user_tokens(self, **kwargs):
        """
        Calls the Tapis authenticator to get user tokens based on username and password.
        """
        if not 'username' in kwargs:
            username = self.username
        else:
            username = kwargs['username']
        if not 'password' in kwargs:
            password = self.password
        else:
            password = kwargs['password']
        if not 'client_id' in kwargs:
            client_id = self.client_id
        else:
            client_id = kwargs['client_id']
        if not 'client_key' in kwargs:
            client_key = self.client_key
        else:
            client_key = kwargs['client_key']
        if client_id and client_key:
            auth_header = {'Authorization': get_basic_auth_header(client_id, client_key)}
        else:
            auth_header = {}
        if 'headers' in kwargs:
            auth_header.update(kwargs['headers'])
        tokens = self.authenticator.create_token(username=username,
                                                 password=password,
                                                 grant_type='password',
                                                 headers=auth_header)
        self.set_access_token(tokens.access_token)
        self.refresh_token = None
        #
        if hasattr(tokens, 'refresh_token'):
            self.set_refresh_token(tokens.refresh_token)

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
        self.service_tokens = {}
        # if the caller passed a tenant_id explicitly, we just use that
        if 'tenant_id' in kwargs:
            self.service_tokens = {kwargs['tenant_id']: {}}
        # otherwise, we compute all the tenant's that this service could need to interact with.
        else:
            try:
                self.service_tokens = {t: {} for t in self.tenant_cache.get_site_admin_tenants_for_service()}
            except AttributeError:
                raise errors.BaseTapyException("Unable to retrieve target tenants for a service. "
                                               "Are you really a Tapis service? "
                                               "Did you pass in your Tenants instance?")
        if not 'access_token_ttl' in kwargs:
            # default to a 24 hour access token -
            access_token_ttl = 86400
        else:
            access_token_ttl = kwargs['access_token_ttl']
        if not 'refresh_token_ttl' in kwargs:
            # default to 1 year refresh token -
            refresh_token_ttl = 3153600000
        else:
            refresh_token_ttl = kwargs['refresh_token_ttl']
        for tenant_id in self.service_tokens.keys():
            try:
                target_site_id = self.tenant_cache.get_tenant_config(tenant_id=tenant_id).site_id
            except Exception as e:
                raise errors.BaseTapyException(f"Got exception computing target site id; e:{e}")
            try:
                tokens = self.tokens.create_token(token_username=username,
                                                  token_tenant_id=self.tenant_id,
                                                  account_type=self.account_type,
                                                  access_token_ttl=access_token_ttl,
                                                  generate_refresh_token=True,
                                                  refresh_token_ttl=refresh_token_ttl,
                                                  target_site_id=target_site_id)
            except Exception as e:
                raise errors.BaseTapyException(f"Could not generate tokens for tenant: {tenant_id}; exception: {e}")
            self.service_tokens[tenant_id] = {'access_token': self.add_claims_to_token(tokens.access_token),
                                              'refresh_token': tokens.refresh_token}

    def validate_token(self, token):
        """
        Validate a Tapis token.
        :param token: The token to validate
        :return:
        """
        # first, decode the token data to determine the tenant associated with the token. We are not able to
        # check the signature until we know which tenant, and thus, which public key, to use for validation.
        if not token:
            raise errors.BaseTapyException("No Tapis access token found in the request.")
        try:
            data = jwt.decode(token, verify=False)
        except Exception as e:
            raise errors.BaseTapyException("Could not parse the Tapis access token.")
        # get the tenant out of the jwt payload and get associated public key
        try:
            token_tenant_id = data['tapis/tenant_id']
        except KeyError:
            raise errors.BaseTapyException(
                "Unable to process Tapis token; could not parse the tenant_id. It is possible "
                "the token is in a format no longer supported by the platform.")
        try:
            public_key_str = self.tenant_cache.get_tenant_config(tenant_id=token_tenant_id).public_key
        except errors.BaseTapyException:
            raise errors.TokenInvalidError("Unable to process Tapis token; unexpected tenant_id.")
        except KeyError:
            raise errors.TokenInvalidError("Unable to process Tapis token; no public key associated with the "
                                             "tenant_id.")
        try:
            return jwt.decode(token, public_key_str, algorithm='RS256')
        except Exception as e:
            raise errors.TokenInvalidError("Invalid Tapis token.")


    def add_claims_to_token(self, token):
        """
        Adds claims and a callable expires_in() function to a Tapis token object.
        :param tokne: (TapisResult) A TapisResult object returned using the t.tokens.create_token() method.
        """
        def _expires_in():
            return access_token.expires_at - datetime.datetime.now(datetime.timezone.utc)

        access_token = token
        access_token.claims = self.validate_token(access_token.access_token)
        access_token.original_ttl = access_token.expires_in
        access_token.expires_at = datetime.datetime.fromtimestamp(access_token.claims['exp'],
                                                                  datetime.timezone.utc)
        access_token.expires_in = _expires_in
        return access_token

    def set_access_token(self, token):
        """
        Set the access token to be used in this session.
        :param token: (TapisResult) A TapisResult object returned using the t.tokens.create_token() method.
        :return:
        """
        try:
            self.access_token = self.add_claims_to_token(token=token)
        except:
            pass


    def refresh_tokens(self, tenant_id=None):
        """
        Use the refresh token on this client to get a new access and refresh token pair.
        """
        if self.account_type == 'service':
            return self.refresh_service_tokens(tenant_id)
        if not self.refresh_token:
            raise errors.TapyClientConfigurationError(msg="No refresh token found.")
        return self.refresh_user_tokens()

    def refresh_user_tokens(self):
        """
        Use the refresh token operation for tokens of type "user".
        """
        if not self.client_id:
            raise errors.TapyClientConfigurationError(msg="client_id not configure.")
        if not self.client_key:
            raise errors.TapyClientConfigurationError(msg="client_key not configure.")
        auth_header = {'Authorization': get_basic_auth_header(self.client_id, self.client_key)}
        tokens = self.authenticator.create_token(grant_type='refresh_token',
                                                 refresh_token=self.refresh_token.refresh_token,
                                                 headers=auth_header)
        self.set_access_token(tokens.access_token)
        self.set_refresh_token(tokens.refresh_token)

    def refresh_service_tokens(self, tenant_id):
        """
        Use the refresh token operation for tokens of type "service".
        """
        refresh_token = self.service_tokens[tenant_id]['refresh_token'].refresh_token
        tokens = self.tokens.refresh_token(refresh_token=refresh_token)
        self.service_tokens[tenant_id]['access_token'] = self.add_claims_to_token(tokens.access_token)
        self.service_tokens[tenant_id]['refresh_token'] = tokens.refresh_token

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

    def set_jwt(self, jwt):
        """
        Set a
        :param jwt: (str) Set the JWT to be used in this session.
        :return:
        """
        self.jwt = jwt

    def get_access_jwt(self):
        """
        Returns the JWT string to use for requests.
        :return:
        """
        if hasattr(self, 'access_token') and hasattr(self.access_token, 'access_token'):
            return self.access_token.access_token
        if hasattr(self, 'jwt'):
            return self.jwt
        return None

    def set_tenant(self, tenant_id, base_url):
        """
        Reconfigure the client to interact with a specific tenant; particularly useful for services that need to serve
        multiple tenants.
        :param tenant_id: (str) The tenant_id to configure the client to interact with.
        :param base_url: (str) The base_url of the tenant to configure the client to interact with.
        :return:
        """
        self.tenant_id = tenant_id
        if self.account_type == 'service':
            self.x_tenant_id = tenant_id
        self.base_url = base_url

    def upload(self, source_file_path, system_id, dest_file_path, **kwargs):
        """
        Convenience method for uploading a file at a local path to a system
        """
        url = f'{self.base_url}/v3/files/ops/{system_id}/{dest_file_path}'
        # check for the _tapis_debug flag for generating debug data
        debug = False
        if '_tapis_debug' in kwargs:
            debug = kwargs.get('_tapis_debug', False)
            # ignore non-boolean values for the debug flag and set it to False.
            if not type(debug) == bool:
                debug = False
        # construct the http headers -
        headers = {}
        # set the X-Tapis-Token header using the client
        if self.get_access_jwt():
            # check for a token about to expire in the next 5 seconds:
            if datetime.timedelta(seconds=5) > self.access_token.expires_in():
                # if the access token is about to expire, try to use refresh, unless this is a call to
                # refresh (otherwise this would never terminate!)
                try:
                    self.refresh_tokens()
                except:
                    # for now, if we get an error trying to refresh the tokens,s, we ignore it and try the
                    # request anyway.
                    pass
            headers = {'X-Tapis-Token': self.get_access_jwt(), }

        headers['Accept'] = 'application/json'
        # the X-Tapis-Tenant and X-Tapis-Username headers can be set when the token represents a service account and the
        # service is making a request on behalf of another user/tenant.
        if self.x_tenant_id:
            headers['X-Tapis-Tenant'] = self.x_tenant_id
        if self.x_username:
            headers['X-Tapis-User'] = self.x_username

        # allow arbitrary headers to be passed in via the special "headers" kwarg -
        try:
            headers.update(kwargs.pop('headers', {}))
        except ValueError:
            raise errors.InvalidInputError(
                msg="The headers argument, if passed, must be a dictionary-like object.")

        r = requests.Request('POST',
                             url,
                             files={"file": open(source_file_path, 'rb')},
                             headers=headers).prepare()
        # make the request and return the response object -
        try:
            resp = self.requests_session.send(r, verify=self.verify)
        except Exception as e:
            # todo - handle different types of requests exceptions
            msg = f"Unable to make request to Tapis server. Exception: {e}"
            raise errors.BaseTapyException(msg=msg, request=r)
        # try to get the error message and version from the Tapis request:
        try:
            error_msg = resp.json().get('message')
        except:
            error_msg = resp.content
        try:
            version = resp.json().get('version')
        except:
            version = None
        # for any kind of non-20x response, we need to raise an error.
        if resp.status_code in (400, 404):
            raise errors.InvalidInputError(msg=error_msg, version=version, request=r, response=resp)
        if resp.status_code in (401, 403):
            raise errors.NotAuthorizedError(msg=error_msg, version=version, request=r, response=resp)
        if resp.status_code in (500,):
            raise errors.ServerDownError(msg=error_msg, version=version, request=r, response=resp)
        # catch-all for any other non-20x response:
        if resp.status_code >= 300:
            raise errors.BaseTapyException(msg=error_msg, version=version, request=r, response=resp)

        # generate the debug_data object
        debug_data = Debug(request=r, response=resp)
        # get the result's operation ids from the custom x-response-operation-ids for this operation id.from
        # results_operation_ids = [...]
        # resp.headers is a case-insensitive dict
        resp_content_type = resp.headers.get('content-type')
        if hasattr(resp_content_type, 'lower') and resp_content_type.lower() == 'application/json':
            try:
                json_content = resp.json()
            except Exception as e:
                msg = f'Requests could not produce JSON from the response even though the content-type was ' \
                      f'application/json. Exception: {e}'
                # TODO -- should this not be an error if the API has described the content-type as application/json?
                #         what valid use cases do we still have for passing raw content in this case?
                if debug:
                    return resp.content, debug_data
                return resp.content
            # get the Tapis result object which could be a JSON object or list.
            try:
                result = json_content.get('result')
            except Exception as e:
                # some Tapis APIs, such as the Meta API, do not return "result" objects and/or do not return the
                # standard Tapis stanzas but rather "raw" JSON documents
                return resp.content
            if result:
                # if it is a list we should return a list of TapisResult objects:
                if _seq_but_not_str(result):
                    if len([item for item in result if type(item) in TapisResult.PRIMITIVE_TYPES]) > 0:
                        if debug:
                            return TapisResult(result), debug_data
                        return TapisResult(result)
                    else:
                        if debug:
                            return [TapisResult(**x) for x in result], debug_data
                        return [TapisResult(**x) for x in result]
                # otherwise, assume it is a JSON object and return that directly as a result -
                try:
                    if debug:
                        return TapisResult(**result), debug_data
                    return TapisResult(**result)
                except TypeError:
                    # result could be an honest string, in which case the use of **result will result in a type
                    # error, which we catch and then return the result as is.
                    if type(result) == str:
                        return result
                except Exception as e:
                    msg = f'Failed to serialize the result object. Got exception: {e}'
                    raise errors.InvalidServerResponseError(msg=msg, version=version, request=r, response=resp)
            else:
                # the response was JSON but not the standard Tapis 4 stanzas, so just return the JSON content:
                if debug:
                    return json_content, debug_data
                return json_content

        # todo - note:
        # For now, we do not try to handle other content-types, such as application/xml, etc. We just return
        # the raw content as the result.
        if debug:
            return resp.content, debug_data
        return resp.content


class Resource(object):
    """
    Represents a top-level API "resource" defined by an OpenAPI spec file.
    """

    def __init__(self, resource_name, resource_spec, tapis_client):
        """
        Instantiate a resource.
        :param resource_name: (str) The name of the resource, such as "files", "apps", etc.
        :param resource_spec: (openapi_core.schema.specs.models.Spec) The Spec object associated with this resource.
        :param tapis_client: (Tapis) Pointer to the Tapis object to which this resource will be attached.
        """
        # resource_name is something like "files", "apps", etc.
        self.resource_name = resource_name

        # resource_spec is the associated definition from the spec file.
        self.resource_spec = resource_spec

        # tapis_client stores configuration data (api_server, token, etc..)
        self.tapis_client = tapis_client

        # Here we create an attr on the object for each operation_id in the spec. The attr is itself an
        # Operation object, defined below, with a special __call__ method.
        # Examples operation_id's inclue "list_files", "upload_file", etc...
        for path_name, path_desc in self.resource_spec.items():
            # Each path_desc is an openapi_core.schema.paths.models.Path object
            # it has an operations object, which is a dictionary of operations associated with the path:
            for op, op_desc in path_desc.operations.items():
                # each op_desc is an openapi_core.schema.operations.models.Operation object.
                # the op_desc has a number of associated attributes, including operation_id, parameters, path_name, etc.
                # we create an Operation object for each one of these:
                if not op_desc.operation_id:
                    print(f"invalid op_dec for {resource_name}; missing operation_id. op_dec: {op_desc}")
                    continue
                setattr(self, op_desc.operation_id, Operation(self.resource_name, op_desc, self.tapis_client))


class Operation(object):
    """
    Represents a single operation on an API resource defined by an OpenAPI spec file.
    Operation objects are in one-to-one correspondence with operation_id's defined in the spec file.
    """

    def __init__(self, resource_name, op_desc, tapis_client):
        """
        Instantiate an operation. The op_desc should an openapi_core Operation object associated with the operation.
        :param resource_name: (str) The resource associated with this operation.
        :param op_desc: (openapi_core.schema.operations.models.Operation) OpenAPI description of the operation.
        :param tapis_client: Pointer to the Tapis object to which this resource will be attached.
        :return:
        """
        self.resource_name = resource_name
        self.op_desc = op_desc
        self.tapis_client = tapis_client

        # derived attributes - for convenience
        self.operation_id = op_desc.operation_id
        self.http_method = op_desc.http_method
        self.path_parameters = [p for _, p in op_desc.parameters.items() if p.location == ParameterLocation.PATH]
        self.query_parameters = [p for _, p in op_desc.parameters.items() if p.location == ParameterLocation.QUERY]
        self.request_body = op_desc.request_body

    def determine_tenant_id_for_service_request(self, **kwargs):
        """
        Determines the tenant_id for a service request from the kwargs to said
        request and the application context (flask thread-local), if available.
        """
        # we need to determine the tenant id for the request.
        tenant_id = None
        # to begin, look for it in the parameters. if the caller explicitly set
        # it, always use that.
        try:
            tenant_id = kwargs.get('_tapis_tenant_id')
        except:
            pass
        # if the caller did not explicitly set the _tapis_tenant_id variable,
        # we can look on the flask thread local for data about the request.
        if not tenant_id:
            try:
                from flask import g
                # g.x_tapis_tenant corresponds to the X-Tapis-Tenant OBO header. if this
                # is set, it is always the tenant_id of the request.
                tenant_id = getattr(g, 'x_tapis_tenant', None)
                # g.request_tenant_id, if set, incorporates the tenant_id claim from
                # the token and the OBO headers.
                if not tenant_id:
                    tenant_id = getattr(g, 'request_tenant_id', None)
                # finally, the g.tenant_id represents the tenant_id claim in the access token.
                # we should only use this if nothing else is set.
                if not tenant_id:
                    tenant_id = getattr(g, 'tenant_id', None)
            # note that flask will throw a RuntimeError if attempting to access the thread local object, j, outside
            # of an application context.
            except (ImportError, RuntimeError):
                pass
        # check the headers to see if X-Tapis-Tenant is being set; if so, use that:
        if not tenant_id and 'headers' in kwargs:
            try:
                tenant_id = kwargs['headers']['X-Tapis-Tenant']
            except:
                pass
        # finally, look for a tenant_id on the tapis client itself
        if not tenant_id:
            tenant_id = self.tapis_client.tenant_id
        return tenant_id

    def determine_user_for_service_request(self, **kwargs):
        """
        Determines the username for a service request from the kwargs to said
        request and the application context (flask thread-local), if available.
        """
        # we need to determine the user for the request.
        user = None
        # to begin, look for it in the parameters. if the caller explicitly set
        # it, always use that.
        try:
            user = kwargs.get('_tapis_user')
        except:
            pass
        # if the caller did not explicitly set the _tapis_user variable,
        # we can look on the flask thread local for data about the request.
        if not user:
            try:
                from flask import g
                # g.x_tapis_user corresponds to the X-Tapis-User OBO header. if this
                # is set, it is always the user of the request.
                user = getattr(g, 'x_tapis_user', None)

                # the g.username corresponds to the tapis/username claim in the access token.
                # we should only use this if x_tapis_user was not set.
                if not user:
                    user = getattr(g, 'username', None)
            # note that flask will throw a RuntimeError if attempting to access attributes on the thread local object,
            # g, outside of an application context.
            except (ImportError, RuntimeError):
                pass
        # finally, look a username on the tapis client itself
        if not user:
            print(f"no user object, returning username on the tapis_client...")
            user = self.tapis_client.username
        return user

    def __call__(self, **kwargs):
        """
        Turns the operation object into a callable. Arguments must be passed as kwargs, where the name of each kwarg
        corresponds to a "parameter" in the OpenApi definition. Here, parameter could be a path parameter, body
        parameter, or query parameter.

        :param kwargs: All allowable arguments to this operation.

        :return:
        """
        # the http method is defined by the operation -
        http_method = self.http_method.upper()

        # determine base_url --
        base_url = None
        # if this call is coming from another tapis service,
        # we must determine base_url for a service to service request.
        if self.tapis_client.is_tapis_service:
            tenant_id = self.determine_tenant_id_for_service_request(**kwargs)
            # if we got a tenant_id, use it to look up the base_url:
            if tenant_id:
                try:
                    base_url = self.tapis_client.tenant_cache.get_base_url_for_service_request(tenant_id, self.resource_name)
                except:
                    pass
            # if all else fails, use the base_url for the client
            if not base_url:
                base_url = self.tapis_client.base_url

        # otherwise, just use the base_url passed into the tapis client --
        else:
            base_url = self.tapis_client.base_url
        # construct the http path -
        # some API definitions, such as SK, chose to not include the "/v3/" at the beginning of their paths,
        # so we add it in:
        if not self.op_desc.path_name.startswith('/v3/'):
            self.url = f'{base_url}/v3{self.op_desc.path_name}'  # base url
        else:
            self.url = f'{base_url}{self.op_desc.path_name}'  # base url
        url = self.url
        for param in self.path_parameters:
            # look for the name in the kwargs
            if param.required:
                if param.name not in kwargs:
                    raise errors.InvalidInputError(msg=f"{param.name} is a required argument.")
            p_val = kwargs.pop(param.name)
            if param.required and not p_val:
                raise errors.InvalidInputError(msg=f"{param.name} is a required argument and cannot be None.")
            # replace the parameter in the path template with the parameter value
            s = '{' + f'{param.name}' + '}'
            url = url.replace(s, p_val)

        # check for the _tapis_debug flag for generating debug data
        debug = False
        if '_tapis_debug' in kwargs:
            debug = kwargs.get('_tapis_debug', False)
            # ignore non-boolean values for the debug flag and set it to False.
            if not type(debug) == bool:
                debug = False

        # construct the http query parameters -
        params = {}
        for param in self.query_parameters:
            # look for the name in the kwargs
            if param.required:
                if param.name not in kwargs:
                    raise errors.InvalidInputError(msg=f"{param.name} is a required argument.")
            # only set the parameter if it was actually sent in the function -
            if param.name in kwargs:
                p_val = kwargs.pop(param.name, None)
                params[param.name] = p_val

        # construct the http headers -
        headers = {}

        # set the X-Tapis-Token header using the client
        access_token = None
        request_site_admin_tenant_id = None    # used for looking up the service token
        if self.tapis_client.is_tapis_service and hasattr(self.tapis_client, 'service_tokens'):
            # the tenant_id for the request could be a user tenant (e.g., "tacc" or "dev") but the
            # service tokens are stored by admin tenant, so we need to get the admin tenant for the
            # owning site of the tenant.
            try:
                request_tenant = self.tapis_client.tenant_cache.get_tenant_config(tenant_id=tenant_id)
            except Exception as e:
                raise errors.BaseTapyException(f"Could not lookup the request tenant for tenant {tenant_id}; e: {e}")
            try:
                request_site = request_tenant.site
            except Exception as e:
                raise errors.BaseTapyException(f"Could not lookup the request site for tenant {tenant_id}; e: {e}")
            try:
                request_site_admin_tenant_id = request_site.site_admin_tenant_id
            except Exception as e:
                raise errors.BaseTapyException(f"Could not lookup the request site_admin tenant for "
                                               f"tenant {tenant_id}; e: {e}")

            # service_tokens may be defined but still be empty dictionaries... this __call__ could be to get
            # the service's first set of tokens.
            if request_site_admin_tenant_id in self.tapis_client.service_tokens.keys() \
                    and 'access_token' in self.tapis_client.service_tokens[request_site_admin_tenant_id].keys():
                try:
                    access_token = self.tapis_client.service_tokens[request_site_admin_tenant_id]['access_token']
                except KeyError:
                    raise errors.BaseTapyException(f"Did not find service tokens for "
                                                   f"tenant {request_site_admin_tenant_id};")
            else:
                pass
        elif self.tapis_client.get_access_jwt():
            access_token = self.tapis_client.get_access_jwt()
        # if we got an access token, check if we need to refresh it
        if access_token:
            # check for a token about to expire in the next 5 seconds; assume by default we have a token with
            # plenty of time remaining.
            time_remaining = datetime.timedelta(days=10)
            try:
                time_remaining = access_token.expires_in()
            except:
                # it is possible the access_token does not have an expires_in attribute and/or that it is not
                # callable. we just pass on these exceptions and do not try to refresh the token.
                pass
            if datetime.timedelta(seconds=5) > time_remaining:
                # if the access token is about to expire, try to use refresh, unless this is a call to
                # refresh (otherwise this would never terminate!)
                if self.resource_name == 'tokens' and self.operation_id == 'refresh_token':
                    pass
                else:
                    try:
                        self.tapis_client.refresh_tokens(tenant_id=request_site_admin_tenant_id)
                    except:
                        # for now, if we get an error trying to refresh the tokens,s, we ignore it and try the
                        # request anyway.
                        pass
        # we may have refreshed the token, so we get it one more time --
        if self.tapis_client.is_tapis_service \
                and hasattr(self.tapis_client, 'service_tokens') \
                and request_site_admin_tenant_id \
                and 'access_token' in self.tapis_client.service_tokens[request_site_admin_tenant_id].keys():
            try:
                jwt = self.tapis_client.service_tokens[request_site_admin_tenant_id]['access_token'].access_token
            except Exception as e:
                msg = f"Could not get JWT from access_token; e: {e}"
                raise errors.BaseTapyException(msg)
        else:
            jwt = self.tapis_client.get_access_jwt()
        if jwt:
            headers = {'X-Tapis-Token':  jwt}

        # the X-Tapis-Tenant and X-Tapis-Username headers must be set when the token represents a service account.
        # if this is a tapis service, we should have derived the correct tenant_id when we determined the tenant_id
        # for the request, above.
        if self.tapis_client.is_tapis_service:
            if tenant_id:
                headers['X-Tapis-Tenant'] = tenant_id
            # similarly for username, we first look in the request content:
            x_username = self.determine_user_for_service_request(**kwargs)
            if x_username:
                headers['X-Tapis-User'] = x_username

        # allow arbitrary headers to be passed in via the special "headers" kwarg -
        try:
            headers.update(kwargs.pop('headers', {}))
        except ValueError:
            raise errors.InvalidInputError(
                msg="The headers argument, if passed, must be a dictionary-like object.")

        # construct the data -
        data = None
        # these are the list of allowable request body content types; ex., 'application/json'.
        if hasattr(self.op_desc.request_body, 'content') and hasattr(self.op_desc.request_body.content, 'keys'):
            # Before we go by spec headers, we first check if the user specified any because the
            # 'else' would overwrite those if the spec specifies one of the following content-types.
            # In the case of custom headers, we just pass whatever in (Christian's decision, probably 
            # should improve).
            if 'Content-Type' in headers:
                # data = kwargs['request_body']
                # TODO -- i changed this just to try something; it is probably breaking things (jfs).
                data = kwargs['message']
            # We go back to the old methods if there is not custom headers.
            # Note: If an operation has many possible content-types, this just kind of breaks because
            # all of the ifs return as true. e.g. sendMessage in Actors w/json, binary, and strings.
            # In that case, these are more of order of which headers should be used. e.g. JSON headers
            # would be used primarily unless it's not in the operation spec.
            else:
                if 'application/json' in self.op_desc.request_body.content.keys():
                    headers['Content-Type'] = 'application/json'
                    required_fields = self.op_desc.request_body.content['application/json'].schema.required
                    data = {}
                    # if the request body has no defined properties, look for a single "request_body" parameter.
                    if self.op_desc.request_body.content['application/json'].schema.properties == {}:
                        # choice of "request_body" is arbitrary, as the property name is not provided by the
                        # openapi spec in this case
                        data = kwargs['request_body']
                    else:
                        # otherwise, the request body has defined properties, so look for each one in the function kwargs
                        for p_name, p_desc in self.op_desc.request_body.content['application/json'].schema.properties.items():
                            if p_name in kwargs:
                                data[p_name] = kwargs[p_name]
                            elif p_name in required_fields:
                                raise errors.InvalidInputError(msg=f'{p_name} is a required argument.')
                        # serialize data before passing it to the request
                    data = json.dumps(data)
                elif 'application/octet-stream' in self.op_desc.request_body.content.keys():
                    headers['Content-Type'] = 'application/octet-stream'
                    data = kwargs['request_body']
                elif 'multipart/form-data' in self.op_desc.request_body.content.keys():
                    # todo - iterate over parts in self.op_desc.request_body.content['multipart/form-data'].schema.properties
                    raise NotImplementedError
                # todo - handle other body content types..

        # create a prepared request -
        # cf., https://requests.kennethreitz.org/en/master/user/advanced/#request-and-response-objects
        r = requests.Request(http_method,
                             url,
                             params=params,
                             data=data,
                             headers=headers).prepare()

        # the create_token operation requires HTTP basic auth, though some services, such as the authenticator, need to
        # use create_token to generate tokens on behalf of other users; in these cases, it is important to not set the
        # BasicAuth header, so we look for a special kwarg in this case
        if self.resource_name == 'tokens' and self.operation_id == 'create_token':
            # look for kwarg, use_basic_auth, to turn off use of BasicAuth; we default this to true so that BasicAuth
            # is used if the argument is not passed.
            if kwargs.get('use_basic_auth', True):
                # construct the requests HTTPBasicAuth header object
                basic_auth_header = requests.auth.HTTPBasicAuth(self.tapis_client.username,
                                                                self.tapis_client.service_password)
                # set the object on the request
                basic_auth_header(r)

        # make the request and return the response object -
        try:
            resp = self.tapis_client.requests_session.send(r, verify=self.tapis_client.verify)
        except Exception as e:
            # todo - handle different types of requests exceptions
            msg = f"Unable to make request to Tapis server. Exception: {e}"
            raise errors.BaseTapyException(msg=msg, request=r)
        # try to get the error message and version from the Tapis request:
        try:
            error_msg = resp.json().get('message')
        except:
            error_msg = resp.content
        try:
            version = resp.json().get('version')
        except:
            version = None
        # for any kind of non-20x response, we need to raise an error.
        if resp.status_code in (400, 404):
            raise errors.InvalidInputError(msg=error_msg, version=version, request=r, response=resp)
        if resp.status_code in (401, 403):
            raise errors.NotAuthorizedError(msg=error_msg, version=version, request=r, response=resp)
        if resp.status_code in (500,):
            raise errors.ServerDownError(msg=error_msg, version=version, request=r, response=resp)
        # catch-all for any other non-20x response:
        if resp.status_code >= 300:
            raise errors.BaseTapyException(msg=error_msg, version=version, request=r, response=resp)

        # generate the debug_data object
        debug_data = Debug(request=r, response=resp)
        # get the result's operation ids from the custom x-response-operation-ids for this operation id.from
        # results_operation_ids = [...]
        # resp.headers is a case-insensitive dict
        resp_content_type = resp.headers.get('content-type')
        if hasattr(resp_content_type, 'lower') and resp_content_type.lower() == 'application/json':
            # get the Tapis result object which could be a JSON object or list.
            try:
                json_content = resp.json()
                result = json_content.get('result')
            except Exception as e:
                # some Tapis APIs, such as the Meta API, do not return "result" objects and/or do not return the
                # standard Tapis stanzas but rather "raw" JSON documents
                if debug:
                    return resp.content, debug_data
                return resp.content
            if result or result == [] or result == {}:
                # if it is a list we should return a list of TapisResult objects:
                if _seq_but_not_str(result):
                    if len([item for item in result if type(item) in TapisResult.PRIMITIVE_TYPES]) > 0:
                        if debug:
                            return TapisResult(result), debug_data
                        return TapisResult(result)
                    else:
                        if debug:
                            return [TapisResult(**x) for x in result if not x == 'self'], debug_data
                        return [TapisResult(**x) for x in result if not x == 'self']
                # otherwise, assume it is a JSON object and return that directly as a result -
                try:
                    if debug:
                        # remove a key "self"
                        try:
                            if 'self' in result.keys():
                                result.pop('self')
                        except:
                            pass
                        return TapisResult(**result), debug_data
                    return TapisResult(**result)
                except TypeError:
                    # result could be an honest string, in which case the use of **result will result in a type
                    # error, which we catch and then return the result as is.
                    if type(result) == str:
                        return result
                except Exception as e:
                    msg = f'Failed to serialize the result object. Got exception: {e}'
                    raise errors.InvalidServerResponseError(msg=msg, version=version, request=r, response=resp)
            else:
                # the response was JSON but not the standard Tapis 4 stanzas, so just return the JSON content:
                if debug:
                    return json_content, debug_data
                return json_content

        # todo - note:
        # For now, we do not try to handle other content-types, such as application/xml, etc. We just return
        # the raw content as the result.
        if debug:
            return resp.content, debug_data
        return resp.content


class TapisResult(object):
    """
    Represents a result returned from a single Tapis operation.
    """

    PRIMITIVE_TYPES = [int, str, bool, bytearray, bytes, float, type(None)]

    def __init__(self, *args, **kwargs):
        if args and kwargs:
            msg = f"Could not instantiate result object; constructor got args and kwargs. args={args}; kwargs={kwargs}"
            raise errors.BaseTapyException(msg=msg)
        # is passing non-key-value args, there should be only one arg;
        # it should be either a list or a primitive type:
        if args:
            if len(args) > 1:
                msg = f"Could not instantiate result object; constructor got args of length > 1. args={args}."
                raise errors.BaseTapyException(msg=msg)
            arg = args[0]
            # the arg is a list and not a string, there are two cases: 1) at least one object in the list is a
            # primitive type, in which case we just return a list of the objects
            if _seq_but_not_str(arg):
                setattr(self, 'result', [x for x in arg])
            else:
                setattr(self, 'result', arg)

        for k, v in kwargs.items():
            # primitive types
            if type(v) in TapisResult.PRIMITIVE_TYPES:
                # just set the attribute to the value
                setattr(self, k, v)
            # lists
            elif _seq_but_not_str(v):
                # if the list has even one item of primitive type, just return a list
                if len([item for item in v if type(item) in TapisResult.PRIMITIVE_TYPES]) > 0:
                    setattr(self, k, v)
                else:
                    setattr(self, k, [TapisResult(**item) for item in v if not item == 'self'])
            # for complex objects, create a TapisResult with the value
            else:
                try:
                    if 'self' in v.keys():
                        v.pop('self')
                except:
                    pass
                setattr(self, k, TapisResult(**v))

    def __str__(self):
        attrs = '\n'.join([f'{str(a)}: {getattr(self, a)}' for a in dir(self) if
                           not a == 'get' and not a.startswith('__') and not a.startswith('PRIMITIVE_TYPES')])
        return f'\n{attrs}'

    def __repr__(self):
        return str(self)

    def get(self, attr, default=None):
        """Provide a dictionary-like get() syntax """
        try:
            return getattr(self, attr)
        except:
            return default


class Debug(object):
    """
    Debug data for an API request.
    """

    def __init__(self, request, response):
        self.request = request
        self.response = response
