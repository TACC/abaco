"""
Webhook example that allows users to link to this image in Abaco and use it for
webhooks. This image provides additional features that the generic webhook
feature does not. This image allows for retry logic, timeout logic, headers for
authentication, and a backoff factor.

This image utilizes urllib3 to enable the retries feature. 

This image utilizes environment variables to set variables. URL is a required
variable, while the others are optional and include, HEADERS, RETRIES_VAR,
TIMEOUT_VAR, and BACKOFF_FAC. Environment variables can be set for an actor
by providing a 'default_environment' dictionary at actor initialization.
"""
import os
import json
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# REQUIRED: Get the URL set in 'default_environment' during actor init
URL=os.environ.get('URL')
# OPTIONAL: Adds in JSON formatted headers to comply with url expectations
HEADERS=os.environ.get('HEADERS').replace("'",'"').replace('/','')
# OPTIONAL: urllib3 variables set at init with 'default_environment'
RETRIES_VAR=int(os.getenv('RETRIES_VAR', 3))
TIMEOUT_VAR=int(os.getenv('TIMEOUT_VAR', 5))
BACKOFF_FAC=float(os.getenv('BACKOFF_FAC', 0.3))
# DERIVED: Gets event message from the linked image
EVENT_MSG=os.environ.get('MSG')


# The actual posting logic using urllib3 for retries, timeouts, and more
def requests_retry_session(
    retries=RETRIES_VAR, 
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


# Creating a requests session so that we can have headers, auth, etc.
s = requests.Session()
# urllib3 basic auth possible, but headers is just more verbose
#s.auth = ('user', 'pass')
# If headers are applied they are then added to session headers
if HEADERS:
    try:
        HEADERS = json.loads(HEADERS)
        s.headers.update(HEADERS)
    except Exception as e:
        raise e
# Trys to make an actual post
try:
    res = requests_retry_session(session=s).post(
        URL,
        data=EVENT_MSG,
        timeout=TIMEOUT_VAR)
except Exception as e:
    print('Error:', e)
