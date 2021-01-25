from functools import partial
import os

from pymongo import errors, TEXT

from store import MongoStore
from common.config import conf
from __init__ import t
import subprocess
import re
import requests as r
import time

from common.logs import get_logger
logger = get_logger(__name__)

# Figure out which sites this deployment has to manage.
# Getting current site and checking if we're primary.
SITE_LIST = []
tenant_object = t.tenant_cache.get_tenant_config(tenant_id=t.tenant_id)
if tenant_object.site.primary:
    for site_object in t.tenants.list_sites():
        if 'actors' in site_object.services:
            SITE_LIST.append(site_object.site_id)
else:
    SITE_LIST = tenant_object.site_id

def get_site_rabbitmq_uri(site):
    """
    Takes site and gets site specific rabbit uri using correct
    site vhost and site's authentication from config.
    """
    # Get rabbit_uri and check if it's the proper format. "amqp://rabbit:5672"
    # There shouldn't be any auth on it anymore.
    rabbit_uri = conf.rabbit_uri
    if "@" in rabbit_uri:
        raise KeyError("rabbit_uri in config.json should no longer have auth. Configure with config.")

    # Getting site object with parameters for specific site.
    site_object = conf.get(f'{site}_site_object') or {}

    # Checking for RabbitMQ credentials for each site.
    site_rabbitmq_user = f"abaco_{site}_user"
    site_rabbitmq_pass = site_object.get('site_rabbitmq_pass') or ""

    # Setting up auth string.
    if site_rabbitmq_user and site_rabbitmq_pass:
        auth = f"{site_rabbitmq_user}:{site_rabbitmq_pass}"
    else:
        auth = site_rabbitmq_user
    
    # Adding auth to rabbit_uri.
    rabbit_uri = rabbit_uri.replace("amqp://", f"amqp://{auth}@")
    # Adding site vhost to rabbit_uri
    rabbit_uri += f"/abaco_{site}"
    return rabbit_uri


def rabbit_initialization():
    """
    Initial site initialization for rabbitmq using the rabbitmqadmin utility.
    Consists of creating a user/pass and vhost for each site. Each site's user
    gets permissions to it's vhosts. Primary site gets control to each vhost.
    One-time/deployment
    """
    rabbit_dash_host = conf.rabbit_dash_host

    # Creating the subprocess call (Has to be str, list not working due to docker
    # aliasing of rabbit with it's IP address (Could work? But this works too.).).
    fn_call = f'/home/tapis/rabbitmqadmin -H {rabbit_dash_host} '

    # Get admin credentials from rabbit_uri. Add auth to fn_call if it exists.
    admin_user = conf.admin_rabbitmq_user or None
    admin_pass = conf.admin_rabbitmq_pass or None

    if admin_user and admin_pass:
        fn_call += (f'-u {admin_user} ')
        fn_call += (f'-p {admin_pass} ')
        logger.debug(f"Administrating rabbitmq with user: {admin_user} with pass.")
    elif admin_user:
        fn_call += (f'-u {admin_user} ')
        logger.warning(f"Administrating rabbitmq with user: {admin_user} without pass.")
    else:
        logger.warning(f"Administrating rabbitmq with no auth.")

    # We poll to check rabbitmq is operational. Done by trying to list vhosts, arbitrary command.
    # Exit code 0 means rabbitmq is running. Need access to rabbitmq dash/management panel.
    while True:
        result = subprocess.run(fn_call + f'list vhosts', shell=True)
        if result.returncode == 0:
            break
        else:
            time.sleep(3)

    # Creating user/pass, vhost, and assigning permissions for rabbitmq.
    for site in SITE_LIST:
        # Getting site object with parameters for specific site.
        site_object = conf.get(f'{site}_site_object') or {}

        # Checking for RabbitMQ credentials for each site.
        site_rabbitmq_user = f"abaco_{site}_user"
        site_rabbitmq_pass = site_object.get('site_rabbitmq_pass') or ""

        # Site DB Name
        site_db_name = f"abaco_{site}"

        # Initializing site user account.
        if not site_rabbitmq_pass:
            logger.warning(f"RabbitMQ site user lacks pass in config. No password being set for site: {site}")
            subprocess.run(fn_call + f'declare user name={site_rabbitmq_user} tags=None', shell=True) # create user w/ no pass
        else:
            subprocess.run(fn_call + f'declare user name={site_rabbitmq_user} password={site_rabbitmq_pass} tags=None', shell=True, capture_output=True) # create user/pass

        # Creating site vhost. Granting permissions to site user and admin.
        logger.debug(f"Creating vhost named '{site_db_name}' for site - {site}. {site_rabbitmq_user} and {admin_user} users are being granted read/write.")
        subprocess.run(fn_call + f'declare vhost name={site_db_name}', shell=True, capture_output=True) # create vhost
        subprocess.run(fn_call + f'declare permission vhost={site_db_name} user={site_rabbitmq_user} configure=.* write=.* read=.*', shell=True) # site user perm
        subprocess.run(fn_call + f'declare permission vhost={site_db_name} user={admin_user} configure=.* write=.* read=.*', shell=True) # admin perm
        logger.debug(f"RabbitMQ init complete for site: {site}.")


def mongo_initialization():
    """
    Initial site initialization for mongo using pymongo.
    Creating database with their site user (abaco_{site}_admin) and
    giving them readWrite role for their site DB. One-time/deployment.

    Initial user is already root, so they should have permissions to
    everything already. (Done in docker-compose).
    """
    # Getting admin/root credentials to create users.
    admin_mongo_user = conf.admin_mongo_user or None
    admin_mongo_pass = conf.admin_mongo_pass or None

    logger.debug(f"Administrating mongodb with user: {admin_mongo_user}.")
    mongo_obj = MongoStore(host=conf.mongo_host,
                           port=conf.mongo_port,
                           database="random", # db doesn't matter when just using mongo_client
                           user=admin_mongo_user,
                           password=admin_mongo_pass)
    mongo_client = mongo_obj._mongo_client

    for site in SITE_LIST:
        # Getting site object with parameters for specific site.
        site_object = conf.get(f"{site}_site_object") or {}

        # Checking for Mongo credentials for each site.
        site_mongo_user = f"abaco_{site}_user"
        site_mongo_pass = site_object.get("site_mongo_pass") or None

        # Site DB Name
        site_db_name = f"abaco_{site}"

        # Get database attr to create user for
        site_database = getattr(mongo_client, site_db_name)
        if site_mongo_pass:
            site_database.command("createUser", site_mongo_user, pwd=site_mongo_pass, roles=[{'role': 'readWrite', 'db': site_db_name}])
        else:
            logger.warning(f"Mongo site user lacks pass in config. No password being set for site: {site}")
            site_database.command("createUser", site_mongo_user, roles=[{'role': 'readWrite', 'db': site_db_name}])
        logger.debug(f"MongoDB init complete for site: {site}.")


def mongo_index_initialization():
    """
    Seperate function to initialize mongo so that he sometimes lengthy process
    doesn't slow down stores.py imports as it only needs to be called once.
    Initialization consists of creating indexes for Mongo. One-time/deployment
    """
    for site in SITE_LIST:
        # Getting site object with parameters for specific site.
        site_object = conf.get(f"{site}_site_object") or {}

        # Sets an expiry variable 'exp'. So whenever a document gets placed with it
        # the doc expires after 0 seconds. BUT! If exp is set as a Mongo Date, the
        # Mongo TTL index will wait until that Date and then delete after 0 seconds.
        # So we can delete at a specific time if we set expireAfterSeconds to 0
        # and set the time to expire on the 'exp' variable.
        try:
            logs_store[site]._db.create_index("exp", expireAfterSeconds=0)
        except errors.OperationFailure:
            # this will happen if the index already exists.
            pass
        
        # Creating wildcard text indexing for full-text mongo search
        logs_store[site].create_index([('$**', TEXT)])
        executions_store[site].create_index([('$**', TEXT)])
        actors_store[site].create_index([('$**', TEXT)])
        workers_store[site].create_index([('$**', TEXT)])


if __name__ == "__main__":
    # Rabbit and Mongo only go through init on primary site.
    rabbit_initialization()
    mongo_initialization()

# We do this outside of a function because the 'store' objects need to be imported
# by other scripts. Functionalizing it would create more code and make it harder
# to read in my opinion.
# Mongo is used for accounting, permissions and logging data for its scalability.
logs_store = {}
permissions_store = {}
executions_store = {}
clients_store = {}
actors_store = {}
workers_store = {}
nonce_store = {}
alias_store = {}
pregen_clients = {}
abaco_metrics_store = {}

# We go through each site, initializing database objects for ones we will use.
for site in SITE_LIST:
    # Getting site object with parameters for specific site.
    site_object = conf.get(f"{site}_site_object") or {}

    # Checking for Mongo credentials for each site.
    site_mongo_user = f"abaco_{site}_user"
    site_mongo_pass = site_object.get("site_mongo_pass") or None

    # Site DB Name
    site_db_name = f"abaco_{site}"

    # Creating partial stores.
    site_config_store = partial(MongoStore,
                                host=conf.mongo_host,
                                port=conf.mongo_port,
                                database=site_db_name,
                                user=site_mongo_user,
                                password=site_mongo_pass,
                                auth_db=site_db_name)

    # Adding site stores to a dict for each collection
    # Note to new people, db here actually refers to mongo collection.
    logs_store.update({site: site_config_store(db='logs_store')})
    permissions_store.update({site: site_config_store(db='permissions_store')})
    executions_store.update({site: site_config_store(db='executions_store')})
    clients_store.update({site: site_config_store(db='clients_store')})
    actors_store.update({site: site_config_store(db='actors_store')})
    workers_store.update({site: site_config_store(db='workers_store')})
    nonce_store.update({site: site_config_store(db='nonce_store')})
    alias_store.update({site: site_config_store(db='alias_store')})
    pregen_clients.update({site: site_config_store(db='pregen_clients')})
    abaco_metrics_store.update({site: site_config_store(db='abaco_metrics_store')})

if __name__ == "__main__":
    # Mongo indexes only go through init on primary site.
    # Needs to be here as it needs to happen after store dictionary creation.
    mongo_index_initialization()