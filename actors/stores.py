from functools import partial
import os

from pymongo import errors, TEXT

from store import MongoStore
from tapisservice.config import conf
from tapisservice.logs import get_logger
from __init__ import t
import subprocess
import re
import requests as r
import time

logger = get_logger(__name__)

# Figure out which sites this deployment has to manage.
# Getting current site and checking if we're primary.
SITE_LIST = []
tenant_object = t.tenant_cache.get_tenant_config(tenant_id=t.tenant_id)
if tenant_object.site.primary:
    for site_object in t.tenants.list_sites(_tapis_set_x_headers_from_service=True):
        SITE_LIST.append(site_object.site_id)
else:
    SITE_LIST = tenant_object.site_id

def get_site_rabbitmq_uri(site):
    """
    Takes site and gets site specific rabbit uri using correct
    site vhost and site's authentication from config.
    """
    logger.debug(f'get_site_rabbitmq_uri is using site: {site}')
    # Get rabbit_uri and check if it's the proper format. "amqp://rabbit:5672"
    # There shouldn't be any auth on it anymore.
    rabbit_uri = conf.rabbit_uri
    if "@" in rabbit_uri:
        raise KeyError("rabbit_uri in config.json should no longer have auth. Configure with config.")

    # Getting site object with parameters for specific site.
    site_object = conf.get(f'{site}_site_object') or {}

    # Checking for RabbitMQ credentials for each site.
    site_rabbitmq_user = f"abaco_{site}_user"
    site_rabbitmq_pass = site_object.get('site_rabbitmq_pass') or conf.get("global_site_object").get("site_rabbitmq_pass")

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
    Initial site initialization for RabbitMQ using the RabbitMQ utility.
    Consists of creating a user/pass and vhost for each site. Each site's user
    gets permissions to it's vhosts. Primary site gets control to each vhost.
    One-time/deployment
    """
    try:
        rabbit_dash_host = conf.rabbit_dash_host

        # Creating the subprocess call (Has to be str, list not working due to docker
        # aliasing of rabbit with it's IP address (Could work? But this works too.).).
        fn_call = f'/home/tapis/rabbitmqadmin -H {rabbit_dash_host} '

        # Get admin credentials from rabbit_uri. Add auth to fn_call.
        # Note: If admin password not set in rabbit env with compose the user and pass
        # or just the pass (if only user is set) will default to "guest".
        admin_rabbitmq_user = conf.get("admin_rabbitmq_user", "guest")
        admin_rabbitmq_pass = conf.get("admin_rabbitmq_pass", "guest")

        if not isinstance(admin_rabbitmq_user, str) or not isinstance(admin_rabbitmq_pass, str):
            msg = f"RabbitMQ creds must be of type 'str'. user: {type(admin_rabbitmq_user)}, pass: {type(admin_rabbitmq_pass)}."
            logger.critical(msg)
            raise RuntimeError(msg)

        if not admin_rabbitmq_user or not admin_rabbitmq_pass:
            msg = f"RabbitMQ creds were given, but were None or empty. user: {admin_rabbitmq_user}, pass: {admin_rabbitmq_pass}" 
            logger.critical(msg)
            raise RuntimeError(msg)

        if admin_rabbitmq_user == "guest" or admin_rabbitmq_pass == "guest":
            logger.warning(f"RabbitMQ using default admin information. Not secure.")
        logger.debug(f"Administrating RabbitMQ with user: {admin_rabbitmq_user} with pass.")

        fn_call += (f'-u {admin_rabbitmq_user} ')
        fn_call += (f'-p {admin_rabbitmq_pass} ')

        # We poll to check rabbitmq is operational. Done by trying to list vhosts, arbitrary command.
        # Exit code 0 means rabbitmq is running. Need access to rabbitmq dash/management panel.
        i = 15
        while i:
            result = subprocess.run(fn_call + f'list vhosts', shell=True, capture_output=True)
            if result.returncode == 0:
                break
            elif result.stderr:
                rabbit_error = result.stderr.decode('UTF-8')
                if "Errno 111" in rabbit_error:
                    msg = "Rabbit still initializing."
                    logger.debug(msg)
                elif "Access refused" in rabbit_error:
                    msg = "Rabbit admin user or pass misconfigured."
                    logger.critical(msg)
                    raise RuntimeError(msg)
                else:
                    msg = f"RabbitMQ has thrown an error. e: {rabbit_error}"
                    logger.critical(msg)
                    raise RuntimeError(msg)
            else:
                msg = "Rabbit still initializing."
                logger.debug(msg)
            time.sleep(2)
            i -= 1
        if not result.returncode == 0:
            msg = "Timeout waiting for RabbitMQ to start."
            logger.critical(msg)
            raise RuntimeError(msg)
    except Exception as e:
        msg = f"Error during RabbitMQ start process. e: {e}"
        logger.critical(msg)
        raise Exception(msg)
    
    # Creating user/pass, vhost, and assigning permissions for rabbitmq.
    try:
        for site in SITE_LIST:
            # Getting site object with parameters for specific site.
            site_object = conf.get(f'{site}_site_object') or {}

            # Checking for RabbitMQ credentials for each site.
            site_rabbitmq_user = f"abaco_{site}_user"
            site_rabbitmq_pass = site_object.get('site_rabbitmq_pass') or ""
            if not site_rabbitmq_pass:
                msg = f'No site_rabbitmq_pass found for site: {site}. Using Global.'
                logger.warning(msg)
                site_rabbitmq_pass = conf.global_site_object.get("site_rabbitmq_pass")
                if not site_rabbitmq_pass:
                    msg = f'No global "site_rabbitmq_pass" to act as default password. Cannot initialize.'
                    logger.critical(msg)
                    raise KeyError(msg)

            # Site DB Name
            site_db_name = f"abaco_{site}"

            # Initializing site user account.
            subprocess.run(fn_call + f'declare user name={site_rabbitmq_user} password={site_rabbitmq_pass} tags=None', shell=True) # create user/pass

            # Creating site vhost. Granting permissions to site user and admin.
            logger.debug(f"Creating vhost named '{site_db_name}' for site - {site}. {site_rabbitmq_user} and {admin_rabbitmq_user} users are being granted read/write.")
            subprocess.run(fn_call + f'declare vhost name={site_db_name}', shell=True) # create vhost
            subprocess.run(fn_call + f'declare permission vhost={site_db_name} user={site_rabbitmq_user} configure=.* write=.* read=.*', shell=True) # site user perm
            subprocess.run(fn_call + f'declare permission vhost={site_db_name} user={admin_rabbitmq_user} configure=.* write=.* read=.*', shell=True) # admin perm
            logger.debug(f"RabbitMQ init complete for site: {site}.")
            print(f"RabbitMQ init complete for site: {site}.")
    except Exception as e:
        msg = f"Error setting up RabbitMQ for site: {site} e: {repr(e)}"
        logger.critical(msg)
        raise Exception(msg)


def mongo_initialization():
    """
    Initial site initialization for mongo using pymongo.
    Creating database with their site user (abaco_{site}_admin) and
    giving them readWrite role for their site DB. One-time/deployment.

    Initial user is already root, so they should have permissions to
    everything already. (Done in docker-compose).
    """
    # Getting admin/root credentials to create users.
    try:
        admin_mongo_user = conf.admin_mongo_user
        admin_mongo_pass = conf.admin_mongo_pass
    except Exception as e:
        msg = f"MongoDB init requires mongo admin user and password. e: {e}"
        logger.critical(msg)
        raise RuntimeError(msg)

    if not isinstance(admin_mongo_user, str) or not isinstance(admin_mongo_pass, str):
        msg = f"MongoDB creds must be of type 'str'. user: {type(admin_mongo_user)}, pass: {type(admin_mongo_pass)}."
        logger.critical(msg)
        raise RuntimeError(msg)

    if not admin_mongo_user or not admin_mongo_pass:
        msg = f"MongoDB creds were given, but were None or empty. user: {admin_mongo_user}, pass: {admin_mongo_pass}" 
        logger.critical(msg)
        raise RuntimeError(msg)

    logger.debug(f"Administrating MongoDB with user: {admin_mongo_user}.")
    mongo_obj = MongoStore(host=conf.mongo_host,
                           port=conf.mongo_port,
                           database="random", # db doesn't matter when just using mongo_client
                           user=admin_mongo_user,
                           password=admin_mongo_pass)
    mongo_client = mongo_obj._mongo_client

    # Initialize the replica set
    if conf.mongo_replica_set:
        i = 15
        while i:
            try:
                mongo_client.admin.command("replSetInitiate")
                msg = "Mongo replica set initialized. Client attached to server."
                print(msg)
                logger.debug(msg)
                break
            except errors.OperationFailure:
                msg = "Mongo replica set already initialized. Client attached to server."
                print(msg)
                logger.debug(msg)
                break
            except Exception as e:
                msg = "Mongo still initializing."
                logger.debug(msg)
                print(msg)
                time.sleep(2)
                i -= 1
                if i == 0:
                    msg = f"Mongo has failed to initialized after 15 attempts. e: {e}"
                    logger.critical(msg)
                    print(msg)
                    raise RuntimeError(msg)
                continue

        # Have to give some time so mongo does it's replicaset stuff
        time.sleep(3)

        try:
            # Get the pod hostname (full) for Mongo K8. We have to set the replica set primary host with
            # this, as there's a chance the conf is still pointing at a dead node if you burned down/up
            # This also works for Docker, but it's neccessary for K8.
            primary_hostname = mongo_client.admin.command("hostInfo")['system']['hostname']

            # Set hostname in replica config to ensure we're the primary of the replica set
            repl_config = mongo_client.admin.command("replSetGetConfig")  
            repl_config['config']['members'][0]['host'] = primary_hostname
            mongo_client.admin.command("replSetReconfig", repl_config['config'], force=True)
            msg = f"Mongo replica set config successfully changed to hostname: {primary_hostname}"
        except Exception as e:
            msg = f"Error while ensuring replica set host was correct. e: {e}"
            logger.critical(msg)
            raise Exception(msg)

    try:    
        for site in SITE_LIST:
            # Getting site object with parameters for specific site.
            site_object = conf.get(f"{site}_site_object") or {}

            # Checking for Mongo credentials for each site.
            site_mongo_user = f"abaco_{site}_user"
            site_mongo_pass = site_object.get("site_mongo_pass")
            if not site_mongo_pass:
                msg = f'No site_mongo_pass found for site: {site}. Using Global.'
                logger.warning(msg)
                site_mongo_pass = conf.global_site_object.get("site_mongo_pass")
                if not site_mongo_pass:
                    msg = f'No global "site_mongo_pass" to act as default password. Cannot initialize.'
                    logger.critical(msg)
                    raise KeyError(msg)

            # Site DB Name
            site_db_name = f"abaco_{site}"
            
            # Getattr gets the database on the mongo_client to create user on
            site_database = getattr(mongo_client, site_db_name)
            try:
                site_database.command("createUser", site_mongo_user, pwd=site_mongo_pass, roles=[{'role': 'readWrite', 'db': site_db_name}])
            except errors.OperationFailure:
                msg = f"User {site_mongo_user} already exists, skipping init. (roles could be wrong)."
                logger.warning(msg)
            logger.debug(f"MongoDB init complete for site: {site}.")
            print(f"MongoDB init complete for site: {site}.")
    except Exception as e:
        # README. If you get this, lots of things could be wrong.
        # "OperationFailure" could mean docker-compose lacks MONGO_INITDB_ROOT_USERNAME/PASS vars.
        # Could also just be Mongo isn't working. Could also mean TLS isn't working.
        msg = f"Error setting up mongo for site: {site} e: {repr(e)}"
        logger.critical(msg)
        raise Exception(msg)

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
        adapters_store[site].create_index([('$**', TEXT)])
        adapter_servers_store[site].create_index([('$**', TEXT)])
        adapter_logs_store[site].create_index([('$**', TEXT)])


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
adapters_store = {}
adapter_servers_store = {}
adapter_permissions_store = {}
adapter_logs_store = {}
workers_store = {}
nonce_store = {}
alias_store = {}
pregen_clients = {}
abaco_metrics_store = {}
configs_store = {}
configs_permissions_store = {}

# We go through each site, initializing database objects for ones we will use.
for site in SITE_LIST:
    # Getting site object with parameters for specific site.
    site_object = conf.get(f"{site}_site_object") or {}

    # Checking for Mongo credentials for each site.
    site_mongo_user = f"abaco_{site}_user"
    site_mongo_pass = site_object.get("site_mongo_pass") or conf.get("global_site_object").get("site_mongo_pass")

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
    configs_store.update({site: site_config_store(db='configs_store')})
    configs_permissions_store.update({site: site_config_store(db='configs_permissions_store')})
    adapters_store.update({site: site_config_store(db='adapters_store')})
    adapter_servers_store.update({site: site_config_store(db='adapter_servers_store')})
    adapter_logs_store.update({site: site_config_store(db='adapter_logs_servers_store')})
    adapter_permissions_store.update({site: site_config_store(db='adapter_permissions_store')})

if __name__ == "__main__":
    # Mongo indexes only go through init on primary site.
    # Needs to be here as it needs to happen after store dictionary creation.
    mongo_index_initialization()