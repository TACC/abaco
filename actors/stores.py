from functools import partial
import os

from pymongo import errors, TEXT

from store import MongoStore
from common.config import conf

# Mongo is used for accounting, permissions and logging data for its scalability.
mongo_user = conf.get("store_mongo_user") or None
# Checking for mongo_password in config file.
mongo_password = os.environ.get('mongo_password', None)
if not mongo_password:
    mongo_password = conf.get("store_mongo_password") or None

mongo_config_store = partial(
    MongoStore, conf.store_mongo_host,
    conf.store_mongo_port,
    user=mongo_user,
    password=mongo_password)

logs_store = mongo_config_store(db='1')
# create an expiry index for the log store if we want logs to expire
log_ex = conf.web_log_ex
if not log_ex == -1:
    try:
        logs_store._db.create_index("exp", expireAfterSeconds=log_ex)
    except errors.OperationFailure:
        # this will happen if the index already exists.
        pass

permissions_store = mongo_config_store(db='2')
executions_store = mongo_config_store(db='3')
clients_store = mongo_config_store(db='4')
actors_store = mongo_config_store(db='5')
workers_store = mongo_config_store(db='6')
nonce_store = mongo_config_store(db='7')
alias_store = mongo_config_store(db='8')
pregen_clients = mongo_config_store(db='9')
abaco_metrics_store = mongo_config_store(db='10')

# Indexing
logs_store.create_index([('$**', TEXT)])
executions_store.create_index([('$**', TEXT)])
actors_store.create_index([('$**', TEXT)])
workers_store.create_index([('$**', TEXT)])