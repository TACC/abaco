
import collections
from datetime import datetime, timedelta
import json
import os
import urllib.parse

import pprint
from pymongo.errors import WriteError, DuplicateKeyError
from pymongo import MongoClient

from tapisservice.config import conf
from tapisservice.logs import get_logger
logger = get_logger(__name__)

def _do_get(getter, key):
    obj = getter(key)
    if obj is None:
        raise KeyError(f'"{key}" not found')
    try:
        return json.loads(obj.decode('utf-8'))
    # handle non-JSON data
    except ValueError:
        return obj.decode('utf-8')


def _do_set(setter, key, value):
    obj = json.dumps(value)
    setter(key, obj.encode('utf-8'))

class StoreMutexException(Exception):
    pass


class AbstractStore(collections.MutableMapping):
    """A persitent dictionary."""

    def __getitem__(self, key):
        pass

    def __setitem__(self, key, value):
        pass

    def __delitem__(self, key):
        pass

    def __iter__(self):
        """Iterator for the keys."""
        pass

    def __len__(self):
        """Size of db."""
        pass

    def set_with_expiry(self, key, obj):
        """Set `key` to `obj` with automatic expiration of the configured seconds."""
        pass

    def update(self, key, field, value):
        "Atomic ``self[key][field] = value``."""
        # Not `pass`: that silently discarded writes for any subclass that does not
        # override it, and MongoStore does not.
        # Use tuple-key assignment: store[key, field] = value.
        raise NotImplementedError("use store[key, field] = value")

    def pop_field(self, key, field):
        "Atomic pop ``self[key][field]``."""
        pass

    def update_subfield(self, key, field1, field2, value):
        "Atomic ``self[key][field1][field2] = value``."""
        pass

    def getset(self, key, value):
        "Atomically: ``self[key] = value`` and return previous ``self[key]``."
        pass

    def mutex_acquire(self, key):
        """Try to use key as a mutex.
        Raise StoreMutexException if not available.
        """

        busy = self.getset(key, True)
        if busy:
            raise StoreMutexException(f'{key} is busy')

    def mutex_release(self, key):
        self[key] = False


class AbstractTransactionalStore(AbstractStore):
    """Adds basic transactional semantics to the AbstractStore interface."""
    def within_transaction(self, f, key):
        """Execute a callable, f, within a lock on key `key`."""
        pass


# MongoClient is thread-safe and owns a connection pool, so it is meant to be shared.
# Every MongoStore used to build its own, and stores.py constructs 12 stores per site,
# so each container ran ~12 independent pools (pymongo defaults to maxPoolSize=100).
# Measured 2026-08-18: the metrics container alone held 516 server-side connections
# against a bench stack, with 10-15k opened per 25-minute run. Keying one client per
# distinct connection target collapses that to a single bounded pool per process.
# Bound the shared pool. Default pymongo is 100 per client, which was never the
# intended budget when a dozen clients existed per process.
MONGO_MAX_POOL_SIZE = int(conf.get('mongo_max_pool_size', 50) or 50)
# Server selection and connect fail fast (5s). socketTimeoutMS is NOT a query timeout:
# it guards a half-open socket, and it is also how long a request thread that already
# holds a pooled connection waits when mongod stalls (server selection only protects NEW
# operations once the monitor has marked the server down). So it must sit above the
# slowest legitimate operation -- bounded server-side by maxTimeMS (see aggregate()) --
# but not much above it: 45s vs maxTimeMS 30s.
MONGO_SOCKET_TIMEOUT_MS = int(conf.get('mongo_socket_timeout_ms', 45000) or 45000)
MONGO_MAX_TIME_MS = int(conf.get('mongo_max_time_ms', 30000) or 30000)
_MONGO_CLIENTS = {}
_MONGO_CLIENTS_PID = os.getpid()


def _shared_mongo_client(cache_key, build, label):
    """Return the process-wide MongoClient for `cache_key`, building it on first use.
    `label` is what gets logged: cache_key[0] is the full mongo URI, which carries the
    password, so it must never reach a log line."""
    global _MONGO_CLIENTS, _MONGO_CLIENTS_PID
    if _MONGO_CLIENTS_PID != os.getpid():
        # this process was forked (gunicorn --preload builds every store in the master).
        # The inherited clients belong to the parent: their monitor threads did not survive
        # the fork and a lock one of them held may stay held. Never reuse -- or close --
        # them here; build this process's own.
        _MONGO_CLIENTS = {}
        _MONGO_CLIENTS_PID = os.getpid()
    client = _MONGO_CLIENTS.get(cache_key)
    if client is None:
        client = build()
        _MONGO_CLIENTS[cache_key] = client
        logger.info(f"created shared MongoClient for {label} "
                    f"(pool #{len(_MONGO_CLIENTS)} in this process)")
    return client


# mongo_tls modes. Trunk semantics are preserved: any truthy value other than the
# explicit opt-in strings below means TLS + client certificate + private CA ("verify").
MONGO_TLS_OFF = 'off'
MONGO_TLS_VERIFY = 'verify'
MONGO_TLS_ONLY = 'tls-only'
_MONGO_TLS_ONLY_ALIASES = ('tls-only', 'no-client-cert')


def mongo_tls_mode(value):
    """Normalize the mongo_tls config value to one of MONGO_TLS_OFF / MONGO_TLS_VERIFY /
    MONGO_TLS_ONLY.
    - false / null / "" / "false"          -> off (plain connection)
    - true / "true" / "verify"             -> verify: TLS with a client certificate and the
                                              private CA (the trunk meaning of `true`)
    - "tls-only" / "no-client-cert"        -> TLS without a client certificate; the server
                                              cert is checked against mongo_tls_ca_cert_name
                                              when configured, else the system CAs
    Any other string is a config error and raises ValueError rather than silently picking
    a mode."""
    if not value:
        return MONGO_TLS_OFF
    if value is True:
        return MONGO_TLS_VERIFY
    if isinstance(value, str):
        v = value.strip().lower()
        if v in ('', 'false', '0', 'no', 'off'):
            return MONGO_TLS_OFF
        if v in ('true', '1', 'yes', 'on', 'verify'):
            return MONGO_TLS_VERIFY
        if v in _MONGO_TLS_ONLY_ALIASES:
            return MONGO_TLS_ONLY
    raise ValueError(f"Invalid mongo_tls value {value!r}; expected false, true/'verify', "
                     f"or one of {list(_MONGO_TLS_ONLY_ALIASES)}.")


class MongoStore(AbstractStore):

    def __init__(self, host, port, database='abaco_tacc', db='0', user=None, password=None, auth_db=""):
        """
        Note: pop_fromlist, append_tolist, and within_transaction were removed from the Redis
        store functions as they weren't necessary, don't work, or don't work in Mongo.
        Creates an abaco `store` which maps to a single mongo
        collection within some database.
        :param host: the IP address of the mongo server.
        :param port: port of the mongo server.
        :param database: the mongo database to use for abaco.
        :param db: an integer mapping to a mongo collection within the
        :param auth_db: For sites. This is where we should check auth. Should be abaco_{site}. 
            Default is usually admin, but Mongo will take care of that for us.
        mongo database.

        :return:
        """
        self._database_name, self._collection_name = database, db
        mongo_uri = f'mongodb://{host}:{port}'

        if user and password:
            logger.info(f"Using mongo user {user} and password: ***")
            u = urllib.parse.quote_plus(user)
            p = urllib.parse.quote_plus(password)
            mongo_uri = f'mongodb://{u}:{p}@{host}:{port}/{auth_db}'
        tls_mode = mongo_tls_mode(conf.get('mongo_tls'))
        # never log mongo_uri: with auth it embeds the password.
        label = f"{host}:{port} (tls: {tls_mode})"
        common = dict(maxPoolSize=MONGO_MAX_POOL_SIZE,
                      serverSelectionTimeoutMS=5000,
                      connectTimeoutMS=5000,
                      # without these, a half-open mongo socket blocks a request thread forever
                      socketTimeoutMS=MONGO_SOCKET_TIMEOUT_MS)
        if tls_mode == MONGO_TLS_VERIFY:
            # trunk behaviour of `mongo_tls: true`: TLS + client certificate + private CA.
            try:
                cert_dir = conf.mongo_tls_certs_path.split(':')[1]
                ca_cert_name = conf.get("mongo_tls_ca_cert_name")
                client_cert_name = conf.get("mongo_tls_client_cert_name")
                if not ca_cert_name or not client_cert_name:
                    msg = "Mongo TLS requires both the 'mongo_tls_client_cert_name' and 'mongo_tls_ca_cert_name' configs to be set."
                    logger.critical(msg)
                    raise SystemError(msg)
                ca_cert_path = os.path.join(cert_dir, ca_cert_name)
                client_cert_path = os.path.join(cert_dir, client_cert_name)
                self._client_spec = (
                    (mongo_uri, MONGO_TLS_VERIFY, client_cert_path, ca_cert_path),
                    lambda: MongoClient(mongo_uri,
                                        tls=True,
                                        tlsCertificateKeyFile=client_cert_path,
                                        tlsCAFile=ca_cert_path,
                                        **common),
                    label)
                self._bind(_shared_mongo_client(*self._client_spec))
            except Exception as e:
                msg = f"Error creating MongoStore object with TLS + client verification. e: {e}"
                logger.critical(msg)
                raise Exception(msg)
        elif tls_mode == MONGO_TLS_ONLY:
            # explicit opt-in: TLS without a client certificate. The server certificate is
            # still verified -- against the private CA when one is configured, otherwise
            # against the system trust store.
            try:
                tls_kwargs = {'tls': True}
                ca_cert_name = conf.get("mongo_tls_ca_cert_name")
                certs_path = conf.get("mongo_tls_certs_path")
                if ca_cert_name and certs_path:
                    tls_kwargs['tlsCAFile'] = os.path.join(certs_path.split(':')[1], ca_cert_name)
                self._client_spec = (
                    (mongo_uri, MONGO_TLS_ONLY, tls_kwargs.get('tlsCAFile')),
                    lambda: MongoClient(mongo_uri, **tls_kwargs, **common),
                    label)
                self._bind(_shared_mongo_client(*self._client_spec))
            except Exception as e:
                msg = f"Error creating MongoStore object with TLS (no client cert). e: {e}"
                logger.critical(msg)
                raise Exception(msg)
        else:
            self._client_spec = (
                (mongo_uri, MONGO_TLS_OFF),
                lambda: MongoClient(mongo_uri, **common),
                label)
            self._bind(_shared_mongo_client(*self._client_spec))

    # --- fork safety ----------------------------------------------------------------
    # gunicorn --preload (entry.sh) imports the app -- and so builds every store -- in the
    # master, then forks the workers. A MongoClient must not be used across a fork, so each
    # store keeps the recipe for its client (_client_spec) and rebinds on first use in a new
    # process. Stores built with __new__ (tests) have no recipe and are used as assigned.
    def _bind(self, client):
        self._pid = os.getpid()
        self._client = client
        self._database = client[self._database_name]
        self._collection = self._database[self._collection_name]

    def _check_fork(self):
        if self.__dict__.get('_client_spec') is not None and self.__dict__.get('_pid') != os.getpid():
            self._bind(_shared_mongo_client(*self._client_spec))

    @property
    def _mongo_client(self):
        self._check_fork()
        return self._client

    @property
    def _mongo_database(self):
        self._check_fork()
        return self._database

    @property
    def _db(self):
        self._check_fork()
        return self._collection

    @_db.setter
    def _db(self, collection):
        self._pid = os.getpid()
        self._collection = collection

    def __getitem__(self, fields):
        """
        Atomically does either:
        Gets and returns 'self[key]' or 'self[key][field1][field2][...]' as a dictionary
        """
        key, _, subscripts = self._process_inputs(fields)
        result = self._db.find_one(
            {'_id': key},
            projection={'_id': False})
        if result == None:
            raise KeyError(f"'_id' of '{key}' not found")
        try:
            return eval('result' + subscripts)
        except KeyError:
            raise KeyError(f"Subscript of {subscripts} does not exists in document of '_id' {key}")

    def __setitem__(self, fields, value):
        """
        Atomically does either:
        Sets 'self[key] = value' or sets 'self[key][field1][field2][...] = value'
        """
        key, dots, _ = self._process_inputs(fields)
        try:
            if isinstance(fields, str) and isinstance(value, dict):
                result = self._db.update_one(
                    filter={'_id': key},
                    update={'$set': value},
                    upsert=True)
            else:
                result = self._db.update_one(
                    filter={'_id': key},
                    update={'$set': {dots: value}},
                    upsert=True)
        except WriteError:
            raise WriteError(
                "Likely due to trying to set a subfield of a field that does not exists." +
                "\n Try setting a dict rather than a value. Ex. store['id_key', 'key', 'field'] = {'subfield': 'value'}")
        if result.raw_result['nModified'] == 0:
            if not 'upserted' in result.raw_result:
                logger.debug(f'Field not modified, old value likely the same as new. Key: {key}, Fields: {dots}, Value: {value}')

    def __delitem__(self, fields):
        """
        Atomically does either:
        Deletes 'self[key]'
        Unsets 'self[key][field1][field2][...]'
        """
        key, dots, subscripts = self._process_inputs(fields)
        if not subscripts:
            result = self._db.delete_one({'_id': key})
            if result.raw_result['n'] == 0:
                logger.debug(f"No document with '_id' found. Key:{key}, Fields:{dots}")
        else:
            result = self._db.update_one(
                filter={'_id': key},
                update={'$unset': {f'{dots}': ''}})
            if result.raw_result['nModified'] == 0:
                logger.debug(f"Doc with specified fields not found. Key:{key}, Fields:{dots}")

    def __iter__(self):
        for cursor in self._db.find():
            yield cursor['_id']
        # return self._db.scan_iter()

    def __len__(self):
        """
        Returns the estimated document count of a store to give length
        We don't use '.count_documents()' as it's O(N) versus O(1) of estimated
        Length for a document or subdocument comes from len(store['key']['field1'][...]) using dict len()
        """
        return self._db.estimated_document_count()

    def __repr__(self):
        """
        Returns a pretty string of the entire store with '_id' visible for developer use
        """
        return pprint.pformat(list(self._db.find()))

    def _process_inputs(self, fields):
        """
        Takes in fields and returns the key corresponding with '_id', dot notation
        for getting to a specific field in a Mongo query/filter (ex. 'field1.field2.field3.field4')
        and the subscript notation for returning a specified field from a result dictionary
        (ex. `['field1']['field2']['field3']['field4']`)
        """
        if isinstance(fields, str):
            key = dots = fields
            subscripts = ''
        elif isinstance(fields, list) and len(fields) == 1:
            key = dots = fields[0]
            subscripts = ''
        else:
            key = fields[0]
            dots = '.'.join(fields[1:])
            subscripts = "['" + "']['".join(fields[1:]) + "']"
        return key, dots, subscripts

    def _prepset(self, value):
        if type(value) is bytes:
            return value.decode('utf-8')
        return value

    def pop_field(self, fields):
        """
        Atomically pops 'self[key] = value' or 'self[key][field1][field2][...] = value'
        """
        key, dots, subscripts = self._process_inputs(fields)
        if not subscripts:
            result = self._db.find_one(
                {'_id': key},
                projection={'_id': False})
            if result == None:
                raise KeyError(f"'_id' of '{key}' not found")
            del_result = self._db.delete_one({'_id': key})
            if del_result.raw_result['n'] == 0:
                raise KeyError(f"No document deleted")
            return result
        else:
            result = self._db.find_one_and_update(
                filter={'_id': key},
                update={'$unset': {dots: ''}})
            try:
                return eval('result' + subscripts)
            except KeyError:
                raise KeyError(f"Subscript of {subscripts} does not exist in document of '_id' {key}")

    def set_with_expiry(self, fields, value, log_ex):
        """
        Atomically:
        Sets 'self[key] = value' or 'self[key][field1][field2][...] = value'
        Creates 'exp' subdocument in document root with current time for use with MongoDB TTL expiration index
        Note: MongoDB TTL checks every 60 secs to delete files
        """
        key, dots, _ = self._process_inputs(fields)
        time_to_expire = datetime.utcnow() + timedelta(seconds=log_ex)
        logger.debug(f"Set with expiry setting time to expire to : {time_to_expire} ")
        if len(fields) == 1 and isinstance(value, dict):
            result = self._db.update_one(
                filter={'_id': key},
                update={'$set': {'exp': time_to_expire},
                        '$set': value},
                upsert=True)
        else:
            result = self._db.update_one(
                filter={'_id': key},
                update={'$set': {'exp': time_to_expire, dots: self._prepset(value)}},
                upsert=True)

    def full_update(self, key, value, upsert=False):
        result = self._db.update_one(key, value, upsert)
        return result

    def getset(self, fields, value):
        """
        Atomically does either:
        Sets 'self[key] = value' and returns previous 'self[key]'
        Sets 'self[key][field1][field2][...] = value' and returns previous 'self[key][field1][field2][...]'
        """
        key, dots, subscripts = self._process_inputs(fields)
        result = self._db.find_one_and_update(
            filter={'_id': key, dots: {'$exists': True}},
            update={'$set': {dots: value}})
        if result == None:
            raise KeyError(f"Subscript of {subscripts} does not exist in document of '_id' {key}")   
        try:
            if len(fields) == 1:
                return eval(f"result['{key}']")
            else:
                return eval('result' + subscripts)
        except KeyError:
            raise KeyError(f"Subscript of {subscripts} does not exist in document of '_id' {key}")

    def items(self, filter_inp=None, proj_inp={'_id': False}):
        " Either returns all with no inputs, or filters when given filters"
        return list(self._db.find(
            filter=filter_inp,
            projection=proj_inp))

    def add_if_empty(self, fields, value):
        """
        Atomically:
        Sets 'self[key] = value' or 'self[key][field1][field2][...] = value'
        Only if the specified key/field(s) combo does not exist or is empty
        Returns the value if it was added; otherwise, returns None
        Note: Will not override a field set to a value in order to create a subfield
        """
        key, dots, _ = self._process_inputs(fields)
        try:
            if len(fields) == 1 and isinstance(value, dict):
                result = self._db.update_one(
                    filter={'_id': key},
                    update={'$setOnInsert': value},
                    upsert=True)
                if result.upserted_id:
                    return key
            elif len(fields) == 1:
                result = self._db.update_one(
                    filter={'_id': key},
                    update={'$setOnInsert': {dots: value}},
                    upsert=True)
                if result.upserted_id:
                    return key
            else:
                try:
                    result = self._db.update_one(
                        filter={'_id': key},
                        update={'$setOnInsert': {dots: value}},
                        upsert=True)
                    if result.upserted_id:
                        return fields
                except WriteError:
                    print("Likely due to trying to set a subfield of a field that is already set to one value")
                    pass
            return None
        except DuplicateKeyError:
            return None
    
    def claim_increment(self, key, field, expected, conditions=None):
        """
        Atomic compare-and-increment, for claiming a bounded retry:
        ``$inc self[key][field]`` by 1 only if it currently equals `expected` (an `expected`
        of 0/None also matches a missing or null field) and every extra `conditions` filter
        on the document holds. Returns True if this caller made the increment (won the
        claim), False if the document changed underneath it or does not match.
        """
        flt = {'_id': key}
        flt.update(conditions or {})
        flt[field] = expected if expected else {'$in': [None, 0]}
        result = self._db.find_one_and_update(
            filter=flt,
            update={'$inc': {field: 1}},
            projection={'_id': True})
        return result is not None

    def aggregate(self, pipeline, options = None):
        # maxTimeMS bounds the server-side work per aggregation; without it a pathological
        # search pins a request thread for as long as the socket timeout allows.
        # pymongo 3.10's signature is aggregate(pipeline, session=None, **kwargs): the old
        # positional `options` landed in `session`. Options are keyword arguments and
        # override these defaults.
        kwargs = {'allowDiskUse': True, 'maxTimeMS': MONGO_MAX_TIME_MS}
        kwargs.update(options or {})
        return self._db.aggregate(pipeline, **kwargs)

    def create_index(self, index_list):
        return self._db.create_index(index_list)