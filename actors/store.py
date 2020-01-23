
import collections
from datetime import datetime
import json
import os
import urllib.parse

import configparser
import redis
from pymongo import MongoClient

from config import Config

from agaveflask.logs import get_logger
logger = get_logger(__name__)

def _do_get(getter, key):
    obj = getter(key)
    if obj is None:
        raise KeyError('"{}" not found'.format(key))
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
        pass

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
            raise StoreMutexException('{} is busy'.format(key))

    def mutex_release(self, key):
        self[key] = False


class AbstractTransactionalStore(AbstractStore):
    """Adds basic transactional semantics to the AbstractStore interface."""
    def within_transaction(self, f, key):
        """Execute a callable, f, within a lock on key `key`."""
        pass


class MongoStore(AbstractStore):

    def __init__(self, host, port, database='abaco', db='0', user=None, password=None):
        """
        Creates an abaco `store` which maps to a single mongo
        collection within some database.
        :param host: the IP address of the mongo server.
        :param port: port of the mongo server.
        :param database: the mongo database to use for abaco.
        :param db: an integer mapping to a mongo collection within the
        mongo database.

        :return:
        """
        mongo_uri = 'mongodb://{}:{}'.format(host, port)
        if user and password:
            logger.info("Using mongo user {} and passowrd: ***".format(user))
            u = urllib.parse.quote_plus(user)
            p = urllib.parse.quote_plus(password)
            mongo_uri = 'mongodb://{}:{}@{}:{}'.format(u, p, host, port)
        self._mongo_client = MongoClient(mongo_uri)
        self._mongo_database = self._mongo_client[database]
        self._db = self._mongo_database[db]

    def __getitem__(self, key):
        result = self._db.find_one(
            {'_id': key},
            projection={'_id': False})
        if not result:
            raise KeyError()
        return result

    def __setitem__(self, key, value):
        self._db.update_one(
            filter={'_id': key},
            update={'$set': {key:value}},
            upsert=True)

    def __delitem__(self, key):
        self._db.delete_one({'_id': key})

    def __iter__(self):
        for cursor in self._db.find():
            yield cursor['_id']
        # return self._db.scan_iter()

    def __len__(self):
        return self._db.estimated_document_count()

    def _prepset(self, value):
        if type(value) is bytes:
            return value.decode('utf-8')
        return value

    def set_with_expiry(self, key, field, value):
        """Set `key` to `obj` with automatic expiration of the configured seconds."""
        self._db.update_one(
            filter={'_id': key},
            update={'$set': {'exp': datetime.utcnow(), field: self._prepset(value)}},
            upsert=True)

    def full_update(self, key, value):
        result = self._db.update_one(key, value)
        return result

    def updateDoc(self, key, value):
        "Atomic ``self[key][field] = value``."""
        result = self._db.update_one(
            filter={'_id': key},
            update={'$set': value},
            upsert=True)
        if not result:
            raise KeyError()

    def update(self, key, field, value):
        "Atomic ``self[key][field] = value``."""
        result = self._db.update_one(
            filter={'_id': key},
            update={'$set': {f'{field}': value}},
            upsert=True)
        if not result:
            raise KeyError()

    def pop_field(self, key, field):
        "Atomic pop ``self[key][field]``."""
        result = self._db.find_one_and_update(
            filter={'_id': key},
            update={'$unset': {f'{field}': ''}})
        return result[field]

    def update_subfield(self, key, field1, field2, value):
        "Atomic ``self[key][field1][field2] = value``."""
        self._db.update_one(
            {'_id': key},
            {'$set': {'{}.{}'.format(field1, field2): value}})

    def getset(self, key, value):
        "Atomically: ``self[key] = value`` and return previous ``self[key]``."
        result = self._db.find_one_and_update(
            filter={'_id': key},
            update={'$set': {key: value}})
        return result[key]

    def items(self, filter_inp=None, proj_inp={'_id': False}):
        " Either returns all with no inputs, or filters when given filters"
        return list(self._db.find(
            filter=filter_inp,
            projection=proj_inp))

    def add_if_empty(self, key, field, value):
        """
        Atomic ``self[key][field] = value`` if s``self[key]`` does not exist or is empty.
        Add a value, `value`, to a field, `field`, under key, `key`, only if the key does not exist
        or it's value is currently empty. Returns the value if it was added; otherwise, returns None.
        """
        # For worker store only
        # Makes the key_str unique so atomicity works and multiples can't be made
        # actor_id, worker_id, worker
        #self._db.create_index(key_str, unique=True)

        res = self._db.update_one(
            {'_id': key},
            {'$setOnInsert': {field: value}},
            upsert=True)

        if res.upserted_id:
            return field
        else:
            return None


    #### CHECK IF THIS WORKS
    def add_key_val_if_empty(self, key, value):
        """
        Atomic ``self[key] = value`` if ``self[key]`` does not exist or is empty.
        If the key does exist, returns None.
        """
        # Makes the key_str unique so atomicity works and multiples can't be made
        # self._db.create_index(, unique=True)

        res = self._db.update_one(
            {'_id': key},
            {'$setOnInsert': value},
            upsert=True)

        if res.upserted_id:
            return key
        else:
            return None







    #### NOT USED. REMNANTS FROM REDIS
    def pop_fromlist(self, key, idx=-1):
        """
        Atomic self[key].pop(idx); assumes the data structure under `key` is a list.
        
        :return: 
        """
        with self._db.pipeline() as pipe:
            while 1:
                try:
                    pipe.watch(key)
                    cur = _do_get(pipe.get, key)
                    value = cur.pop(idx)
                    pipe.multi()
                    _do_set(pipe.set, key, cur)
                    pipe.execute()
                    return value
                except redis.WatchError:
                    continue

    def append_tolist(self, key, obj):
        """
        Atomic self[key].append(obj); assumes the data structure under `key` is a list.

        :return: 
        """
        with self._db.pipeline() as pipe:
            while 1:
                try:
                    pipe.watch(key)
                    cur = _do_get(pipe.get, key)
                    cur.append(obj)
                    pipe.multi()
                    _do_set(pipe.set, key, cur)
                    pipe.execute()
                    return None
                except redis.WatchError:
                    continue
                finally:
                    pipe.reset()

    def within_transaction(self, f, key):
        """Execute a callable, f, within a lock on key `key`. The executable, f, should take a single argument that
        is the current value under the key """
        def _transaction(pipe):
            cur = _do_get(pipe.get, key)
            f(cur)

        return self._db.transaction(_transaction, key)