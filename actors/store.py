
import collections
from datetime import datetime
import json
import os
import urllib.parse

import pprint
import redis
from pymongo.errors import WriteError, DuplicateKeyError
from pymongo import MongoClient

from common.logs import get_logger
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
        Note: pop_fromlist, append_tolist, and within_transaction were removed from the Redis
        store functions as they weren't necessary, don't work, or don't work in Mongo.
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

    def set_with_expiry(self, fields, value):
        """
        Atomically:
        Sets 'self[key] = value' or 'self[key][field1][field2][...] = value'
        Creates 'exp' subdocument in document root with current time for use with MongoDB TTL expiration index
        Note: MongoDB TTL checks every 60 secs to delete files
        """
        key, dots, _ = self._process_inputs(fields)
        if len(fields) == 1 and isinstance(value, dict):
            result = self._db.update_one(
                filter={'_id': key},
                update={'$set': {'exp': datetime.utcnow()},
                        '$set': value},
                upsert=True)
        else:
            result = self._db.update_one(
                filter={'_id': key},
                update={'$set': {'exp': datetime.utcnow(), dots: self._prepset(value)}},
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
            raise KeyError(f"1Subscript of {subscripts} does not exist in document of '_id' {key}")   
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
    
    def aggregate(self, pipeline, options = None):
        return self._db.aggregate(pipeline, options)

    def create_index(self, index_list):
        return self._db.create_index(index_list)