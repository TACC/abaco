# Unit test suite for the RedisStore class.
# This test suite now runs in its own docker container. To build the image, run
#     docker build -f Dockerfile-test -t abaco/testsuite .
# from within the tests directory.
#
# To run the tests execute, first start the development stack using:
#  1. export abaco_path=$(pwd)
#  2. docker-compose -f docker-compose-local-db.yml up -d (from within the root directory)
#  3. docker-compose -f docker-compose-local.yml up -d (from within the root directory)
# Then, also from the root directory, execute:
#     docker run -e base_url=http://172.17.0.1:8000 -v $(pwd)/local-dev.conf:/etc/service.conf --entrypoint=py.test -it --rm abaco/testsuite:dev /tests/test_store.py


from _datetime import datetime
import pytest
import os
import sys
import threading
import time
import timeit
sys.path.append(os.path.split(os.getcwd())[0])
sys.path.append('/actors')

from config import Config
from store import MongoStore

store = 'mongo'
# this is the number of iterations executed in each thread, per test.
n = 500

@pytest.fixture(scope='session')
def st():
    ms = MongoStore(Config.get('store', 'mongo_host'), Config.getint('store', 'mongo_port'), db='11')
    # we want to recreate the index each time so we start off trying to drop it, but the first time we run
    # after the db is instantiated the index won't exist.
    try:
        ms._db.drop_index('exp_1')
    except Exception:
        pass
    ms._db.create_index('exp', expireAfterSeconds=1)
    return ms

def test_set_key(st):
    st['test_val'] = 'val'
    assert st.get('test_val')['test_val'] == 'val'

def test_set_with_expiry(st):
    st.set_with_expiry('test_exp', 'field', 'val')
    assert st.get('test_exp')['field'] == 'val'
    # in our tests, the mongo expiry functionality is NOT dependable; it seems to eventually remove the key but the time
    # it takes seems to fluctuate. for mongo, we'll test at the end of the suite to make sure the key is removed.

def _thread(st, n):
    for i in range(n):
        st.update('test', 'k2', f'w{i}')

def test_update(st):
    st.updateDoc('test', {'k': 'v', 'k2': 'v2'})
    t = threading.Thread(target=_thread, args=(st, n))
    t.start()
    for i in range(n):
        st.update('test', 'k', f'v{i}')
    t.join()
    assert st['test'] == {'k': f'v{n-1}', 'k2': f'w{n-1}'}

def test_pop_field(st):
    st.updateDoc('test', {'k': 'v', 'k2': 'v2', 'key': 'val'})
    # this is the naive functionality we want; of course, this is not thread safe:
    cur = st['test']
    val = cur.pop('key')
    st.updateDoc('test', cur)
    assert val == 'val'
    assert st['test'] == {'k': 'v', 'k2': 'v2', 'key': 'val'}

    # here's the non-threaded test:
    st.updateDoc('test', {'k': 'v', 'k2': 'v2', 'key': 'val'})
    assert st['test']['key'] == 'val'
    val = st.pop_field('test', 'key')
    assert val == 'val'
    assert not type(st['test']) == str

    # and finally, a threaded test:
    st.updateDoc('test', {'k': 'v', 'k2': 'v2'})
    for i in range(n):
        st.update('test', f'key{i}', f'v{i}')
    st['test']['key0'] = 'v0'
    assert st['test']['key0'] == 'v0'
    assert st['test'][f'key{n-1}'] == f'v{n-1}'
    t = threading.Thread(target=_thread, args=(st, n))
    t.start()
    for i in range(n):
        val = st.pop_field('test', f'key{i}')
        assert val == f'v{i}'
    t.join()
    assert st['test'] == {'k': 'v', 'k2': f'w{n-1}'}

def test_update_subfield(st):
    st.updateDoc('test', {'k': {'sub': 'v'}, 'k2': 'v2'})
    t = threading.Thread(target=_thread, args=(st, n))
    t.start()
    for i in range(n):
        st.update_subfield('test', 'k', 'sub', f'v{i}')
    t.join()
    assert st['test'] == {'k': {'sub': f'v{n-1}'}, 'k2': f'w{n-1}'}

def test_getset(st):
    st.updateDoc('test', {'k': 'v', 'k2': 'v2'})
    st.update('k', 'k', 'v0')
    t = threading.Thread(target=_thread, args=(st, n))
    t.start()
    for i in range(n):
        v = st.getset('k', f'v{i}')
        if i == 0:
            assert v == 'v0'
        else:
            assert v == f'v{i-1}'
    t.join()
    assert st['test'] == {'k': 'v', 'k2': f'w{n-1}'}
    assert st['k']['k'] == f'v{n-1}'

def test_within_transaction(st):
        # mongo store does not support within_transaction
    if not store == 'redis':
        return

    def _th():
        """A separate thread that is going to compete with the main thread to make updates on the key."""
        assert st['k'] == 'v'
        time.sleep(1)
        # try to update the value of 'k'; this should take a while since the other thread has a lock
        start = timeit.default_timer()
        res = st.full_update(
            {'_id': 'k'},
            {'$set': {'k':'v2'}})
        stop = timeit.default_timer()
        assert st['k'] == 'v2'
        tot = stop - start
        assert tot > 2.0

    def _transaction(val):
        """Represents business logic that should be wrapped in a transaction."""
        # make sure we are passed the value
        assert val == 'v'
        # also, get the key and assert the original value
        assert st['k'] == 'v'
        # now sleep some time
        time.sleep(3)
        # now, update the key:
        st['k'] = 'foo'
        assert st['k'] == 'foo'

    st['k'] = 'v'
    # first start a new thread that will sleep for 1 second before trying to change the value
    t = threading.Thread(target=_th)
    t.start()
    # now, start a transaction in the main thread:
    st.within_transaction(_transaction, 'k')

def test_redis_one_list_pop(st):
    # mongo store does not support list mutations
    if not store == 'redis':
        return

    def _th():
        """ A separate thread that will compete with the main thread to make mutations to the two lists."""
        for i in range(n):
            st.pop_fromlist('l')

    st['l'] = ['a'] * 5000
    t = threading.Thread(target=_th)
    t.start()
    # now, in the main thread, perform the same processing but in reverse
    for i in range(n):
        st.pop_fromlist('l')
    # wait for second thread to complete
    t.join()
    # verify that
    assert len(st['l']) == 5000 - 2*n

def test_redis_one_list_append(st):
    # mongo store does not support list mutations
    if not store == 'redis':
        return

    def _th():
        """ A separate thread that will compete with the main thread to make mutations to the two lists."""
        for i in range(n):
            st.append_tolist('l', 't2')

    st['l'] = []
    t = threading.Thread(target=_th)
    t.start()
    # now, in the main thread, perform the same processing but in reverse
    for i in range(n):
        st.append_tolist('l', 't1')
    # wait for second thread to complete
    t.join()
    # verify that
    assert len(st['l']) == 2*n

def test_redis_one_list_pop_append(st):
    # mongo store does not support list mutations
    if not store == 'redis':
        return

    def _th():
        """ A separate thread that will compete with the main thread to make mutations to the two lists."""
        for i in range(n):
            st.append_tolist('l', 't2_{}'.format(i))
            st.pop_fromlist('l')

    st['l'] = ['a', 'b', 'c']
    t = threading.Thread(target=_th)
    t.start()
    # now, in the main thread, perform the same processing but in reverse
    for i in range(n):
        st.append_tolist('l', 't1_{}'.format(i))
        st.pop_fromlist('l')
    # wait for second thread to complete
    t.join()
    # verify that free and locked lists each have three items; we cannot gu
    assert len(st['l']) == 3

def test_redis_two_lists(st):
    # mongo store does not support list mutations
    if not store == 'redis':
        return

    def _th():
        """ A separate thread that will compete with the main thread to make mutations to the two lists."""
        # first, get the first free element
        for i in range(n):
            # first, get the first free item and move it to the locked list
            first_free = st.pop_fromlist('free', 0)
            st.append_tolist('locked', first_free)
            # now, get first locked and move to free list
            first_locked = st.pop_fromlist('locked', 0)
            st.append_tolist('free', first_locked)

    st['free'] = ['a', 'b', 'c']
    st['locked'] = ['1', '2', '3']
    t = threading.Thread(target=_th)
    t.start()
    # now, in the main thread, perform the same processing but in reverse
    for i in range(n):
        # first, get the first locked item and move it to the free list
        first_locked = st.pop_fromlist('locked', 0)
        st.append_tolist('free', first_locked)
        # now, get first free and move to locked list
        first_free = st.pop_fromlist('free', 0)
        st.append_tolist('locked', first_free)
    # wait for second thread to complete
    t.join()
    # verify that free and locked lists each have three items; we cannot gu
    assert len(st['free']) == 3
    assert len(st['locked']) == 3

def test_set_with_expiry2(st):
    # in our tests, the mongo expiry functionality is NOT dependable; it seems to eventually remove the key but the time
    # it takes seems to fluctuate. for mongo, we'll test at the end of the suite to make sure the key is removed.
    tot = 0
    while tot < 5:
        try:
            st['test_exp']
        except KeyError:
            return
        tot += 1
        time.sleep(2)