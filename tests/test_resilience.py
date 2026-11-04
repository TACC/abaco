"""
Verification tests for spawner, health, and RabbitMQ resilience hardening.

These are unit-level tests using mocks — they do not require a running Abaco stack,
RabbitMQ, or MongoDB.  Run with:

    cd tests && python -m pytest test_resilience.py -v
"""
import os
import sys
import threading
import time

sys.path.append(os.path.split(os.getcwd())[0])
sys.path.append('/actors')

from unittest import mock
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

# ---------------------------------------------------------------------------
# 1. Worker.delete_worker – idempotent (double-delete is a no-op)
# ---------------------------------------------------------------------------

class TestDeleteWorkerIdempotent:
    """Calling delete_worker twice should not raise — the second call logs a warning."""

    @patch('models.site', return_value='test_site')
    @patch('models.workers_store', new_callable=dict)
    def test_double_delete_no_exception(self, mock_store_dict, mock_site):
        from models import Worker

        # Set up a mock store that behaves like the real store for pop_field/KeyError.
        store = MagicMock()
        mock_store_dict['test_site'] = store

        # First call succeeds.
        store.pop_field.return_value = {'id': 'w1', 'status': 'READY'}
        Worker.delete_worker('actor_abc', 'w1')
        store.pop_field.assert_called_once_with(['actor_abc_w1'])

        # Second call — worker already gone — should not raise.
        store.pop_field.side_effect = KeyError('actor_abc_w1')
        Worker.delete_worker('actor_abc', 'w1')  # must not raise

    @patch('models.site', return_value='test_site')
    @patch('models.workers_store', new_callable=dict)
    def test_double_delete_logs_warning(self, mock_store_dict, mock_site):
        from models import Worker

        store = MagicMock()
        mock_store_dict['test_site'] = store
        store.pop_field.side_effect = KeyError('gone')

        with patch('models.logger') as mock_logger:
            Worker.delete_worker('actor_abc', 'w1')
            mock_logger.warning.assert_called_once()
            assert 'already gone' in mock_logger.warning.call_args[0][0]


# ---------------------------------------------------------------------------
# 2. hard_delete_worker – DB fallback when normal deletion raises
# ---------------------------------------------------------------------------

class TestHardDeleteWorkerFallback:

    @patch('health.site', return_value='test_site')
    @patch('health.rm_container')
    @patch('health.workers_store', new_callable=dict)
    @patch('health.Worker')
    def test_fallback_to_direct_db_delete(self, MockWorker, mock_ws_dict, mock_rm, mock_site):
        from health import hard_delete_worker

        # Normal deletion raises.
        MockWorker.delete_worker.side_effect = Exception("corrupt record")

        db_mock = MagicMock()
        store_mock = MagicMock()
        store_mock._db = db_mock
        mock_ws_dict['test_site'] = store_mock

        hard_delete_worker('actor_abc', 'w1', worker_container_id='cid123')

        # Verify fallback was attempted.
        db_mock.delete_one.assert_called_once_with({'_id': 'actor_abc_w1'})
        # Container removal should still be attempted.
        mock_rm.assert_called_once_with('cid123')


# ---------------------------------------------------------------------------
# 3. check_workers_store – corrupt worker doesn't crash the loop
# ---------------------------------------------------------------------------

class TestCheckWorkersStore:

    @patch('health.site', return_value='test_site')
    @patch('health.workers_store', new_callable=dict)
    @patch('health.check_worker_health')
    def test_corrupt_worker_does_not_crash(self, mock_check, mock_ws_dict, mock_site):
        from health import check_workers_store

        good_worker = {'id': 'w_good', 'actor_id': 'a1', 'status': 'READY'}
        bad_worker = {'_id': 'a2_w_bad'}  # missing 'id' and 'status'

        store_mock = MagicMock()
        store_mock.items.return_value = [good_worker, bad_worker]
        mock_ws_dict['test_site'] = store_mock

        # check_worker_health raises on the bad worker.
        def side_effect(worker, ttl):
            if worker is bad_worker:
                raise KeyError('id')
        mock_check.side_effect = side_effect

        # Must not raise — the good worker should still have been checked.
        check_workers_store(ttl=600)
        assert mock_check.call_count == 2


# ---------------------------------------------------------------------------
# 4. check_workers – corrupt record (missing 'id') triggers cleanup
# ---------------------------------------------------------------------------

class TestCheckWorkersCorruptRecord:

    @patch('health.conf', new_callable=MagicMock)
    @patch('health.site', return_value='test_site')
    @patch('health.get_current_worker_containers', return_value=[])
    @patch('health.workers_store', new_callable=dict)
    @patch('health.Worker')
    @patch('health.hard_delete_worker')
    def test_missing_id_field_cleaned_up(self, mock_hard_del, MockWorker, mock_ws_dict,
                                          mock_containers, mock_site, mock_conf):
        from health import check_workers

        mock_conf.container_backend = 'docker'
        mock_conf.spawner_host_id = 'host1'

        # One valid worker, one corrupt (no 'id').
        corrupt = {'_id': 'actor1_w_corrupt', 'actor_id': 'actor1'}
        valid = {'id': 'w_ok', 'actor_id': 'actor1', 'host_id': 'host1',
                 'status': 'READY', 'last_health_check_time': None, 'create_time': None}

        MockWorker.get_workers.return_value = [corrupt, valid]

        store_mock = MagicMock()
        db_mock = MagicMock()
        store_mock._db = db_mock
        mock_ws_dict['test_site'] = store_mock

        # Accessing corrupt['id'] will raise KeyError — the code should catch it.
        # We need to patch the WorkerChannel and actor health check path.
        with patch('health.WorkerChannel') as MockWC, \
             patch('health.os.environ', {'SPAWNER_HOST_ID': 'host1'}):
            mock_ch = MagicMock()
            MockWC.return_value = mock_ch
            check_workers('actor1', ttl=600)

        # The corrupt record should have triggered the cleanup path (delete via _db).
        db_mock.delete_one.assert_called_with({'_id': 'actor1_w_corrupt'})


# ---------------------------------------------------------------------------
# 5. connect_with_backoff / RabbitConnection – bounded retry on connect failure
# ---------------------------------------------------------------------------

class TestRabbitConnectionBackoff:

    @patch('queues.get_site_rabbitmq_uri', return_value='amqp://fake:5672')
    @patch('queues.site', return_value='test')
    @patch('queues.time.sleep')
    @patch('queues.pika.BlockingConnection')
    def test_retries_with_backoff_then_succeeds(self, MockConn, mock_sleep, mock_site, mock_uri):
        from queues import RabbitConnection

        good_conn = MagicMock()
        MockConn.side_effect = [RuntimeError("down"), RuntimeError("down"),
                                RuntimeError("down"), good_conn]

        rc = RabbitConnection(retries=10)

        assert rc._conn is good_conn
        # exponential backoff between attempts
        sleep_args = [call.args[0] for call in mock_sleep.call_args_list]
        assert sleep_args == [0.5, 1.0, 2.0]
        # the connection parameters carry an applied heartbeat
        params = MockConn.call_args_list[0].args[0]
        assert params.heartbeat == 30
        assert params.blocked_connection_timeout == 30

    @patch('queues.get_site_rabbitmq_uri', return_value='amqp://fake:5672')
    @patch('queues.site', return_value='test')
    @patch('queues.time.sleep')
    @patch('queues.pika.BlockingConnection')
    def test_raises_after_exhausting_retries(self, MockConn, mock_sleep, mock_site, mock_uri):
        from queues import RabbitConnection, RabbitUnavailableError

        MockConn.side_effect = RuntimeError("permanently down")

        with pytest.raises(RabbitUnavailableError, match="Could not connect to RabbitMQ") as ei:
            RabbitConnection(retries=3)

        assert mock_sleep.call_count == 2
        # a ResourceError (503 for the API) that is also a RuntimeError (daemon loops)
        assert isinstance(ei.value, RuntimeError)
        assert ei.value.code == 503

    @patch('queues.get_site_rabbitmq_uri', return_value='amqp://fake:5672')
    @patch('queues.site', return_value='test')
    @patch('queues.time.sleep')
    @patch('queues.time.monotonic')
    @patch('queues.pika.BlockingConnection')
    def test_gives_up_at_deadline(self, MockConn, mock_mono, mock_sleep, mock_site, mock_uri):
        from queues import RabbitConnection, RabbitUnavailableError

        MockConn.side_effect = RuntimeError("down")
        # clock reads: start, then one read per failed attempt; deadline is 10s
        mock_mono.side_effect = [0, 4, 8, 12, 16, 20]

        with pytest.raises(RabbitUnavailableError, match="within 10s"):
            RabbitConnection(deadline_s=10)

        # attempts at t=4 and t=8 slept; the attempt at t=12 gave up
        assert MockConn.call_count == 3
        assert mock_sleep.call_count == 2

    def test_request_threads_get_the_short_deadline(self):
        import queues

        with patch('queues.has_request_context', return_value=True):
            assert queues._default_deadline_s() == queues.REQUEST_CONNECT_DEADLINE_S
        with patch('queues.has_request_context', return_value=False):
            assert queues._default_deadline_s() == queues.CONNECT_DEADLINE_S
        assert queues.REQUEST_CONNECT_DEADLINE_S < queues.CONNECT_DEADLINE_S


# ---------------------------------------------------------------------------
# 6. TaskQueue.close – exception during close doesn't propagate
# ---------------------------------------------------------------------------

class TestTaskQueueClose:

    def test_close_swallows_exception(self):
        from queues import TaskQueue

        tq = TaskQueue.__new__(TaskQueue)  # bypass __init__
        tq.conn = MagicMock()
        tq.conn.close.side_effect = Exception("socket error")

        tq.close()  # must not raise; close is synchronous under pika
        tq.conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# 7. Spawner.run – reconnects on RabbitMQ connection loss
# ---------------------------------------------------------------------------

class TestSpawnerRunReconnect:

    @patch('spawner.CommandChannel')
    @patch('spawner.time.sleep')
    def test_reconnects_after_connection_error(self, mock_sleep, MockCmdCh):
        from spawner import Spawner

        spawner = Spawner.__new__(Spawner)
        spawner.host_id = 'host1'
        spawner.queue = 'test_queue'

        # First call to get_one raises a connection error.
        old_ch = MagicMock()
        old_ch.get_one.side_effect = ConnectionResetError("connection reset")
        spawner.cmd_ch = old_ch

        # CommandChannel() creates a new channel; second get_one works.
        new_ch = MagicMock()
        msg_obj = MagicMock()
        new_ch.get_one.return_value = ({'actor_id': 'a1'}, msg_obj)
        MockCmdCh.return_value = new_ch

        # We'll break out of the infinite loop after the second successful get_one
        # by having process() raise SystemExit.
        call_count = 0
        def fake_process(cmd):
            nonlocal call_count
            call_count += 1
            raise SystemExit("break test loop")

        spawner.process = fake_process
        spawner.overloaded = lambda: False

        with pytest.raises(SystemExit):
            spawner.run()

        # Verify reconnect happened and the redelivered command was acked once.
        MockCmdCh.assert_called_once_with(name='test_queue')
        assert call_count == 1
        msg_obj.ack.assert_called_once()

    @patch('spawner.CommandChannel')
    @patch('spawner.time.sleep')
    def test_reconnect_retries_on_repeated_failure(self, mock_sleep, MockCmdCh):
        from spawner import Spawner

        spawner = Spawner.__new__(Spawner)
        spawner.host_id = 'host1'
        spawner.queue = 'test_queue'

        old_ch = MagicMock()
        old_ch.get_one.side_effect = OSError("broken pipe")
        spawner.cmd_ch = old_ch

        # CommandChannel fails twice, then succeeds.
        new_ch = MagicMock()
        msg_obj = MagicMock()
        new_ch.get_one.return_value = ({'actor_id': 'a1'}, msg_obj)
        MockCmdCh.side_effect = [Exception("still down"), Exception("still down"), new_ch]

        spawner.process = lambda cmd: (_ for _ in ()).throw(SystemExit("done"))
        spawner.overloaded = lambda: False

        with pytest.raises(SystemExit):
            spawner.run()

        assert MockCmdCh.call_count == 3
        # Sleep was called with exponential backoff values.
        sleep_vals = [c.args[0] for c in mock_sleep.call_args_list]
        assert sleep_vals[0] == 1
        assert sleep_vals[1] == 2

    @patch('spawner.CommandChannel')
    @patch('spawner.time.sleep')
    def test_ack_failure_takes_the_reconnect_path(self, mock_sleep, MockCmdCh):
        import pika.exceptions
        from spawner import Spawner

        spawner = Spawner.__new__(Spawner)
        spawner.host_id = 'host1'
        spawner.queue = 'test_queue'

        # delivery succeeds but the channel dies before the ack
        dead_msg = MagicMock()
        dead_msg.ack.side_effect = pika.exceptions.ChannelWrongStateError("closed")
        old_ch = MagicMock()
        old_ch.get_one.return_value = ({'actor_id': 'a1'}, dead_msg)
        spawner.cmd_ch = old_ch

        new_ch = MagicMock()
        new_ch.get_one.return_value = ({'actor_id': 'a1'}, MagicMock())
        MockCmdCh.return_value = new_ch

        processed = []
        def fake_process(cmd):
            processed.append(cmd)
            raise SystemExit("done")
        spawner.process = fake_process
        spawner.overloaded = lambda: False

        with pytest.raises(SystemExit):
            spawner.run()

        # the un-acked command was not processed on the dead channel; the broker
        # redelivers it and the reconnected channel processes it exactly once
        MockCmdCh.assert_called_once_with(name='test_queue')
        assert processed == [{'actor_id': 'a1'}]


# ---------------------------------------------------------------------------
# 8. Spawner.process – corrupt worker record triggers cleanup
# ---------------------------------------------------------------------------

class TestSpawnerProcessCorruptWorker:

    @patch('spawner.site', return_value='test_site')
    @patch('spawner.actors_store', new_callable=dict)
    @patch('spawner.Worker')
    def test_corrupt_worker_deleted(self, MockWorker, mock_as_dict, mock_site):
        from spawner import Spawner

        spawner = Spawner.__new__(Spawner)
        spawner.host_id = 'host1'

        # actors_store lookup succeeds.
        mock_as_dict['test_site'] = {'actor1': {'image': 'img', 'status': 'READY',
                                                 'db_id': 'actor1', 'tenant': 'dev'}}

        # Worker.get_worker returns a corrupt record (missing 'status' and 'id').
        MockWorker.get_worker.return_value = {'_id': 'actor1_w1'}

        cmd = {'actor_id': 'actor1', 'worker_id': 'w1', 'image': 'img',
               'revision': 1, 'tenant': 'dev', 'site_id': 'test_site'}

        with patch('spawner.Actor') as MockActor:
            MockActor.from_db.return_value = MagicMock(status='READY')
            spawner.process(cmd)

        # Worker should have been deleted because of missing fields.
        MockWorker.delete_worker.assert_called_once_with('actor1', 'w1', 'test_site')


# ---------------------------------------------------------------------------
# 9. get_site_rabbitmq_uri – plain AMQP URI; the heartbeat is applied by RabbitConnection
# ---------------------------------------------------------------------------

class TestRabbitURI:

    @patch('stores.conf')
    def test_uri_has_vhost_and_no_query_string(self, mock_conf):
        from stores import get_site_rabbitmq_uri

        mock_conf.rabbit_uri = 'amqp://rabbit:5672'
        mock_conf.get.side_effect = lambda key: {
            'test_site_object': {'site_rabbitmq_pass': 'pass123'},
            'global_site_object': {'site_rabbitmq_pass': 'pass123'},
        }.get(key, {})

        uri = get_site_rabbitmq_uri('test')
        assert uri == 'amqp://abaco_test_user:pass123@rabbit:5672/abaco_test'
        # a ?heartbeat= query parameter here was dead code: pika.URLParameters parsed
        # it and RabbitConnection then applied its own heartbeat anyway
        assert '?' not in uri


# ---------------------------------------------------------------------------
# 10. worker.py / kubernetes_worker.py -- pika worker loop (review fixups)
#
# Hermetic: the worker modules are imported with their Mongo/Docker/Tapis/k8s
# dependencies (and tapisservice.config) stubbed in sys.modules, so this section
# also runs without a live stack:
#     python3 -m pytest tests/test_resilience.py -k Worker -p no:cacheprovider
# ---------------------------------------------------------------------------

import contextlib


class _StubConf(dict):
    def __getattr__(self, name):
        return self.get(name)


@contextlib.contextmanager
def _stubbed_worker_module(name):
    """Import actors/<name>.py with everything that needs a live stack stubbed. sys.modules is
    restored afterwards, so the stubs (and the module under test) never leak into other tests."""
    import importlib
    import logging
    import types

    conf = _StubConf(worker_leave_containers=False, worker_mem_limit='1g', worker_max_cpus=1,
                     worker_socket_paths='/tmp/_s:/tmp/_s', worker_fifo_paths='/tmp/_f:/tmp/_f',
                     global_tenant_object={})
    config_mod = types.ModuleType('tapisservice.config')
    config_mod.conf = conf
    logs_mod = types.ModuleType('tapisservice.logs')
    logs_mod.get_logger = logging.getLogger

    docker_utils = MagicMock()
    kubernetes_utils = MagicMock()
    for mod, prefix in ((docker_utils, 'Docker'), (kubernetes_utils, 'Kubernetes')):
        base = type(f'{prefix}Error', (Exception,), {})
        setattr(mod, f'{prefix}Error', base)
        setattr(mod, f'{prefix}StartContainerError', type(f'{prefix}StartContainerError', (base,), {}))
        setattr(mod, f'{prefix}StopContainerError', type(f'{prefix}StopContainerError', (base,), {}))
    stubs = {'tapisservice.config': config_mod, 'tapisservice.logs': logs_mod,
             '__init__': MagicMock(), 'auth': MagicMock(), 'stores': MagicMock(), 'models': MagicMock(),
             'docker_utils': docker_utils, 'kubernetes_utils': kubernetes_utils, 'kubernetes': MagicMock()}
    with patch.dict(sys.modules, stubs):
        for fresh in (name, 'queues', 'channels', 'errors', 'globals'):
            sys.modules.pop(fresh, None)
        mod = importlib.import_module(name)
        mod._stubs = stubs
        mod._conf = conf
        yield mod


@pytest.fixture
def pika_worker():
    with _stubbed_worker_module('worker') as mod:
        yield mod


@pytest.fixture
def k8s_worker():
    with _stubbed_worker_module('kubernetes_worker') as mod:
        yield mod


def _actor_msg(execution_id='e1'):
    import cloudpickle
    return cloudpickle.dumps({'message': 'hi', '_abaco_execution_id': execution_id,
                              '_abaco_Content_Type': 'str'})


def _run_subscribe(worker, revision=1):
    worker.subscribe('tenant', 'actor1', 'img', revision, 'w1', 'https://api')


class TestPikaWorkerHeartbeats:

    def test_run_with_heartbeats_pumps_while_fn_runs(self, pika_worker):
        conn = MagicMock()
        pumped = threading.Event()
        conn.process_data_events.side_effect = lambda time_limit: pumped.set()
        seen = {}

        def fn(a, b):
            seen['thread'] = threading.current_thread()
            assert pumped.wait(5), "connection was not serviced while fn ran"
            return a + b

        assert pika_worker.run_with_heartbeats(conn, fn, 1, 2) == 3
        assert seen['thread'] is not threading.current_thread()
        conn.process_data_events.assert_called_with(time_limit=1)

    def test_run_with_heartbeats_reraises_on_the_calling_thread(self, pika_worker):
        err_cls = pika_worker.DockerStartContainerError

        def fn():
            raise err_cls("no start")

        with pytest.raises(err_cls, match="no start"):
            pika_worker.run_with_heartbeats(MagicMock(), fn)

    def test_dead_connection_does_not_abandon_the_execution(self, pika_worker):
        import pika.exceptions
        conn = MagicMock()
        conn.process_data_events.side_effect = pika.exceptions.StreamLostError("gone")
        release = threading.Event()

        def fn():
            release.wait(5)
            return 'done'

        threading.Timer(0.3, release.set).start()
        assert pika_worker.run_with_heartbeats(conn, fn) == 'done'
        # stopped pumping after the first failure instead of spinning on a dead connection
        assert conn.process_data_events.call_count == 1

    def test_settle_swallows_a_dead_connection(self, pika_worker):
        import pika.exceptions
        ch = MagicMock()
        ch.basic_ack.side_effect = pika.exceptions.ChannelWrongStateError("closed")
        assert pika_worker._settle(ch, 7) is False
        ch.basic_nack.side_effect = pika.exceptions.ChannelWrongStateError("closed")
        assert pika_worker._settle(ch, 7, ack=False, requeue=True) is False


class TestPikaWorkerSubscribe:

    @patch('time.sleep')
    def test_execution_runs_off_the_connection_thread_and_acks_on_it(self, mock_sleep, pika_worker):
        w = pika_worker
        conn, ch = MagicMock(), MagicMock()
        method = MagicMock(delivery_tag=11)

        def basic_get(queue, auto_ack):
            if ch.basic_get.call_count == 1:
                return method, MagicMock(), _actor_msg()
            w.globals.keep_running = False
            return None, None, None
        ch.basic_get.side_effect = basic_get
        pumped = threading.Event()
        conn.process_data_events.side_effect = lambda time_limit: pumped.set()
        seen = {}

        def fake_execute(*args):
            seen['thread'] = threading.current_thread()
            seen['args'] = args
            assert pumped.wait(5)
            return {}, 'logs', 'final', 0, 'start'
        w.execute_actor.side_effect = fake_execute
        w.Actor.from_db.return_value = MagicMock(revision=1, mounts=[])

        with patch.object(w, 'get_pika_channel', return_value=(conn, ch)), \
             patch.object(w, 'process_worker_ch'):
            _run_subscribe(w)

        assert seen['thread'] is not threading.current_thread()
        ch.basic_ack.assert_called_once_with(delivery_tag=11)
        ch.basic_nack.assert_not_called()
        # leave_containers comes from config again (the debug override hard-coded True)
        assert seen['args'][9] is False

    @patch('time.sleep')
    def test_closed_channel_with_queue_gone_raises_instead_of_sys_exit(self, mock_sleep, pika_worker):
        import pika.exceptions
        w = pika_worker
        conn1, ch1 = MagicMock(), MagicMock()
        ch1.basic_get.side_effect = pika.exceptions.StreamLostError("connection lost")
        conn2, ch2 = MagicMock(), MagicMock()
        ch2.queue_declare.side_effect = pika.exceptions.ChannelClosedByBroker(404, "NOT_FOUND")

        with patch.object(w, 'get_pika_channel', side_effect=[(conn1, ch1), (conn2, ch2)]) as gpc, \
             patch.object(w, 'process_worker_ch'):
            # a WorkerException reaches __main__ (stop-no-delete + exit_process); SystemExit did not
            with pytest.raises(w.WorkerException, match="is gone"):
                _run_subscribe(w)

        assert gpc.call_args_list[1].kwargs == {'declare': False}  # never resurrects the queue
        ch2.queue_declare.assert_called_once_with(queue='actor_msg_actor1', passive=True)
        conn1.close.assert_called()
        conn2.close.assert_called()

    @patch('time.sleep')
    def test_closed_channel_reconnects_and_keeps_consuming(self, mock_sleep, pika_worker):
        import pika.exceptions
        w = pika_worker
        conn1, ch1 = MagicMock(), MagicMock()
        ch1.basic_get.side_effect = pika.exceptions.ChannelWrongStateError("closed")
        conn2, ch2 = MagicMock(), MagicMock()

        def basic_get(queue, auto_ack):
            w.globals.keep_running = False
            return None, None, None
        ch2.basic_get.side_effect = basic_get

        with patch.object(w, 'get_pika_channel', side_effect=[(conn1, ch1), (conn2, ch2)]), \
             patch.object(w, 'process_worker_ch'):
            _run_subscribe(w)  # returns normally

        ch2.basic_get.assert_called_once()

    @patch('time.sleep')
    def test_reconnect_gives_up_when_the_broker_stays_down(self, mock_sleep, pika_worker):
        import pika.exceptions
        w = pika_worker
        conn1, ch1 = MagicMock(), MagicMock()
        ch1.basic_get.side_effect = pika.exceptions.StreamLostError("connection lost")

        with patch.object(w, 'get_pika_channel',
                          side_effect=[(conn1, ch1), RuntimeError("Could not connect to RabbitMQ within 60s")]), \
             patch.object(w, 'process_worker_ch'):
            with pytest.raises(w.WorkerException, match="could not reconnect"):
                _run_subscribe(w)

    @patch('time.sleep')
    def test_undecodable_message_is_nacked_without_requeue(self, mock_sleep, pika_worker):
        w = pika_worker
        conn, ch = MagicMock(), MagicMock()
        method = MagicMock(delivery_tag=5)

        def basic_get(queue, auto_ack):
            if ch.basic_get.call_count == 1:
                return method, MagicMock(), b'\xff\xfe not pickle, not utf-8'
            w.globals.keep_running = False
            return None, None, None
        ch.basic_get.side_effect = basic_get

        with patch.object(w, 'get_pika_channel', return_value=(conn, ch)), \
             patch.object(w, 'process_worker_ch'):
            _run_subscribe(w)

        ch.basic_nack.assert_called_once_with(delivery_tag=5, requeue=False)
        w.execute_actor.assert_not_called()


class TestPikaWorkerProcessExit:

    def test_exit_process_waits_a_bounded_time_for_the_worker_thread(self, pika_worker):
        release = threading.Event()
        stuck = threading.Thread(target=release.wait, args=(30,), daemon=False)
        stuck.start()
        try:
            with patch.object(pika_worker.os, '_exit') as mock_exit, \
                 patch.object(pika_worker.logging, 'shutdown'):
                t0 = time.monotonic()
                pika_worker.exit_process(1, grace_s=0.2)
                assert time.monotonic() - t0 < 5
            mock_exit.assert_called_once_with(1)
        finally:
            release.set()
            stuck.join()

    def test_spawner_handshake_has_a_deadline(self, pika_worker):
        w = pika_worker
        conn, ch = MagicMock(), MagicMock()
        ch.basic_get.return_value = (None, None, None)
        clock = iter(range(0, 10000, 100))
        fake_time = MagicMock()
        fake_time.monotonic.side_effect = lambda: next(clock)
        env = {'worker_id': 'w1', 'image': 'img', 'actor_id': 'a1', 'revision': '1'}

        with patch.object(w, 'get_pika_channel', return_value=(conn, ch)), \
             patch.object(w, 'time', fake_time), \
             patch.object(w, 'subscribe') as mock_subscribe, \
             patch.dict(os.environ, env):
            with pytest.raises(w.WorkerException, match="no handshake from the spawner"):
                w.main()

        mock_subscribe.assert_not_called()


class TestK8sWorkerChannelThread:

    def test_worker_ch_failure_with_queue_gone_exits_the_process(self, k8s_worker):
        import pika.exceptions
        kw = k8s_worker
        worker_ch = MagicMock()
        worker_ch.get_one.side_effect = pika.exceptions.StreamLostError("connection lost")
        probe = MagicMock()
        probe.__enter__.return_value = probe
        probe._ch.queue_declare.side_effect = pika.exceptions.ChannelClosedByBroker(404, "NOT_FOUND")

        with patch.object(kw, 'WorkerChannel', return_value=worker_ch) as MockWC, \
             patch.object(kw, 'RabbitConnection', return_value=probe), \
             patch.object(kw, '_thread') as mock_thread, \
             patch.object(kw.os, '_exit', side_effect=SystemExit) as mock_exit, \
             patch('time.sleep'):
            MockWC.get_name.side_effect = lambda worker_id: f'worker_{worker_id}'
            with pytest.raises(SystemExit):
                kw.process_worker_ch('tenant', 'actor1', 'w1')

        probe._ch.queue_declare.assert_called_once_with(queue='worker_w1', passive=True)
        # the only WorkerChannel() is the thread's own initial one -- no blind redeclare
        MockWC.assert_called_once_with(worker_id='w1')
        mock_exit.assert_called_once_with(0)
        mock_thread.interrupt_main.assert_called_once()

    def test_stop_deletes_the_actor_queue_on_the_threads_own_connection(self, k8s_worker):
        kw = k8s_worker
        worker_ch = MagicMock()
        worker_ch.get_one.return_value = ('stop', MagicMock())

        with patch.object(kw, 'WorkerChannel', return_value=worker_ch), \
             patch.object(kw, 'ActorMsgChannel') as MockAMC, \
             patch.object(kw, 'delete_queue') as mock_delete, \
             patch.object(kw, '_thread'), \
             patch.object(kw.os, '_exit', side_effect=SystemExit):
            with pytest.raises(SystemExit):
                kw.process_worker_ch('tenant', 'actor1', 'w1')

        # deleted without being declared first (ActorMsgChannel() declares; see queues.delete_queue)
        mock_delete.assert_called_once_with('actor_msg_actor1')
        MockAMC.assert_not_called()
        worker_ch.delete.assert_called_once()

    def test_closed_actor_channel_raises_instead_of_sys_exit(self, k8s_worker):
        kw = k8s_worker
        actor_ch = MagicMock()
        actor_ch.get_one.side_effect = kw.ChannelClosedException()

        with patch.object(kw, 'ActorMsgChannel', return_value=actor_ch), \
             patch.object(kw, 'process_worker_ch'):
            with pytest.raises(kw.WorkerException, match="Channel closed"):
                kw.subscribe('tenant', 'actor1', 'img', 1, 'w1', 'https://api')


# ---------------------------------------------------------------------------
# 11. worker.py / kubernetes_worker.py -- delivery-limit accounting (review fixups)
#     Messages a worker never ran are republished + acked, not nack(requeue=True),
#     so they do not burn the actor_msg_* quorum queue's x-delivery-limit.
#     Uses the stubbed-import fixtures from section 10.
# ---------------------------------------------------------------------------

class TestWorkerDeliveryLimit:

    def test_release_republishes_without_the_delivery_count_and_acks(self, pika_worker):
        import pika
        ch = MagicMock()
        props = pika.BasicProperties(delivery_mode=2, headers={'x-delivery-count': 2, 'keep': 'me'})

        assert pika_worker.release_unattempted(ch, 'actor_msg_a1', 9, props, b'body') is True

        kwargs = ch.basic_publish.call_args.kwargs
        assert kwargs['routing_key'] == 'actor_msg_a1'
        assert kwargs['body'] == b'body'
        assert kwargs['mandatory'] is True
        assert kwargs['properties'].headers == {'keep': 'me'}
        assert kwargs['properties'].delivery_mode == 2
        assert props.headers == {'x-delivery-count': 2, 'keep': 'me'}  # original untouched
        ch.basic_ack.assert_called_once_with(delivery_tag=9)
        ch.basic_nack.assert_not_called()

    def test_release_falls_back_to_a_counted_requeue(self, pika_worker):
        import pika.exceptions
        ch = MagicMock()
        ch.basic_publish.side_effect = pika.exceptions.UnroutableError([])

        pika_worker.release_unattempted(ch, 'actor_msg_a1', 9, None, b'body')

        ch.basic_nack.assert_called_once_with(delivery_tag=9, requeue=True)
        ch.basic_ack.assert_not_called()

    @patch('time.sleep')
    def test_stale_revision_releases_without_counting(self, mock_sleep, pika_worker):
        w = pika_worker
        conn, ch = MagicMock(), MagicMock()
        body = _actor_msg()
        ch.basic_get.return_value = (MagicMock(delivery_tag=3), MagicMock(headers=None), body)
        w.Actor.from_db.return_value = MagicMock(revision=2, mounts=[])

        with patch.object(w, 'get_pika_channel', return_value=(conn, ch)), \
             patch.object(w, 'process_worker_ch'):
            with pytest.raises(Exception):
                _run_subscribe(w, revision=1)

        ch.confirm_delivery.assert_called_once()
        assert ch.basic_publish.call_args.kwargs['body'] == body
        ch.basic_ack.assert_called_once_with(delivery_tag=3)
        ch.basic_nack.assert_not_called()
        w.execute_actor.assert_not_called()

    @patch('time.sleep')
    def test_docker_start_failure_releases_without_counting(self, mock_sleep, pika_worker):
        w = pika_worker
        conn, ch = MagicMock(), MagicMock()
        body = _actor_msg()

        def basic_get(queue, auto_ack):
            if ch.basic_get.call_count == 1:
                return MagicMock(delivery_tag=4), MagicMock(headers=None), body
            w.globals.keep_running = False
            return None, None, None
        ch.basic_get.side_effect = basic_get
        w.execute_actor.side_effect = w.DockerStartContainerError("docker down")
        w.Actor.from_db.return_value = MagicMock(revision=1, mounts=[])

        with patch.object(w, 'get_pika_channel', return_value=(conn, ch)), \
             patch.object(w, 'process_worker_ch'):
            _run_subscribe(w)  # worker stays up and keeps consuming

        assert ch.basic_publish.call_args.kwargs['body'] == body
        ch.basic_ack.assert_called_once_with(delivery_tag=4)
        ch.basic_nack.assert_not_called()

    def test_k8s_release_republishes_and_acks(self, k8s_worker):
        actor_ch = MagicMock()
        actor_ch.name = 'actor_msg_a1'
        msg_obj = MagicMock(body=b'body')

        k8s_worker.release_unattempted(actor_ch, msg_obj)

        kwargs = actor_ch._ch.basic_publish.call_args.kwargs
        assert kwargs['routing_key'] == 'actor_msg_a1' and kwargs['body'] == b'body'
        msg_obj.ack.assert_called_once()
        msg_obj.nack.assert_not_called()


# ===========================================================================
# Review fixups (2026-11): rabbit transport / DLX / quorum review findings.
# Same mock-only style as the sections above.
# ===========================================================================

# ---------------------------------------------------------------------------
# R1. ExecutionResultsChannel.get honours the channelpy timeout contract
# ---------------------------------------------------------------------------

def _bare_results_channel():
    from channels import ExecutionResultsChannel
    ch = ExecutionResultsChannel.__new__(ExecutionResultsChannel)  # bypass _connect
    ch.queue_name = 'results_a_e'
    ch._channel = MagicMock()
    ch._channel.is_open = True
    ch._connection = MagicMock()
    return ch


class TestResultsChannelGet:

    def test_returns_a_result_that_is_already_there(self):
        import cloudpickle
        ch = _bare_results_channel()
        ch._channel.basic_get.return_value = (MagicMock(), MagicMock(), cloudpickle.dumps({'r': 1}))
        assert ch.get(timeout=1) == {'r': 1}

    def test_waits_then_returns_a_late_result(self):
        import cloudpickle
        ch = _bare_results_channel()
        ch._channel.basic_get.side_effect = [(None, None, None), (None, None, None),
                                             (MagicMock(), MagicMock(), cloudpickle.dumps('late'))]
        assert ch.get(timeout=5) == 'late'
        # waited by servicing the connection (heartbeats), not by a bare sleep
        assert ch._connection.process_data_events.call_count == 2

    def test_empty_queue_raises_timeout_instead_of_returning_none(self):
        from queues import ChannelTimeoutException
        ch = _bare_results_channel()
        ch._channel.basic_get.return_value = (None, None, None)
        with pytest.raises(ChannelTimeoutException):
            ch.get(timeout=0.05)

    def test_closed_channel_raises_channel_closed(self):
        from queues import ChannelClosedException
        ch = _bare_results_channel()
        ch._channel.is_open = False
        with pytest.raises(ChannelClosedException):
            ch.get(timeout=1)

    def test_len_of_an_expired_queue_is_zero(self):
        import pika.exceptions
        ch = _bare_results_channel()
        ch._channel.queue_declare.side_effect = pika.exceptions.ChannelClosedByBroker(404, 'NOT_FOUND')
        from channels import ExecutionResultsChannel
        assert len(ExecutionResultsChannel._QueueHandle(ch)) == 0


class TestResultsChannelSurvivesIdleDrop:
    """The worker opens the results channel when an execution starts and next uses it when
    the actor emits a result; nothing services its heartbeats in between, so on a long
    execution the broker drops it. Live repro (2026-11-02): a 130s execution lost its
    result ('Stream connection lost') and then failed its cleanup with
    ChannelWrongStateError -> execution ERROR, worker shut down."""

    def _with_reconnect(self, ch):
        fresh = MagicMock()
        fresh.is_open = True

        def reconnect():
            ch._channel = fresh
            ch._connection = MagicMock(is_open=True)
        ch._connect = MagicMock(side_effect=reconnect)
        return fresh

    def test_put_on_a_dropped_connection_reconnects_first(self):
        ch = _bare_results_channel()
        ch._channel.is_open = False
        fresh = self._with_reconnect(ch)
        ch.put('result')
        ch._connect.assert_called_once()
        fresh.basic_publish.assert_called_once()

    def test_put_retries_once_when_the_first_write_finds_the_stream_lost(self):
        import pika.exceptions
        ch = _bare_results_channel()
        stale = ch._channel
        stale.basic_publish.side_effect = pika.exceptions.StreamLostError('Connection reset by peer')
        fresh = self._with_reconnect(ch)
        ch.put('result')
        stale.basic_publish.assert_called_once()
        fresh.basic_publish.assert_called_once()

    def test_len_on_a_dropped_channel_reconnects_instead_of_raising(self):
        from channels import ExecutionResultsChannel
        ch = _bare_results_channel()
        ch._channel.is_open = False
        fresh = self._with_reconnect(ch)
        fresh.queue_declare.return_value.method.message_count = 0
        assert len(ExecutionResultsChannel._QueueHandle(ch)) == 0
        ch._connect.assert_called_once()


# ---------------------------------------------------------------------------
# R2. A closed channel (ChannelClosedException) takes the daemons' reconnect path
# ---------------------------------------------------------------------------

class TestChannelClosedIsARabbitError:

    def test_in_rabbit_errors(self):
        from queues import RABBIT_ERRORS, ChannelClosedException
        assert issubclass(ChannelClosedException, RABBIT_ERRORS)

    @patch('spawner.CommandChannel')
    @patch('spawner.time.sleep')
    def test_spawner_reconnects_on_closed_channel(self, mock_sleep, MockCmdCh):
        from queues import ChannelClosedException
        from spawner import Spawner

        spawner = Spawner.__new__(Spawner)
        spawner.host_id = 'host1'
        spawner.queue = 'test_queue'
        old_ch = MagicMock()
        old_ch.get_one.side_effect = ChannelClosedException()
        spawner.cmd_ch = old_ch
        new_ch = MagicMock()
        new_ch.get_one.return_value = ({'actor_id': 'a1'}, MagicMock())
        MockCmdCh.return_value = new_ch
        spawner.process = lambda cmd: (_ for _ in ()).throw(SystemExit("done"))
        spawner.overloaded = lambda: False

        with pytest.raises(SystemExit):
            spawner.run()
        MockCmdCh.assert_called_once_with(name='test_queue')
        old_ch.close.assert_called_once()


# ---------------------------------------------------------------------------
# R3. Spawner opens the spawner->worker channel only after the image pull
# ---------------------------------------------------------------------------

class TestSpawnerChannelOpenedLate:

    @patch('spawner.site', return_value='test_site')
    @patch('spawner.actors_store', new_callable=dict)
    @patch('spawner.Actor')
    @patch('spawner.Worker')
    @patch('spawner.SpawnerWorkerChannel')
    def test_channel_opened_after_pull_and_closed(self, MockSWCh, MockWorker, MockActor,
                                                  mock_as_dict, mock_site):
        import spawner as spawner_mod
        from spawner import Spawner

        events = []
        fake_docker = MagicMock()
        fake_docker.pull_image.side_effect = lambda image: events.append('pull')
        fake_docker.run_worker.return_value = {'id': 'w1', 'status': 'READY'}
        MockSWCh.side_effect = lambda worker_id: events.append('open') or MagicMock()
        mock_as_dict['test_site'] = {'a1': {}}
        MockActor.from_db.return_value = MagicMock(status='READY')

        sp = Spawner.__new__(Spawner)
        sp.container_backend = 'docker'
        with patch.object(spawner_mod, 'docker_utils', fake_docker, create=True), \
                patch.object(spawner_mod, 'DockerError', Exception, create=True):
            sp.start_worker('img', 1, 'dev', 'a1', 'w1', None, 'test_site')

        assert events == ['pull', 'open']
        assert MockSWCh.call_count == 1

    @patch('spawner.site', return_value='test_site')
    @patch('spawner.actors_store', new_callable=dict)
    @patch('spawner.Actor')
    @patch('spawner.Worker')
    @patch('spawner.SpawnerWorkerChannel')
    def test_channel_closed_when_put_fails(self, MockSWCh, MockWorker, MockActor,
                                           mock_as_dict, mock_site):
        import pika.exceptions
        import spawner as spawner_mod
        from spawner import Spawner

        ch = MagicMock()
        ch.put.side_effect = pika.exceptions.StreamLostError('gone')
        MockSWCh.return_value = ch
        fake_docker = MagicMock()
        fake_docker.run_worker.return_value = {'id': 'w1', 'status': 'READY'}
        mock_as_dict['test_site'] = {'a1': {}}
        MockActor.from_db.return_value = MagicMock(status='READY')

        sp = Spawner.__new__(Spawner)
        sp.container_backend = 'docker'
        with patch.object(spawner_mod, 'docker_utils', fake_docker, create=True), \
                patch.object(spawner_mod, 'DockerError', Exception, create=True):
            with pytest.raises(pika.exceptions.StreamLostError):
                sp.start_worker('img', 1, 'dev', 'a1', 'w1', None, 'test_site')
        ch.close.assert_called_once()


# ---------------------------------------------------------------------------
# R4. POST /messages: which publish errors prove the message was NOT queued
# ---------------------------------------------------------------------------

class TestPublishErrorClassification:

    def test_refused_vs_ambiguous(self):
        import pika.exceptions
        from queues import PUBLISH_REFUSED_ERRORS, RABBIT_ERRORS

        # refused => controllers marks the execution ERROR
        for e in (pika.exceptions.UnroutableError([]), pika.exceptions.NackError([]),
                  pika.exceptions.ChannelWrongStateError('closed')):
            assert isinstance(e, PUBLISH_REFUSED_ERRORS)
        # connection lost while awaiting the confirm => ambiguous, execution left SUBMITTED
        for e in (pika.exceptions.StreamLostError('lost'),
                  pika.exceptions.ConnectionClosedByBroker(320, 'shutdown')):
            assert isinstance(e, RABBIT_ERRORS)
            assert not isinstance(e, PUBLISH_REFUSED_ERRORS)


# ---------------------------------------------------------------------------
# R5. do_synch_message: ERROR ends the wait; the executions store is polled ~2x/s
# ---------------------------------------------------------------------------

class TestSynchMessageTerminalStates:

    def _run(self, statuses, get_delay=0.0, results_ch_error=None):
        """Drive do_synch_message with an empty results channel and a store whose
        execution status walks through `statuses` (last one repeats). Returns
        (outcome, number_of_store_reads, results_channel). results_ch_error: the
        results channel cannot be opened at all."""
        import controllers
        from queues import ChannelTimeoutException

        reads = []
        def read_exc(key):
            reads.append(key)
            st = statuses[min(len(reads), len(statuses)) - 1]
            return {'status': st, 'status_message': 'container failed to start'}
        store = MagicMock()
        store.__getitem__.side_effect = read_exc

        def empty_get(timeout=None):
            time.sleep(get_delay)
            raise ChannelTimeoutException()
        results_ch = MagicMock()
        results_ch.get.side_effect = empty_get

        with patch.object(controllers, 'g', MagicMock(db_id='a1')), \
                patch.object(controllers, 'site', return_value='s'), \
                patch.object(controllers, 'executions_store', {'s': store}), \
                patch.object(controllers, 'logs_store', {'s': {'e1': {'logs': 'the logs'}}}), \
                patch.object(controllers, 'ExecutionResultsChannel', return_value=results_ch,
                             side_effect=results_ch_error), \
                patch.object(controllers, 'make_response', side_effect=lambda r: MagicMock(body=r)), \
                patch.object(controllers.Execution, 'from_db',
                             side_effect=lambda d: MagicMock(status=d['status'])):
            res = controllers.MessagesResource.do_synch_message(MagicMock(), 'e1')
        return res, len(reads), results_ch

    def test_error_returns_promptly_with_the_status_message(self):
        from errors import ResourceError
        t0 = time.monotonic()
        with pytest.raises(ResourceError) as ei:
            self._run(['ERROR'])
        assert time.monotonic() - t0 < 5  # not the 90s deadline
        assert ei.value.code == 500
        assert 'container failed to start' in str(ei.value.msg)

    def test_complete_without_result_returns_logs(self):
        res, _, results_ch = self._run(['SUBMITTED', 'COMPLETE'])
        assert res.body == 'the logs'
        results_ch.close.assert_called()

    def test_results_channel_unavailable_falls_back_to_the_store(self):
        # the message is already queued: failing here would make the client retry it
        from queues import RabbitUnavailableError
        res, _, results_ch = self._run(['SUBMITTED', 'COMPLETE'],
                                       results_ch_error=RabbitUnavailableError("broker down"))
        assert res.body == 'the logs'
        results_ch.get.assert_not_called()

    def test_store_polled_at_most_about_twice_a_second(self):
        from codes import SUBMITTED
        # ~1.5s of an empty results channel (0.1s per get) before COMPLETE
        t0 = time.monotonic()
        res, n_reads, _ = self._run([SUBMITTED] * 3 + ['COMPLETE'], get_delay=0.1)
        elapsed = time.monotonic() - t0
        # old loop: one store read per 0.1s pass (~15 here)
        assert n_reads == 4
        assert elapsed < 3


# ---------------------------------------------------------------------------
# R6. Queue declare: a 406 argument/type mismatch is logged loudly and re-raised
# ---------------------------------------------------------------------------

class TestDeclareMismatchIsLoud:

    def test_406_logged_with_doc_pointer_and_reraised(self):
        import pika.exceptions
        import queues

        ch = MagicMock()
        ch.queue_declare.side_effect = pika.exceptions.ChannelClosedByBroker(
            406, "PRECONDITION_FAILED - inequivalent arg 'x-queue-type'")
        with patch.object(queues.logger, 'critical') as crit:
            with pytest.raises(pika.exceptions.ChannelClosedByBroker):
                queues.declare_queue(ch, 'actor_msg_x', arguments={'x-queue-type': 'quorum'})
        msg = crit.call_args.args[0]
        assert msg.startswith('queue type mismatch')
        assert queues.QUEUE_MIGRATION_DOC in msg

    def test_other_errors_are_not_called_a_mismatch(self):
        import pika.exceptions
        import queues

        ch = MagicMock()
        ch.queue_declare.side_effect = pika.exceptions.ChannelClosedByBroker(403, 'ACCESS_REFUSED')
        with patch.object(queues.logger, 'critical') as crit:
            with pytest.raises(pika.exceptions.ChannelClosedByBroker):
                queues.declare_queue(ch, 'actor_msg_x')
        crit.assert_not_called()

    @patch('queues.RabbitConnection')
    def test_taskqueue_closes_its_connection_when_the_declare_fails(self, MockConn):
        import pika.exceptions
        import queues

        conn = MockConn.return_value
        conn._ch.queue_declare.side_effect = pika.exceptions.ChannelClosedByBroker(406, 'x')
        with pytest.raises(pika.exceptions.ChannelClosedByBroker):
            queues.TaskQueue('actor_msg_x')
        conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# R7. Delivery limit is a policy, not a queue argument
# ---------------------------------------------------------------------------

class TestQueueDeclareArgs:

    @pytest.mark.parametrize('queue_type,name,expected', [
        ('classic', 'actor_msg_a1', {}),
        (None, 'actor_msg_a1', {}),
        ('quorum', 'actor_msg_a1', {'x-queue-type': 'quorum'}),   # no x-delivery-limit
        ('quorum', 'command_channel_default', {'x-queue-type': 'quorum'}),
        ('quorum', 'worker_w1', {}),
        ('quorum', 'spawner_worker_w1', {}),
    ])
    def test_args(self, queue_type, name, expected):
        import queues
        fake_conf = MagicMock()
        fake_conf.get.side_effect = lambda k, *a: queue_type if k == 'rabbit_queue_type' else None
        with patch.object(queues, 'conf', fake_conf):
            assert queues.queue_declare_args(name) == expected

# ---------------------------------------------------------------------------
# R8. pika follow-ups (2026-11-09): polling reads, events ack-first, broker 503s,
#     worker-channel thread, best-effort steps after a confirmed publish, and
#     infrastructure failures that must not spend a message's delivery limit.
# ---------------------------------------------------------------------------

class TestRabbitMgmt:

    @patch('rabbit_mgmt.requests')
    def test_one_client_builds_every_url(self, mock_requests):
        import rabbit_mgmt
        mock_requests.get.return_value = _rsp(200, [])
        with patch.object(rabbit_mgmt, 'conf', {'rabbit_dash_host': 'mq', 'rabbit_dash_port': 15999,
                                                 'admin_rabbitmq_user': 'u', 'admin_rabbitmq_pass': 'p'}):
            rabbit_mgmt.list_queues('tacc', ('name', 'messages'))
        args, kwargs = mock_requests.get.call_args
        assert args[0] == 'http://mq:15999/api/queues/abaco_tacc'
        assert kwargs['params'] == {'columns': 'name,messages'}
        assert kwargs['auth'] == ('u', 'p')
        assert kwargs['timeout'] == rabbit_mgmt.TIMEOUT

    @patch('rabbit_mgmt.requests')
    def test_defaults_and_404s(self, mock_requests):
        import rabbit_mgmt
        mock_requests.get.return_value = _rsp(404)
        mock_requests.delete.return_value = _rsp(404)
        with patch.object(rabbit_mgmt, 'conf', {}):
            assert rabbit_mgmt.get_queue('tacc', 'actor_msg_x') is None
            assert rabbit_mgmt.delete_queue('tacc', 'worker_w1') is False
        assert mock_requests.get.call_args.args[0] == 'http://rabbit:15672/api/queues/abaco_tacc/actor_msg_x'
        assert mock_requests.delete.call_args.kwargs['params'] is None

    @patch('rabbit_mgmt.requests')
    def test_other_failures_raise(self, mock_requests):
        import requests
        import rabbit_mgmt
        mock_requests.get.return_value = _rsp(500)
        with patch.object(rabbit_mgmt, 'conf', {}):
            with pytest.raises(requests.HTTPError):
                rabbit_mgmt.get_queue('tacc', 'q')


def _taskqueue(name='command_channel_default'):
    from queues import BinaryTaskQueue
    q = BinaryTaskQueue.__new__(BinaryTaskQueue)
    q._ch = MagicMock()
    q._ch.is_open = True
    q.conn = MagicMock()
    q.name = name
    return q


class TestTaskQueueGetOnePolls:

    def test_polls_with_basic_get_and_services_the_connection_between_polls(self):
        import cloudpickle
        q = _taskqueue()
        method = MagicMock(delivery_tag=7)
        q._ch.basic_get.side_effect = [(None, None, None), (method, MagicMock(), cloudpickle.dumps({'a': 1}))]

        payload, msg = q.get_one()

        assert payload == {'a': 1}
        q._ch.basic_get.assert_called_with('command_channel_default', auto_ack=False)
        q.conn._conn.process_data_events.assert_called_once_with(time_limit=q.POLL_INTERVAL_S)
        q._ch.consume.assert_not_called()  # nothing is prefetched behind the message being worked on
        msg.ack()
        q._ch.basic_ack.assert_called_once_with(7)

    def test_undecodable_message_is_rejected_without_requeue(self):
        import cloudpickle
        q = _taskqueue()
        q._ch.basic_get.side_effect = [(MagicMock(delivery_tag=1), MagicMock(), b'not a pickle'),
                                       (MagicMock(delivery_tag=2), MagicMock(), cloudpickle.dumps('ok'))]

        payload, _ = q.get_one()

        assert payload == 'ok'
        q._ch.basic_nack.assert_called_once_with(1, requeue=False)

    def test_timeout_and_closed_channel(self):
        from queues import ChannelClosedException, ChannelTimeoutException
        q = _taskqueue()
        q._ch.basic_get.return_value = (None, None, None)
        with pytest.raises(ChannelTimeoutException):
            q.get_one(timeout=0.05)
        q._ch.is_open = False
        with pytest.raises(ChannelClosedException):
            q.get_one()


class TestEventsAckAfterProcessing:

    def test_event_is_acked_after_it_is_processed(self):
        # ack after processing: an event in flight when the events container is killed is redelivered
        import events

        class Stop(Exception):
            pass
        msg_obj = MagicMock()
        ch = MagicMock()
        ch.get_one.side_effect = [({'tenant_id': 't'}, msg_obj), Stop()]
        acked_first = []
        with patch.object(events, 'process_event_msg',
                          side_effect=lambda m: acked_first.append(msg_obj.ack.called)):
            with pytest.raises(Stop):
                events.run(ch)
        assert acked_first == [False]
        msg_obj.ack.assert_called_once()

    def test_a_failing_event_is_still_acked_once(self):
        import events

        class Stop(Exception):
            pass
        msg_obj = MagicMock()
        ch = MagicMock()
        ch.get_one.side_effect = [({'tenant_id': 't'}, msg_obj), Stop()]
        with patch.object(events, 'process_event_msg', side_effect=KeyError('no actor')):
            with pytest.raises(Stop):
                events.run(ch)
        msg_obj.ack.assert_called_once()


class TestBrokerErrorsAre503:

    @pytest.mark.parametrize('make_exc', [
        lambda: __import__('pika').exceptions.StreamLostError('lost'),
        lambda: __import__('pika').exceptions.ChannelClosedByBroker(404, 'NOT_FOUND'),
        lambda: __import__('pika').exceptions.ConnectionClosedByBroker(320, 'CONNECTION_FORCED'),
        lambda: __import__('queues').ChannelClosedException(),
    ])
    def test_broker_failures_mid_request_are_503(self, make_exc):
        import api_errors
        with patch.object(api_errors, 'tapis_handle_error', side_effect=lambda e: e):
            out = api_errors.handle_error(make_exc())
        assert out.code == 503

    def test_real_bugs_stay_500(self):
        import api_errors
        err = ValueError('bug')
        with patch.object(api_errors, 'tapis_handle_error', side_effect=lambda e: e):
            assert api_errors.handle_error(err) is err


class TestEnsureOneWorkerCleanup:

    def _actor(self):
        return MagicMock(db_id='a1', tenant='t', queue='default', image='img', revision=1)

    def test_failed_command_removes_the_requested_worker(self):
        import models
        from queues import RabbitUnavailableError
        with patch.object(models, 'site', return_value='s'), \
             patch.object(models.Worker, 'ensure_one_worker', return_value='w9'), \
             patch.object(models.Worker, 'delete_worker') as mock_delete, \
             patch.object(models, 'CommandChannel', side_effect=RabbitUnavailableError('down')):
            with pytest.raises(RabbitUnavailableError):
                models.Actor.ensure_one_worker(self._actor())
        mock_delete.assert_called_once_with('a1', 'w9', 's')

    def test_failed_publish_closes_the_channel_and_removes_the_worker(self):
        import pika.exceptions
        import models
        ch = MagicMock()
        ch.put_cmd.side_effect = pika.exceptions.UnroutableError([])
        with patch.object(models, 'site', return_value='s'), \
             patch.object(models.Worker, 'ensure_one_worker', return_value='w9'), \
             patch.object(models.Worker, 'delete_worker') as mock_delete, \
             patch.object(models, 'CommandChannel', return_value=ch):
            with pytest.raises(pika.exceptions.UnroutableError):
                models.Actor.ensure_one_worker(self._actor())
        ch.close.assert_called_once()
        mock_delete.assert_called_once_with('a1', 'w9', 's')


class TestShutdownWorkersBrokerDown:

    def test_stops_messaging_after_the_broker_is_unreachable(self, pika_worker):
        w = pika_worker
        w.Worker.get_workers.return_value = [{'id': 'w1'}, {'id': 'w2'}, {'id': 'w3'}]
        with patch.object(w, 'get_pika_channel', side_effect=w.RabbitUnavailableError('down')) as gpc:
            w.shutdown_workers('a1')
        assert gpc.call_count == 1  # one connect deadline, not one per worker
        marked = [c.args for c in w.Worker.update_worker_status.call_args_list]
        assert marked == [('a1', 'w1', w.SHUTDOWN_REQUESTED), ('a1', 'w2', w.SHUTDOWN_REQUESTED),
                          ('a1', 'w3', w.SHUTDOWN_REQUESTED)]

    def test_stop_waits_in_the_queue_for_a_worker_still_starting(self, pika_worker):
        # the worker is READY in Mongo before its process declares worker_<id>; the stop must not be dropped
        w = pika_worker
        conn, ch = MagicMock(), MagicMock()
        with patch.object(w, 'get_pika_channel', return_value=(conn, ch)) as gpc:
            assert w.shutdown_worker('a1', 'w1', delete_actor_ch=False) is True
        gpc.assert_called_once_with('worker_w1')  # declares; declare=False made the publish unroutable
        assert ch.basic_publish.call_args.kwargs['mandatory'] is True
        conn.close.assert_called_once()

    def test_a_missing_worker_queue_does_not_stop_the_others(self, pika_worker):
        import pika.exceptions
        w = pika_worker
        w.Worker.get_workers.return_value = [{'id': 'w1'}, {'id': 'w2'}]
        conn, ch = MagicMock(), MagicMock()
        ch.basic_publish.side_effect = pika.exceptions.UnroutableError([])
        with patch.object(w, 'get_pika_channel', return_value=(conn, ch)) as gpc:
            w.shutdown_workers('a1')
        assert gpc.call_count == 2
        assert conn.close.call_count == 2


class TestWorkerChannelThread:

    def _run(self, w, channels):
        with patch.object(w, 'get_pika_channel', side_effect=channels) as gpc, \
             patch.object(w, '_thread'), \
             patch.object(w.os, '_exit', side_effect=SystemExit) as mock_exit, \
             patch.object(w.time, 'sleep'):
            with pytest.raises(SystemExit):
                w.process_worker_ch('tenant', 'w1', 'actor1', 'actor_msg_actor1')
        return gpc, mock_exit

    def test_ack_failure_reconnects_instead_of_killing_the_thread(self, pika_worker):
        import cloudpickle
        import pika.exceptions
        w = pika_worker
        conn1, ch1 = MagicMock(), MagicMock()
        ch1.basic_get.return_value = (MagicMock(delivery_tag=1), None, cloudpickle.dumps('status'))
        ch1.basic_ack.side_effect = pika.exceptions.StreamLostError('lost')
        conn2, ch2 = MagicMock(), MagicMock()
        ch2.basic_get.return_value = (MagicMock(delivery_tag=1), None, cloudpickle.dumps('stop-no-delete'))

        gpc, mock_exit = self._run(w, [(conn1, ch1), (conn2, ch2)])

        assert gpc.call_args_list[1].kwargs == {'declare': False}
        ch2.queue_declare.assert_called_once_with(queue='worker_w1', passive=True)
        ch2.basic_ack.assert_called_once_with(delivery_tag=1)
        w.Worker.delete_worker.assert_called_once_with('actor1', 'w1')  # the stop was acted on
        mock_exit.assert_called_once_with(0)

    def test_only_a_404_means_reaped(self, pika_worker):
        import pika.exceptions
        w = pika_worker
        conn, ch = MagicMock(), MagicMock()
        ch.queue_declare.side_effect = [pika.exceptions.ChannelClosedByBroker(541, 'INTERNAL_ERROR'), None]
        with patch.object(w, 'get_pika_channel',
                          side_effect=[w.RabbitUnavailableError('down'), (conn, ch), (conn, ch)]), \
             patch.object(w.time, 'sleep'):
            assert w._open_worker_channel('worker_w1', 'w1') == (conn, ch)

        ch.queue_declare.side_effect = pika.exceptions.ChannelClosedByBroker(404, 'NOT_FOUND')
        with patch.object(w, 'get_pika_channel', return_value=(conn, ch)):
            assert w._open_worker_channel('worker_w1', 'w1') is None

    def test_undecodable_control_message_is_dropped_not_redelivered(self, pika_worker):
        import cloudpickle
        w = pika_worker
        conn, ch = MagicMock(), MagicMock()
        ch.basic_get.side_effect = [(MagicMock(delivery_tag=1), None, b'\xff\xfe junk'),
                                    (MagicMock(delivery_tag=2), None, cloudpickle.dumps('stop-no-delete'))]

        gpc, _ = self._run(w, [(conn, ch)])

        assert gpc.call_count == 1  # no reconnect
        assert [c.kwargs for c in ch.basic_ack.call_args_list] == [{'delivery_tag': 1}, {'delivery_tag': 2}]

    def test_k8s_reopen_retries_a_broker_error(self, k8s_worker):
        kw = k8s_worker
        probe = MagicMock()
        probe.__enter__.return_value = probe
        with patch.object(kw, 'RabbitConnection', side_effect=[RuntimeError('down'), probe]), \
             patch.object(kw, 'WorkerChannel') as MockWC, \
             patch.object(kw, '_thread'), \
             patch.object(kw.os, '_exit') as mock_exit, \
             patch('time.sleep'):
            MockWC.get_name.side_effect = lambda worker_id: f'worker_{worker_id}'
            assert kw._reopen_worker_ch('w1') is MockWC.return_value
        mock_exit.assert_not_called()


def _one_message(w, ch, tag):
    body = _actor_msg()

    def basic_get(queue, auto_ack):
        if ch.basic_get.call_count == 1:
            return MagicMock(delivery_tag=tag), MagicMock(headers=None), body
        w.globals.keep_running = False
        return None, None, None
    ch.basic_get.side_effect = basic_get
    return body


class TestRedeliveryAlreadyHandled:
    """A broker restart redelivers messages whose executions are still running on their original worker.
    Running the copy executed the actor twice; skipping it on a stale BUSY record lost the execution."""

    def _check(self, w, execution, records, answers=True, wait_s=0.5):
        """records: successive worker-record reads for the owner (last one repeats)."""
        ex_store, wk_store = MagicMock(), MagicMock()
        if execution is None:
            ex_store.__getitem__.side_effect = KeyError('gone')
        else:
            ex_store.__getitem__.return_value = execution
        reads = []
        def get(key, default=None):
            reads.append(key)
            return records[min(len(reads), len(records)) - 1] if records else default
        wk_store.get.side_effect = get
        ping, pump = MagicMock(return_value=answers), MagicMock()
        with patch.object(w, 'executions_store', {'s': ex_store}), \
             patch.object(w, 'workers_store', {'s': wk_store}), \
             patch.object(w, 'site', return_value='s'):
            out = w.redelivery_already_handled('a1', 'e1', 'w2', ping, pump, wait_s=wait_s)
        return out, ping, pump

    @pytest.mark.parametrize('module', ['pika_worker', 'k8s_worker'])
    def test_rules(self, module, request):
        w = request.getfixturevalue(module)
        busy_t0 = {'status': 'BUSY', 'last_health_check_time': 't0'}
        busy_t1 = {'status': 'BUSY', 'last_health_check_time': 't1'}
        running = {'status': 'RUNNING', 'worker_id': 'w1'}
        assert 'already COMPLETE' in self._check(w, {'status': 'COMPLETE'}, [])[0]
        assert 'already ERROR' in self._check(w, {'status': 'ERROR'}, [])[0]
        # the owner answered the ping (its health time moved): it is alive and running it
        out, ping, pump = self._check(w, running, [busy_t0, busy_t0, busy_t1])
        assert 'which answered a ping' in out
        ping.assert_called_once_with('w1')
        pump.assert_called_with(1)
        # a BUSY record alone is not proof: an OOM-killed owner never answers, so the message runs again
        assert self._check(w, running, [busy_t0])[0] is None
        # owner's control queue is gone (ping unroutable), or record gone / not busy: run it
        assert self._check(w, running, [busy_t0], answers=False)[0] is None
        assert self._check(w, running, [])[0] is None
        assert self._check(w, running, [{'status': 'SHUTDOWN_REQUESTED'}])[0] is None
        # not started yet (SUBMITTED with an owner: e.g. a counted nack before the run): run it, no ping
        out, ping, _ = self._check(w, {'status': 'SUBMITTED', 'worker_id': 'w1'}, [busy_t0])
        assert out is None
        ping.assert_not_called()
        assert self._check(w, None, [])[0] is None

    def test_lookup_failure_runs_the_message(self, pika_worker):
        w = pika_worker
        ex_store = MagicMock()
        ex_store.__getitem__.side_effect = RuntimeError('mongo down')
        with patch.object(w, 'executions_store', {'s': ex_store}), patch.object(w, 'site', return_value='s'):
            assert w.redelivery_already_handled('a1', 'e1', 'w2', MagicMock(), MagicMock()) is None

    def _subscribe_one(self, w, redelivered, handled):
        ch = MagicMock()

        def basic_get(queue, auto_ack):
            if ch.basic_get.call_count == 1:
                return MagicMock(delivery_tag=31, redelivered=redelivered), MagicMock(headers=None), _actor_msg()
            w.globals.keep_running = False
            return None, None, None
        ch.basic_get.side_effect = basic_get
        w.Actor.from_db.return_value = MagicMock(revision=1, mounts=[])
        w.execute_actor.return_value = ({}, 'logs', 'final', 0, 'start')
        with patch.object(w, 'get_pika_channel', return_value=(MagicMock(), ch)), \
             patch.object(w, 'process_worker_ch'), \
             patch.object(w, 'redelivery_already_handled', return_value=handled) as check, \
             patch('time.sleep'):
            _run_subscribe(w)
        return ch, check

    def test_handled_redelivery_is_acked_and_not_run(self, pika_worker):
        w = pika_worker
        ch, check = self._subscribe_one(w, True, 'execution e1 is already COMPLETE')
        assert check.call_args.args[:3] == ('actor1', 'e1', 'w1')
        ch.basic_ack.assert_called_once_with(delivery_tag=31)
        w.execute_actor.assert_not_called()
        assert 'BUSY' not in [c.args[2] for c in w.Worker.update_worker_status.call_args_list]

    def test_first_delivery_is_not_checked(self, pika_worker):
        w = pika_worker
        ch, check = self._subscribe_one(w, False, 'would skip')
        check.assert_not_called()
        w.execute_actor.assert_called_once()

    def test_ping_publishes_status_and_reports_unroutable(self, pika_worker):
        import cloudpickle
        import pika.exceptions
        ch = MagicMock()
        assert pika_worker._ping_worker(ch, 'w1') is True
        kwargs = ch.basic_publish.call_args.kwargs
        assert kwargs['routing_key'] == 'worker_w1' and kwargs['mandatory'] is True
        assert cloudpickle.loads(kwargs['body']) == 'status'
        ch.basic_publish.side_effect = pika.exceptions.UnroutableError([])
        assert pika_worker._ping_worker(ch, 'w1') is False

    def test_k8s_message_carries_the_redelivered_flag(self):
        from queues import Message
        assert Message(MagicMock(), MagicMock(delivery_tag=1, redelivered=True), b'x').redelivered is True
        assert Message(MagicMock(), MagicMock(delivery_tag=1, redelivered=False), b'x').redelivered is False



class TestOutboxCopyIsPrivate:

    def test_nonce_is_never_stored(self):
        import models
        with patch.object(models, 'conf', {'message_outbox': True}), \
             patch.object(models, 'site', return_value='s'), \
             patch.object(models, 'executions_store', {'s': MagicMock()}) as stores:
            assert models.Execution.store_queued_message('a1', 'e1', 'hi', {'x-nonce': 'SECRET', 'k': 'v'})
        (key, value), = [c.args for c in stores['s'].__setitem__.call_args_list]
        assert value == {'message': 'hi', 'd': {'k': 'v'}}

    def test_copy_dropped_once_the_execution_leaves_submitted(self):
        import models
        store = MagicMock()
        with patch.object(models, 'conf', {'message_outbox': True}), \
             patch.object(models, 'site', return_value='s'), \
             patch.object(models, 'executions_store', {'s': store}):
            models.Execution.update_status('a1', 'e1', 'RUNNING')
        flt, update = store.full_update.call_args.args
        assert flt['_id'] == 'a1_e1' and update == {'$unset': {'queued_message': ''}}

    def test_executions_search_projects_the_copy_out(self):
        # structural: Search.search needs a live store to run end to end (covered by the live check)
        import inspect
        import models
        assert "{'$project': {'queued_message': 0}}" in inspect.getsource(models.Search.search)


class TestNonceNeverGetsAdminScope:

    def test_nonce_request_is_not_admin_scoped(self):
        import controllers
        req = MagicMock(headers={'X-Actors-Admin': 'true'}, args={'x-nonce': 'N', 'x-actors-admin': 'true'})
        with patch.object(controllers, 'request', req), patch.object(controllers, 'g', MagicMock(admin=True)):
            assert controllers.admin_search_scope() is False
        req = MagicMock(headers={'X-Actors-Admin': 'true'}, args={})
        with patch.object(controllers, 'request', req), patch.object(controllers, 'g', MagicMock(admin=True)):
            assert controllers.admin_search_scope() is True

class TestHeldMessageAndPreRunChecks:

    def _subscribe(self, w, ch, conn, before_run=None):
        body = _one_message(w, ch, 41)
        w.Actor.from_db.return_value = MagicMock(revision=1, mounts=[])
        w.execute_actor.return_value = ({}, 'logs', 'final', 0, 'start')
        if before_run:
            w.Execution.add_worker_id.side_effect = lambda *a: before_run()
        with patch.object(w, 'get_pika_channel', return_value=(conn, ch)), \
             patch.object(w, 'process_worker_ch'), \
             patch('time.sleep'):
            _run_subscribe(w)
        return body

    def test_dead_connection_before_the_run_does_not_run(self, pika_worker):
        w = pika_worker
        conn, ch = MagicMock(), MagicMock()
        conn.process_data_events.side_effect = __import__('pika').exceptions.StreamLostError('dropped')
        self._subscribe(w, ch, conn)
        w.execute_actor.assert_not_called()   # the broker already requeued it
        ch.basic_ack.assert_not_called()

    def test_stop_during_preparation_gives_the_message_back(self, pika_worker):
        w = pika_worker
        conn, ch = MagicMock(), MagicMock()
        def stop_arrives():
            assert w.HELD.is_set()            # the control thread would be waiting on this
            w.globals.keep_running = False
        body = self._subscribe(w, ch, conn, before_run=stop_arrives)
        w.execute_actor.assert_not_called()
        assert ch.basic_publish.call_args.kwargs['body'] == body   # released, not counted
        ch.basic_ack.assert_called_once_with(delivery_tag=41)
        assert not w.HELD.is_set()

    def test_held_is_cleared_before_the_run(self, pika_worker):
        w = pika_worker
        conn, ch = MagicMock(), MagicMock()
        seen = {}
        def execute(*args):
            seen['held'] = w.HELD.is_set()
            return {}, 'logs', 'final', 0, 'start'
        w.execute_actor.side_effect = execute
        self._subscribe(w, ch, conn)
        assert seen == {'held': False}

    def test_control_thread_waits_for_a_held_message_but_not_forever(self, pika_worker):
        w = pika_worker
        w.HELD.set()
        t0 = time.monotonic()
        w._wait_for_held_message(grace_s=0.3)
        assert 0.25 < time.monotonic() - t0 < 2
        threading.Timer(0.2, w.HELD.clear).start()
        t0 = time.monotonic()
        w._wait_for_held_message(grace_s=5)
        assert time.monotonic() - t0 < 2

    def test_k8s_nfs_lookup_failure_releases(self, k8s_worker):
        kw = k8s_worker
        actor_ch = MagicMock()
        actor_ch.name = 'actor_msg_actor1'
        msg_obj = MagicMock(body=b'body', redelivered=False)
        actor_ch.get_one.return_value = ({'message': 'hi', '_abaco_execution_id': 'e1',
                                          '_abaco_Content_Type': 'str'}, msg_obj)
        kw.Actor.from_db.return_value = MagicMock(revision=1, mounts=[])
        kw.k8.read_namespaced_service.side_effect = RuntimeError('k8s api 503')
        with patch.object(kw, 'ActorMsgChannel', return_value=actor_ch), \
             patch.object(kw, 'process_worker_ch'):
            with pytest.raises(RuntimeError):
                kw.subscribe('tenant', 'actor1', 'img', 1, 'w1', 'https://api')
        assert actor_ch._ch.basic_publish.call_args.kwargs['body'] == b'body'
        msg_obj.ack.assert_called_once()
        msg_obj.nack.assert_not_called()


class TestInboxDeletesNeverDeclare:
    """With quorum inboxes under the at-least-once dead-letter policy, declare+delete races during an actor
    DELETE killed RabbitMQ 3.12's dead-letter supervisor; afterwards no new quorum inbox could be declared."""

    def test_delete_queue_only_deletes(self):
        import queues
        conn = MagicMock()
        conn.__enter__.return_value = conn
        with patch.object(queues, 'RabbitConnection', return_value=conn):
            queues.delete_queue('actor_msg_a1')
        conn._ch.queue_delete.assert_called_once_with(queue='actor_msg_a1')
        conn._ch.queue_declare.assert_not_called()

    def test_worker_stop_deletes_the_inbox_without_declaring(self, pika_worker):
        import cloudpickle
        w = pika_worker
        conn, ch = MagicMock(), MagicMock()
        ch.basic_get.return_value = (MagicMock(delivery_tag=1), None, cloudpickle.dumps('stop'))
        with patch.object(w, 'get_pika_channel', return_value=(conn, ch)) as gpc, \
             patch.object(w, 'delete_queue') as mock_delete, \
             patch.object(w, '_thread'), \
             patch.object(w.os, '_exit', side_effect=SystemExit), \
             patch.object(w.time, 'sleep'):
            with pytest.raises(SystemExit):
                w.process_worker_ch('tenant', 'w1', 'actor1', 'actor_msg_actor1')
        mock_delete.assert_called_once_with('actor_msg_actor1')
        assert [c.args[0] for c in gpc.call_args_list] == ['worker_w1']  # only its own control queue

    def test_quorum_policy_is_not_at_least_once(self):
        # at-least-once took down rabbit_fifo_dlx_sup on 3.12.14 whenever an inbox was deleted while being
        # created (reproduced 2026-11-12); the supervisor never came back without a broker restart
        import inspect
        import stores
        src = inspect.getsource(stores.rabbit_initialization)
        assert '"dead-letter-strategy"' not in src and '"delivery-limit": delivery_limit' in src

    def test_rabbit_init_passes_the_management_port(self):
        import inspect
        import stores
        assert "-P {rabbit_dash_port}" in inspect.getsource(stores.rabbit_initialization)

class TestInfraFailuresAreNotCounted:
    """A Mongo or Tokens outage before the run gives the message back uncounted: on a quorum inbox every
    dying worker used to spend one of its deliveries, so an outage could dead-letter healthy messages."""

    def _subscribe(self, w, ch, exc_type):
        with patch.object(w, 'get_pika_channel', return_value=(MagicMock(), ch)), \
             patch.object(w, 'process_worker_ch'), \
             patch('time.sleep'):
            with pytest.raises(exc_type):
                _run_subscribe(w)

    def _assert_released(self, ch, body, tag):
        assert ch.basic_publish.call_args.kwargs['body'] == body
        ch.basic_ack.assert_called_once_with(delivery_tag=tag)
        ch.basic_nack.assert_not_called()

    def test_token_failure(self, pika_worker):
        w = pika_worker
        ch = MagicMock()
        body = _one_message(w, ch, 21)
        w._conf['global_tenant_object'] = {'generate_clients': True}
        w.Actor.from_db.return_value = MagicMock(revision=1, mounts=[])
        with patch.object(w, 'get_execution_token', side_effect=w.WorkerException('tokens down')):
            self._subscribe(w, ch, w.WorkerException)
        self._assert_released(ch, body, 21)
        w.execute_actor.assert_not_called()

    def test_actor_reload_failure(self, pika_worker):
        from pymongo.errors import ServerSelectionTimeoutError
        w = pika_worker
        ch = MagicMock()
        body = _one_message(w, ch, 22)
        w.Actor.from_db.side_effect = ServerSelectionTimeoutError('mongo down')
        self._subscribe(w, ch, ServerSelectionTimeoutError)
        self._assert_released(ch, body, 22)

    def test_mongo_outage_on_the_execution_record(self, pika_worker):
        from pymongo.errors import AutoReconnect
        w = pika_worker
        ch = MagicMock()
        body = _one_message(w, ch, 23)
        w.Actor.from_db.return_value = MagicMock(revision=1, mounts=[])
        w.Execution.add_worker_id.side_effect = AutoReconnect('mongo blip')
        self._subscribe(w, ch, AutoReconnect)
        self._assert_released(ch, body, 23)

    def test_missing_execution_record_is_still_counted(self, pika_worker):
        w = pika_worker
        ch = MagicMock()
        _one_message(w, ch, 24)
        w.Actor.from_db.return_value = MagicMock(revision=1, mounts=[])
        w.Execution.add_worker_id.side_effect = Exception('Execution e1 not found.')
        self._subscribe(w, ch, Exception)
        ch.basic_nack.assert_called_once_with(delivery_tag=24, requeue=True)
        ch.basic_publish.assert_not_called()

    def test_k8s_token_failure(self, k8s_worker):
        kw = k8s_worker
        actor_ch = MagicMock()
        actor_ch.name = 'actor_msg_actor1'
        msg_obj = MagicMock(body=b'body')
        actor_ch.get_one.return_value = ({'message': 'hi', '_abaco_execution_id': 'e1',
                                          '_abaco_Content_Type': 'str'}, msg_obj)
        kw._conf['global_tenant_object'] = {'generate_clients': True}
        kw.Actor.from_db.return_value = MagicMock(revision=1, mounts=[])
        with patch.object(kw, 'ActorMsgChannel', return_value=actor_ch), \
             patch.object(kw, 'process_worker_ch'), \
             patch.object(kw, 'get_execution_token', side_effect=kw.WorkerException('tokens down')), \
             patch.object(kw, 'Path'), patch.object(kw, 'client'):
            with pytest.raises(kw.WorkerException):
                kw.subscribe('tenant', 'actor1', 'img', 1, 'w1', 'https://api')
        assert actor_ch._ch.basic_publish.call_args.kwargs['body'] == b'body'
        msg_obj.ack.assert_called_once()
        msg_obj.nack.assert_not_called()


# ===========================================================================
# REVIEW FIXUPS (2026-11) -- tests for the review findings fixed in the
# fixups to the RabbitMQ transport, quorum and dead-letter commits. Mock-only, like the
# sections above.
# ===========================================================================

# ---------------------------------------------------------------------------
# R1. store.py -- mongo_tls modes, no password in logs, aggregate kwargs
# ---------------------------------------------------------------------------

class TestMongoTlsMode:

    def test_trunk_true_still_means_client_cert_verify(self):
        import store
        assert store.mongo_tls_mode(True) == store.MONGO_TLS_VERIFY
        assert store.mongo_tls_mode('true') == store.MONGO_TLS_VERIFY
        assert store.mongo_tls_mode('verify') == store.MONGO_TLS_VERIFY

    def test_off_values(self):
        import store
        for v in (False, None, '', 'false'):
            assert store.mongo_tls_mode(v) == store.MONGO_TLS_OFF

    def test_plain_tls_is_explicit_opt_in(self):
        import store
        assert store.mongo_tls_mode('tls-only') == store.MONGO_TLS_ONLY
        assert store.mongo_tls_mode('no-client-cert') == store.MONGO_TLS_ONLY

    def test_unknown_string_fails_loudly(self):
        import store
        with pytest.raises(ValueError):
            store.mongo_tls_mode('yes-please')


class TestMongoStoreClient:

    def _conf(self, **values):
        c = MagicMock()
        c.get.side_effect = lambda k, d=None: values.get(k, d)
        for k, v in values.items():
            setattr(c, k, v)
        return c

    def test_password_never_logged(self):
        import store
        with patch.object(store, 'conf', self._conf(mongo_tls=False)), \
             patch.object(store, 'MongoClient') as MockClient, \
             patch.object(store, 'logger') as mock_logger, \
             patch.dict(store._MONGO_CLIENTS, clear=True):
            store.MongoStore('mongo', 27017, user='abaco', password='s3cr3t-pw', auth_db='admin')
            MockClient.assert_called_once()
            logged = ' '.join(str(c) for c in mock_logger.mock_calls)
            assert 's3cr3t-pw' not in logged
            assert 'mongo:27017' in logged

    def test_true_uses_client_cert_and_ca(self):
        import store
        conf = self._conf(mongo_tls=True, mongo_tls_certs_path='/host/certs:/certs',
                          mongo_tls_ca_cert_name='ca.pem', mongo_tls_client_cert_name='client.pem')
        with patch.object(store, 'conf', conf), \
             patch.object(store, 'MongoClient') as MockClient, \
             patch.dict(store._MONGO_CLIENTS, clear=True):
            store.MongoStore('mongo', 27017)
            kwargs = MockClient.call_args.kwargs
            assert kwargs['tls'] is True
            assert kwargs['tlsCAFile'] == '/certs/ca.pem'
            assert kwargs['tlsCertificateKeyFile'] == '/certs/client.pem'

    def test_tls_only_passes_ca_without_client_cert(self):
        import store
        conf = self._conf(mongo_tls='tls-only', mongo_tls_certs_path='/host/certs:/certs',
                          mongo_tls_ca_cert_name='ca.pem')
        with patch.object(store, 'conf', conf), \
             patch.object(store, 'MongoClient') as MockClient, \
             patch.dict(store._MONGO_CLIENTS, clear=True):
            store.MongoStore('mongo', 27017)
            kwargs = MockClient.call_args.kwargs
            assert kwargs['tls'] is True
            assert kwargs['tlsCAFile'] == '/certs/ca.pem'
            assert 'tlsCertificateKeyFile' not in kwargs

    def test_aggregate_passes_options_as_kwargs(self):
        import store
        s = store.MongoStore.__new__(store.MongoStore)
        s._db = MagicMock()
        s.aggregate([{'$match': {}}], {'collation': {'locale': 'en'}})
        args, kwargs = s._db.aggregate.call_args
        assert args == ([{'$match': {}}],)  # nothing positional lands in `session`
        assert kwargs['collation'] == {'locale': 'en'}
        assert kwargs['allowDiskUse'] is True
        assert kwargs['maxTimeMS'] == store.MONGO_MAX_TIME_MS


# ---------------------------------------------------------------------------
# R2. Search -- actor_id prefixed exactly once, element-wise for .in, 400s for
#     client input errors, stable paging; list endpoints' search switch
# ---------------------------------------------------------------------------

class TestSearchActorIdAndErrors:

    def _parse(self, args, tenant='dev'):
        import models
        with patch('models.conf') as mock_conf:
            mock_conf.web_case = 'snake'
            return models.Search(dict(args), 'executions', tenant, 'user1').arg_parser()

    def _matches(self, query):
        return [stage['$match'] for stage in query]

    def test_per_actor_endpoint_dbid_is_not_prefixed_twice(self):
        # controllers pass g.db_id ("<tenant>_<id>") for the per-actor endpoints
        _, query, *_ = self._parse({'actor_id': 'dev_abc123', 'status': 'COMPLETE'})
        assert {'actor_id': 'dev_abc123'} in self._matches(query)

    def test_display_id_is_prefixed(self):
        _, query, *_ = self._parse({'actor_id': 'abc123'})
        assert self._matches(query) == [{'actor_id': 'dev_abc123'}]

    def test_in_list_is_mapped_element_wise_after_parsing(self):
        _, query, *_ = self._parse({'actor_id.in': '["a1", "dev_b2"]'})
        assert self._matches(query) == [{'actor_id': {'$in': ['dev_a1', 'dev_b2']}}]

    def test_like_is_left_alone(self):
        from models import parse_actor_id_filter_value
        assert parse_actor_id_filter_value('dev', 'actor_id.like', 'abc') == 'abc'

    def test_numeric_looking_id_stays_a_string(self):
        from models import parse_actor_id_filter_value
        assert parse_actor_id_filter_value('dev', 'actor_id', '123e4') == 'dev_123e4'

    @pytest.mark.parametrize('args', [
        {'sort_by': 'secret_field'},
        {'order_by': 'sideways'},
        {'limit': '0'},
        {'limit': 'ten'},
        {'skip': '-1'},
        {'start_time.between': 'nocomma'},
    ])
    def test_client_input_errors_are_400(self, args):
        from errors import ResourceError
        with pytest.raises(ResourceError) as ei:
            self._parse(args)
        assert ei.value.code == 400

    def test_sorts_have_an_id_tiebreaker(self):
        import models
        store = MagicMock()
        store.aggregate.return_value = []
        s = models.Search({'sort_by': 'start_time', 'order_by': 'asc'}, 'executions', 'dev', 'user1')
        with patch('models.conf') as mock_conf, \
             patch.object(models.Search, 'get_db_specific_sections', return_value=(store, [])), \
             patch.object(models.Search, 'post_processing', return_value={}):
            mock_conf.web_case = 'snake'
            s.search()
        pipeline = store.aggregate.call_args.args[0]
        assert pipeline[1] == {'$sort': {'score': {'$meta': 'textScore'}, '_id': 1}}
        data = pipeline[-1]['$facet']['data']
        assert data[0] == {'$sort': {'start_time': 1, '_id': 1}}


class TestListEndpointSearchSwitch:

    def test_admin_flag_alone_is_not_a_search(self):
        from controllers import is_search_request
        assert not is_search_request({})
        assert not is_search_request({'x-nonce': 'n'})
        assert not is_search_request({'x-actors-admin': 'true'})
        assert not is_search_request({'x-nonce': 'n', 'x-actors-admin': 'true'})
        assert is_search_request({'x-actors-admin': 'true', 'status': 'READY'})
        assert is_search_request({'limit': '5'})


# ---------------------------------------------------------------------------
# R3. health -- worker rabbit queue cleanup, orphan sweep, rabbit-unreachable
# ---------------------------------------------------------------------------

def _rsp(status=200, json_body=None):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = json_body
    if status >= 400:
        import requests
        r.raise_for_status.side_effect = requests.HTTPError(f"{status}")
    return r


class TestDeleteWorkerRabbitQueues:

    @patch('health.site', return_value='tacc')
    @patch('rabbit_mgmt.requests')
    def test_deletes_both_queues(self, mock_requests, mock_site):
        from health import delete_worker_rabbit_queues
        mock_requests.delete.return_value = _rsp(204)
        delete_worker_rabbit_queues('w1')
        urls = [c.args[0] for c in mock_requests.delete.call_args_list]
        assert urls[0].endswith('/api/queues/abaco_tacc/worker_w1')
        assert urls[1].endswith('/api/queues/abaco_tacc/spawner_worker_w1')

    @patch('health.site', return_value='tacc')
    @patch('rabbit_mgmt.requests')
    def test_404_and_errors_do_not_stop_the_other_queue(self, mock_requests, mock_site):
        from health import delete_worker_rabbit_queues
        # first queue already gone, second one fails: neither raises
        mock_requests.delete.side_effect = [_rsp(404), Exception('mgmt down')]
        with patch('health.logger') as mock_logger:
            delete_worker_rabbit_queues('w1')
        assert mock_requests.delete.call_count == 2
        assert mock_logger.error.call_count == 1
        assert 'spawner_worker_w1' in mock_logger.error.call_args.args[0]


class TestCheckOrphanWorkerQueues:

    def _run(self, queues, live_ids, delete_rsp=None, list_exc=None):
        from health import check_orphan_worker_queues
        store = MagicMock()
        store.items.return_value = [{'id': w} for w in live_ids]
        with patch('health.site', return_value='tacc'), \
             patch('health.workers_store', {'tacc': store}), \
             patch('rabbit_mgmt.requests') as mock_requests:
            if list_exc:
                mock_requests.get.side_effect = list_exc
            else:
                mock_requests.get.return_value = _rsp(200, queues)
            mock_requests.delete.return_value = delete_rsp or _rsp(204)
            check_orphan_worker_queues()
        return mock_requests

    def test_only_orphans_without_consumers_are_deleted(self):
        queues = [
            {'name': 'worker_live1', 'consumers': 0},           # live worker: keep
            {'name': 'spawner_worker_live1', 'consumers': 0},   # live worker: keep
            {'name': 'worker_gone2', 'consumers': 0},           # orphan: delete
            {'name': 'spawner_worker_gone3', 'consumers': 0},   # orphan: delete (prefix parsed first)
            {'name': 'worker_gone4', 'consumers': 1},           # has a consumer: keep
            {'name': 'actor_msg_tacc_abc', 'consumers': 0},     # not a worker queue
            {'name': 'command_channel_default', 'consumers': 0},
        ]
        mock_requests = self._run(queues, live_ids=['live1'])
        deleted = [c.args[0].rsplit('/', 1)[1] for c in mock_requests.delete.call_args_list]
        assert deleted == ['worker_gone2', 'spawner_worker_gone3']
        # never deletes a queue that gained a consumer since the listing
        for c in mock_requests.delete.call_args_list:
            assert c.kwargs['params'] == {'if-unused': 'true'}

    def test_404_on_delete_is_not_an_error(self):
        with patch('health.logger') as mock_logger:
            self._run([{'name': 'worker_gone', 'consumers': 0}], live_ids=[], delete_rsp=_rsp(404))
        mock_logger.error.assert_not_called()

    def test_listing_failure_deletes_nothing(self):
        mock_requests = self._run(None, live_ids=[], list_exc=Exception('mgmt down'))
        mock_requests.delete.assert_not_called()


class TestCheckWorkersRabbitUnreachable:

    @patch('health.conf', new_callable=MagicMock)
    @patch('health.site', return_value='test_site')
    @patch('health.get_current_worker_containers', return_value=[])
    @patch('health.Worker')
    @patch('health.shutdown_worker')
    @patch('health.hard_delete_worker')
    def test_ttl_checks_still_run_for_remaining_workers(self, mock_hard_del, mock_shutdown, MockWorker,
                                                        mock_containers, mock_site, mock_conf):
        import datetime
        from health import check_workers

        mock_conf.container_backend = 'docker'
        mock_conf.spawner_host_id = 'host1'
        now = datetime.datetime.utcnow()
        old = now - datetime.timedelta(hours=2)

        def worker(wid):
            return {'id': wid, 'actor_id': 'a1', 'host_id': 'host1', 'status': 'BUSY_X', 'ch_name': f'worker_{wid}',
                    'last_health_check_time': now, 'create_time': old, 'last_execution_time': old}
        MockWorker.get_workers.return_value = [worker('w1'), worker('w2')]

        with patch('health.WorkerChannel', side_effect=OSError('rabbit down')) as MockWC, \
             patch('health.os.environ', {'SPAWNER_HOST_ID': 'host1'}):
            check_workers('a1', ttl=60)

        # rabbit was tried once, then skipped -- but both idle workers hit the TTL check
        assert MockWC.call_count == 1
        assert [c.args for c in mock_shutdown.call_args_list] == [('a1', 'w1'), ('a1', 'w2')]


# ---------------------------------------------------------------------------
# R4. autoscaler -- queue depths count READY messages only
# ---------------------------------------------------------------------------

class TestGetQueueDepths:

    @patch('rabbit_mgmt.requests')
    def test_uses_messages_ready_not_total(self, mock_requests):
        from controllers import get_queue_depths
        # 1 waiting + 4 being processed (unacked): only the waiting one is pending work
        mock_requests.get.return_value = _rsp(200, [
            {'name': 'actor_msg_tacc_a1', 'messages_ready': 1, 'messages': 5},
            {'name': 'command_channel_default', 'messages_ready': 0, 'messages': 0}])
        depths = get_queue_depths('tacc')
        assert depths == {'actor_msg_tacc_a1': 1, 'command_channel_default': 0}
        assert mock_requests.get.call_args.kwargs['params'] == {'columns': 'name,messages_ready'}

    @patch('rabbit_mgmt.requests')
    def test_unreachable_returns_none(self, mock_requests):
        from controllers import get_queue_depths
        mock_requests.get.side_effect = Exception('mgmt down')
        assert get_queue_depths('tacc') is None


# ---------------------------------------------------------------------------
# R5. auth -- admin_usernames is tenant-qualified and validated
# ---------------------------------------------------------------------------

class TestAdminUsernames:

    def test_parse_valid(self):
        from auth import parse_admin_usernames
        assert parse_admin_usernames(None) == frozenset()
        assert parse_admin_usernames([]) == frozenset()
        assert parse_admin_usernames(['admin:jdoe', ' dev:jdoe ']) == {'admin:jdoe', 'dev:jdoe'}

    @pytest.mark.parametrize('bad', ['admin:jdoe', {'admin': 'jdoe'}, ['jdoe'], ['admin:'],
                                     [':jdoe'], ['a:b:c'], [7]])
    def test_parse_rejects_malformed(self, bad):
        from auth import parse_admin_usernames
        with pytest.raises(ValueError):
            parse_admin_usernames(bad)

    def _roles_for(self, tenant, username, sk_roles=(), sk_fails=False):
        import flask
        import auth
        app = flask.Flask('t')
        with app.app_context(), \
             patch.object(auth, 'ADMIN_USERNAMES', frozenset({'admin:jdoe'})), \
             patch.object(auth, 't') as mock_t:
            if sk_fails:
                mock_t.sk.getUserRoles.side_effect = Exception('SK down')
            else:
                mock_t.sk.getUserRoles.return_value = MagicMock(names=list(sk_roles))
            flask.g.request_tenant_id = tenant
            flask.g.request_username = username
            auth.get_user_sk_roles()
            return flask.g.roles

    def test_admin_only_in_the_listed_tenant(self):
        import codes
        assert codes.ADMIN_ROLE in self._roles_for('admin', 'jdoe')
        # same username, different tenant: a different person
        assert codes.ADMIN_ROLE not in self._roles_for('dev', 'jdoe')

    def test_sk_failure_fallback_is_tenant_qualified(self):
        import codes
        assert self._roles_for('admin', 'jdoe', sk_fails=True) == [codes.ADMIN_ROLE]
        with pytest.raises(Exception, match='SK down'):
            self._roles_for('dev', 'jdoe', sk_fails=True)


# ---------------------------------------------------------------------------
# R6. reconciler -- claim before publish, deleted actors, channel closed
# ---------------------------------------------------------------------------

class TestClaimIncrement:

    def test_filter_and_update(self):
        import store
        s = store.MongoStore.__new__(store.MongoStore)
        s._db = MagicMock()
        s._db.find_one_and_update.return_value = {'_id': 'k'}
        assert s.claim_increment('k', 'reconcile_requeues', 0, {'status': 'SUBMITTED'}) is True
        kw = s._db.find_one_and_update.call_args.kwargs
        assert kw['filter'] == {'_id': 'k', 'status': 'SUBMITTED', 'reconcile_requeues': {'$in': [None, 0]}}
        assert kw['update'] == {'$inc': {'reconcile_requeues': 1}}

        s._db.find_one_and_update.return_value = None   # lost the race
        assert s.claim_increment('k', 'reconcile_requeues', 1) is False
        assert s._db.find_one_and_update.call_args.kwargs['filter'] == {'_id': 'k', 'reconcile_requeues': 1}


class TestReconcileLostMessages:

    def _run(self, execs, conf_values=None, actor_exists=True, claim=True, depth=0, put_exc=None):
        import datetime
        import health
        values = {'reconcile_lost_messages_after_s': 60, 'message_outbox': True}
        values.update(conf_values or {})
        conf = MagicMock()
        conf.get.side_effect = lambda k, d=None: values.get(k, d)
        old = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
        for e in execs:
            e.setdefault('message_received_time', old)
        ex_store = MagicMock()
        ex_store.items.return_value = execs
        ex_store.claim_increment.return_value = claim
        actors = {'tacc_a1': {'id': 'a1'}} if actor_exists else {}
        ch = MagicMock()
        if put_exc:
            ch.put_msg.side_effect = put_exc
        with patch.object(health, 'conf', conf), \
             patch.object(health, 'site', return_value='tacc'), \
             patch.object(health, 'executions_store', {'tacc': ex_store}), \
             patch.object(health, 'actors_store', {'tacc': actors}), \
             patch.object(health, 'get_queue_depth', return_value=depth) as mock_depth, \
             patch.object(health, 'ActorMsgChannel', return_value=ch) as MockCh, \
             patch.object(health, 'Execution') as MockExecution:
            health.reconcile_lost_messages()
        return ex_store, MockCh, ch, MockExecution, mock_depth

    def _exec(self, requeues=0):
        return {'id': 'e1', 'actor_id': 'tacc_a1', 'status': 'SUBMITTED', 'reconcile_requeues': requeues,
                'queued_message': {'message': 'hi', 'd': {'k': 'v'}}}

    def test_claims_before_publishing(self):
        ex_store, MockCh, ch, MockExecution, _ = self._run([self._exec()])
        ex_store.claim_increment.assert_called_once_with('tacc_a1_e1', 'reconcile_requeues', 0,
                                                         {'status': 'SUBMITTED'})
        ch.put_msg.assert_called_once_with(message='hi', d={'k': 'v'})
        ch.close.assert_called_once()
        MockExecution.update_status.assert_not_called()

    def test_lost_claim_does_not_publish(self):
        ex_store, MockCh, ch, MockExecution, _ = self._run([self._exec()], claim=False)
        MockCh.assert_not_called()
        MockExecution.update_status.assert_not_called()

    def test_channel_closed_when_publish_fails(self):
        ex_store, MockCh, ch, MockExecution, _ = self._run([self._exec()], put_exc=RuntimeError('nack'))
        ch.close.assert_called_once()

    def test_deleted_actor_is_marked_error_and_never_republished(self):
        ex_store, MockCh, ch, MockExecution, mock_depth = self._run([self._exec()], actor_exists=False)
        MockCh.assert_not_called()
        mock_depth.assert_not_called()
        (flt, update), = [c.args for c in ex_store.full_update.call_args_list]
        assert flt == {'_id': 'tacc_a1_e1', 'status': 'SUBMITTED'}  # never overwrites a finished execution
        assert update['$set']['status'] == 'ERROR'
        assert 'deleted' in update['$set']['status_message']

    def test_max_requeues_zero_is_respected(self):
        ex_store, MockCh, ch, MockExecution, _ = self._run([self._exec()],
                                                           conf_values={'reconcile_max_requeues': 0})
        MockCh.assert_not_called()
        ex_store.claim_increment.assert_not_called()
        assert ex_store.full_update.call_args.args[1]['$set']['status'] == 'ERROR'

    def test_finished_meanwhile_is_left_alone(self):
        # the snapshot said SUBMITTED, but a worker finished it before the reconciler got there
        import health
        ex_store = MagicMock()
        ex_store.full_update.return_value = MagicMock(modified_count=0)
        with patch.object(health, 'executions_store', {'tacc': ex_store}), \
             patch.object(health, 'site', return_value='tacc'), \
             patch.object(health, 'logger') as mock_logger:
            health._reconciler_mark_error('tacc_a1', 'e1', 'why')
        mock_logger.critical.assert_not_called()

    def test_lists_only_the_fields_it_needs(self):
        ex_store, *_ = self._run([self._exec()], depth=3)
        flt, proj = ex_store.items.call_args.args
        assert flt == {'status': 'SUBMITTED'}
        assert proj['queued_message'] is True and proj['_id'] is False and 'permissions' not in proj

    def test_nonempty_queue_is_left_alone(self):
        ex_store, MockCh, ch, MockExecution, _ = self._run([self._exec()], depth=3)
        MockCh.assert_not_called()
        MockExecution.update_status.assert_not_called()


# ---------------------------------------------------------------------------
# R7. reconciler after dead-lettering -- one replay by default, neutral reason
# ---------------------------------------------------------------------------

class TestReconcilerAfterDeadLettering:
    # reuse R6's harness without re-collecting its tests
    _run = TestReconcileLostMessages._run
    _exec = TestReconcileLostMessages._exec

    def test_default_is_a_single_republish(self):
        # first pass: published once
        ex_store, MockCh, *_ = self._run([self._exec(requeues=0)])
        assert MockCh.call_count == 1
        # second pass (dead-lettered again, queue empty): ERROR, no second replay
        ex_store, MockCh, ch, MockExecution, _ = self._run([self._exec(requeues=1)])
        MockCh.assert_not_called()
        (flt, update), = [c.args for c in ex_store.full_update.call_args_list]
        reason = update['$set']['status_message']
        assert 'dead-lettered to abaco_dlq or lost' in reason
        assert 're-published 1x' in reason


# ---------------------------------------------------------------------------
# 10. Vendored tapisservice auth (actors/auth_tapisservice.py) – service-token refresh
#
# Loads the vendored file directly with tapisservice.config/tenants/logs stubbed, so this
# section needs neither a config.json nor a reachable tenants API (the real
# tapisservice.tenants builds a TenantCache over the network at import). Run alone with
#   python3 -m pytest tests/test_resilience.py -q -p no:cacheprovider -k VendoredAuth
# ---------------------------------------------------------------------------

import datetime
import importlib.util
import logging
import types

_VENDORED_AUTH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'actors', 'auth_tapisservice.py')


def _load_vendored_auth(conf_values=None):
    values = dict(conf_values or {})
    conf = types.SimpleNamespace(service_tenant_id='admin', service_name='abaco', python_framework_type='flask',
                                 get=lambda k, d=None: values.get(k, d))
    cfg = types.ModuleType('tapisservice.config'); cfg.conf = conf
    ten = types.ModuleType('tapisservice.tenants'); ten.tenant_cache = MagicMock()
    logs = types.ModuleType('tapisservice.logs'); logs.get_logger = logging.getLogger
    with patch.dict(sys.modules, {'tapisservice.config': cfg, 'tapisservice.tenants': ten, 'tapisservice.logs': logs}):
        spec = importlib.util.spec_from_file_location('vendored_auth_under_test', _VENDORED_AUTH)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    return mod


class _Tok:
    def __init__(self, jwt_str, remaining_s, ttl=86400):
        self.access_token = jwt_str
        self.original_ttl = ttl
        self._exp = time.time() + remaining_s

    def expires_in(self):
        return datetime.timedelta(seconds=self._exp - time.time())


def _client(tokens):
    tc = MagicMock()
    tc.get_site_and_base_url_for_service_request.return_value = ('tacc', 'https://admin.tapis.io')
    tc.tenants = {'admin': types.SimpleNamespace(site=types.SimpleNamespace(site_id='tacc', site_admin_tenant_id='admin'))}
    return types.SimpleNamespace(username='abaco', tenant_cache=tc, service_tokens=tokens,
                                 refresh_service_tokens=MagicMock(), get_tokens=MagicMock())


def _call(mod, client, resource='sk', op='getUsersWithRole', **kwargs):
    operation = types.SimpleNamespace(http_method='post', path_name=op, resource_name=resource,
                                      operation_id=op, tapis_client=client)
    req = types.SimpleNamespace(url=f'https://admin.tapis.io/v3/{resource}/x', headers={})
    mod.preprocess_service_request(operation, req, _tapis_set_x_headers_from_service=True, **kwargs)
    return req.headers


class TestVendoredAuthServiceTokens:

    def test_own_create_token_goes_out_with_basic_auth(self):
        # upstream emptied service_tokens before create_token, so tapipy added basic auth; the
        # vendored copy keeps the (here: expired) token, which must not be attached or refreshed.
        mod = _load_vendored_auth()
        client = _client({'admin': {'access_token': _Tok('stale.jwt', -10)}})
        client.refresh_service_tokens.side_effect = AssertionError('must not refresh')
        headers = _call(mod, client, 'tokens', 'create_token', token_username='abaco')
        assert 'X-Tapis-Token' not in headers
        assert mod._service_token_refresh_lock.acquire(blocking=False)

    def test_create_token_for_another_subject_keeps_service_jwt(self):
        mod = _load_vendored_auth()
        client = _client({'admin': {'access_token': _Tok('svc.jwt', 80000)}})
        headers = _call(mod, client, 'tokens', 'create_token', token_username='someuser')
        assert headers['X-Tapis-Token'] == 'svc.jwt'

    def test_fallback_reacquire_does_not_reenter_the_lock(self):
        # refresh fails -> get_tokens -> nested create_token (and any other nested call) runs
        # preprocess on the same thread that holds the non-reentrant lock: used to stall 30s.
        mod = _load_vendored_auth()
        client = _client({'admin': {'access_token': _Tok('stale.jwt', -10)}})
        client.refresh_service_tokens.side_effect = Exception('refresh token rejected')
        nested = {}

        def get_tokens(tenant_id):
            nested['create'] = _call(mod, client, 'tokens', 'create_token', token_username='abaco')
            nested['other'] = _call(mod, client, 'sk', 'getUsersWithRole')
            client.service_tokens = {'admin': {'access_token': _Tok('new.jwt', 86400)}}
        client.get_tokens.side_effect = get_tokens

        start = time.monotonic()
        headers = _call(mod, client)
        assert time.monotonic() - start < 5
        assert headers['X-Tapis-Token'] == 'new.jwt'
        assert 'X-Tapis-Token' not in nested['create']
        assert client.refresh_service_tokens.call_count == 1

    def test_cooldown_applies_to_an_expired_token(self):
        mod = _load_vendored_auth()
        client = _client({'admin': {'access_token': _Tok('stale.jwt', -10)}})
        mod._service_token_refresh_backoff_until['admin'] = time.monotonic() + 60
        mod._service_token_refresh_lock.acquire()  # a refresh in flight elsewhere: must not wait on it
        try:
            start = time.monotonic()
            with pytest.raises(mod.errors.BaseTapisError) as exc:
                _call(mod, client)
            assert exc.value.code == 503
            assert time.monotonic() - start < 2
        finally:
            mod._service_token_refresh_lock.release()
        client.refresh_service_tokens.assert_not_called()
        client.get_tokens.assert_not_called()

    def test_expired_token_and_failed_ladder_is_503_not_expired_token(self):
        mod = _load_vendored_auth()
        client = _client({'admin': {'access_token': _Tok('stale.jwt', -10)}})
        client.refresh_service_tokens.side_effect = Exception('auth down')
        client.get_tokens.side_effect = Exception('auth down')
        with pytest.raises(mod.errors.BaseTapisError) as exc:
            _call(mod, client)
        assert exc.value.code == 503
        assert mod._service_token_refresh_backoff_until['admin'] > time.monotonic()

    def test_still_valid_token_proceeds_when_ladder_fails(self):
        mod = _load_vendored_auth()
        client = _client({'admin': {'access_token': _Tok('old.jwt', 600)}})
        client.refresh_service_tokens.side_effect = Exception('auth down')
        client.get_tokens.side_effect = Exception('auth down')
        assert _call(mod, client)['X-Tapis-Token'] == 'old.jwt'

    @pytest.mark.parametrize('outcome', ['cooldown', 'healed'])
    def test_self_heal_waiters_recheck_after_lock(self, outcome):
        mod = _load_vendored_auth()
        client = _client({})
        results = []
        mod._service_token_refresh_lock.acquire()  # first thread mid re-acquisition
        threads = [threading.Thread(target=lambda: results.append(_call(mod, client))) for _ in range(3)]
        for th in threads:
            th.start()
        time.sleep(0.3)
        if outcome == 'cooldown':
            mod._service_token_refresh_backoff_until['admin'] = time.monotonic() + 60
        else:
            client.service_tokens = {'admin': {'access_token': _Tok('healed.jwt', 86400)}}
        mod._service_token_refresh_lock.release()
        for th in threads:
            th.join(5)
        assert len(results) == 3
        client.get_tokens.assert_not_called()
        if outcome == 'healed':
            assert all(h['X-Tapis-Token'] == 'healed.jwt' for h in results)

    def test_get_service_tokens_redacts_credentials(self):
        mod = _load_vendored_auth()
        err = Exception('boom')
        err.request = types.SimpleNamespace(url='https://admin.tapis.io/v3/tokens', headers={
            'Authorization': 'Basic YWJhY286c2VydmljZXBhc3N3b3Jk', 'X-Tapis-Token': 'eyJ.service.jwt',
            'Content-Type': 'application/json'})
        svc = types.SimpleNamespace(username='abaco', tenant_id='admin', account_type='service', service_tokens={},
                                    tenant_cache=MagicMock(), tokens=MagicMock())
        svc.tokens.create_token.side_effect = err
        with pytest.raises(mod.errors.BaseTapisError) as exc:
            mod.get_service_tokens(svc, tenant_id='admin')
        assert 'YWJhY286' not in exc.value.msg and 'eyJ' not in exc.value.msg
        assert 'application/json' in exc.value.msg

    def test_refresh_margin_capped_by_short_ttl(self):
        mod = _load_vendored_auth()
        client = _client({'admin': {'access_token': _Tok('ok.jwt', 250, ttl=300)}})
        _call(mod, client)  # margin min(3600, 300/4=75s) < 250s remaining: no refresh
        client.refresh_service_tokens.assert_not_called()
        client.service_tokens['admin']['access_token'] = _Tok('old.jwt', 60, ttl=300)
        client.refresh_service_tokens.return_value = _Tok('new.jwt', 300, ttl=300)
        assert _call(mod, client)['X-Tapis-Token'] == 'new.jwt'

    def test_jwt_leeway_cast_once_at_load(self):
        assert _load_vendored_auth({'jwt_leeway_seconds': '5'}).JWT_LEEWAY_S == 5
        assert _load_vendored_auth().JWT_LEEWAY_S == 0
        with pytest.raises(Exception) as exc:
            _load_vendored_auth({'jwt_leeway_seconds': 'five'})
        assert 'jwt_leeway_seconds' in str(exc.value)


# ===========================================================================
# gunicorn --preload: nothing fork-unsafe crosses into the workers
# ===========================================================================

class TestForkSafeMongoClients:
    """--preload builds every store in the gunicorn master; each worker must get its own
    MongoClient (an inherited one has dead monitor threads and possibly a held lock)."""

    def _store(self, build):
        import store
        s = store.MongoStore.__new__(store.MongoStore)
        s._database_name, s._collection_name = 'abaco_x', 'actors_store'
        s._client_spec = (('mongodb://h:1', 'off', id(build)), build, 'h:1 (tls: off)')
        s._bind(store._shared_mongo_client(*s._client_spec))
        return s

    def test_store_rebinds_to_a_new_client_after_fork(self):
        import store
        clients = []

        def build():
            clients.append(MagicMock(name=f'client{len(clients)}'))
            return clients[-1]
        s = self._store(build)
        assert s._mongo_client is clients[0]
        with patch('store.os.getpid', return_value=os.getpid() + 1):
            assert s._mongo_client is clients[1]
            assert s._db is clients[1]['abaco_x']['actors_store']
        assert len(clients) == 2

    def test_no_rebind_without_a_fork(self):
        clients = []

        def build():
            clients.append(MagicMock())
            return clients[-1]
        s = self._store(build)
        s._db, s._mongo_client, s._mongo_database
        assert len(clients) == 1

    def test_shared_cache_is_dropped_in_a_forked_process(self):
        import store
        parent = MagicMock(name='parent')
        child = MagicMock(name='child')
        key = ('mongodb://cache-test', 'off')
        assert store._shared_mongo_client(key, lambda: parent, 'l') is parent
        with patch('store.os.getpid', return_value=os.getpid() + 7):
            assert store._shared_mongo_client(key, lambda: child, 'l') is child
            parent.close.assert_not_called()   # never close the parent's client from a child

    def test_tests_can_still_assign_db_directly(self):
        import store
        s = store.MongoStore.__new__(store.MongoStore)
        s._db = MagicMock()
        assert s._db is s._collection


class TestGunicornPostFork:

    def _load_conf(self):
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'actors', 'gunicorn_conf.py')
        spec = importlib.util.spec_from_file_location('gunicorn_conf_under_test', path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def test_post_fork_drops_the_service_clients_pooled_connections(self):
        conf_mod = self._load_conf()
        boot = types.ModuleType('__init__')
        boot.t = MagicMock()
        with patch.dict(sys.modules, {'__init__': boot}):
            conf_mod.post_fork(MagicMock(), MagicMock(pid=123))
        boot.t.requests_session.close.assert_called_once()

    def test_post_fork_is_a_noop_before_the_app_is_loaded(self):
        conf_mod = self._load_conf()
        with patch.dict(sys.modules, {'__init__': types.ModuleType('__init__')}):
            conf_mod.post_fork(MagicMock(), MagicMock(pid=1))   # no t: must not raise
