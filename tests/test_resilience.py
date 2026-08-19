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
