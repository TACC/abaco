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


