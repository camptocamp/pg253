"""Module for testing the metrics module"""

import sys
from datetime import datetime

from unittest.mock import patch, MagicMock
from prometheus_client import REGISTRY, CollectorRegistry
import pytest


@pytest.fixture(autouse=True)
def setup():
    """Actions to run before and after each test"""
    collector = CollectorRegistry(auto_describe=True)
    REGISTRY.register(collector)
    yield
    collectors = list(REGISTRY._collector_to_names.keys()) # pylint: disable=W0212
    for collector in collectors:
        REGISTRY.unregister(collector)

@patch('prometheus_client.start_http_server', autospec=True)
def test_refresh_metrics_ok(_):
    """Test Metrics.refreshMetrics() with valid parameters"""
    mock_remote = MagicMock()
    current_time = datetime.now()
    mock_remote.BACKUPS = {"foo": [(current_time, 10)]}

    metric_labels = {'database': 'foo'}

    with patch.dict("sys.modules", {"pg253.remote": MagicMock(Remote=mock_remote)}):
        if "pg253.metrics" in sys.modules:
            del sys.modules['pg253.metrics']

        from pg253.metrics import Metrics # pylint: disable=C0415
        m = Metrics()
        m.refreshMetrics()

        assert REGISTRY.get_sample_value('first_backup', metric_labels) == current_time.timestamp()
        assert REGISTRY.get_sample_value('last_backup', metric_labels) == current_time.timestamp()


@patch('prometheus_client.start_http_server', autospec=True)
def test_refresh_metrics_databases_and_backups_removed(_):
    """Test Metrics.refreshMetrics() when database and all backups have been removed"""
    mock_remote = MagicMock()
    current_time = datetime.now()
    mock_remote.BACKUPS = {"foo": [(current_time, 10)]}

    metric_labels = {'database': 'foo'}

    with patch.dict("sys.modules", {"pg253.remote": MagicMock(Remote=mock_remote)}):
        if "pg253.metrics" in sys.modules:
            del sys.modules['pg253.metrics']

        from pg253.metrics import Metrics # pylint: disable=C0415
        m = Metrics()
        m.refreshMetrics()

        # We have a database backup, we should have metrics
        assert REGISTRY.get_sample_value('first_backup', metric_labels) == current_time.timestamp()
        assert REGISTRY.get_sample_value('last_backup', metric_labels) == current_time.timestamp()

        mock_remote.BACKUPS = {"foo": []}
        m.refreshMetrics()

        # We removed database's backups, we shouldn't have metrics anymore
        assert REGISTRY.get_sample_value('first_backup', metric_labels) is None
        assert REGISTRY.get_sample_value('last_backup', metric_labels) is None

@patch('prometheus_client.start_http_server', autospec=True)
def test_refresh_metrics_database_but_no_backup(_):
    """Test Metrics.refreshMetrics() when database exists but no backup exists"""
    mock_remote = MagicMock()
    current_time = datetime.now()
    mock_remote.BACKUPS = {"foo": []}

    metric_labels = {'database': 'foo'}

    with patch.dict("sys.modules", {"pg253.remote": MagicMock(Remote=mock_remote)}):
        if "pg253.metrics" in sys.modules:
            del sys.modules['pg253.metrics']

        from pg253.metrics import Metrics # pylint: disable=C0415
        m = Metrics()

        # Removing non-existing metrics shouldn't raise an error
        m.refreshMetrics()

        # Metrics should not exist
        assert REGISTRY.get_sample_value('first_backup', metric_labels) is None
        assert REGISTRY.get_sample_value('last_backup', metric_labels) is None
