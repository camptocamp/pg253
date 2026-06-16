"""Module for testing the main module"""

import unittest.mock
from unittest.mock import patch, MagicMock

from pg253.metrics import Metrics
from pg253.app import App


class MockingTransfer: # pylint: disable=too-few-public-methods
    """
    Mocking class for Transfer from pg253.transfer module.
    """

    backup_count: int

    def __init__(self, **_):
        pass

    def backup_database(self, database, _cmd):
        """ Don't launch backup but raise an exception. """
        if database == "app2":
            raise RuntimeError()

@patch('pg253.metrics.start_http_server', autospec=True, return_value=(None, None))
@patch('pg253.app.App.list_databases', autospec=True, return_value=["app1", "app2", "app3"])
@patch('pg253.app.Transfer', wraps=MockingTransfer)
def test_backup_error_continue(*_):

    """Ensure backup continue on other dbs on error"""
    mock_remote = MagicMock()
    m = Metrics(mock_remote, 9352)
    app = App(metrics=m, remote=None,
              exclude_dbs="", buffer_size=10000000,
              retention_days=15, encryption_passphrase="",
              backup_roles=True)

    with patch('pg253.app.logging.error') as mock_log_error:
        app.backup()
        mock_log_error.assert_called_once_with(
            "Error during backup of '%s' : %s", "app2", unittest.mock.ANY
        )
