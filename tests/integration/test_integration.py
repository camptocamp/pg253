""" Module to run integration tests """

import sys
import os
import re
from unittest.mock import patch
import pytest
import boto3
import requests

from pg253.app import run


class MockBlockingScheduler:
    """
    Mocking class for BlockScheduler from apscheduler's module.
    """

    def __init__(self):
        self.func = None

    def add_job(self, *args):
        """ Disable the default add_job method and retrieve the function to execute. """
        self.func = args[0]

    def start(self):
        """ Replace the default start method and execute the function given in add_job. """
        self.func()


@pytest.fixture(autouse=True)
def setup():
    """
    Prepare the environment to run the integration tests and
    remove the created resources after the tests.
    """

    os.environ['PGHOST'] = '127.0.0.1'
    os.environ['PGUSER'] = 'pguser'
    os.environ['PGPASSWORD'] = 'pgpass'
    os.environ['AWS_ENDPOINT_URL'] = 'http://127.0.0.1:3900'
    os.environ['AWS_ACCESS_KEY_ID'] = 'GK3515373e4c851ebaad366558'
    os.environ['AWS_SECRET_ACCESS_KEY'] = (
            '7d37d093435a41f2aab8f13c19ba067d9776c90215f'
            '56614adad6ece597dbb34')
    os.environ['AWS_DEFAULT_REGION'] = 'garage'
    os.environ['AWS_S3_BUCKET'] = 'pgbackups'
    os.environ['EXCLUDE_DATABASES'] = 'pguser|postgres|template.*'
    os.environ['AWS_S3_PREFIX'] = 'pg253-integration-tests/'
    s3 = boto3.resource(
            's3',
            endpoint_url=os.environ['AWS_ENDPOINT_URL'],
            region_name=os.environ['AWS_DEFAULT_REGION'],
            aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
    s3_bucket = s3.Bucket(os.environ['AWS_S3_BUCKET'])

    yield

    s3_bucket.objects.filter(Prefix=os.environ['AWS_S3_PREFIX']).delete()


@patch.object(sys, 'argv', ['pg253'])
@patch('pg253.app.BlockingScheduler', wraps=MockBlockingScheduler)
def test_backup_databases_success(_):
    """
    Validate the behavior of PG253 when executed for real,
    on existing PostgreSQL and S3 resources.
    """

    run()

    expected_dbs = ['application', 'big', 'data']
    s3 = boto3.resource(
            's3',
            endpoint_url=os.environ['AWS_ENDPOINT_URL'],
            region_name=os.environ['AWS_DEFAULT_REGION'],
            aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
    s3_bucket = s3.Bucket(os.environ['AWS_S3_BUCKET'])

    for i, obj in enumerate(s3_bucket.objects.all()):
        pattern = re.compile("%spostgres.%s.[0-9]{8}-[0-9]{4}.dump" % (
            os.environ['AWS_S3_PREFIX'],
            expected_dbs[i]))
        assert pattern.match(obj.key)
        assert obj.size > 30000000 # Arbitrary value to ensure the object is not empty

    r = requests.get('http://127.0.0.1:9352/metrics', timeout=5)
    assert r.status_code == 200
    assert re.search('^total_bytes_read_total .*$', r.text, re.MULTILINE)
    assert re.search('^total_bytes_read_created .*$', r.text, re.MULTILINE)
    assert re.search('^total_bytes_write_total .*$', r.text, re.MULTILINE)
    assert re.search('^total_bytes_write_created .*$', r.text, re.MULTILINE)
    for database in expected_dbs:
        assert re.search(
                '^current_bytes_read{database="%s"} .*$' % (database),
                r.text,
                re.MULTILINE)
        assert re.search(
                '^current_bytes_write{database="%s"} .*$' % (database),
                r.text,
                re.MULTILINE)
        assert re.search(
                '^part_count{database="%s"} .*$' % (database),
                r.text,
                re.MULTILINE)
        assert re.search(
                '^first_backup{database="%s"} .*$' % (database),
                r.text,
                re.MULTILINE)
        assert re.search(
                '^last_backup{database="%s"} .*$' % (database),
                r.text,
                re.MULTILINE)
        assert re.search(
                '^backups{database="%s",date="[0-9]{8}-[0-9]{4}",size="[0-9]+"} .*$' % (database),
                r.text,
                re.MULTILINE)
        assert re.search(
                '^backup_duration{database="%s"} .*$' % (database),
                r.text,
                re.MULTILINE)
        assert re.search(
                '^error{database="%s"} .*$' % (database),
                r.text,
                re.MULTILINE)
