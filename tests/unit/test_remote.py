"""Module for testing the remote module"""

import sys
from datetime import datetime
from freezegun import freeze_time

import pytest
import boto3
from moto import mock_aws

from pg253.remote import S3Remote, Backup

FAKE_REMOTE_ARGS = {
    "endpoint_url": None,
    "region_name": "us-east-1",
    "access_key": "accesskey",
    "secret_key": "secretkey",
    "bucket": "mybucket",
    "path_prefix": "/",
    "client": None,
}

@mock_aws
def test_remote_generate_filename_clear_format():
    database = "mydb"
    dt = datetime(2021, 8, 6, 0, 22, 48, 236214)

    s3_remote = S3Remote(**FAKE_REMOTE_ARGS)
    result = s3_remote.generate_filename(database, dt)

    assert result == "/postgres.mydb.20210806-0022.dump"

@mock_aws
def test_remote_list_empty():
    conn = boto3.resource(
            service_name="s3",
            region_name=FAKE_REMOTE_ARGS["region_name"])
    conn.create_bucket(Bucket=FAKE_REMOTE_ARGS["bucket"])

    s3_remote = S3Remote(**FAKE_REMOTE_ARGS)

    for path, size in s3_remote._list():
        assert False

@mock_aws
def test_remote_list_multiple_objects():
    conn = boto3.resource(
            service_name="s3",
            region_name=FAKE_REMOTE_ARGS["region_name"])
    conn.create_bucket(Bucket=FAKE_REMOTE_ARGS["bucket"])
    conn.Object(FAKE_REMOTE_ARGS["bucket"], "/alpha").put(Body=bytearray(55))
    conn.Object(FAKE_REMOTE_ARGS["bucket"], "/beta").put(Body=bytearray(93))

    expected_results = [
            ("alpha", 55),
            ("beta", 93),
    ]

    s3_remote = S3Remote(**FAKE_REMOTE_ARGS)

    for i, (path, size) in enumerate(s3_remote._list()):
        assert path == expected_results[i][0]
        assert size == expected_results[i][1]

# TODO: Implement a test for pagination/v1-v2 list_objects

@mock_aws
def test_remote_fetch_ok():
    conn = boto3.resource(
            service_name="s3",
            region_name=FAKE_REMOTE_ARGS["region_name"])
    conn.create_bucket(Bucket=FAKE_REMOTE_ARGS["bucket"])
    conn.Object(FAKE_REMOTE_ARGS["bucket"], "/alpha").put(Body=bytearray(55))
    conn.Object(FAKE_REMOTE_ARGS["bucket"], "/beta").put(Body=bytearray(93))
    conn.Object(FAKE_REMOTE_ARGS["bucket"], "/postgres.mydb.20210806-0022.dump").put(Body=bytearray(22))
    conn.Object(FAKE_REMOTE_ARGS["bucket"], "/postgres.myotherdb.20210806-0022.dump.gpg").put(Body=bytearray(48))

    expected_results = [
            ("mydb", 22, datetime(2021, 8, 6, 0, 22), False),
            ("myotherdb", 48, datetime(2021, 8, 6, 0, 22), True),
    ]

    s3_remote = S3Remote(**FAKE_REMOTE_ARGS)
    backups = s3_remote.fetch_backups()

    assert len(backups) == 2
    for i, backup in enumerate(backups):
        assert backup.database == expected_results[i][0]
        assert backup.size == expected_results[i][1]
        assert backup.dt == expected_results[i][2]

@mock_aws
def test_remote_delete_backup_clear_format():
    conn = boto3.resource(
            service_name="s3",
            region_name=FAKE_REMOTE_ARGS["region_name"])
    conn.create_bucket(Bucket=FAKE_REMOTE_ARGS["bucket"])
    conn.Object(FAKE_REMOTE_ARGS["bucket"], "/postgres.mydb.20210806-0022.dump").put(Body=bytearray(22))

    database = "mydb"
    dt = datetime(2021, 8, 6, 0, 22, 48, 236214)

    backup = Backup(database=database, dt=dt, size=0, path="/postgres.mydb.20210806-0022.dump")
    s3_remote = S3Remote(**FAKE_REMOTE_ARGS)
    backups = s3_remote.delete_backup(backup)

    with pytest.raises(s3_remote.client.exceptions.NoSuchKey):
        conn.Object(FAKE_REMOTE_ARGS["bucket"], "/postgres.mydb.20210806-0022.dump").get()

@freeze_time("2026-01-01 00:00:01")
@mock_aws
def test_remote_start_upload():
    conn = boto3.resource(
            service_name="s3",
            region_name=FAKE_REMOTE_ARGS["region_name"])
    conn.create_bucket(Bucket=FAKE_REMOTE_ARGS["bucket"])

    s3_remote = S3Remote(**FAKE_REMOTE_ARGS)
    upload = s3_remote.start_upload("mydb")

    assert upload.database == "mydb"
    assert upload.start_time == datetime(2026, 1, 1, 0, 0, 1)
    assert upload.target['Bucket'] == FAKE_REMOTE_ARGS["bucket"]
    assert upload.target['Key'] == "/postgres.mydb.20260101-0000.dump"

@mock_aws
def test_remote_upload_upload_part():
    conn = boto3.resource(
            service_name="s3",
            region_name=FAKE_REMOTE_ARGS["region_name"])
    conn.create_bucket(Bucket=FAKE_REMOTE_ARGS["bucket"])

    s3_remote = S3Remote(**FAKE_REMOTE_ARGS)
    upload = s3_remote.start_upload("mydb")
    upload.uploadPart(bytearray(100), 100, 100)

    assert upload.part_count == 2
    assert upload.bytes_uploaded == 100

@mock_aws
def test_remote_upload_complete():
    conn = boto3.resource(
            service_name="s3",
            region_name=FAKE_REMOTE_ARGS["region_name"])
    conn.create_bucket(Bucket=FAKE_REMOTE_ARGS["bucket"])

    s3_remote = S3Remote(**FAKE_REMOTE_ARGS)
    upload = s3_remote.start_upload("mydb")
    upload.uploadPart(bytearray(100), 100, 100)
    upload.complete()

@mock_aws
def test_remote_upload_abort():
    conn = boto3.resource(
            service_name="s3",
            region_name=FAKE_REMOTE_ARGS["region_name"])
    conn.create_bucket(Bucket=FAKE_REMOTE_ARGS["bucket"])

    s3_remote = S3Remote(**FAKE_REMOTE_ARGS)
    upload = s3_remote.start_upload("mydb")
    upload.uploadPart(bytearray(100), 100, 100)
    upload.abort()
