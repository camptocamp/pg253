"""Module for testing the remote module"""

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
    """ Should successfully return a backup filename. """

    database = "mydb"
    dt = datetime(2021, 8, 6, 0, 22, 48, 236214)

    s3_remote = S3Remote(**FAKE_REMOTE_ARGS)
    result = s3_remote.generate_filename(database, dt, False)

    assert result == "/postgres.mydb.20210806-0022.dump"

@mock_aws
def test_remote_generate_filename_encrypted_format():
    """ Should successfully return an encrypted backup filename. """

    database = "mydb"
    dt = datetime(2021, 8, 6, 0, 22, 48, 236214)

    s3_remote = S3Remote(**FAKE_REMOTE_ARGS)
    result = s3_remote.generate_filename(database, dt, True)

    assert result == "/postgres.mydb.20210806-0022.dump.gpg"

@mock_aws
def test_remote_list_empty():
    """ Should return an empty list of backup files. """

    conn = boto3.resource(
            service_name="s3",
            region_name=FAKE_REMOTE_ARGS["region_name"])
    conn.create_bucket(Bucket=FAKE_REMOTE_ARGS["bucket"])

    s3_remote = S3Remote(**FAKE_REMOTE_ARGS)

    remote_objects = list(s3_remote._list_objects()) # pylint: disable=protected-access
    assert len(remote_objects) == 0

@mock_aws
def test_remote_list_multiple_objects():
    """ Should return the list of objects stored in the S3 bucket. """
    conn = boto3.resource(
            service_name="s3",
            region_name=FAKE_REMOTE_ARGS["region_name"])
    conn.create_bucket(Bucket=FAKE_REMOTE_ARGS["bucket"])

    expected_results = []
    for i in range(0, 1200):
        conn.Object(FAKE_REMOTE_ARGS["bucket"], f"/fakeobject-{i:04}").put(Body=bytearray(10))
        expected_results.append((f"fakeobject-{i:04}", 10))

    s3_remote = S3Remote(**FAKE_REMOTE_ARGS)

    for i, (path, size) in enumerate(s3_remote._list_objects()): # pylint: disable=protected-access
        assert path == expected_results[i][0]
        assert size == expected_results[i][1]


@mock_aws
def test_remote_fetch_ok():
    """ Should return the list of backups stored in the S3 bucket. """

    conn = boto3.resource(
            service_name="s3",
            region_name=FAKE_REMOTE_ARGS["region_name"])
    conn.create_bucket(Bucket=FAKE_REMOTE_ARGS["bucket"])
    conn.Object(FAKE_REMOTE_ARGS["bucket"], "/alpha").put(Body=bytearray(55))
    conn.Object(FAKE_REMOTE_ARGS["bucket"], "/beta").put(Body=bytearray(93))
    conn.Object(FAKE_REMOTE_ARGS["bucket"], "/postgres.mydb.20210806-0022.dump").put(
            Body=bytearray(22))
    conn.Object(FAKE_REMOTE_ARGS["bucket"], "/postgres.myotherdb.20210806-0022.dump.gpg").put(
            Body=bytearray(48))

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
    """ Should successfully remove a backup file from the S3 bucket. """

    conn = boto3.resource(
            service_name="s3",
            region_name=FAKE_REMOTE_ARGS["region_name"])
    conn.create_bucket(Bucket=FAKE_REMOTE_ARGS["bucket"])
    conn.Object(FAKE_REMOTE_ARGS["bucket"], "/postgres.mydb.20210806-0022.dump").put(
            Body=bytearray(22))

    database = "mydb"
    dt = datetime(2021, 8, 6, 0, 22, 48, 236214)

    backup = Backup(
            database=database,
            dt=dt,
            size=0,
            path="/postgres.mydb.20210806-0022.dump",
            encrypted=False)
    s3_remote = S3Remote(**FAKE_REMOTE_ARGS)
    s3_remote.delete_backup(backup)

    with pytest.raises(s3_remote.client.exceptions.NoSuchKey):
        conn.Object(FAKE_REMOTE_ARGS["bucket"], "/postgres.mydb.20210806-0022.dump").get()

@freeze_time("2026-01-01 00:00:01")
@mock_aws
def test_remote_start_upload():
    """ Should create an Upload object with the correct values. """

    conn = boto3.resource(
            service_name="s3",
            region_name=FAKE_REMOTE_ARGS["region_name"])
    conn.create_bucket(Bucket=FAKE_REMOTE_ARGS["bucket"])

    s3_remote = S3Remote(**FAKE_REMOTE_ARGS)
    upload = s3_remote.start_upload("mydb", False)

    assert upload.start_time == datetime(2026, 1, 1, 0, 0, 1)
    assert upload.target['Bucket'] == FAKE_REMOTE_ARGS["bucket"]
    assert upload.target['Key'] == "/postgres.mydb.20260101-0000.dump"

@mock_aws
def test_remote_upload_upload_part():
    """ Should successfully upload a part of file to S3. """

    conn = boto3.resource(
            service_name="s3",
            region_name=FAKE_REMOTE_ARGS["region_name"])
    conn.create_bucket(Bucket=FAKE_REMOTE_ARGS["bucket"])

    s3_remote = S3Remote(**FAKE_REMOTE_ARGS)
    upload = s3_remote.start_upload("mydb", False)
    upload.upload_part(bytearray(100), 100, 100)

    assert len(upload.parts) == 1
    assert upload.bytes_uploaded == 100

@mock_aws
def test_remote_upload_complete():
    """ Should successfully upload an entire file to S3. """

    conn = boto3.resource(
            service_name="s3",
            region_name=FAKE_REMOTE_ARGS["region_name"])
    conn.create_bucket(Bucket=FAKE_REMOTE_ARGS["bucket"])

    s3_remote = S3Remote(**FAKE_REMOTE_ARGS)
    upload = s3_remote.start_upload("mydb", False)
    upload.upload_part(bytearray(100), 100, 100)
    upload.complete()

@mock_aws
def test_remote_upload_abort():
    """ Should successfully abort an upload of a file to S3. """

    conn = boto3.resource(
            service_name="s3",
            region_name=FAKE_REMOTE_ARGS["region_name"])
    conn.create_bucket(Bucket=FAKE_REMOTE_ARGS["bucket"])

    s3_remote = S3Remote(**FAKE_REMOTE_ARGS)
    upload = s3_remote.start_upload("mydb", False)
    upload.upload_part(bytearray(100), 100, 100)
    upload.abort()
