""" Module managing the backups on the remote storage. """

import re
import logging
from datetime import datetime
from dataclasses import dataclass, field
from botocore.client import BaseClient
import boto3


DATETIME_FORMAT = '%Y%m%d-%H%M'
PARSE_FILENAME = re.compile(r'postgres.([^\.]+).([^\.]+).dump($|.gpg)')

@dataclass
class Backup:
    """ Stores information related to a single PostgreSQL database backup. """
    database: str
    dt: datetime
    size: int
    path: str

@dataclass
class S3Remote: # pylint: disable=too-many-instance-attributes
    """
    Manages the operations related to a remote S3 bucket.
    It allows to retrieve the backups, delete and upload them.
    """

    endpoint_url: str
    region_name: str
    access_key: str
    secret_key: str
    bucket: str
    path_prefix: str
    client: BaseClient = None
    s3_args: dict = None

    def __post_init__(self):
        self.client = boto3.client(
            's3',
            endpoint_url=self.endpoint_url,
            region_name=self.region_name,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key)
        self.s3_args = {
            'Bucket': self.bucket,
            'Prefix': self.path_prefix,
        }

    def generate_filename(self, database, dt):
        """ Generate an object path for a backup. """

        return f"{self.path_prefix}postgres.{database}.{dt.strftime(DATETIME_FORMAT)}.dump"

    def fetch_backups(self):
        """ Retrieve backups from the remote bucket. """

        backups = []
        for path, size in self._list_objects():
            if PARSE_FILENAME.search(path):
                matches = PARSE_FILENAME.match(path)
                database = matches.group(1)
                date = datetime.strptime(
                        matches.group(2),
                        DATETIME_FORMAT)

                backup = Backup(
                        database=database,
                        dt=date,
                        size=size,
                        path=path)
                backups.append(backup)
        return backups

    def _list_objects(self):
        """ List all objects from the remote bucket. """

        if 'Marker' in self.s3_args:
            response = self.client.list_objects(**self.s3_args)
        else:
            response = self.client.list_objects_v2(**self.s3_args)

        if response['ResponseMetadata']['HTTPStatusCode'] >= 300:
            raise RuntimeError(f"Error during listing of {self.path_prefix}")

        if (response.get('IsTruncated') and
                'NextContinuationToken' not in response):
            logging.warning("Pagination broken, falling back to list_object V1")
            response = self.client.list_objects(**self.s3_args)

        for item in response.get("Contents", []):
            path = item['Key'][len(self.path_prefix):len(item['Key'])]
            size = int(item['Size'])
            if '/' not in path:
                yield (path, size)

        if response.get("IsTruncated"):
            if response.get('NextMarker'):
                self.s3_args['Marker'] = response.get('NextMarker')
            elif response.get('NextContinuationToken'):
                self.s3_args['ContinuationToken'] = response.get('NextContinuationToken')
            yield from self._list_objects()

    def delete_backup(self, backup):
        """ Delete backup from the S3 remote bucket. """

        res = self.client.delete_object(
            Bucket=self.bucket,
            Key=backup.path,
        )
        if res['ResponseMetadata']['HTTPStatusCode'] >= 300:
            raise RuntimeError(f"Error during deletion of {backup.path}")

    def start_upload(self, database):
        """ Returns an Upload object that manages multipart uploads. """

        now = datetime.now()
        path = self.generate_filename(database, dt=now)
        return Upload(
                remote=self,
                start_time=now,
                path=path)


@dataclass
class Upload:
    """ Manages the operations related to uploading a backup on a remote S3 bucket. """

    remote: S3Remote
    start_time: datetime
    path: str
    upload_id: str = ""
    parts: list[str] = field(default_factory=list)
    bytes_uploaded: int = 0
    target: dict = None

    def __post_init__(self):
        self.target = {
                'Bucket': self.remote.bucket,
                'Key': self.path,
        }
        multipart_upload = self.remote.client.create_multipart_upload(**self.target)
        self.upload_id = multipart_upload['UploadId']

    def upload_part(self, body, size, buffer_size):
        """ Sends a part of a file to the S3 bucket. """

        next_part_number = len(self.parts) + 1
        res = self.remote.client.upload_part(**self.target,
                                        UploadId=self.upload_id,
                                        PartNumber=next_part_number,
                                        Body=body if size == buffer_size
                                        else body[0:size])

        if res['ResponseMetadata']['HTTPStatusCode'] >= 300:
            raise RuntimeError(f"Error during upload of part {next_part_number} of {self.target}")
        self.parts.append({'ETag': res['ETag'], 'PartNumber': next_part_number})
        self.bytes_uploaded += size
        return res

    def abort(self):
        """ Aborts a running multipart upload. """

        try:
            res = self.remote.client.abort_multipart_upload(**self.target,
                                                       UploadId=self.upload_id)
            if res['ResponseMetadata']['HTTPStatusCode'] >= 300:
                raise RuntimeError(f"Error during abort of upload  of {self.target}")
        except self.remote.client.exceptions.NoSuchUpload:
            pass

    def complete(self):
        """ Completes a running multipart upload. """

        res = self.remote.client.complete_multipart_upload(**self.target,
                                                      MultipartUpload={'Parts': self.parts},
                                                      UploadId=self.upload_id)
        if res['ResponseMetadata']['HTTPStatusCode'] >= 300:
            raise RuntimeError(f"Error during complete of upload  of {self.target}")
