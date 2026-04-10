import re
import datetime
from dataclasses import dataclass
import boto3


DATETIME_FORMAT = '%Y%m%d-%H%M'
PARSE_FILENAME = re.compile(r'postgres.([^\.]+).([^\.]+).dump($|.gpg)')

@dataclass
class Backup:
    database: str
    dt: datetime.datetime
    size: int
    path: str

@dataclass
class S3Remote:
    endpoint_url: str
    region_name: str
    access_key: str
    secret_key: str
    bucket: str
    path_prefix: str
    client: None

    def __post_init__(self):
        self.client = boto3.client(
            's3',
            endpoint_url=self.endpoint_url,
            region_name=self.region_name,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key)


    """ Generate an object path for a backup """
    def generate_filename(self, database, dt):
        suffix = ""
        return '%spostgres.%s.%s.dump%s' % (
                self.path_prefix,
                database,
                dt.strftime(DATETIME_FORMAT),
                suffix)

    """ Retrieve backups from the remote bucket """
    def fetch_backups(self):
        backups = []
        for path, size in self._list():
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

    """ List all objects from the remote bucket """
    def _list(self):
        continuation_token = None
        marker = None
        fetch_method = "V2"
        # prefix_length = len(prefix)
        while True:
            s3_args = {
                "Bucket": self.bucket,
                "Prefix": self.path_prefix,
            }
            if fetch_method == "V2" and continuation_token:
                s3_args["ContinuationToken"] = continuation_token
            if fetch_method == "V1" and marker:
                s3_args["Marker"] = marker

            # Fetch results by on method
            if fetch_method == "V1":
                response = self.client.list_objects(**s3_args)
            elif fetch_method == "V2":
                response = self.client.list_objects_v2(**s3_args)
            else:
                raise Exception("Invalid fetch method")

            if response['ResponseMetadata']['HTTPStatusCode'] >= 300:
                raise Exception('Error during listing of %s'
                                % self.path_prefix)

            # Check if pagination is broken in V2
            if (fetch_method == "V2" and response.get("IsTruncated")
                    and "NextContinuationToken" not in response):
                # Fallback to list_object() V1 if NextContinuationToken
                # is not in response
                print("Pagination broken, falling back to list_object V1")
                fetch_method = "V1"
                response = self.client.list_objects(**s3_args)

            for item in response.get("Contents", []):
                # print('Item: %s' % item['Key'])
                # path = item['Key'][prefix_length:len(item['Key'])]
                path = item['Key'][len(self.path_prefix):len(item['Key'])]
                size = int(item['Size'])
                if '/' not in path:
                    yield (path, size)

            if response.get("IsTruncated"):
                if fetch_method == "V1":
                    marker = response.get('NextMarker')
                elif fetch_method == "V2":
                    continuation_token = response["NextContinuationToken"]
                else:
                    raise Exception("Invalid fetch method")
            else:
                break

    """ Delete backup from the S3 remote bucket """
    def delete_backup(backup):
        res = self.client.delete_object(
            Bucket=self.bucket,
            Key=backup.path,
        )
        if res['ResponseMetadata']['HTTPStatusCode'] >= 300:
            raise Exception('Error during deletion of %s'
                            % backup.path)

    """ Returns an Upload object that manages multipart uploads """
    def start_upload(database):
        now = datetime.datetime.now()
        path = self.generate_filename(database, dt=now)
        return Upload(self, database, now, self.bucket, path)


class Upload:

    def __init__(self, remote, database, start_time, path):
        self.remote = remote
        self.database = database
        self.start_time = start_time
        self.target = {'Bucket': self.remote.bucket,
                       'Key': path}
        multipart_upload = self.remote.client.create_multipart_upload(**self.target)
        self.upload_id = multipart_upload['UploadId']
        self.part_count = 1
        self.parts = []
        self.bytes_uploaded = 0

    def getBytesUploaded(self):
        return self.bytes_uploaded

    def uploadPart(self, body, size, buffer_size):
        res = self.remote.upload_part(**self.target,
                                        UploadId=self.upload_id,
                                        PartNumber=self.part_count,
                                        Body=body if size == buffer_size
                                        else body[0:size])

        if res['ResponseMetadata']['HTTPStatusCode'] >= 300:
            raise Exception('Error during upload of part %s of %s'
                            % (self.part_count, self.target))
        self.parts.append({'ETag': res['ETag'], 'PartNumber': self.part_count})
        self.part_count += 1
        self.bytes_uploaded += size
        return res

    def abort(self):
        try:
            res = self.remote.client.abort_multipart_upload(**self.target,
                                                       UploadId=self.upload_id)
            if res['ResponseMetadata']['HTTPStatusCode'] >= 300:
                raise Exception('Error during abort of upload  of %s'
                                % self.target)
        except self.remote.client.exceptions.NoSuchUpload:
            pass

    def complete(self):
        res = self.remote.client.complete_multipart_upload(**self.target,
                                                      MultipartUpload={'Parts': self.parts},
                                                      UploadId=self.upload_id)
        if res['ResponseMetadata']['HTTPStatusCode'] >= 300:
            raise Exception('Error during complete of upload  of %s'
                            % self.target)
