import re
import datetime

import boto3
from botocore.errorfactory import NoSuchUpload

from pg253.configuration import Configuration


class Remote:

    DATETIME_FORMAT = '%Y%m%d-%H%M'
    PARSE_FILENAME = re.compile(r'postgres.([^\.]+).([^\.]+).dump')
    BACKUPS = {}
    CLIENT = boto3.client(
        's3',
        region_name=Configuration.get('aws_s3_region_name'),
        endpoint_url=Configuration.get('aws_endpoint'),
        aws_access_key_id=Configuration.get('aws_access_key_id'),
        aws_secret_access_key=Configuration.get('aws_secret_access_key'))

    @staticmethod
    def generateKey(database, dt=datetime.datetime.now()):
        return ('%spostgres.%s.%s.dump'
                % (Configuration.get('aws_s3_prefix'),
                   database,
                   dt.strftime('%Y%m%d-%H%M')))

    @staticmethod
    def fetch(*unused):
        for filename, size in Remote.list():
            if Remote.PARSE_FILENAME.search(filename):
                matches = Remote.PARSE_FILENAME.match(filename)
                database = matches.group(1)
                date = datetime.datetime.strptime(matches.group(2),
                                                  Remote.DATETIME_FORMAT)
                Remote.add(database, date, size)

    @staticmethod
    def list():

        prefix = Configuration.get('aws_s3_prefix')
        continuation_token = None
        marker = None
        fetch_method = "V2"
        # prefix_length = len(prefix)
        while True:
            s3_args = {
                "Bucket": Configuration.get('aws_s3_bucket'),
                "Prefix": prefix,
            }
            if fetch_method == "V2" and continuation_token:
                s3_args["ContinuationToken"] = continuation_token
            if fetch_method == "V1" and marker:
                s3_args["Marker"] = marker

            # Fetch results by on method
            if fetch_method == "V1":
                response = Remote.CLIENT.list_objects(**s3_args)
            elif fetch_method == "V2":
                response = Remote.CLIENT.list_objects_v2(**s3_args)
            else:
                raise Exception("Invalid fetch method")

            if response['ResponseMetadata']['HTTPStatusCode'] >= 300:
                raise Exception('Error during listing of %s'
                                % prefix)

            # Check if pagination is broken in V2
            if (fetch_method == "V2" and response.get("IsTruncated")
                    and "NextContinuationToken" not in response):
                # Fallback to list_object() V1 if NextContinuationToken
                # is not in response
                print("Pagination broken, falling back to list_object V1")
                fetch_method = "V1"
                response = Remote.CLIENT.list_objects(**s3_args)

            for item in response.get("Contents", []):
                # print('Item: %s' % item['Key'])
                # path = item['Key'][prefix_length:len(item['Key'])]
                path = item['Key'][len(prefix):len(item['Key'])]
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

    @staticmethod
    def add(database, date, size):
        if database not in Remote.BACKUPS:
            Remote.BACKUPS[database] = [(date, size)]
        else:
            Remote.BACKUPS[database].append((date, size))

    @staticmethod
    def delete(database, date, size):
        # Build filename
        filename = Remote.generateKey(database, date)

        # Delete on object storage
        res = Remote.CLIENT.delete_object(
            Bucket=Configuration.get('aws_s3_bucket'),
            Key=filename,
        )
        if res['ResponseMetadata']['HTTPStatusCode'] >= 300:
            raise Exception('Error during deletion of %s'
                            % filename)

        # Update local cache
        Remote.BACKUPS[database].remove((date, size))

    @staticmethod
    def createUpload(database):
        now = datetime.datetime.now()
        key = Remote.generateKey(database, dt=now)
        return Upload(database, now, Configuration.get('aws_s3_bucket'), key)


class Upload:

    def __init__(self, database, start_time, bucket, key):
        self.database = database
        self.start_time = start_time
        self.target = {'Bucket': bucket,
                       'Key': key}
        multipart_upload = Remote.CLIENT.create_multipart_upload(**self.target)
        self.upload_id = multipart_upload['UploadId']
        self.part_count = 1
        self.parts = []
        self.bytes_uploaded = 0

    def getBytesUploaded(self):
        return self.bytes_uploaded

    def uploadPart(self, body, size, buffer_size):
        res = Remote.CLIENT.upload_part(**self.target,
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
            res = Remote.CLIENT.abort_multipart_upload(**self.target,
                                                       UploadId=self.upload_id)
            if res['ResponseMetadata']['HTTPStatusCode'] >= 300:
                raise Exception('Error during abort of upload  of %s'
                                % self.target)
        except NoSuchUpload:
            pass

    def complete(self):
        res = Remote.CLIENT.complete_multipart_upload(**self.target,
                                                      MultipartUpload={'Parts': self.parts},
                                                      UploadId=self.upload_id)
        if res['ResponseMetadata']['HTTPStatusCode'] >= 300:
            raise Exception('Error during complete of upload  of %s'
                            % self.target)
        Remote.add(self.database, self.start_time, self.bytes_uploaded)
