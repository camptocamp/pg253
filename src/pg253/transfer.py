""" Module containing the functions to dump PostgreSQL databases. """

from subprocess import Popen, PIPE
from datetime import datetime
from threading import Thread
from dataclasses import dataclass
import logging
from gnupg import GPG

from pg253.utils import sizeof_fmt
from pg253.remote import S3Remote, Upload
from pg253.metrics import Metrics


class StdErr(Thread):
    """ Overrides the default Thread class. """
    def __init__(self, stream):
        Thread.__init__(self)
        self.stream = stream
        self.output = ""

    def run(self):
        while True:
            output = self.stream.read().decode()
            if len(output) == 0:
                break
            self.output += output


@dataclass
class Transfer: # pylint: disable=too-few-public-methods
    """ Coordinates the PostgreSQL dump and sending of the dump to S3. """

    metrics: Metrics
    buffer_size: int
    s3_remote: S3Remote
    encryption_passphrase: str
    buffer: bytearray = None
    upload: Upload = None
    database: str = ''

    def __post_init__(self):
        self.buffer = bytearray(int(self.buffer_size))

    def upload_data(self, chunk):
        """ Uploads a chunk to the remote bucket. """

        if len(chunk) == 0:
            return False

        self.metrics.set_part(self.database, len(self.upload.parts))

        # Retrieve data from input in the buffer
        self.metrics.increment_read(self.database, len(chunk))

        # Push buffer to object storage
        self.upload.upload_part(chunk,
                          len(chunk),
                          self.buffer_size)

        self.metrics.increment_write(self.database, len(chunk))

        logging.info("Backup of database '%s': upload part %d, %s bytes written",
            self.database,
            len(self.upload.parts),
            sizeof_fmt(self.upload.bytes_uploaded))

        return False


    def backup_database(self, database, dump_cmd):
        """ Execute a PostgreSQL dump and upload it in multiple parts to S3. """

        self.database = database
        backup_start = datetime.now()
        # Use compression level 1 to reduce CPU pressure, keep an acceptable
        # transfer rate and reduce the size of backups to a minimum
        encrypted = self.encryption_passphrase != ''
        self.upload = self.s3_remote.start_upload(self.database, encrypted)
        self.metrics.reset_transfer(self.database)

        logging.info("Starting backup of database '%s' to %s/%s...",
            self.database,
            self.upload.target['Bucket'],
            self.upload.target['Key'])

        with Popen(dump_cmd.split(), stdout=PIPE, stderr=PIPE) as cmd_exec:
            s = StdErr(cmd_exec.stderr)
            s.start()

            if self.encryption_passphrase != '':
                gpg = GPG()
                gpg.buffer_size = self.buffer_size
                gpg.on_data = self.upload_data
                gpg.encrypt_file(
                    cmd_exec.stdout,
                    recipients=[],
                    passphrase=self.encryption_passphrase,
                    symmetric=True)
            else:
                while data := cmd_exec.stdout.read(self.buffer_size):
                    if len(data) == 0:
                        break
                    self.upload_data(bytes(data))

            if cmd_exec.poll() is not None:
                if self.upload.bytes_uploaded == 0 or cmd_exec.returncode != 0:
                    self.upload.abort()
                    raise RuntimeError(
                        f"Error: no data transfered or error on pg_dump: {s.output}")

                self.upload.complete()
                self.metrics.add_backup(
                        self.database,
                        self.upload.start_time,
                        self.upload.bytes_uploaded,
                        encrypted)
                backup_end = datetime.now()
                self.metrics.set_backup_duration(
                        self.database,
                        backup_end.timestamp() - backup_start.timestamp())
                self.metrics.refresh_metrics()
                logging.info("Backup of database '%s' has been successfully uploaded.",
                             self.database)
            else:
                raise RuntimeError(
                        'Read of input finished but process is not finished, should not happen')
