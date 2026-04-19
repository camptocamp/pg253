""" Module containing the functions to dump PostgreSQL databases. """

from subprocess import Popen, PIPE
from datetime import datetime
from threading import Thread
import logging

from pg253.utils import sizeof_fmt


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


class Transfer: # pylint: disable=too-few-public-methods
    """ Coordinates the PostgreSQL dump and sending of the dump to S3. """

    def __init__(self, database, metrics, buffer_size, s3_remote):
        self.database = database
        self.metrics = metrics
        self.buffer_size = buffer_size
        self.buffer = bytearray(int(self.buffer_size))
        self.s3_remote = s3_remote

    def run(self):
        """ Execute a PostgreSQL dump and upload it in multiple parts to S3. """

        backup_start = datetime.now()
        # Use compression level 1 to reduce CPU pressure, keep an acceptable
        # transfer rate and reduce the size of backups to a minimum
        dump_cmd = f"pg_dump -Fc -Z1 -v -d {self.database}"
        upload = self.s3_remote.start_upload(self.database)
        self.metrics.reset_transfer(self.database)

        logging.info("Starting backup of database '%s' to %s/%s...",
            self.database,
            upload.target['Bucket'],
            upload.target['Key'])

        with Popen(dump_cmd.split(), stdout=PIPE, stderr=PIPE) as cmd_exec:
            s = StdErr(cmd_exec.stderr)
            s.start()
            while True:
                self.metrics.set_part(self.database, upload.part_count)

                # Retrieve data from input in the buffer
                bytes_read = cmd_exec.stdout.readinto(self.buffer)
                self.metrics.increment_read(self.database, bytes_read)

                if bytes_read == 0:
                    break

                # Push buffer to object storage
                upload.uploadPart(self.buffer,
                                  bytes_read,
                                  self.buffer_size)

                self.metrics.increment_write(self.database, bytes_read)
                logging.info("Backup of database '%s': upload part %d, %s bytes written",
                    self.database,
                    upload.part_count - 1,
                    sizeof_fmt(upload.bytes_uploaded))

            if cmd_exec.poll() is not None:
                if upload.getBytesUploaded() == 0 or cmd_exec.returncode != 0:
                    upload.abort()
                    raise RuntimeError(
                        "Error: no data transfered or error on pg_dump: {s.output}")

                upload.complete()
                self.metrics.add_backup(self.database, upload.start_time, upload.bytes_uploaded)
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
