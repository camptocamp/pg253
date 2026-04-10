from subprocess import Popen, PIPE
from datetime import datetime
from threading import Thread

from pg253.remote import Remote
from pg253.configuration import Configuration
from pg253.utils import sizeof_fmt

class StdErr(Thread):
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


class Transfer:
    def __init__(self, database, metrics):
        self.database = database
        self.metrics = metrics
        self.buffer_size = int(Configuration.get('buffer_size'))
        self.buffer = bytearray(self.buffer_size)
        self.key = Remote.generateKey(database)

    def run(self):
        backup_start = datetime.now()
        # Use compression level 1 to reduce CPU pressure, keep an acceptable
        # transfer rate and reduce the size of backups to a minimum
        input_cmd = 'pg_dump -Fc -Z1 -v -d %s' % self.database
        upload = Remote.createUpload(self.database)
        self.metrics.resetTransfer(self.database)

        print("Begin backup of '%s' database to %s/%s"
              % (self.database, upload.target['Bucket'], upload.target['Key']))

        with Popen(input_cmd.split(), stdout=PIPE, stderr=PIPE) as input:
            s = StdErr(input.stderr)
            s.start()
            while True:
                self.metrics.setPart(self.database, upload.part_count)

                # Retrieve data from input in the buffer
                bytes_read = input.stdout.readinto(self.buffer)
                self.metrics.incrementRead(self.database, bytes_read)

                if bytes_read == 0:
                    break

                # Push buffer to object storage
                upload.uploadPart(self.buffer,
                                  bytes_read,
                                  self.buffer_size)

                self.metrics.incrementWrite(self.database, bytes_read)
                print('  Part %s, %s bytes written'
                      % (upload.part_count - 1, sizeof_fmt(upload.bytes_uploaded)))

            if input.poll() is not None:
                if upload.getBytesUploaded() == 0 or input.returncode != 0:
                    upload.abort()
                    raise Exception(
                        'Error: no data transfered or error on pg_dump: %s'
                        % s.output)

                upload.complete()
                self.metrics.addBackup(self.database, upload.start_time, upload.bytes_uploaded)
                backup_end = datetime.now()
                self.metrics.setBackupDuration(self.database, backup_end.timestamp() - backup_start.timestamp())
                self.metrics.refreshMetrics()
                print("End backup of '%s'" % self.database)
            else:
                raise Exception('Read of input finished but process is not finished, should not happen')
