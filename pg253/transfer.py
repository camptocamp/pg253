from subprocess import Popen, PIPE
from datetime import datetime

from pg253.remote import Remote
from pg253.configuration import Configuration
from pg253.utils import sizeof_fmt


class Transfer:
    def __init__(self, database, metrics):
        self.database = database
        self.metrics = metrics
        self.buffer_size = int(Configuration.get('buffer_size'))
        self.buffer = bytearray(self.buffer_size)
        self.key = Remote.generateKey(database)

    def run(self):
        backup_start = datetime.now()
        input_cmd = 'pg_dump -Fc -v -d %s' % self.database
        upload = Remote.createUpload(self.database)
        self.metrics.resetTransfer(self.database)

        print("Begin backup of '%s' database to %s/%s"
              % (self.database, upload.target['Bucket'], upload.target['Key']))

        with Popen(input_cmd.split(), stdout=PIPE, stderr=PIPE) as input:
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
                        % input.stderr.read())

                upload.complete()
                self.metrics.addBackup(self.database, upload.start_time)
                backup_end = datetime.now()
                self.metrics.setBackupDuration(self.database, backup_end.timestamp() - backup_start.timestamp())
                self.metrics.refreshMetrics()
                print("End backup of '%s'" % self.database)
            else:
                raise Exception('Read of input finished but process is not finished, should not happen')
