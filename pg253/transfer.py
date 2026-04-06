from subprocess import Popen, PIPE
from datetime import datetime
from threading import Thread
from gnupg import GPG

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
        self.encryption_passphrase = str(Configuration.get('encryption_passphrase'))

    def upload_data(self, chunk):
        if len(chunk) == 0:
            return False

        self.metrics.setPart(self.database, self.upload.part_count)

        # Retrieve data from input in the buffer
        self.metrics.incrementRead(self.database, len(chunk))

        # Push buffer to object storage
        self.upload.uploadPart(chunk,
                          len(chunk),
                          self.buffer_size)

        self.metrics.incrementWrite(self.database, len(chunk))

        print('  Part %s, %s bytes written'
              % (self.upload.part_count - 1, sizeof_fmt(self.upload.bytes_uploaded)))

        return False

    def run(self):
        backup_start = datetime.now()
        # Use compression level 1 to reduce CPU pressure, keep an acceptable
        # transfer rate and reduce the size of backups to a minimum
        input_cmd = 'pg_dump -Fc -Z1 -v -d %s' % self.database
        self.upload = Remote.createUpload(self.database)
        self.metrics.resetTransfer(self.database)

        print("Begin backup of '%s' database to %s/%s"
              % (self.database, self.upload.target['Bucket'], self.upload.target['Key']))

        with Popen(input_cmd.split(), stdout=PIPE, stderr=PIPE) as input:
            s = StdErr(input.stderr)
            s.start()

            if self.encryption_passphrase != "":
                gpg = GPG()
                gpg.buffer_size = self.buffer_size
                gpg.on_data = self.upload_data
                gpg.encrypt_file(
                    input.stdout,
                    recipients=[],
                    passphrase=self.encryption_passphrase,
                    symmetric=True)
            else:
                while data := input.stdout.read(self.buffer_size):
                    if len(data) == 0:
                        break
                    self.upload_data(bytes(data))

            if input.poll() is not None:
                if self.upload.getBytesUploaded() == 0 or input.returncode != 0:
                    self.upload.abort()
                    raise Exception(
                        'Error: no data transfered or error on pg_dump: %s'
                        % s.output)

                self.upload.complete()
                self.metrics.addBackup(self.database, self.upload.start_time, self.upload.bytes_uploaded)
                backup_end = datetime.now()
                self.metrics.setBackupDuration(self.database, backup_end.timestamp() - backup_start.timestamp())
                self.metrics.refreshMetrics()
                print("End backup of '%s'" % self.database)
            else:
                raise Exception('Read of input finished but process is not finished, should not happen')
