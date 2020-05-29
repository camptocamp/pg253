from subprocess import Popen, PIPE
import datetime

from pg253.remote import Remote
from pg253.configuration import Configuration


class Transfer:
    def __init__(self, database, metrics):
        self.database = database
        self.metrics = metrics
        self.buffer_size = int(Configuration.get('buffer_size'))
        self.buffer = bytearray(self.buffer_size)
        self.key = Remote.generateKey(database)

    def run(self):
        input_cmd = 'pg_dump -Fc -v -d %s' % self.database
        upload = Remote.createUpload(self.database)
        self.metrics.resetTransfer(self.database)

        print('%s --> %s' % (input_cmd, self.key))
        with Popen(input_cmd.split(), stdout=PIPE, stderr=PIPE) as input:
            while True:

                self.metrics.setPart(self.database, upload.getPart())

                # Retrieve data from input in the buffer
                bytes_read = input.stdout.readinto(self.buffer)
                self.metrics.incrementRead(self.database, bytes_read)

                if bytes_read == 0:
                    break

                # Push buffer to object storage
                res = upload.uploadPart(self.buffer,
                                        bytes_read,
                                        self.buffer_size)
                print(res)
                self.metrics.incrementWrite(self.database, bytes_read)
                print('Write %s bytes' % bytes_read)

            if input.poll() is not None:
                if upload.getBytesUploaded() == 0 or input.returncode != 0:
                    upload.abort()
                    raise Exception(
                        'Error: no data transfered or error on pg_dump: %s'
                        % input.stderr.read())
                else:
                    upload.complete()
                    self.metrics.refreshMetrics()
                print('%s bytes written'
                      % self.metrics.getCurrentWrite(self.database))
            else:
                raise Exception('Read of input finished but process is not finished, should not happen')

        print('Done')
