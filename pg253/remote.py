import re
import datetime

from pg253.clientS3 import ClientS3


class Remote:

    DATETIME_FORMAT = '%Y%m%d-%H%M'

    def __init__(self, config):
        self.parse_filename = re.compile(r'postgres.([^\.]+).([^\.]+).dump')
        self.config = config
        self.client = ClientS3(self.config)

    def generateKey(self, database, dt=datetime.datetime.now()):
        return ('%spostgres.%s.%s.dump'
                % (self.config.aws_s3_prefix,
                   database,
                   dt.strftime('%Y%m%d-%H%M')))

    def fetch(self):
        self.backups = {}
        for item in self.client.listContent(self.config.aws_s3_prefix):
            if self.parse_filename.search(item):
                matches = self.parse_filename.match(item)
                database = matches.group(1)
                date = datetime.datetime.strptime(matches.group(2),
                                                  Remote.DATETIME_FORMAT)
                if database not in self.backups:
                    self.backups[database] = [date]
                else:
                    self.backups[database].append(date)

    def delete(self, database, date):
        # Build filename
        filename = self.generateKey(database, date)
        self.client.delete(filename)
