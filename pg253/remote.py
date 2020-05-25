import re
import datetime

from pg253.clientS3 import ClientS3


class Remote:

    def __init__(self, config):
        self.parse_filename = re.compile(r'postgres.([^\.]+).([^\.]+).dump')
        self.config = config

    def fetch(self):
        client = ClientS3(self.config)
        self.backups = {}
        for item in client.listContent(self.config.aws_s3_prefix):
            if self.parse_filename.search(item):
                matches = self.parse_filename.match(item)
                database = matches.group(1)
                date = datetime.datetime.strptime(matches.group(2), '%Y%m%d-%H%M')
                if database not in self.backups:
                    self.backups[database] = [date]
                else:
                    self.backups[database].append(date)
