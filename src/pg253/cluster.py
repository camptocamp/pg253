import re
from datetime import datetime, timedelta
from subprocess import run

from pg253.transfer import Transfer
from pg253.configuration import Configuration


class Cluster:
    def __init__(self, metrics, remote):
        self.running = False
        self.metrics = metrics
        self.db_exclude = \
            re.compile(Configuration.get('blacklisted_databases'))
        self.buffer_size = int(Configuration.get('BUFFER_SIZE'))
        self.retention_days = int(Configuration.get('RETENTION_DAYS'))
        self.remote = remote

    def listDatabase(self):
        cmd = ['psql', '-qAtX', '-c', 'SELECT datname FROM pg_database']
        res = run(cmd, capture_output=True)
        if res.returncode != 0:
            raise Exception('Unable to retrieve database list: %s'
                            % res.stderr.decode())
        dbs = res.stdout.decode().strip().split("\n")
        dbs = list(filter(lambda x: not self.db_exclude.search(x), dbs))
        return dbs

    def backup_and_prune(self, *unused):
        if not self.running:
            try:
                self.running = True
                print("Backup...")
                self.backup()
                print("Prune...")
                self.prune(self.retention_days)
                self.running = False
                self.metrics.error.labels('').set(0)
            except Exception as e:
                self.running = False
                self.metrics.error.labels('').set(1)
                raise e
        else:
            print('Backup already running')

    def backup(self):
        for database in self.listDatabase():
            try:
                Transfer(database, self.metrics, self.buffer_size, self.remote).run()
                self.metrics.error.labels(database).set(0)
            except Exception as e:
                self.metrics.error.labels(database).set(1)
                raise e


    def prune(self, retention):
        for backup in self.remote.fetch_backups():
            if backup.dt + timedelta(days=retention) < datetime.now():
                print("Remove backup of '%s' at %s" % (backup.database, backup.dt))
                self.remote.delete_backup(backup)
                self.metrics.removeBackup(database, to_delete[0], to_delete[1])
