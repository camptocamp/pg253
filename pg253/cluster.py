import re
from datetime import datetime, timedelta
from subprocess import run

from pg253.transfer import Transfer
from pg253.remote import Remote
from pg253.configuration import Configuration


class Cluster:
    def __init__(self, metrics):
        self.metrics = metrics
        self.db_exclude = \
            re.compile(Configuration.get('blacklisted_databases'))

    def listDatabase(self):
        cmd = ['psql', '-qAtX', '-c', 'SELECT datname FROM pg_database']
        res = run(cmd, capture_output=True)
        if res.returncode != 0:
            raise Exception('Unable to retrieve database list: %s'
                            % res.stderr.decode())
        dbs = res.stdout.decode().strip().split("\n")
        dbs = list(filter(lambda x: not self.db_exclude.search(x), dbs))
        dbs.remove('template0')
        return dbs

    def backup_and_prune(self):
        print("Backup...")
        self.backup()
        print("Prune...")
        self.prune()

    def backup(self):
        for database in self.listDatabase():
            backup_start = datetime.now()
            print("Begin backup of '%s' database" % database)
            transfer = Transfer(database, self.metrics)
            transfer.run()
            backup_end = datetime.now()
            self.metrics.setLastBackup(database, backup_end)
            self.metrics.setBackupDuration(database, backup_end.timestamp() - backup_start.timestamp())
            print('End backup of %s' % database)

    def prune(self):
        # Compute date of oldest backup we need to keep
        delete_before = \
            (datetime.now()
             - timedelta(days=float(Configuration.get('retention_days'))))
        for database in Remote.BACKUPS:
            for to_delete in [dt for dt in Remote.BACKUPS[database]
                              if dt < delete_before]:
                print("Remove backup of '%s' at %s" % (database, to_delete))
                Remote.delete(database, to_delete)
                self.metrics.removeBackup(database, to_delete)
