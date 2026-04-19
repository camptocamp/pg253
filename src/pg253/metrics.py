""" Module managing the Prometheus metrics collection. """

from prometheus_client import start_http_server
from prometheus_client import Gauge, Counter

class Metrics: # pylint: disable=too-many-instance-attributes
    """ Contains all Prometheus metrics and starts the exporter. """

    def __init__(self, remote, exporter_port):
        self.remote = remote
        self.current_read = {}
        self.current_write = {}

        # Start and configure prometheus exporter
        start_http_server(int(exporter_port))

        self.total_bytes_read = (
            Counter('total_bytes_read',
                    'Total bytes reads from input'))
        self.current_bytes_read = (
            Gauge('current_bytes_read',
                  'Bytes reads from input for current transfer',
                  ['database']))
        self.total_bytes_write = (
            Counter('total_bytes_write',
                    'Total bytes uploaded to object storage'))
        self.current_bytes_write = (
            Gauge('current_bytes_write',
                  'Bytes uploaded to object storage for current transfer',
                  ['database']))
        self.part_count = (
            Gauge('part_count',
                  'Part of the Multipart upload uploaded',
                  ['database']))
        self.first_backup = (
            Gauge('first_backup',
                  'Date of first backup',
                  ['database']))
        self.last_backup = (
            Gauge('last_backup',
                  'Date of last backup',
                  ['database']))
        self.backups = (
            Gauge('backups',
                  'All backups',
                  ['database', 'date', 'size']))
        self.backup_duration = (
            Gauge('backup_duration',
                  'Duration of backup',
                  ['database']))
        self.error = (
            Gauge('error',
                  'Error during backup',
                  ['database']))
        self._read_remote_backup()


    def _read_remote_backup(self):
        """ Fetch remote backups and set metrics. """

        self.refresh_metrics()
        for backup in self.remote.fetch_backups():
            self.add_backup(backup.database, backup.dt, backup.size)


    def refresh_metrics(self):
        """ Fetch remote backups and set the first and last backups metrics. """

        self.first_backup.clear()
        self.last_backup.clear()
        backup_dates_per_db = {}
        for backup in self.remote.fetch_backups():
            if backup.database in backup_dates_per_db:
                backup_dates_per_db[backup.database].append(backup.dt)
            else:
                backup_dates_per_db[backup.database] = [backup.dt]

        for database, backup_dates in backup_dates_per_db.items():
            self.first_backup.labels(database).set(min(backup_dates).timestamp())
            self.last_backup.labels(database).set(max(backup_dates).timestamp())

    def remove_backup(self, database, date, size):
        """ Remove a "backups" metric. """

        self.backups.remove(database, date.strftime('%Y%m%d-%H%M'), size)
        self.refresh_metrics()

    def add_backup(self, database, date, size):
        """ Add a "backups" metric. """

        self.backups.labels(database, date.strftime('%Y%m%d-%H%M'), size).set(date.timestamp())
        self.refresh_metrics()

    def set_last_backup(self, database, backup_datetime):
        """ Set a "last_backup" metric. """

        self.last_backup.labels(database).set(backup_datetime.timestamp())

    def set_backup_duration(self, database, duration):
        """ Set a "backup_duration" metric. """

        self.backup_duration.labels(database).set(duration)

    def reset_transfer(self, database):
        """ Reset the transfer related metrics. """

        self.current_bytes_read.labels(database).set(0)
        self.current_bytes_write.labels(database).set(0)
        self.current_read[database] = 0
        self.current_write[database] = 0

    def increment_read(self, database, count):
        """ Increment the bytes read related metrics. """

        self.current_bytes_read.labels(database).inc(count)
        self.current_read[database] += count
        self.total_bytes_read.inc(count)

    def increment_write(self, database, count):
        """ Increment the bytes write related metrics. """

        self.current_bytes_write.labels(database).inc(count)
        self.current_write[database] += count
        self.total_bytes_write.inc(count)

    def set_part(self, database, count):
        """ Increment the part count metrics. """

        self.part_count.labels(database).set(count)

    def get_current_read(self, database):
        """ Returns the current read metric for a database. """

        return self.current_read[database]

    def get_current_write(self, database):
        """ Returns the current write metric for a database. """

        return self.current_write[database]
