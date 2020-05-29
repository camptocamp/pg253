import re
import datetime

from prometheus_client import start_http_server
from prometheus_client import Gauge, Counter

from pg253.remote import Remote
from pg253.configuration import Configuration


class Metrics:
    def __init__(self):
        self.current_read = {}
        self.current_write = {}

        # Start and configure prometheus exporter
        start_http_server(int(Configuration.get('prometheus_exporter_port')))

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
                  ['database', 'date']))
        self.backup_duration = (
            Gauge('backup_duration',
                  'Duration of backup',
                  ['database']))
        self._readRemoteBackup()


    def _readRemoteBackup(self):
        Remote.fetch()
        self.refreshMetrics()
        for database in Remote.BACKUPS:
            for backup_datetime in Remote.BACKUPS[database]:
                self.addBackup(database, backup_datetime)


    def refreshMetrics(self):
        for database in Remote.BACKUPS:
            (self.first_backup.labels(database)
             .set(min(Remote.BACKUPS[database]).timestamp()))
            (self.last_backup.labels(database)
             .set(max(Remote.BACKUPS[database]).timestamp()))

    def removeBackup(self, database, dt):
        self.backups.remove(database, dt.strftime('%Y%m%d-%H%M'))
        self.refreshMetrics()

    def addBackup(self, database, dt):
        self.backups.labels(database, dt.strftime('%Y%m%d-%H%M')).set(dt.timestamp())
        self.refreshMetrics()

    def setLastBackup(self, database, backup_datetime):
        self.last_backup.labels(database).set(backup_datetime.timestamp())

    def setBackupDuration(self, database, duration):
        self.backup_duration.labels(database).set(duration)

    def resetTransfer(self, database):
        self.current_bytes_read.labels(database).set(0)
        self.current_bytes_write.labels(database).set(0)
        self.current_read[database] = 0
        self.current_write[database] = 0

    def incrementRead(self, database, count):
        self.current_bytes_read.labels(database).inc(count)
        self.current_read[database] += count
        self.total_bytes_read.inc(count)

    def incrementWrite(self, database, count):
        self.current_bytes_write.labels(database).inc(count)
        self.current_write[database] += count
        self.total_bytes_write.inc(count)

    def setPart(self, database, count):
        self.part_count.labels(database).set(count)

    def getCurrentRead(self, database):
        return self.current_read[database]

    def getCurrentWrite(self, database):
        return self.current_write[database]
