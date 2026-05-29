""" This module manages the application's parameters and the backup jobs scheduling. """

import os
import signal
import argparse
import subprocess
import re
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from pg253.metrics import Metrics
from pg253.remote import S3Remote
from pg253.transfer import Transfer


@dataclass
class App:
    """ Core application's class managing the backup process. """

    metrics: Metrics
    remote: S3Remote
    exclude_dbs: str
    buffer_size: int
    retention_days: int
    encryption_passphrase: str
    backup_roles: bool

    def list_databases(self):
        """ Returns the list of databases to backup. """

        dbs = []
        cmd = ['psql', '-qAtX', '-c', 'SELECT datname FROM pg_database']
        try:
            res = subprocess.run(cmd, capture_output=True, check=True)
            dbs = res.stdout.decode().strip().split("\n")
            dbs = list(filter(lambda x: not re.search(self.exclude_dbs, x), dbs))
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                    f"Unable to retrieve database list: {e.stderr.decode('utf-8')}") from e
        return dbs

    def backup_and_prune(self, *_):
        """ Run the backup and prune jobs. """

        try:
            self.backup()
            self.prune()
        except Exception as e:
            raise e

    def backup(self):
        """ For each database, create a PostgreSQL dump and upload it to S3. """

        transfer = Transfer(metrics=self.metrics,
                            buffer_size=self.buffer_size,
                            s3_remote=self.remote,
                            encryption_passphrase=self.encryption_passphrase)

        if self.backup_roles:
            try:
                transfer.backup_database('pgroles', 'pg_dumpall --roles-only --no-role-passwords')
                self.metrics.error.labels('pgroles').set(0)
            except Exception as e:
                self.metrics.error.labels('pgroles').set(1)
                raise e

        for database in self.list_databases():
            try:
                transfer.backup_database(database, f"pg_dump -Fc -Z1 -v -d {database}")
                self.metrics.error.labels(database).set(0)
            except Exception as e:
                self.metrics.error.labels(database).set(1)
                raise e

    def prune(self):
        """ For each database, create a PostgreSQL dump and upload it to S3. """

        for backup in self.remote.fetch_backups():
            if backup.dt + timedelta(days=self.retention_days) < datetime.now():
                logging.info("Removing backup of '%s' at %s...",
                             backup.database,
                             backup.dt)
                self.remote.delete_backup(backup)


def build_args():
    """ Return parameters from the command line and the environment variables """

    parser = argparse.ArgumentParser()
    parser.add_argument(
            '--schedule',
            type=str,
            default=os.getenv('SCHEDULE', '20 2 * * *'),
            help='Cron like scheduling definition to run backups.')
    parser.add_argument(
            '--metrics-port',
            type=int,
            default=os.getenv('METRICS_PORT', '9352'),
            help='Port on which to expose Prometheus metrics.')
    parser.add_argument(
            '--buffer-size',
            type=int,
            default=os.getenv('BUFFER_SIZE', '10485760'),
            help='Size of the main buffer, this parameter affect backup speed and memory usage.')
    parser.add_argument(
            '--retention-days',
            type=int,
            default=os.getenv('RETENTION_DAYS', '15'),
            help='Retention in days. Backups older than this value will be deleted.')
    parser.add_argument(
            '--exclude-databases',
            type=str,
            default=os.getenv('EXCLUDE_DATABASES', '.*backup.*|postgres|rdsadmin|rdb|template.*'),
            help='Regexp to exclude databases.')
    parser.add_argument(
            '--encryption-passphrase',
            type=str,
            default=os.getenv('ENCRYPTION_PASSPHRASE', ''),
            help='Passphrase used to encrypt backups with GPG.')
    parser.add_argument(
            '--backup-roles',
            action='store_true',
            default=os.getenv('BACKUP_ROLES', '').lower() == 'true',
            help='Backup the PostgreSQL roles.')

    # PostgreSQL parameters
    parser.add_argument(
            '--pghost',
            type=str,
            default=os.getenv('PGHOST'),
            help='Address of the PostgreSQL host to connect to.')
    parser.add_argument(
            '--pgport',
            type=int,
            default=os.getenv('PGPORT'),
            help='Port number to connect to at the PostgreSQL server.')
    parser.add_argument(
            '--pguser',
            type=str,
            default=os.getenv('PGUSER'),
            help='PostgreSQL user name to connect as.')
    parser.add_argument(
            '--pgpassword',
            type=str,
            default=os.getenv('PGPASSWORD'),
            help='Password to be used if the PostgreSQL server demands password authentication.')

    # S3 Remote parameters
    parser.add_argument(
            '--aws-endpoint-url',
            type=str,
            default=os.getenv('AWS_ENDPOINT_URL'),
            help='Custom S3 service endpoint URL.')
    parser.add_argument(
            '--aws-default-region',
            type=str,
            default=os.getenv('AWS_DEFAULT_REGION'),
            help='Region of the S3 service.')
    parser.add_argument(
            '--aws-access-key-id',
            type=str,
            default=os.getenv('AWS_ACCESS_KEY_ID'),
            help='Access key used to connect to the S3 service.')
    parser.add_argument(
            '--aws-secret-access-key',
            type=str,
            default=os.getenv('AWS_SECRET_ACCESS_KEY'),
            help='Secret key used to connect to the S3 service.')
    parser.add_argument(
            '--aws-s3-bucket',
            type=str,
            default=os.getenv('AWS_S3_BUCKET'),
            help='S3 bucket in which the backups will be stored.')
    parser.add_argument(
            '--aws-s3-prefix',
            type=str,
            default=os.getenv('AWS_S3_PREFIX', ''),
            help='Prefix to apply on objects when storing backups in the S3 bucket.')

    return parser.parse_args()


def run():
    """ Application's entry point """

    args = build_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    logging.info("Connecting to the remote S3 endpoint...")
    s3_remote = S3Remote(
            endpoint_url=args.aws_endpoint_url,
            region_name=args.aws_default_region,
            access_key=args.aws_access_key_id,
            secret_key=args.aws_secret_access_key,
            bucket=args.aws_s3_bucket,
            path_prefix=args.aws_s3_prefix)

    logging.info("Setting up the Prometheus endpoint...")
    metrics = Metrics(s3_remote, args.metrics_port)

    app = App(
            metrics=metrics,
            remote=s3_remote,
            exclude_dbs=args.exclude_databases,
            buffer_size=args.buffer_size,
            retention_days=args.retention_days,
            encryption_passphrase=args.encryption_passphrase,
            backup_roles=args.backup_roles)

    logging.info("Detected databases: %s",
                 app.list_databases())

    logging.info("Starting scheduler...")
    # Start scheduler
    scheduler = BlockingScheduler()
    scheduler.add_job(app.backup_and_prune, CronTrigger.from_crontab(args.schedule))

    # Setup some signal
    signal.signal(signal.SIGHUP, s3_remote.fetch_backups)
    signal.signal(signal.SIGUSR1, app.backup_and_prune)
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        metrics.shutdown()
