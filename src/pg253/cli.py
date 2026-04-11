""" This module manages the application's parameters and the backup jobs scheduling. """

import os
import signal
import argparse

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from pg253.metrics import Metrics
from pg253.cluster import Cluster
from pg253.remote import S3Remote


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
            default=os.getenv('AWS_S3_PREFIX'),
            help='Prefix to apply on objectswhen storing backups in the S3 bucket.')

    return parser.parse_args()

def app():
    """ Application's entry point """

    args = build_args()

    s3_remote = S3Remote(
            endpoint_url=args.aws_endpoint_url,
            region_name=args.aws_default_region,
            access_key=args.aws_access_key_id,
            secret_key=args.aws_secret_access_key,
            bucket=args.aws_s3_bucket,
            path_prefix=args.aws_s3_prefix)

    metrics = Metrics(s3_remote, args.metrics_port)

    cluster = Cluster(
            metrics=metrics,
            remote=s3_remote,
            db_exclude=args.exclude_databases,
            buffer_size=args.buffer_size,
            retention_days=args.retention_days)

    print("Databases : {cluster.listDatabase()}")

    # Start scheduler
    scheduler = BlockingScheduler()
    scheduler.add_job(cluster.backup_and_prune, CronTrigger.from_crontab(args.schedule))

    # Setup some signal
    signal.signal(signal.SIGHUP, s3_remote.fetch_backups)
    signal.signal(signal.SIGUSR1, cluster.backup_and_prune)
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
