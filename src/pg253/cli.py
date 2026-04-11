import os
import signal

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from pg253.metrics import Metrics
from pg253.cluster import Cluster
from pg253.configuration import Configuration
from pg253.remote import S3Remote


def app():
    print(Configuration.str())
    s3_remote = S3Remote(
            endpoint_url=Configuration.get('AWS_ENDPOINT'),
            region_name=Configuration.get('AWS_S3_REGION_NAME'),
            access_key=Configuration.get('AWS_ACCESS_KEY_ID'),
            secret_key=Configuration.get('AWS_SECRET_ACCESS_KEY'),
            bucket=Configuration.get('AWS_S3_BUCKET'),
            path_prefix=Configuration.get('AWS_S3_PREFIX'))

    metrics = Metrics(s3_remote)
    cluster = Cluster(metrics, s3_remote)

    print('Databases : %s' % cluster.listDatabase())

    # Start scheduler
    scheduler = BlockingScheduler()
    scheduler.add_job(cluster.backup_and_prune, CronTrigger.from_crontab(Configuration.get('schedule')))
    # scheduler.add_job(cluster.backup_and_prune, 'interval', seconds=3)

    # Setup some signal
    signal.signal(signal.SIGHUP, s3_remote.fetch_backups)
    signal.signal(signal.SIGUSR1, cluster.backup_and_prune)
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
