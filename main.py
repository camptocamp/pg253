import os
import signal

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from pg253.metrics import Metrics
from pg253.cluster import Cluster
from pg253.configuration import Configuration
from pg253.remote import Remote


def main():
    print(Configuration.str())
    metrics = Metrics()
    cluster = Cluster(metrics)
    print('Databases : %s' % cluster.listDatabase())

    # Start scheduler
    scheduler = BlockingScheduler()
    scheduler.add_job(cluster.backup_and_prune, CronTrigger.from_crontab(Configuration.get('schedule')))
    # scheduler.add_job(cluster.backup_and_prune, 'interval', seconds=3)

    # Setup some signal
    signal.signal(signal.SIGHUP, Remote.fetch)
    signal.signal(signal.SIGUSR1, cluster.backup_and_prune)
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == '__main__':
    main()
