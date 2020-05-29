import os

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from pg253.metrics import Metrics
from pg253.cluster import Cluster
from pg253.configuration import Configuration


def main():
    print(Configuration.str())
    metrics = Metrics()
    cluster = Cluster(metrics)
    print('Databases : %s' % cluster.listDatabase())

    # Start scheduler
    scheduler = BlockingScheduler()
    scheduler.add_job(cluster.backup_and_prune, CronTrigger.from_crontab(Configuration.get('schedule')))
    # scheduler.add_job(cluster.backup_and_prune, 'interval', seconds=3)
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == '__main__':
    main()
