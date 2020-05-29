import os

class Configuration:

    CONFIG = {'SCHEDULE': '20 2 * * *',
              'PROMETHEUS_EXPORTER_PORT': '9352',
              'BUFFER_SIZE': str(10 * 1024 * 1024),
              'PGHOST': None,
              'PGUSER': None,
              'PGPASSWORD': None,
              'BLACKLISTED_DATABASES': '.*backup.*|postgres|rdsadmin',
              'RETENTION_DAYS': '15',
              'AWS_ENDPOINT': None,
              'AWS_S3_BUCKET': None,
              'AWS_S3_PREFIX': '',
              'AWS_ACCESS_KEY_ID': None,
              'AWS_SECRET_ACCESS_KEY': None,
              }

    @staticmethod
    def get(obj):
        """Get value of configuration

        First look at the env then in default value."""
        if obj.upper() not in Configuration.CONFIG:
            raise Exception('Unknown config key: %s' % obj)
        # return value from env
        if obj.upper() in os.environ:
            return os.environ[obj.upper()]
        # return default value
        if Configuration.CONFIG[obj.upper()] is not None:
            return Configuration.CONFIG[obj.upper()]
        raise Exception('Missing value for %s' % obj)

    @staticmethod
    def str():
        """Generate a string description of the configuration"""
        def sizeof_fmt(num):
            num = int(num)
            for unit in ['B', 'KB', 'MB', 'GB']:
                if abs(num) < 1024.0:
                    return "%3.1f%s" % (num, unit)
                num /= 1024.0
            return "%.1f%s" % (num, 'TB')

        res = ''
        res += "Source configuration:\n"
        res += "\tHost : %s\n" % Configuration.get('pghost')
        res += "\tUser : %s\n" % Configuration.get('pguser')
        res += ("\tPassword : %s\n"
                % ('X' * len(Configuration.get('pgpassword'))))
        res += ("\tBlacklisted DBs: %s\n"
                % Configuration.get('blacklisted_databases'))
        res += "Backup configuration:\n"
        res += ("\tBuffer size: %s\n"
                % sizeof_fmt(Configuration.get('buffer_size')))
        res += "\tSchedule: %s\n" % Configuration.get('schedule')
        res += "\tRetention: %s days\n" % Configuration.get('retention_days')
        res += "Target configuration:\n"
        res += "\tEndpoint: %s\n" % Configuration.get('aws_endpoint')
        res += "\tBucket: %s\n" % Configuration.get('aws_s3_bucket')
        res += "\tPrefix: %s\n" % Configuration.get('aws_s3_prefix')
        res += "\tAccess Key: %s\n" % Configuration.get('aws_access_key_id')
        res += ("\tSecret Key : %s\n"
                % ('X' * len(Configuration.get('aws_secret_access_key'))))
        return res
