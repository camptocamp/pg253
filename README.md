![Build status](https://gitlab.com/camptocamp/is/tools/docker-postgres-backup-to-s3/badges/master/pipeline.svg "Build Status")

PG253 : Backup tools for DB As A Service like AWS RDS
=====================================================

This tools use `pg_dump` to backup a postgres instance and send result to object
storage. It focus on backuping big database with a little memory usage. It use a
fixed memory buffer that correspond to the multipart upload size. This tools
expose many through a prometheus endpoint.

# Configuration

## Scheduling

* `SCHEDULE`: cron like scheduling definition. Default to `20 2 * * *`

## Source configuration

Environment variables for PostgreSQL can be used to configure access to database:
 https://www.postgresql.org/docs/12/libpq-envars.html

At least following variables must be defined:

* `PGHOST`
* `PGUSER`
* `PGPASSWORD`

Optional variables:

* `EXCLUDE_DATABASES`: Regexp to exclude databases. Default to
  `.*backup.*|postgres|rdsadmin`

## Backup configuration

* `RETENTION_DAYS`: Retention in days. Backups older than this value will be
  deleted. Default to `15`.
* `BUFFER_SIZE`: Size of the main buffer, this parameter affect backup speed and
  memory usage. Default to `10 MB`.
* `ENCRYPTION_PASSPHRASE`: Passphrase used to encrypt the backups with GPG. If omitted, encryption is disabled. Default to `""`.

## Target configuration

The following variables are mandatory:

* `AWS_ENDPOINT_URL`: Object storage endpoint, example `http://minio:9000`
* `AWS_S3_BUCKET`: Bucket name
* `AWS_ACCESS_KEY_ID`
* `AWS_SECRET_ACCESS_KEY`

Optional variables:

* `AWS_S3_PREFIX`: Path in bucket. Default to a empty string.

## Monitoring

* `METRICS_PORT`: prometheus endpoint port. Default to `9352`

# Encryption / Decryption

PG253 allows you to symmetrically encrypt your backups with GPG. To enable this feature, you must configure the environment variable `ENCRYPTION_PASSPHRASE`. If omitted, backups will not be encrypted.

To restore an encrypted backup, you must first decrypt it with a standard GPG binary:

```bash
gpg --output mydatabase.dump --decrypt mydatabase-encrypted.dump
```
