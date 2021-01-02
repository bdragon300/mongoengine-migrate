#!/usr/bin/env python3
import functools
import logging
import sys
from typing import Optional

import click

import mongoengine_migrate.flags as flags
from mongoengine_migrate.exceptions import MongoengineMigrateError
from mongoengine_migrate.loader import MongoengineMigrate, import_module

mongoengine_migrate: Optional[MongoengineMigrate] = None


log = logging.getLogger('mongoengine-migrate')


def setup_logger(log_level: str):
    fmt = logging.Formatter('[%(levelname)s] %(message)s')
    hdlr = logging.StreamHandler()
    hdlr.setFormatter(fmt)
    log.addHandler(hdlr)

    log.setLevel(log_level.upper())


def error_handler(func):
    """Function decorator which handles MongoengineMigrateError
    exception. Depending on current log level either reraise those
    exception or print error to logger and exit with error code
    """
    @functools.wraps(func)
    def w(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except MongoengineMigrateError as e:
            if log.level == logging.DEBUG:
                raise

            log.error('%s: %s (use `--log-level=debug` argument to get more info)',
                      e.__class__.__name__,
                      str(e))
            sys.exit(1)
    return w


def cli_options(f):
    decorators = [
        click.option(
            "-u",
            "--uri",
            default='mongodb://localhost/mydb',
            envvar="MONGOENGINE_MIGRATE_URI",
            metavar="URI",
            help="MongoDB connect URI",
            show_default=True,
        ),
        click.option(
            "-d",
            "--directory",
            default=MongoengineMigrate.default_directory,
            envvar="MONGOENGINE_MIGRATE_DIRECTORY",
            metavar="DIR",
            help="Directory with migrations",
            show_default=True,
        ),
        click.option(
            "-c",
            "--collection",
            default=MongoengineMigrate.default_collection_name,
            envvar="MONGOENGINE_MIGRATE_COLLECTION",
            metavar="COLLECTION",
            help="Collection where schema and state will be stored",
            show_default=True
        ),
        click.option(
            '--mongo-version',
            help="Manually set MongoDB server version. By default it's determined automatically, "
                 "but this requires a permission for 'buildinfo' admin command",
            envvar="MONGOENGINE_MIGRATE_MONGO_VERSION",
            metavar="MONGO_VERSION"
        ),
        click.option(
            '--log-level',
            type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR'], case_sensitive=False),
            default='INFO',
            envvar="MONGOENGINE_MIGRATE_LOG_LEVEL",
            metavar='LOG_LEVEL',
            help="Logging verbosity level",
            show_default=True
        )
    ]
    for decorator in reversed(decorators):
        f = decorator(f)
    return f


def migration_options(f):
    decorators = [
        click.option(
            '--dry-run',
            default=False,
            is_flag=True,
            help='Dry run mode. Just show queries to be executed, without running migrations'
        ),
        click.option(
            '--schema-only',
            default=False,
            is_flag=True,
            help='Perform migrations without doing any database modifications'
        )
    ]
    for decorator in reversed(decorators):
        f = decorator(f)
    return f


@click.group()
@cli_options
@error_handler
def cli(uri, directory, collection, **kwargs):
    global mongoengine_migrate
    setup_logger(kwargs['log_level'])
    flags.mongo_version = kwargs.get('mongo_version')
    mongoengine_migrate = MongoengineMigrate(mongo_uri=uri,
                                             collection_name=collection,
                                             migrations_dir=directory)
    flags.database2 = mongoengine_migrate.db2


@click.command(short_help='Upgrade db to the given migration')
@click.argument('migration', required=True)
@migration_options
@error_handler
def upgrade(migration, dry_run, schema_only):
    flags.dry_run = dry_run
    flags.schema_only = schema_only

    mongoengine_migrate.upgrade(migration)


@click.command(short_help='Downgrade db to the given migration')
@click.argument('migration', required=True)
@migration_options
@error_handler
def downgrade(migration, dry_run, schema_only):
    flags.dry_run = dry_run
    flags.schema_only = schema_only
    mongoengine_migrate.downgrade(migration)


@click.command(short_help='Migrate db to the given migration. By default is to the last one')
@click.argument('migration', required=False)
@migration_options
@error_handler
def migrate(migration, dry_run, schema_only):
    flags.dry_run = dry_run
    flags.schema_only = schema_only
    mongoengine_migrate.migrate(migration)


@click.command(short_help='Generate migration file based on mongoengine model changes')
@click.option(
    "-m",
    "--models-module",
    default=MongoengineMigrate.default_models_module,
    envvar="MONGOENGINE_MIGRATE_MODELS_MODULE",
    metavar="MODULE_NAME",
    help="Python module where mongoengine models are loaded from",
    show_default=True,
)
@error_handler
def makemigrations(models_module):
    sys.path.append('.')  # Import modules relative to the current dir
    import_module(models_module)
    mongoengine_migrate.makemigrations()


cli.add_command(upgrade)
cli.add_command(downgrade)
cli.add_command(makemigrations)
cli.add_command(migrate)


if __name__ == '__main__':
    cli()
