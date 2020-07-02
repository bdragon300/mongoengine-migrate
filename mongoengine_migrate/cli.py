#!/usr/bin/env python3
from typing import Optional

import click

import mongoengine_migrate.flags as runtime_flags
from mongoengine_migrate.loader import MongoengineMigrate, import_module

mongoengine_migrate: Optional[MongoengineMigrate] = None


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
            envvar="MONGOENGINE_MIGRATE_DIR",
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
            metavar="MONGO_VERSION"
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
            help='Dry run mode. Don\'t modify the database and print '
                 'modification commands which would get executed'
        ),
        click.option(
            '--schema-only',
            default=False,
            is_flag=True,
            help='Migrate only schema, do not perform any modifications'
                 'on database'
        )
    ]
    for decorator in reversed(decorators):
        f = decorator(f)
    return f


@click.group()
@cli_options
def cli(uri, directory, collection, **kwargs):
    global mongoengine_migrate
    runtime_flags.mongo_version = kwargs.get('mongo_version')

    mongoengine_migrate = MongoengineMigrate(mongo_uri=uri,
                                             collection_name=collection,
                                             migrations_dir=directory)


@click.command(short_help='Upgrade db to the given migration')
@click.argument('migration', required=True)
@migration_options
def upgrade(migration, dry_run, schema_only):
    runtime_flags.dry_run = dry_run
    runtime_flags.schema_only = schema_only
    mongoengine_migrate.upgrade(migration)


@click.command(short_help='Downgrade db to the given migration')
@click.argument('migration', required=True)
@migration_options
def downgrade(migration, dry_run, schema_only):
    runtime_flags.dry_run = dry_run
    runtime_flags.schema_only = schema_only
    mongoengine_migrate.downgrade(migration)


@click.command(short_help='Migrate db to the given migration. By default is to the last one')
@click.argument('migration', required=False)
@migration_options
def migrate(migration, dry_run, schema_only):
    runtime_flags.dry_run = dry_run
    runtime_flags.schema_only = schema_only
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
def makemigrations(models_module):
    import_module(models_module)
    mongoengine_migrate.makemigrations()


cli.add_command(upgrade)
cli.add_command(downgrade)
cli.add_command(makemigrations)
cli.add_command(migrate)


if __name__ == '__main__':
    cli()
