#!/usr/bin/env python3
from typing import Optional

import click

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
            default='./migrations',
            envvar="MONGOENGINE_MIGRATE_DIR",
            metavar="DIR",
            help="Directory with migrations",
            show_default=True,
        ),
        click.option(
            "-m",
            "--models-module",
            default='models',
            envvar="MONGOENGINE_MIGRATE_MODELS_MODULE",
            metavar="MODULE_NAME",
            help="Python module where mongoengine models are loaded from",
            show_default=True,
        ),
        click.option(
            "-c",
            "--collection",
            default='_migrations_data',
            envvar="MONGOENGINE_MIGRATE_COLLECTION",
            metavar="COLLECTION",
            help="Collection where schema and state will be stored",
            show_default=True
        )
    ]
    for decorator in reversed(decorators):
        f = decorator(f)
    return f


@click.group()
@cli_options
def cli(uri, models_module, directory, collection):
    global mongoengine_migrate
    import_module(models_module)
    mongoengine_migrate = MongoengineMigrate(mongo_uri=uri,
                                             collection_name=collection,
                                             migrations_dir=directory)


@click.command(short_help='Upgrade db to the given migration')
@click.argument('migration', required=True)
def upgrade(migration):
    mongoengine_migrate.upgrade(migration)


@click.command(short_help='Downgrade db to the given migration')
@click.argument('migration', required=True)
def downgrade(migration):
    mongoengine_migrate.downgrade(migration)


@click.command(short_help='Migrate db to the given migration. By default is to the last one')
@click.argument('migration', required=False)
def migrate(migration):
    mongoengine_migrate.migrate(migration)


@click.command(short_help='Generate migration file based on mongoengine model changes')
def makemigrations():
    mongoengine_migrate.makemigrations()


cli.add_command(upgrade)
cli.add_command(downgrade)
cli.add_command(makemigrations)
cli.add_command(migrate)


if __name__ == '__main__':
    cli()
