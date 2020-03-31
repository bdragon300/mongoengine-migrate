import importlib.util
from datetime import timezone, datetime
from pathlib import Path
from types import ModuleType
from typing import Tuple, Iterable, Optional

import pymongo.database
from bson import CodecOptions
from dictdiffer import patch, swap
from jinja2 import Environment
from mongoengine.base import _document_registry
from pymongo import MongoClient

from mongoengine_migrate.actions.factory import build_actions_chain
from mongoengine_migrate.exceptions import MigrationError
from mongoengine_migrate.exceptions import SchemaError
from mongoengine_migrate.fields import mongoengine_fields_mapping
from mongoengine_migrate.graph import Migration, MigrationsGraph


def symbol_wrap(value: str, width: int = 80, wrap_by: str = ',', wrapstring: str = '\n'):
    """
    Jinja2 filter which implements word wrap, but can split the string
    only by given character. If no character found then does nothing.
    :param value: Incoming value
    :param width: Maximum width of string. Default is 80
    :param wrap_by: Symbol which we're split by. Default is comma (,)
    :param wrapstring: String to join each wrapped line. Default is
     newline
    :return: Wrapped string
    """
    pos = 1
    lines = []
    content_width = width - len(wrapstring.replace('\n', ''))
    while pos:
        pos = value.rfind(wrap_by, 0, content_width) + 1 or None
        if len(value) <= content_width:
            lines.append(value)
            break
        else:
            lines.append(value[:pos])
        value = value[pos:].lstrip(' \t')

    return wrapstring.join(lines)


def import_module(path: str) -> Tuple[ModuleType, str]:
    """
    Import module by dot notation path
    :param path: dot path
    :return: tuple with module object and path part inside it
    """
    attrs = []
    while 1:
        try:
            module = importlib.import_module(path)
            rest = '.'.join(reversed(attrs))
            return module, rest
        except ModuleNotFoundError as e:
            try:
                path, attr = path.rsplit('.', 1)
            except ValueError:
                raise e
            attrs.append(attr)


def collect_models_schema() -> dict:
    """
    Build full db schema dict from all mongoengine models available
    :return:
    """
    schema = {}

    # Retrieve models from mongoengine global document registry
    for model_cls in _document_registry.values():
        collection_name = model_cls._get_collection_name()
        if collection_name in schema:
            raise SchemaError(f'Models with the same collection names {collection_name!r} found')
        schema[collection_name] = {}

        # Collect schema for every field
        for field_name, field_obj in model_cls._fields.items():
            field_cls = field_obj.__class__
            field_type_cls = mongoengine_fields_mapping.get(field_cls)
            if field_type_cls:
                schema[collection_name][field_name] = field_type_cls().build_schema(field_obj)
            # TODO: warning about field type not implemented

    return schema


class MongoengineMigrate:
    def __init__(self,
                 mongo_uri: str,
                 collection_name: str = '_migrations_data',
                 migrations_dir: str = './migrations',
                 **kwargs):
        self.mongo_uri = mongo_uri
        self.migrations_collection_name = collection_name
        self.migration_dir = migrations_dir
        self._kwargs = kwargs
        self.client = MongoClient(mongo_uri)

    @property
    def db(self) -> pymongo.database.Database:
        """Return MongoDB database object"""
        return self.client.get_database()

    @property
    def migration_collection(self) -> pymongo.collection.Collection:
        """Return collection object where we keep migration data"""
        return self.client.get_database()[self.migrations_collection_name].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=timezone.utc)
        )

    def get_db_migration_names(self) -> Iterable[str]:
        """
        Return iterable with migration names was written in db in
        applying order
        """
        fltr = {'type': 'migrations'}
        for migration in self.migration_collection.find(fltr).sort('ordering_number'):
            return [m['name'] for m in migration['value']]

        return []

    def write_db_migrations_graph(self, graph: MigrationsGraph):
        """
        Write migrations graph to db
        :param graph: migrations graph
        """
        fltr = {'type': 'migrations'}
        records = []
        num = 0
        for migration in graph.walk_down(graph.initial, False):
            if migration.applied:
                records.append({
                    'name': migration.name,
                    'ordering_number': num
                })
                num += 1

        data = {'type': 'migrations', 'value': records}
        self.migration_collection.replace_one(fltr, data, upsert=True)

    def load_db_schema(self) -> Optional[dict]:
        """Load schema from db"""
        fltr = {'type': 'schema'}
        res = self.migration_collection.find_one(fltr)
        return res.get('value') if res else None

    def write_db_schema(self, schema: dict):
        """
        Write schema to db
        :param schema: schema dict
        :return:
        """
        fltr = {'type': 'schema'}
        data = {'type': 'schema', 'value': schema}
        self.migration_collection.replace_one(fltr, data, upsert=True)

    def load_migrations(self,
                        directory: Path,
                        namespace: str = f"{__name__}._migrations") -> Iterable[Migration]:
        """
        Load migrations python modules located in a given directory.
        Every module is loaded on a given namespace. Function skips
        modules which names are started from double underscore
        :param directory: directory where modules will be searhed for
        :param namespace: namespace name where module names will be
         loaded
        :return: unsorted iterable with Migration objects
        """
        # FIXME: do not load to current namespace
        if not directory.exists():
            raise MigrationError(f'Directory {directory} does not exist')

        for module_file in directory.glob("*.py"):
            if module_file.name.startswith("__"):
                continue
            migration_name = module_file.stem
            spec = importlib.util.spec_from_file_location(
                f"{namespace}.{migration_name}", str(module_file)
            )
            migration_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(migration_module)
            yield Migration(
                name=migration_name,
                module=migration_module,
                dependencies=migration_module.dependencies
            )

    def build_graph(self) -> MigrationsGraph:
        """Build migrations graph with all migration modules"""
        graph = MigrationsGraph()
        for m in self.load_migrations(Path(self.migration_dir)):
            graph.add(m)

        for migration_name in self.get_db_migration_names():
            if migration_name not in graph.migrations:
                # TODO: ability to override with --force
                raise MigrationError(f'Migration module {migration_name} not found')
            graph.migrations[migration_name].applied = True

        return graph

    def upgrade(self, migration_name: str = None):
        graph = self.build_graph()
        current_schema = self.load_db_schema() or {}

        # TODO: transaction
        # TODO: error handling
        for migration in graph.walk_down(graph.initial, unapplied_only=True):
            for action_object in migration.get_forward_actions():
                action_object.prepare(self.db, current_schema)
                action_object.run_forward()
                action_object.cleanup()
                current_schema = patch(action_object.to_schema_patch(current_schema),
                                       current_schema)

            graph.migrations[migration.name].applied = True
            if migration.name == migration_name:
                break   # We're reached the target migration

        self.write_db_schema(current_schema)
        self.write_db_migrations_graph(graph)

    def downgrade(self, migration_name: str = None):
        graph = self.build_graph()
        current_schema = self.load_db_schema() or {}

        # TODO: transaction
        # TODO: error handling
        for migration in graph.walk_up(graph.last, applied_only=True):
            if migration.name == migration_name:
                break  # We're reached the target migration

            for action_object in migration.get_backward_actions():
                action_object.prepare(self.db, current_schema)
                action_object.run_backward()
                action_object.cleanup()
                reverse_patch = list(swap(action_object.to_schema_patch(current_schema)))
                current_schema = patch(reverse_patch, current_schema)

            graph.migrations[migration.name].applied = False

        self.write_db_schema(current_schema)
        self.write_db_migrations_graph(graph)

    def makemigrations(self):
        graph = self.build_graph()
        db_schema = self.load_db_schema() or {}

        # Obtain schema changes which migrations would make (including
        #  unapplied ones)
        # If mongoengine models schema was changed regarding db schema
        #  then try to guess which actions would reflect such changes
        for migration in graph.walk_down(graph.initial, unapplied_only=False):
            for action_object in migration.get_forward_actions():
                db_schema = patch(action_object.to_schema_patch(db_schema), db_schema)

        models_schema = collect_models_schema()
        if db_schema == models_schema:
            print('No changes detected')
            return

        actions_chain = build_actions_chain(db_schema, models_schema)

        env = Environment()
        env.filters['symbol_wrap'] = symbol_wrap
        tpl_ctx = {
            'graph': graph,
            'actions_chain': actions_chain
        }
        tpl_path = Path(__file__).parent / 'migration_template.tpl'
        tpl = env.from_string(tpl_path.read_text())
        migration_source = tpl.render(tpl_ctx)

        migration_file = Path(self.migration_dir) / str(datetime.now().isoformat() + '.py')
        migration_file.write_text(migration_source)
