__all__ = [
    'import_module',
    'collect_models_schema',
    'MongoengineMigrate',
]

import functools
import importlib.util
import logging
import random
import re
import string
from datetime import timezone, datetime
from pathlib import Path
from types import ModuleType
from typing import Tuple, Dict, List, Type, Optional, Iterable

import pymongo.database
import pymongo.errors
from bson import CodecOptions
from dictdiffer import patch, swap
from jinja2 import Environment
from mongoengine.base import _document_registry
from mongoengine.document import Document, BaseDocument
from pymongo import MongoClient

import mongoengine_migrate.flags as runtime_flags
from mongoengine_migrate.actions.factory import build_actions_chain
from mongoengine_migrate.actions.base import BaseIndexAction
from mongoengine_migrate.exceptions import MongoengineMigrateError, ActionError, MigrationGraphError
from mongoengine_migrate.fields.registry import type_key_registry
from mongoengine_migrate.graph import Migration, MigrationsGraph, MigrationPolicy
from mongoengine_migrate.query_tracer import DatabaseQueryTracer
from mongoengine_migrate.schema import Schema
from mongoengine_migrate.utils import (
    get_closest_parent,
    get_document_type,
    get_index_name,
    normalize_index_fields_spec
)

log = logging.getLogger('mongoengine-migrate')


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
    Import module by python path
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
                raise MongoengineMigrateError(f"Cannot find module '{path}'") from e
            attrs.append(attr)


def collect_models_schema() -> Schema:
    """
    Transform all available mongoengine document objects to db schema
    :return:
    """
    schema = Schema()
    collections: Dict[str, set] = {}  # {collection_name: set(top_level_documents)}

    # Retrieve models from mongoengine global document registry
    for model_cls in _document_registry.values():
        log.debug('> Reading document %s', repr(model_cls))
        # NOTE: EmbeddedDocuments are not append 'abstract' in meta if
        # `meta` is defined
        if model_cls._meta.get('abstract'):
            log.debug('> Skip %s since it is an abstract document')
            continue

        document_type = get_document_type(model_cls)
        if document_type is None:
            raise ActionError(f'Could not get document type for {model_cls!r}')

        if document_type in schema:
            raise ActionError(f'Models with the same document types {document_type!r} found')

        schema[document_type] = Schema.Document()
        if not document_type.startswith(runtime_flags.EMBEDDED_DOCUMENT_NAME_PREFIX):
            col = schema[document_type].parameters['collection'] = model_cls._get_collection_name()

            # Determine if unrelated documents have the same collection
            # E.g. DropDocument could drop all of these documents
            top_lvl_doc = document_type.split(runtime_flags.DOCUMENT_NAME_SEPARATOR)[0]
            collections.setdefault(col, set())
            if collections[col] and top_lvl_doc not in collections[col]:
                collections[col].add(top_lvl_doc)
                log.warning(f'These mongoengine documents use the same collection '
                            f'which can be a cause of data corruption: {collections[col]}')
            else:
                collections[col].add(top_lvl_doc)

        if model_cls._meta.get('allow_inheritance'):
            schema[document_type].parameters['inherit'] = True

        if model_cls._dynamic:
            schema[document_type].parameters['dynamic'] = True

        if issubclass(model_cls, Document):
            schema[document_type].indexes.update(_extract_indexes(model_cls))

        # {field_cls: TypeKeyRegistryItem}
        field_mapping_registry = {x.field_cls: x for x in type_key_registry.values()}

        # Collect schema for every field
        for field_name, field_obj in model_cls._fields.items():
            # Exclude '_id' special MongoDB field since it is immutable
            # and should not be a part of migrations
            if field_obj.db_field == '_id':
                continue

            field_cls = field_obj.__class__

            if field_cls in field_mapping_registry:
                registry_field_cls = field_cls
            else:
                registry_field_cls = get_closest_parent(
                    field_cls,
                    field_mapping_registry.keys()
                )

            if registry_field_cls is None:
                raise ActionError(f'Could not find {field_cls!r} or one of its base classes '
                                  f'in type_key registry')

            handler_cls = field_mapping_registry[registry_field_cls].field_handler_cls
            schema[document_type][field_name] = handler_cls.build_schema(field_obj)
            # TODO: validate default against all field restrictions such as min_length, regex, etc.

        log.debug("> Schema '%s' => %s", document_type, str(schema[document_type]))

    return schema


def _extract_indexes(model_cls: Type[BaseDocument]) -> Dict[str, dict]:
    """
    Extract index declarations from a Document. Return dict
    {index_name: params_dict}. `params_dict` contains:

    - `create_index()` keyword arguments
    - | `fields` - fields specification list, e.g.
      | [('db_field1, 1), ('db_field2', 'geoHaystack')]

    See: https://docs.mongoengine.org/guide/defining-documents.html#indexes

    :param model_cls: Document class
    :return: dict with index parameters
    """
    assert isinstance(model_cls, type) and issubclass(model_cls, Document)

    global_spec = model_cls._meta.get('index_opts') or {}  # type: dict
    field_specs = model_cls._build_index_specs(model_cls._meta['indexes'])  # type: List[dict]
    indexes = {}

    # determine if an index which we are creating includes
    # _cls as its first field; if so, we can avoid creating
    # an extra index on _cls, as mongodb will use the existing
    # index to service queries against _cls
    cls_indexed = False
    index_cls = model_cls._meta.get("index_cls", True)

    for spec in field_specs:
        spec = {**global_spec, **spec}
        fields = spec.pop('fields')  # Key must present, otherwise this is fatal anyway
        fields = list(normalize_index_fields_spec(fields))
        cls_indexed = cls_indexed or any(s[0] == '_cls' for s in fields)

        # Discard bad values in 'name' (if any): '', None, etc.
        index_name = spec.get('name') or get_index_name(fields)

        if index_name in indexes:
            log.warning("Index for fields %s was declared multiple times in %s document, replacing",
                        fields, model_cls.__name__)

        indexes[index_name] = {**spec, 'fields': fields}

    # If _cls field is being used (for polymorphism), it needs an index,
    # only if another index doesn't begin with _cls
    if index_cls and not cls_indexed and model_cls._meta.get("allow_inheritance"):
        index_name = global_spec.get('name') or '_cls'

        if index_name in indexes:
            log.warning(
                "Auto-created index %s was declared manually in %s document, use another name",
                index_name, model_cls.__name__
            )
            # Append random letters to the end of name
            index_name += '_' + ''.join(random.choice(string.ascii_letters) for _ in range(8))

        field_spec = list(normalize_index_fields_spec(['_cls']))
        indexes[index_name] = {'fields': field_spec, **global_spec}

    return indexes


class MongoengineMigrate:
    default_collection_name: str = 'mongoengine_migrate'
    default_directory: str = './migrations'
    default_models_module = 'models'

    def __init__(self, mongo_uri: str, collection_name: str, migrations_dir: str, **kwargs):
        self.mongo_uri = mongo_uri
        self.migrations_collection_name = collection_name
        self.migration_dir = migrations_dir
        self._kwargs = kwargs
        self.client = MongoClient(mongo_uri)
        # Another MongoClient for additional operations which should
        # be performed in separate connection such as parallel bulk
        # writes
        self.client2 = MongoClient(mongo_uri)

        # Initiate immediate connect to MongoDB in order to ensure
        # that it is accessible
        log.debug('Connecting to MongoDB...')
        self.client.get_database().command('ping')

        # Trying to figure out server version if not specified
        if runtime_flags.mongo_version is None:
            try:
                server_info = self.client.server_info()
                runtime_flags.mongo_version = server_info['version']
                log.info('MongoDB version: %s', runtime_flags.mongo_version)
            except pymongo.errors.OperationFailure as e:
                raise MongoengineMigrateError(
                    'Could not figure out MongoDB version. Please set up '
                    'right permissions to be able to execute "buildinfo" '
                    'command or use --mongo-version argument to set version by hand'
                ) from e

    @functools.cached_property
    def db(self) -> pymongo.database.Database:
        """Return MongoDB database object"""
        db = self.client.get_database()
        if runtime_flags.dry_run:
            log.debug('> Dry run mode requested, use mock database object for main connection')
            db = DatabaseQueryTracer(db)

        return db

    @functools.cached_property
    def db2(self) -> pymongo.database.Database:
        """Return MongoDB database object for client2"""
        db = self.client2.get_database()
        if runtime_flags.dry_run:
            log.debug('> Dry run mode requested, use mock database object for second connection')
            db = DatabaseQueryTracer(db)

        return db

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

    def load_db_schema(self) -> Schema:
        """Load schema from db"""
        fltr = {'type': 'schema'}
        res = self.migration_collection.find_one(fltr)
        schema = Schema()
        schema.load(res.get('value', {}) if res else {})
        return schema

    def write_db_schema(self, schema: Schema) -> None:
        """
        Write schema to db
        :param schema:
        :return:
        """
        fltr = {'type': 'schema'}
        data = {'type': 'schema', 'value': schema.dump()}
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
            raise MongoengineMigrateError(f"Directory '{directory}' does not exist")

        for module_file in directory.glob("*.py"):
            if module_file.name.startswith("__"):
                continue

            log.debug('> Loading migration file %s', module_file)
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

        applied = []
        for migration_name in self.get_db_migration_names():
            if migration_name not in graph.migrations:
                raise MigrationGraphError(
                    f'Migration {migration_name} was applied, but its python module not found. '
                    f'You can use schema repair to fix this issue'
                )
            graph.migrations[migration_name].applied = True
            applied.append(migration_name)

        log.debug('> Applied migrations: %s', applied)
        if graph.last:
            log.debug('> Last migration is: %s', graph.last.name)
        else:
            log.debug('> Last migration is: %s', 'None')

        return graph

    def upgrade(self, migration_name: str, graph: Optional[MigrationsGraph] = None):
        """
        Upgrade db to the given migration
        :param migration_name: target migration name
        :param graph: Optional. Migrations graph. If omitted, then it
         will be loaded
        :return:
        """
        if graph is None:
            log.debug('Loading migration files...')
            graph = self.build_graph()
        log.debug('Loading schema from database...')
        left_schema = self.load_db_schema()

        if migration_name not in graph.migrations:
            raise MigrationGraphError(f'Migration {migration_name} not found')

        db = self.db
        for migration in graph.walk_down(graph.initial, unapplied_only=True):
            log.info('Upgrading %s...', migration.name)
            for idx, action_object in enumerate(migration.get_actions(), start=1):
                log.debug('> [%d] %s', idx, str(action_object))
                if not action_object.dummy_action and not runtime_flags.schema_only:
                    action_object.prepare(db, left_schema, migration.policy)
                    action_object.run_forward()
                    action_object.cleanup()

                try:
                    left_schema = patch(action_object.to_schema_patch(left_schema), left_schema)
                except (TypeError, ValueError, KeyError) as e:
                    raise ActionError(
                        f"Unable to apply schema patch of {action_object!r}. More likely that the "
                        f"schema is corrupted. You can use schema repair tools to fix this issue"
                    ) from e

            graph.migrations[migration.name].applied = True

            if not runtime_flags.dry_run:
                log.debug('Writing db schema and migrations graph...')
                self.write_db_schema(left_schema)
                self.write_db_migrations_graph(graph)

            if migration.name == migration_name:
                break   # We've reached the target migration

        self._verify_schema(left_schema)

    def downgrade(self, migration_name: str, graph: Optional[MigrationsGraph] = None):
        """
        Downgrade db to the given migration
        :param migration_name: target migration name
        :param graph: Optional. Migrations graph. If omitted, then it
         will be loaded
        :return:
        """
        if graph is None:
            log.debug('Loading migration files...')
            graph = self.build_graph()
        log.debug('Loading schema from database...')
        left_schema = self.load_db_schema()

        if migration_name not in graph.migrations:
            raise MigrationGraphError(f'Migration {migration_name} not found')

        log.debug('Precalculating schema diffs...')
        # Collect schema diffs across all migrations
        migration_diffs = {}  # {migration_name: [action1_diff, ...]}
        temp_left_schema = Schema()
        for migration in graph.walk_down(graph.initial, unapplied_only=False):
            migration_diffs[migration.name] = []
            for action in migration.get_actions():
                forward_patch = action.to_schema_patch(temp_left_schema)
                migration_diffs[migration.name].append(forward_patch)

                try:
                    temp_left_schema = patch(forward_patch, temp_left_schema)
                except (TypeError, ValueError, KeyError) as e:
                    raise ActionError(
                        f"Unable to apply schema patch of {action!r}. More likely that the "
                        f"schema is corrupted. You can use schema repair tools to fix this issue"
                    ) from e

        db = self.db
        for migration in graph.walk_up(graph.last, applied_only=True):
            if migration.name == migration_name:
                break  # We've reached the target migration

            log.info('Downgrading %s...', migration.name)

            action_diffs = zip(
                migration.get_actions(),
                migration_diffs[migration.name],
                range(1, len(migration.get_actions()) + 1)
            )
            for action_object, action_diff, idx in reversed(list(action_diffs)):
                log.debug('> [%d] %s', idx, str(action_object))

                try:
                    left_schema = patch(list(swap(action_diff)), left_schema)
                except (TypeError, ValueError, KeyError) as e:
                    raise ActionError(
                        f"Unable to apply schema patch of {action_object!r}. More likely that the "
                        f"schema is corrupted. You can use schema repair tools to fix this issue"
                    ) from e

                if not action_object.dummy_action and not runtime_flags.schema_only:
                    action_object.prepare(db, left_schema, migration.policy)
                    action_object.run_backward()
                    action_object.cleanup()

            graph.migrations[migration.name].applied = False

            if not runtime_flags.dry_run:
                log.debug('Writing db schema and migrations graph...')
                self.write_db_schema(left_schema)
                self.write_db_migrations_graph(graph)

        self._verify_schema(left_schema)

    def migrate(self, migration_name: str = None):
        """
        Migrate db in order to reach a given migration. This process
        may require either upgrading or downgrading
        :param migration_name: target migration name
        :return:
        """
        log.debug('Loading migration files...')
        graph = self.build_graph()
        if not graph.last:
            raise MigrationGraphError('No migrations found')

        if migration_name is None:
            migration_name = graph.last.name

        if migration_name not in graph.migrations:
            raise MigrationGraphError(f'Migration {migration_name} not found')

        migration = graph.migrations[migration_name]
        if migration.applied:
            self.downgrade(migration_name, graph)
        else:
            self.upgrade(migration_name, graph)

    def makemigrations(self):
        """
        Compare current mongoengine documents state and the last db
        state and make a migration file if needed
        """
        log.debug('Loading migration files...')
        graph = self.build_graph()
        log.debug('Loading schema from database...')
        db_schema = self.load_db_schema()

        # Obtain schema changes which migrations would make (including
        #  unapplied ones)
        # If mongoengine models schema was changed regarding db schema
        #  then try to guess which actions would reflect such changes
        for migration in graph.walk_down(graph.initial, unapplied_only=False):
            for action_object in migration.get_actions():
                try:
                    db_schema = patch(action_object.to_schema_patch(db_schema), db_schema)
                except (TypeError, ValueError, KeyError) as e:
                    raise ActionError(
                        f"Unable to apply schema patch of {action_object!r}. More likely that the "
                        f"schema is corrupted. You can use schema repair tools to fix this issue"
                    ) from e

        log.debug('Collecting schema from mongoengine documents...')
        models_schema = collect_models_schema()
        if db_schema == models_schema:
            log.info('No changes detected')
            return

        log.debug('Building actions chain...')
        actions_chain = build_actions_chain(db_schema, models_schema)

        import_expressions = {'from mongoengine_migrate.actions import *'}
        for action in actions_chain:
            # If `regex` is set in action, then we probably need 're'
            if isinstance(action.parameters.get('regex'), re.Pattern):
                import_expressions.add('import re')

            # *Index actions use index type pymongo.* constants
            # in fields spec
            if isinstance(action, BaseIndexAction):
                import_expressions.add('import pymongo')

        log.debug('Writing migrations file...')
        env = Environment()
        env.filters['symbol_wrap'] = symbol_wrap
        tpl_ctx = {
            'graph': graph,
            'actions_chain': actions_chain,
            'policy_enum': MigrationPolicy,
            'import_expressions': import_expressions
        }
        tpl_path = Path(__file__).parent / 'migration_template.tpl'
        tpl = env.from_string(tpl_path.read_text())
        migration_source = tpl.render(tpl_ctx)

        seq_number = str(len(graph.migrations)).zfill(4)
        name = f'{seq_number}_auto_{datetime.now().strftime("%Y%m%d_%H%M")}.py'
        migration_file = Path(self.migration_dir) / name
        migration_file.write_text(migration_source)

        log.info('Migration file "%s" was created', migration_file)

    def _verify_schema(self, schema: Schema):
        # Check if all derived documents have the same collection as
        # their parents.
        # E.g. user could comment/remove AlterDocument(collection=...)
        # for any derived document, but leave for base one)
        collections = {}  # {top_level_document: collection}
        for document_type, doc_schema in schema.items():
            if 'collection' not in doc_schema.parameters:
                continue

            col = doc_schema.parameters['collection']
            top_lvl_doc = document_type.split(runtime_flags.DOCUMENT_NAME_SEPARATOR)[0]
            if top_lvl_doc in collections and collections[top_lvl_doc] != col:
                log.warning(f'The collection in derived document {document_type} ({col}) '
                            f'is differ than its base document {top_lvl_doc} '
                            f'({collections[top_lvl_doc]}). Please fix collection name and rerun '
                            f'an affected migration')
            collections.setdefault(top_lvl_doc, col)
