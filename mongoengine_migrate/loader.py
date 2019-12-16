import importlib.util
from pathlib import Path
from mongoengine_migrate.migration import Migration, MigrationsGraph
from dictdiffer import patch, swap
from pymongo import MongoClient
from bson import CodecOptions
from datetime import timezone


class MongoengineMigrate:
    def __init__(self, mongo_uri, **kwargs):
        self.mongo_uri = mongo_uri
        self.collection_name = kwargs.pop('migrations_collection', '_migrations_data')
        self.migration_directory = kwargs.pop('migrations_directory', './migrations')
        self._kwargs = kwargs
        self.client = MongoClient(mongo_uri)

    @property
    def db(self):
        return self.client.get_database()

    @property
    def migration_collection(self):
        return self.client.get_database()[self.collection_name].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=timezone.utc)
        )

    def get_db_migration_names(self):
        fltr = {'type': 'migration'}
        for migration in self.migration_collection.find(fltr).sort('ordering_number'):
            # TODO: check if document is correct
            yield migration['name']

    def load_db_schema(self):
        fltr = {'type': 'schema'}
        return self.migration_collection.find_one(fltr)

    def write_db_schema(self, schema):
        fltr = {'type': 'schema'}
        self.migration_collection.replace_one(fltr, schema)

    def load_migrations(self, directory: Path, namespace=f"{__name__}._migrations"):
        # FIXME: do not load to current namespace
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

    def build_graph(self):
        graph = MigrationsGraph()
        for m in self.load_migrations(self.migration_directory):
            graph.add(m)

        for migration_name in self.get_db_migration_names():
            # TODO: check if such migration exists
            graph.migrations[migration_name].applied = True

        return graph

    def upgrade(self, migration_name=None):
        graph = self.build_graph()  # type: MigrationsGraph
        current_schema = self.load_db_schema() or {}
        # TODO: transaction
        # TODO: error handling
        for migration in graph.walk_down(graph.initial, unapplied_only=True):
            for action_object in migration.get_forward_actions():
                action_object.prepare(current_schema)
                action_object.run_forward(self.db, self.collection_name)
                action_object.cleanup()
                current_schema = patch(action_object.as_schema_patch(), current_schema)

            if migration.name == migration_name:
                break

        self.write_db_schema(current_schema)  # FIXME: kwargs

    def downgrade(self, migration_name=None):
        graph = self.build_graph()  # type: MigrationsGraph
        current_schema = self.load_db_schema() or {}
        # TODO: transaction
        # TODO: error handling
        for migration in graph.walk_up(graph.last, applied_only=True):
            for action_object in migration.get_backward_actions():
                action_object.prepare(current_schema)
                action_object.run_backward(self.db, self.collection_name)
                action_object.cleanup()
                reverse_patch = swap(action_object.as_schema_patch())
                current_schema = patch(reverse_patch, current_schema)

            if migration.name == migration_name:
                break

        self.write_db_schema(current_schema)
