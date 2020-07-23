import os
import sys
from importlib import import_module, reload

import pytest
from mongoengine import connect
from pymongo import MongoClient
import mongoengine_migrate.flags as flags

package_name = __package__


@pytest.fixture(autouse=True)
def test_db():
    if 'DATABASE_URL' not in os.environ:
        raise RuntimeError(f'Please set DATABASE_URL env variable')

    client = MongoClient(os.environ['DATABASE_URL'])
    # Check if we accidentally using the main db instead of test db
    db = client.get_database()
    if not db.name.endswith('_test'):
        raise RuntimeError(f'DATABASE_URL must point to testing db, not to master db ({db.name})')

    connect(host=os.environ['DATABASE_URL'])
    flags.mongo_version = '999.9'

    # Drop test db if exists. (e.g if previous session was interrupted)
    client.drop_database(db.name)

    yield db

    client.drop_database(db.name)


@pytest.fixture
def dump_db(test_db):
    def w():
        for collection_name in test_db.list_collection_names():
            docs = []
            for doc in test_db[collection_name].find():
                docs.append(doc)

            yield collection_name, docs

    return w


@pytest.fixture(autouse=True)
def load_fixture(dump_db):
    module_names = {}

    def f(fixture_name):
        nonlocal module_names

        if fixture_name in module_names:
            return sys.modules[module_names[fixture_name]]

        module_name = f'.fixtures.{fixture_name}'
        module = import_module(module_name, package=package_name)
        module_names[fixture_name] = module.__name__

        module.setup_db()
        return module

    yield f

    # Cleanup. Unload fixture module
    for mod in module_names.values():
        del sys.modules[mod]

    # Quite tricky place -- clear mongoengine documents registry
    global _document_registry
    _document_registry = {}
