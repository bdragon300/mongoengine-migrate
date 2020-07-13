from copy import deepcopy

import pytest

from mongoengine_migrate.actions import DropDocument
from mongoengine_migrate.exceptions import SchemaError


class TestDropDocument:
    def test_forward__should_drop_collection(self, load_fixture, test_db, dump_db):
        schema = load_fixture('schema1').get_schema()
        dump = dict(dump_db())

        action = DropDocument('Schema1Doc1')
        action.prepare(test_db, schema)
        expect = deepcopy(dump)
        del expect['schema1_doc1']

        action.run_forward()

        assert expect == dict(dump_db())

    def test_forward__on_unexistance_collection_specified__should_do_nothing(self,
                                                                             load_fixture,
                                                                             test_db,
                                                                             dump_db):
        schema = load_fixture('schema1').get_schema()
        schema['Schema1Doc1'].parameters['collection'] = 'unknown_collection'
        dump = dict(dump_db())

        action = DropDocument('Schema1Doc1')
        action.prepare(test_db, schema)

        action.run_forward()

        assert dump == dict(dump_db())

    def test_backward__should_do_nothing(self, load_fixture, test_db, dump_db):
        schema = load_fixture('schema1').get_schema()
        dump = dict(dump_db())

        action = DropDocument('Schema1Doc1')
        action.prepare(test_db, schema)

        action.run_backward()

        assert dump == dict(dump_db())

    def test_backward__on_unexistance_collection_specified__should_do_nothing(self,
                                                                              load_fixture,
                                                                              test_db,
                                                                              dump_db):
        schema = load_fixture('schema1').get_schema()
        schema['Schema1Doc1'].parameters['collection'] = 'unknown_collection'
        dump = dict(dump_db())

        action = DropDocument('Schema1Doc1')
        action.prepare(test_db, schema)

        action.run_backward()

        assert dump == dict(dump_db())

    def test_prepare__if_such_document_is_not_in_schema__should_raise_error(self,
                                                                            load_fixture,
                                                                            test_db):
        schema = load_fixture('schema1').get_schema()
        del schema['Schema1Doc1']

        action = DropDocument('Schema1Doc1')

        with pytest.raises(SchemaError):
            action.prepare(test_db, schema)
