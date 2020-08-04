from copy import deepcopy

import pytest

from mongoengine_migrate.actions import CreateDocument
from mongoengine_migrate.exceptions import SchemaError
from mongoengine_migrate.graph import MigrationPolicy


class TestCreateDocument:
    def test_forward__should_do_nothing(self, load_fixture, test_db, dump_db):
        schema = load_fixture('schema1').get_schema()
        del schema['Schema1Doc1']
        dump = dump_db()

        action = CreateDocument('Schema1Doc1', collection='schema1_doc1')
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump == dump_db()

    def test_forward__on_unexistance_collection_specified__should_do_nothing(self,
                                                                             load_fixture,
                                                                             test_db,
                                                                             dump_db):
        schema = load_fixture('schema1').get_schema()
        del schema['Schema1Doc1']
        dump = dump_db()

        action = CreateDocument('Schema1Doc1', collection='unknown_collection')
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump == dump_db()

    def test_backward__should_drop_collection(self, load_fixture, test_db, dump_db):
        schema = load_fixture('schema1').get_schema()
        del schema['Schema1Doc1']
        dump = dump_db()
        expect = deepcopy(dump)
        del expect['schema1_doc1']

        action = CreateDocument('Schema1Doc1', collection='schema1_doc1')
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_backward()

        assert expect == dump_db()

    def test_backward__on_unexistance_collection_specified__should_do_nothing(self,
                                                                              load_fixture,
                                                                              test_db,
                                                                              dump_db):
        schema = load_fixture('schema1').get_schema()
        del schema['Schema1Doc1']
        dump = dump_db()

        action = CreateDocument('Schema1Doc1', collection='unknown_collection')
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_backward()

        assert dump == dump_db()

    def test_prepare__if_such_document_is_in_schema__should_raise_error(self,
                                                                        load_fixture,
                                                                        test_db):
        schema = load_fixture('schema1').get_schema()

        action = CreateDocument('Schema1Doc1', collection='schema1_doc1')

        with pytest.raises(SchemaError):
            action.prepare(test_db, schema, MigrationPolicy.strict)
