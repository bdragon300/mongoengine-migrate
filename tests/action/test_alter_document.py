from copy import deepcopy

import pytest

from mongoengine_migrate.actions import AlterDocument
from mongoengine_migrate.exceptions import SchemaError
from mongoengine_migrate.graph import MigrationPolicy
from mongoengine_migrate.schema import Schema


class TestAlterDocument:
    def test_build_object__on_embdedded_document_type__should_return_none(self):
        left_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            '~EmbeddedDocument': Schema.Document({
                'field1': {'param': 'schemavalue'},
            }, parameters={'collection': 'document', 'test_parameter': 'test_value'})
        })

        right_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            '~EmbeddedDocument': Schema.Document({
                'field1': {'param_new': 'schemavalue_new'},
            }, parameters={'collection': 'document_new', 'test_parameter': 'test_value'})
        })

        res = AlterDocument.build_object('~EmbeddedDocument', left_schema, right_schema)

        assert res is None

    def test_build_object__if_document_in_schema_has_changed__should_return_object(self):
        left_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            'Document2': Schema.Document({
                'field1': {'param': 'schemavalue'},
            }, parameters={'collection': 'document', 'test_parameter': 'test_value'})
        })
        right_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            'Document2': Schema.Document({
                'field1': {'param_new': 'schemavalue_new'},
            }, parameters={'collection': 'document_new', 'test_parameter': 'test_value'})
        })

        res = AlterDocument.build_object('Document2', left_schema, right_schema)

        assert isinstance(res, AlterDocument)
        assert res.document_type == 'Document2'
        assert res.parameters == {'collection': 'document_new', 'test_parameter': 'test_value'}

    @pytest.mark.parametrize('document_type', ('Document2', 'Document3', 'Document_unknown'))
    def test_build_object__if_document_is_not_in_both_schemas__should_return_none(
            self, document_type
    ):
        left_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            'Document2': Schema.Document({
                'field1': {'param': 'schemavalue'},
            }, parameters={'collection': 'document', 'test_parameter': 'test_value'})
        })
        right_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            'Document3': Schema.Document({
                'field1': {'param_new': 'schemavalue_new'},
            }, parameters={'collection': 'document_new', 'test_parameter': 'test_value'})
        })

        res = AlterDocument.build_object(document_type, left_schema, right_schema)

        assert res is None

    def test_build_object__if_document_parameters_has_not_changed__should_return_none(self):
        left_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            'Document2': Schema.Document({
                'field1': {'param': 'schemavalue'},
            }, parameters={'collection': 'document', 'test_parameter': 'test_value'})
        })
        right_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            'Document2': Schema.Document({
                'field1': {'param': 'schemavalue'},
            }, parameters={'collection': 'document', 'test_parameter': 'test_value'})
        })

        res = AlterDocument.build_object('Document1', left_schema, right_schema)

        assert res is None

    def test_forward__on_new_collection_specified__should_rename_collection(self,
                                                                            load_fixture,
                                                                            test_db,
                                                                            dump_db):
        schema = load_fixture('schema1').get_schema()
        dump = dump_db()

        action = AlterDocument('Schema1Doc1', collection='new_name1')
        action.prepare(test_db, schema, MigrationPolicy.strict)
        expect = deepcopy(dump)
        expect['new_name1'] = expect.pop('schema1_doc1')

        action.run_forward()

        assert expect == dump_db()

    def test_forward__on_unexistance_collection_specified__should_do_nothing(self,
                                                                             load_fixture,
                                                                             test_db,
                                                                             dump_db):
        schema = load_fixture('schema1').get_schema()
        schema['Schema1Doc1'].parameters['collection'] = 'unknown_collection'
        dump = dump_db()

        action = AlterDocument('Schema1Doc1', collection='new_name1')
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump == dump_db()

    def test_backward__on_new_collection_specified__should_rename_collection_back(self,
                                                                                  load_fixture,
                                                                                  test_db,
                                                                                  dump_db):
        schema = load_fixture('schema1').get_schema()
        dump = dump_db()

        action = AlterDocument('Schema1Doc1', collection='new_name1')
        action.prepare(test_db, schema, MigrationPolicy.strict)
        action.run_forward()

        action.run_backward()

        assert dump == dump_db()

    def test_backward__on_unexistance_collection_specified__should_do_nothing(self,
                                                                              load_fixture,
                                                                              test_db,
                                                                              dump_db):
        schema = load_fixture('schema1').get_schema()
        test_db['schema1_doc1'].rename('unknown_collection')
        dump = dump_db()

        action = AlterDocument('Schema1Doc1', collection='new_name1')
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_backward()

        assert dump == dump_db()

    def test_prepare__if_such_document_is_not_in_schema__should_raise_error(self,
                                                                            load_fixture,
                                                                            test_db):
        schema = load_fixture('schema1').get_schema()
        del schema['Schema1Doc1']

        action = AlterDocument('Schema1Doc1')

        with pytest.raises(SchemaError):
            action.prepare(test_db, schema, MigrationPolicy.strict)
