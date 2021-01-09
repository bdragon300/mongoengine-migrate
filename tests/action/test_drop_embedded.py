import pytest

from mongoengine_migrate.actions import DropEmbedded
from mongoengine_migrate.graph import MigrationPolicy
from mongoengine_migrate.schema import Schema


class TestDropEmbedded:
    def test_build_object__on_usual_document_type__should_return_none(self):
        left_schema = Schema({
            '~EmbeddedDocument1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={}),
            'Document1': Schema.Document({
                'field1': {'param_new': 'schemavalue_new'},
            }, parameters={'collection': 'document_new', 'test_parameter': 'test_value'})
        })
        right_schema = Schema({
            '~EmbeddedDocument1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={}),
        })

        res = DropEmbedded.build_object('Document1', left_schema, right_schema)

        assert res is None

    def test_build_object__if_embedded_document_is_droppoing__should_return_object(self):
        left_schema = Schema({
            '~EmbeddedDocument1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'param1': 'value1'}),
            'Document1': Schema.Document({
                'field1': {'param_new': 'schemavalue_new'},
            }, parameters={'collection': 'document_new', 'test_parameter': 'test_value'})
        })
        right_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param_new': 'schemavalue_new'},
            }, parameters={'collection': 'document_new', 'test_parameter': 'test_value'})
        })

        res = DropEmbedded.build_object('~EmbeddedDocument1', left_schema, right_schema)

        assert isinstance(res, DropEmbedded)
        assert res.document_type == '~EmbeddedDocument1'
        assert res.parameters == {}

    @pytest.mark.parametrize('document_type', ('~EmbeddedDocument2', '~Document_unknown'))
    def test_build_object__if_document_is_not_dropping_in_schema__should_return_none(
            self, document_type
    ):
        left_schema = Schema({
            '~EmbeddedDocument1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'param1': 'value1'}),
            '~EmbeddedDocument2': Schema.Document({
                'field1': {'param_new': 'schemavalue_new'},
            }, parameters={'test_parameter': 'test_value'})
        })
        right_schema = Schema({
            '~EmbeddedDocument2': Schema.Document({
                'field1': {'param_new': 'schemavalue_new'},
            }, parameters={'test_parameter': 'test_value'})
        })

        res = DropEmbedded.build_object(document_type, left_schema, right_schema)

        assert res is None

    def test_forward__should_do_nothing(self, load_fixture, test_db, dump_db):
        schema = load_fixture('schema1').get_schema()
        dump = dump_db()

        action = DropEmbedded('~Schema1EmbDoc1')
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump == dump_db()

    def test_backward__should_do_nothing(self, load_fixture, test_db, dump_db):
        schema = load_fixture('schema1').get_schema()
        dump = dump_db()

        action = DropEmbedded('~Schema1EmbDoc1')
        action.prepare(test_db, schema, MigrationPolicy.strict)
        action.run_backward()

        assert dump == dump_db()
