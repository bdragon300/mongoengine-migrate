import pytest

from mongoengine_migrate.actions import AlterEmbedded
from mongoengine_migrate.graph import MigrationPolicy
from mongoengine_migrate.schema import Schema


class TestAlterEmbedded:
    def test_build_object__on_usual_document_type__should_return_none(self):
        left_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            '~EmbeddedDocument1': Schema.Document({
                'field21': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
                'field22': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
            }, parameters={}),
        })
        right_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param_new': 'schemavalue_new', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            '~EmbeddedDocument1': Schema.Document({
                'field21': {'param_new': 'schemavalue_new'},
                'field22': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
            }, parameters={}),
        })

        res = AlterEmbedded.build_object('Document1', left_schema, right_schema)

        assert res is None

    def test_build_object__if_embedded_document_schema_has_changed__should_return_object(self):
        left_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            '~EmbeddedDocument1': Schema.Document({
                'field21': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
                'field22': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
            }, parameters={'param': 'value'}),
        })
        right_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            '~EmbeddedDocument1': Schema.Document({
                'field21': {'param_new': 'schemavalue_new'},
                'field22': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
            }, parameters={'param123': 'value123'}),
        })

        res = AlterEmbedded.build_object('~EmbeddedDocument1', left_schema, right_schema)

        assert isinstance(res, AlterEmbedded)
        assert res.document_type == '~EmbeddedDocument1'
        assert res.parameters == {'param123': 'value123'}

    @pytest.mark.parametrize('document_type', (
            '~EmbeddedDocument2', '~EmbeddedDocument3', '!EmbeddedDocument_unknown'
    ))
    def test_build_object__if_document_is_not_in_both_schemas__should_return_none(
            self, document_type
    ):
        left_schema = Schema({
            '~EmbeddedDocument1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'param': 'value'}),
            '~EmbeddedDocument2': Schema.Document({
                'field1': {'param': 'schemavalue'},
            }, parameters={'test_parameter': 'test_value'})
        })
        right_schema = Schema({
            '~EmbeddedDocument1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'param': 'value'}),
            '~EmbeddedDocument3': Schema.Document({
                'field1': {'param_new': 'schemavalue_new'},
            }, parameters={'test_parameter': 'test_value'})
        })

        res = AlterEmbedded.build_object(document_type, left_schema, right_schema)

        assert res is None

    def test_build_object__if_document_schema_has_not_changed__should_return_none(self):
        left_schema = Schema({
            '~EmbeddedDocument1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'param': 'value'}),
            '~EmbeddedDocument2': Schema.Document({
                'field1': {'param': 'schemavalue'},
            }, parameters={'test_parameter': 'test_value'})
        })
        right_schema = Schema({
            '~EmbeddedDocument1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'param': 'value'}),
            '~EmbeddedDocument2': Schema.Document({
                'field1': {'param': 'schemavalue'},
            }, parameters={'test_parameter': 'test_value'})
        })

        res = AlterEmbedded.build_object('~EmbeddedDocument1', left_schema, right_schema)

        assert res is None


# TODO: inheritance
class TestAlterEmbeddedInherit:
    def test_forward__if_embedded_document_became_inherited__should_do_nothing(
            self, load_fixture, test_db, dump_db
    ):
        schema = load_fixture('schema1').get_schema()

        expect = dump_db()

        action = AlterEmbedded('~Schema1EmbDoc1', inherit=True)
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump_db() == expect


# TODO: dynamic fields
class TestAlterEmbeddedDynamic:
    pass
