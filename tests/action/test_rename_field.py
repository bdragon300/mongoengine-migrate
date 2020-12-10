import pytest

from mongoengine_migrate.actions import RenameField
from mongoengine_migrate.exceptions import SchemaError
from mongoengine_migrate.graph import MigrationPolicy
from mongoengine_migrate.schema import Schema


class TestRenameField:
    @pytest.mark.parametrize('new_schema', (
            {'param1': 'val1', 'param2': 'val2', 'param3': 'val3', 'param4': 'val4'},
            {'param1': 'val_changed', 'param2': 'val2', 'param3': 'val3', 'param4': 'val4'},
            {'param_changed': 'val1', 'param2': 'val2', 'param3': 'val3', 'param4': 'val4'},
    ))
    def test_build_object__if_changes_similarity_more_than_threshold__should_return_object(
            self, new_schema
    ):
        left_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param1': 'val1', 'param2': 'val2', 'param3': 'val3', 'param4': 'val4'},
            }, parameters={'collection': 'document1'}),
            'Document2': Schema.Document({
                'field21': {'param21': 'val21', 'param22': 'val22'},
            }, parameters={'collection': 'document21'}),
        })
        right_schema = Schema({
            'Document1': Schema.Document({
                'field_new': new_schema,
            }, parameters={'collection': 'document1'}),
            'Document2': Schema.Document({
                'field21': {'param21': 'val21', 'param22': 'val22'},
            }, parameters={'collection': 'document21'}),
        })

        res = RenameField.build_object('Document1', 'field1', left_schema, right_schema)

        assert isinstance(res, RenameField)
        assert res.document_type == 'Document1'
        assert res.field_name == 'field1'
        assert res.new_name == 'field_new'
        assert res.parameters == {'new_name': 'field_new'}

    def test_build_object__if_db_field_remains_the_same__should_return_object(self):
        left_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'db_field': 'field1', 'param1': 'value1', 'param2': 'value2'},
            }, parameters={'collection': 'document1'}),
            'Document2': Schema.Document({
                'field21': {'param21': 'value21', 'param22': 'value22'},
            }, parameters={'collection': 'document21'}),
        })
        right_schema = Schema({
            'Document1': Schema.Document({
                'field_new': {
                    'db_field': 'field1', 'param_changed': 'value1', 'param2': 'value_changed'
                },
            }, parameters={'collection': 'document1'}),
            'Document2': Schema.Document({
                'field21': {'param21': 'value21', 'param22': 'value22'},
            }, parameters={'collection': 'document21'}),
        })

        res = RenameField.build_object('Document1', 'field1', left_schema, right_schema)

        assert isinstance(res, RenameField)
        assert res.document_type == 'Document1'
        assert res.field_name == 'field1'
        assert res.new_name == 'field_new'
        assert res.parameters == {'new_name': 'field_new'}

    def test_build_object__if_changes_similarity_less_than_threshold__should_return_none(self):
        left_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            'Document2': Schema.Document({
                'field21': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
            }, parameters={'collection': 'document21'}),
        })
        right_schema = Schema({
            'Document1': Schema.Document({
                'field_new1': {'param_changed': 'value1', 'param2': 'value_changed'},
            }, parameters={'collection': 'document1'}),
            'Document2': Schema.Document({
                'field21': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
            }, parameters={'collection': 'document21'}),
        })

        res = RenameField.build_object('Document1', 'field1', left_schema, right_schema)

        assert res is None

    def test_build_object__if_there_are_several_rename_candidates__should_return_none(self):
        left_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            'Document2': Schema.Document({
                'field21': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
            }, parameters={'collection': 'document21'}),
        })
        right_schema = Schema({
            'Document1': Schema.Document({
                'field_new1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
                'field_new2': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            'Document2': Schema.Document({
                'field21': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
            }, parameters={'collection': 'document21'}),
        })

        res = RenameField.build_object('Document1', 'field1', left_schema, right_schema)

        assert res is None

    @pytest.mark.parametrize('document_type', ('Document1', 'Document_new', 'Document_unknown'))
    def test_build_object__if_document_not_in_both_schemas__should_return_none(self, document_type):
        left_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            'Document2': Schema.Document({
                'field21': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
            }, parameters={'collection': 'document21'}),
        })
        right_schema = Schema({
            'Document_new': Schema.Document({
                'field_new1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
                'field_new2': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            'Document2': Schema.Document({
                'field21': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
            }, parameters={'collection': 'document21'}),
        })

        res = RenameField.build_object(document_type, 'field2', left_schema, right_schema)

        assert res is None

    @pytest.mark.parametrize('field_name', ('field1', 'field3', 'field_unknown'))
    def test_build_object__if_field_not_in_left_schema_only__should_return_none(self, field_name):
        left_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
                'field2': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            'Document2': Schema.Document({
                'field21': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
            }, parameters={'collection': 'document21'}),
        })
        right_schema = Schema({
            'Document_new': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
                'field3': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            'Document2': Schema.Document({
                'field21': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
            }, parameters={'collection': 'document21'}),
        })

        res = RenameField.build_object('Document1', field_name, left_schema, right_schema)

        assert res is None

    def test_to_schema_patch__should_return_dictdiff_object(self):
        left_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param11': 'schemavalue11', 'param12': 'schemavalue12'},
            }, parameters={'collection': 'document1'})
        })

        expect = [
            (
                'remove',
                'Document1',
                [('field1', {'param11': 'schemavalue11', 'param12': 'schemavalue12'})]
            ),
            (
                'add',
                'Document1',
                [('field2', {'param11': 'schemavalue11', 'param12': 'schemavalue12'})]
            )
        ]
        action = RenameField('Document1', 'field1', new_name='field2')

        res = action.to_schema_patch(left_schema)

        assert res == expect

    @pytest.mark.parametrize('document_type,field_name', (
            ('Document_unknown', 'field1'),
            ('Document1', 'field_unknown'),
    ))
    def test_to_schema_patch__if_document_and_field_not_exist_in_left_schema__should_raise_error(
            self, document_type, field_name
    ):
        left_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param11': 'schemavalue11', 'param12': 'schemavalue12'},
            }, parameters={'collection': 'document1'})
        })
        action = RenameField(document_type, field_name, new_name='field2')

        with pytest.raises(SchemaError):
            action.to_schema_patch(left_schema)

    def test_forward__should_do_nothing(self, load_fixture, test_db, dump_db):
        schema = load_fixture('schema1').get_schema()
        dump = dump_db()

        action = RenameField('Schema1Doc1', 'doc1_str', new_name='field_new')
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump == dump_db()

    def test_backward__should_do_nothing(self, load_fixture, test_db, dump_db):
        schema = load_fixture('schema1').get_schema()
        dump = dump_db()

        action = RenameField('Schema1Doc1', 'doc1_str', new_name='field_new')
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_backward()

        assert dump == dump_db()
