import pymongo
import pytest
from bson import SON

from mongoengine_migrate.actions import AlterIndex
from mongoengine_migrate.exceptions import SchemaError, MigrationError
from mongoengine_migrate.graph import MigrationPolicy
from mongoengine_migrate.schema import Schema


@pytest.fixture
def left_schema():
    return Schema({
        'Document1': Schema.Document(
            {
                'field1': {'param11': 'schemavalue11', 'param12': 'schemavalue12'},
                'field2': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
            },
            parameters={'collection': 'document1'},
            indexes={
                'index1': {'fields': [('field1', pymongo.ASCENDING)]},
                'index2': {
                    'fields': [('field1', pymongo.ASCENDING), ('field2', pymongo.DESCENDING)],
                    'name': 'index2',
                    'sparse': True
                }
            },
        ),
        '~EmbeddedDocument2': Schema.Document(
            {
                'field1': {'param3': 'schemavalue3'},
                'field2': {'param4': 'schemavalue4'},
            },
            indexes={'index3': {'fields': [('field1', pymongo.DESCENDING)]}}
        )
    })


class TestAlterIndex:
    def test_forward__if_name_is_set_and_not_changed_and_field_spec_the_same__should_recreate_index(
            self, test_db, left_schema
    ):
        fields = [('field1', pymongo.ASCENDING), ('field2', pymongo.DESCENDING)]
        test_db['document1'].create_index(fields, name='index2', sparse=False)
        action = AlterIndex('Document1', 'index2', fields=fields, name='index2', sparse=True)
        action.prepare(test_db, left_schema, MigrationPolicy.strict)

        action.run_forward()

        indexes = [x for x in test_db['document1'].list_indexes() if x['key'] == SON(fields)]
        assert len(indexes) == 1
        assert indexes[0]['name'] == 'index2'
        assert indexes[0]['sparse'] is True

    def test_forward__if_name_is_set_and_changed_and_field_spec_is_the_same__should_recreate_index(
            self, test_db, left_schema
    ):
        fields = [('field1', pymongo.ASCENDING), ('field2', pymongo.DESCENDING)]
        test_db['document1'].create_index(fields, name='index_old', sparse=False)
        action = AlterIndex('Document1', 'index2', fields=fields, name='index2', sparse=True)
        action.prepare(test_db, left_schema, MigrationPolicy.strict)

        action.run_forward()

        indexes = [x for x in test_db['document1'].list_indexes() if x['key'] == SON(fields)]
        assert len(indexes) == 1
        assert indexes[0]['name'] == 'index2'
        assert indexes[0]['sparse'] is True

    def test_forward__if_name_is_set_and_changed_and_field_spec_also_changed__should_create_index(
            self, test_db, left_schema
    ):
        fields1 = [('field2', pymongo.ASCENDING)]
        fields2 = [('field1', pymongo.ASCENDING), ('field2', pymongo.DESCENDING)]
        test_db['document1'].create_index(fields1, name='index_old', sparse=False)
        action = AlterIndex('Document1', 'index2', fields=fields2, name='index2', sparse=True)
        action.prepare(test_db, left_schema, MigrationPolicy.strict)

        action.run_forward()

        indexes1 = [x for x in test_db['document1'].list_indexes() if x['key'] == SON(fields1)]
        assert len(indexes1) == 1
        assert indexes1[0]['sparse'] is False
        assert indexes1[0]['name'] == 'index_old'
        indexes2 = [x for x in test_db['document1'].list_indexes() if x['key'] == SON(fields2)]
        assert len(indexes2) == 1
        assert indexes2[0]['name'] == 'index2'
        assert indexes2[0]['sparse'] is True

    def test_forward__if_name_is_not_set_and_field_spec_is_the_same__should_recreate_index(
            self, test_db, left_schema
    ):
        fields = [('field1', pymongo.ASCENDING)]
        test_db['document1'].create_index(fields, sparse=False)
        action = AlterIndex('Document1', 'index1', fields=fields, sparse=True)
        action.prepare(test_db, left_schema, MigrationPolicy.strict)

        action.run_forward()

        indexes = [x for x in test_db['document1'].list_indexes() if x['key'] == SON(fields)]
        assert len(indexes) == 1
        assert indexes[0]['sparse'] is True

    def test_forward__if_name_is_not_set_and_field_spec_also_changed__should_create_new_index(
            self, test_db, left_schema
    ):
        fields1 = [('field1', pymongo.ASCENDING), ('field2', pymongo.DESCENDING)]
        fields2 = [('field1', pymongo.ASCENDING)]
        test_db['document1'].create_index(fields1, sparse=False)
        action = AlterIndex('Document1', 'index1', fields=fields2, sparse=True)
        action.prepare(test_db, left_schema, MigrationPolicy.strict)

        action.run_forward()

        indexes1 = [x for x in test_db['document1'].list_indexes() if x['key'] == SON(fields1)]
        assert len(indexes1) == 1
        assert indexes1[0]['sparse'] is False
        indexes2 = [x for x in test_db['document1'].list_indexes() if x['key'] == SON(fields2)]
        assert len(indexes2) == 1
        assert indexes2[0]['sparse'] is True

    def test_forward_backward__if_name_is_set_and_not_changed_and_field_spec_the_same__should_undo_changes(
            self, test_db, left_schema
    ):
        fields = [('field1', pymongo.ASCENDING), ('field2', pymongo.DESCENDING)]
        test_db['document1'].create_index(fields, name='index2', sparse=True)
        action = AlterIndex('Document1', 'index2', fields=fields, name='index2', sparse=False)
        action.prepare(test_db, left_schema, MigrationPolicy.strict)
        action.run_forward()
        action.cleanup()
        action.prepare(test_db, left_schema, MigrationPolicy.strict)

        action.run_backward()

        indexes = [x for x in test_db['document1'].list_indexes() if x['key'] == SON(fields)]
        assert len(indexes) == 1
        assert indexes[0]['name'] == 'index2'
        assert indexes[0]['sparse'] is True  # See index2 schema

    def test_forward_backward__if_name_is_set_and_changed_and_field_spec_is_the_same__should_undo_changes(
            self, test_db, left_schema
    ):
        fields = [('field1', pymongo.ASCENDING), ('field2', pymongo.DESCENDING)]
        test_db['document1'].create_index(fields, name='index_old', sparse=True)
        action = AlterIndex('Document1', 'index2', fields=fields, name='index2', sparse=False)
        action.prepare(test_db, left_schema, MigrationPolicy.strict)
        action.run_forward()
        action.cleanup()
        action.prepare(test_db, left_schema, MigrationPolicy.strict)

        action.run_backward()

        indexes = [x for x in test_db['document1'].list_indexes() if x['key'] == SON(fields)]
        assert len(indexes) == 1
        assert indexes[0]['name'] == 'index2'
        assert indexes[0]['sparse'] is True  # See index2 schema

    def test_forward_backward__if_name_is_not_set_and_field_spec_is_the_same__should_undo_changes(
            self, test_db, left_schema
    ):
        fields = [('field1', pymongo.ASCENDING)]
        test_db['document1'].create_index(fields, sparse=False)
        action = AlterIndex('Document1', 'index1', fields=fields, sparse=True)
        action.prepare(test_db, left_schema, MigrationPolicy.strict)
        action.run_forward()
        action.cleanup()
        action.prepare(test_db, left_schema, MigrationPolicy.strict)

        action.run_backward()

        indexes = [x for x in test_db['document1'].list_indexes() if x['key'] == SON(fields)]
        assert len(indexes) == 1
        assert 'sparse' not in indexes[0]  # See index1 schema

    def test_prepare__if_document_not_in_schema__should_raise_error(self, test_db, left_schema):
        action = AlterIndex('UnknownDocument', 'index1', fields=[('field1', pymongo.ASCENDING)])

        with pytest.raises(SchemaError):
            action.prepare(test_db, left_schema, MigrationPolicy.strict)

    def test_prepare__if_index_is_not_in_schema__should_raise_error(self, test_db, left_schema):
        action = AlterIndex('Document1', 'unknown_index', fields=[('field1', pymongo.ASCENDING)])

        with pytest.raises(SchemaError):
            action.prepare(test_db, left_schema, MigrationPolicy.strict)

    def test_build_object__if_index_has_changed__should_return_object(self, left_schema):
        right_schema = Schema({
            'Document1': Schema.Document(
                {
                    'field1': {'param11': 'schemavalue11', 'param12': 'schemavalue12'},
                    'field2': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
                },
                parameters={'collection': 'document1'},
                indexes={
                    'index1': {'fields': [('field1', pymongo.ASCENDING)]},
                    'index2': {
                        'fields': [('field1', pymongo.ASCENDING), ('field2', pymongo.DESCENDING)],
                        'name': 'index2',
                        'sparse': False
                    }
                }
            ),
            '~EmbeddedDocument2': Schema.Document({
                'field1': {'param3': 'schemavalue3'},
                'field2': {'param4': 'schemavalue4'},
            })
        })

        res = AlterIndex.build_object('Document1', 'index2', left_schema, right_schema)

        assert isinstance(res, AlterIndex)
        assert res.document_type == 'Document1'
        assert res.index_name == 'index2'
        assert res.parameters == {
            'fields': [('field1', pymongo.ASCENDING), ('field2', pymongo.DESCENDING)],
            'name': 'index2',
            'sparse': False,
        }

    @pytest.mark.parametrize('document_type', ('Document1', 'Document_new', 'Document_unknown'))
    def test_build_object__if_document_not_in_both_schemas__should_return_none(
            self, left_schema, document_type
    ):
        right_schema = Schema({
            'Document_new': Schema.Document(
                {
                    'field1': {'param11': 'schemavalue11', 'param12': 'schemavalue12'},
                    'field2': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
                    'field3': {'param31': 'schemavalue31', 'param32': 'schemavalue32'},
                },
                parameters={'collection': 'document1'},
                indexes={
                    'index1': {'fields': [('field1', pymongo.ASCENDING)]},
                    'index2': {
                        'fields': [('field1', pymongo.ASCENDING), ('field2', pymongo.DESCENDING)],
                        'name': 'index2',
                        'sparse': False
                    }
                }
            ),
            '~EmbeddedDocument2': Schema.Document({
                'field1': {'param3': 'schemavalue3'},
                'field2': {'param4': 'schemavalue4'},
            })
        })

        res = AlterIndex.build_object(document_type, 'index2', left_schema, right_schema)

        assert res is None

    @pytest.mark.parametrize('index_name', ('index3', 'index2', 'unknown_index'))
    def test_build_object__if_index_does_not_in_both_schemas__should_return_none(
            self, left_schema, index_name
    ):
        right_schema = Schema({
            'Document1': Schema.Document(
                {
                    'field1': {'param11': 'schemavalue11', 'param12': 'schemavalue12'},
                    'field3': {'param31': 'schemavalue31', 'param32': 'schemavalue32'},
                },
                parameters={'collection': 'document1'},
                indexes={
                    'index3': {'fields': [('field2', pymongo.DESCENDING)],
                               'name': 'index3',
                               'sparse': True},
                    'index1': {'fields': [('field1', pymongo.DESCENDING)]}
                }
            ),
            '~EmbeddedDocument2': Schema.Document({
                'field1': {'param3': 'schemavalue3'},
                'field2': {'param4': 'schemavalue4'},
            })
        })

        res = AlterIndex.build_object('Document1', index_name, left_schema, right_schema)

        assert res is None

    def test_build_object__if_index_in_embedded_document__should_return_none(self, left_schema):
        right_schema = Schema({
            'Document1': Schema.Document(
                {
                    'field1': {'param11': 'schemavalue11', 'param12': 'schemavalue12'},
                    'field3': {'param31': 'schemavalue31', 'param32': 'schemavalue32'},
                },
                parameters={'collection': 'document1'}
            ),
            '~EmbeddedDocument2': Schema.Document(
                {
                    'field1': {'param3': 'schemavalue3'},
                    'field2': {'param4': 'schemavalue4'},
                },
                indexes={'index3': {'fields': [('field1', pymongo.ASCENDING)]}}
            )
        })

        res = AlterIndex.build_object(
            '~EmbeddedDocument2', 'index3', left_schema, right_schema
        )

        assert res is None

    def test_to_schema_patch__should_return_dictdiffer_diff(self, left_schema):
        right_schema = Schema({
            'Document1': Schema.Document(
                {
                    'field1': {'param11': 'schemavalue11', 'param12': 'schemavalue12'},
                    'field2': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
                },
                parameters={'collection': 'document1'},
                indexes={
                    'index1': {'fields': [('field1', pymongo.ASCENDING)]},
                    'index2': {
                        'fields': [('field1', pymongo.ASCENDING)],
                        'name': 'index2'
                    }
                }
            ),
            '~EmbeddedDocument2': Schema.Document({
                'field1': {'param3': 'schemavalue3'},
                'field2': {'param4': 'schemavalue4'},
            })
        })
        action = AlterIndex(
            'Document1', 'index2', name='index2', fields=[('field1', pymongo.ASCENDING)]
        )
        expect = [('change', 'Document1', (left_schema['Document1'], right_schema['Document1']))]

        res = action.to_schema_patch(left_schema)

        assert res == expect
