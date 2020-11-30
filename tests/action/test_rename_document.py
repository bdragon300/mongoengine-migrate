import pytest

from mongoengine_migrate.actions import RenameDocument
from mongoengine_migrate.exceptions import SchemaError
from mongoengine_migrate.graph import MigrationPolicy
from mongoengine_migrate.schema import Schema


class TestRenameDocument:
    def test_build_object__on_embdedded_document_type__should_return_none(self):
        left_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            '~EmbeddedDocument': Schema.Document({
                'field21': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
                'field22': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
            }, parameters={}),
        })
        right_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            'Document2': Schema.Document({
                'field21': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
                'field22': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
            }, parameters={'collection': 'document21'}),
        })

        res = RenameDocument.build_object('~EmbeddedDocument', left_schema, right_schema)

        assert res is None

    @pytest.mark.xfail
    def test_build_object__if_document_is_similar_to_embedded_document__should_return_none(self):
        left_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            'Document2': Schema.Document({
                'field21': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
                'field22': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
            }, parameters={'collection': 'document21'}),
        })
        right_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            '~EmbeddedDocument': Schema.Document({
                'field21': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
                'field22': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
            }, parameters={}),
        })

        res = RenameDocument.build_object('Document2', left_schema, right_schema)

        assert res is None

    def test_build_object__if_document_was_just_renamed__should_return_object(self):
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
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            'Document2_new': Schema.Document({
                'field21': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
            }, parameters={'collection': 'document21'}),
        })

        res = RenameDocument.build_object('Document2', left_schema, right_schema)

        assert isinstance(res, RenameDocument)
        assert res.document_type == 'Document2'
        assert res.new_name == 'Document2_new'
        assert res.parameters == {}

    @pytest.mark.parametrize('new_schema', (
        Schema.Document({
            'field21': {'param23': 'schemavalue21', 'param22': 'schemavalue22'},
        }, parameters={'collection': 'document1'}),
        Schema.Document({
            'field21': {'param21': 'schemavalue23', 'param22': 'schemavalue22'},
        }, parameters={'collection': 'document1'}),
        Schema.Document({
            'field21': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
        }, parameters={'collection': 'another_document'}),
    ))
    def test_build_object__if_changes_similarity_more_than_threshold__should_return_object(
            self, new_schema
    ):
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
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            'Document2_new': new_schema,
        })

        res = RenameDocument.build_object('Document2', left_schema, right_schema)

        assert isinstance(res, RenameDocument)
        assert res.document_type == 'Document2'
        assert res.new_name == 'Document2_new'
        assert res.parameters == {}

    @pytest.mark.parametrize('document_type', ('Document1', 'Document_unknown'))
    def test_build_object__if_document_is_not_disappears_in_right_schema__should_return_none(
            self, document_type
    ):
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
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            'Document2': Schema.Document({
                'field2': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document21'}),
        })

        res = RenameDocument.build_object(document_type, left_schema, right_schema)

        assert res is None

    def test_build_object__if_changes_similarity_less_than_threshold__should_return_object(
            self, left_schema, baserenamedocumentaction_stub
    ):
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
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            'Document2': Schema.Document({
                'field2': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'another_collection', 'parameter': 'value'}),
        })

        res = RenameDocument.build_object('Document2', left_schema, right_schema)

        assert res is None

    def test_forward__should_do_nothing(self, load_fixture, test_db, dump_db):
        schema = load_fixture('schema1').get_schema()
        dump = dump_db()

        action = RenameDocument('Schema1Doc1', new_name='NewNameDoc')
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump == dump_db()

    def test_backward__should_do_nothing(self, load_fixture, test_db, dump_db):
        schema = load_fixture('schema1').get_schema()
        dump = dump_db()

        action = RenameDocument('Schema1Doc1', new_name='NewNameDoc')
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_backward()

        assert dump == dump_db()

    def test_prepare__if_such_document_is_not_in_schema__should_raise_error(self,
                                                                            load_fixture,
                                                                            test_db):
        schema = load_fixture('schema1').get_schema()
        del schema['Schema1Doc1']

        action = RenameDocument('Schema1Doc1', new_name='NewNameDoc')

        with pytest.raises(SchemaError):
            action.prepare(test_db, schema, MigrationPolicy.strict)
