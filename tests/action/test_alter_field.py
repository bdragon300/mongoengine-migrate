import itertools
import re
from copy import deepcopy
from unittest.mock import patch

import bson
import jsonpath_rw
import mongoengine
import pytest
from bson import ObjectId

from mongoengine_migrate.actions import AlterField
from mongoengine_migrate.exceptions import SchemaError, InconsistencyError
from mongoengine_migrate.graph import MigrationPolicy
from mongoengine_migrate.schema import Schema


@pytest.fixture
def left_schema():
    return Schema({
        'Document1': Schema.Document({
            'field1': {'param11': 'value11', 'param12': 'value12', 'param13': 'value13'},
            'field2': {'param21': 'value21', 'param22': 'value22', 'param23': 'value23'},
        }, parameters={'collection': 'document1'}),
        '~EmbeddedDocument2': Schema.Document({
            'field1': {'param3': 'value3'},
            'field2': {'param4': 'value4'},
        })
    })


class TestAlterField:
    def test_build_object__should_return_object(self, left_schema):
        right_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param11': 'value11', 'param12': 'value12'},
                'field2': {'param21': 'value21_new', 'param_new': 'value_new',
                           'param23': 'value23'},
            }, parameters={'collection': 'document1'}),
            '~EmbeddedDocument2': Schema.Document({
                'field1': {'param3': 'value3'},
                'field2': {'param4': 'value4'},
            })
        })

        res = AlterField.build_object('Document1', 'field2', left_schema, right_schema)

        assert isinstance(res, AlterField)
        assert res.document_type == 'Document1'
        assert res.field_name == 'field2'
        assert res.parameters == {'param21': 'value21_new', 'param_new': 'value_new'}

    @pytest.mark.parametrize('document_type', ('Document1', 'Document_new', 'Document_unknown'))
    def test_build_object__if_document_not_in_both_schemas__should_return_none(
            self, left_schema, document_type
    ):
        right_schema = Schema({
            'Document_new': Schema.Document({
                'field1': {'param11': 'value11', 'param12': 'value12', 'param13': 'value13'},
                'field2': {'param21': 'value21_new', 'param_new': 'value_new',
                           'param23': 'value23'},
            }, parameters={'collection': 'document1'}),
            '~EmbeddedDocument2': Schema.Document({
                'field1': {'param3': 'value3'},
                'field2': {'param4': 'value4'},
            })
        })

        res = AlterField.build_object(document_type, 'field2', left_schema, right_schema)

        assert res is None

    @pytest.mark.parametrize('field_name', ('field2', 'field3', 'field_unknown'))
    def test_build_object__if_field_not_in_both_schemas__should_return_none(
            self, left_schema, field_name
    ):
        right_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param11': 'value11', 'param12': 'value12', 'param13': 'value13'},
                'field3': {'param31': 'value31_new', 'param_new': 'value_new',
                           'param33': 'value33'},
            }, parameters={'collection': 'document1'}),
            '~EmbeddedDocument2': Schema.Document({
                'field1': {'param3': 'value3'},
                'field2': {'param4': 'value4'},
            })
        })

        res = AlterField.build_object('Document1', field_name, left_schema, right_schema)

        assert res is None

    def test_build_object__if_field_schema_has_not_changed__should_return_none(self, left_schema):
        right_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param11': 'value11', 'param12': 'value12', 'param13': 'value13'},
                'field2': {'param21': 'value21', 'param22': 'value22', 'param23': 'value23'},
            }, parameters={'collection': 'document1'}),
            '~EmbeddedDocument2': Schema.Document({
                'field1': {'param3': 'value3'},
                'field2': {'param4': 'value4'},
            })
        })

        res = AlterField.build_object('Document1', 'field2', left_schema, right_schema)

        assert res is None

    def test_to_schema_patch__should_return_dictdiffer_object(self):
        left_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param11': 'value11', 'param12': 'value12', 'param13': 'value13'},
                'field2': {'type_key': 'StringField', 'db_field': 'field2', 'param22': 'value22',
                           'param23': 'value23'},
            }, parameters={'collection': 'document1'}),
            '~EmbeddedDocument2': Schema.Document({
                'field1': {'param3': 'value3'},
                'field2': {'param4': 'value4'},
            })
        })
        test_schema_skel = {'type_key': None, 'db_field': None, 'param24': None}
        expect = [
            ('remove', 'Document1.field2', [('param22', ())]),
            ('remove', 'Document1.field2', [('param23', ())]),
            ('add', 'Document1.field2', [('param24', 'value24')]),
            ('change', 'Document1.field2.type_key', ('StringField', 'IntField'))
        ]
        action = AlterField('Document1', 'field2', type_key='IntField', param24='value24')

        patcher = patch.object(action, 'get_field_handler_cls')
        with patcher as get_field_handler_cls_mock:
            get_field_handler_cls_mock().schema_skel.return_value = test_schema_skel

            res = action.to_schema_patch(left_schema)

        assert res == expect

    @pytest.mark.parametrize('document_type,field_name', (
            ('Document_unknown', 'field2'),
            ('Document1', 'field_unknown'),
    ))
    def test_to_schema_patch__if_document_or_field_does_not_exist_should_raise_error(
            self, document_type, field_name
    ):
        left_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param11': 'value11', 'param12': 'value12', 'param13': 'value13'},
                'field2': {'type_key': 'StringField', 'db_field': 'field2', 'param22': 'value22',
                           'param23': 'value23'},
            }, parameters={'collection': 'document1'}),
            '~EmbeddedDocument2': Schema.Document({
                'field1': {'param3': 'value3'},
                'field2': {'param4': 'value4'},
            })
        })

        action = AlterField(document_type, field_name, type_key='IntField', param24='value24')
        test_schema_skel = {'param21': None, 'param22': None, 'param23': None}

        patcher = patch.object(action, 'get_field_handler_cls')
        with patcher as get_field_handler_cls_mock:
            get_field_handler_cls_mock.schema_skel.return_value = test_schema_skel

            with pytest.raises(SchemaError):
                action.to_schema_patch(left_schema)

    def test_to_schema_patch__if_parameters_not_in_schema__should_raise_error(self):
        left_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param11': 'value11', 'param12': 'value12', 'param13': 'value13'},
                'field2': {'type_key': 'StringField', 'db_field': 'field2', 'param22': 'value22',
                           'param23': 'value23'},
            }, parameters={'collection': 'document1'}),
            '~EmbeddedDocument2': Schema.Document({
                'field1': {'param3': 'value3'},
                'field2': {'param4': 'value4'},
            })
        })
        test_schema_skel = {'type_key': None, 'db_field': None, 'param24': None}
        action = AlterField('Document1', 'field2', type_key='IntField', param_unknown='value')

        patcher = patch.object(action, 'get_field_handler_cls')
        with patcher as get_field_handler_cls_mock:
            get_field_handler_cls_mock.schema_skel.return_value = test_schema_skel

            with pytest.raises(SchemaError):
                res = action.to_schema_patch(left_schema)


class TestAlterFieldCommonDbField:
    def test_forward__for_document__should_rename_field(self, load_fixture, test_db, dump_db):
        schema = load_fixture('schema1').get_schema()

        dump = dump_db()
        expect = deepcopy(dump)
        parser = jsonpath_rw.parse('schema1_doc1[*]')
        for rec in parser.find(expect):
            rec.value['new_field'] = rec.value.pop('doc1_str')

        action = AlterField('Schema1Doc1', 'doc1_str', db_field='new_field')
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump_db() == expect

    def test_forward__for_embedded_document__should_rename_field(
            self, load_fixture, test_db, dump_db
    ):
        schema = load_fixture('schema1').get_schema()
        dump = dump_db()
        expect = deepcopy(dump)
        parsers = load_fixture('schema1').get_embedded_jsonpath_parsers('~Schema1EmbDoc1')
        for rec in itertools.chain.from_iterable(p.find(expect) for p in parsers):
            rec.value['new_embfield'] = rec.value.pop('embdoc1_str')

        action = AlterField('~Schema1EmbDoc1', 'embdoc1_str', db_field='new_embfield')
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump_db() == expect

    @pytest.mark.parametrize('document_type,field_name', (
        ('Schema1Doc1', 'doc1_str'),
        ('~Schema1EmbDoc1', 'embdoc1_str'),
        ('~Schema1EmbDoc2', 'embdoc2_str')
    ))
    def test_forward_backward__should_rename_field_back(
            self, load_fixture, test_db, dump_db, document_type, field_name
    ):
        schema = load_fixture('schema1').get_schema()

        dump = dump_db()

        action = AlterField(document_type, field_name, db_field='new_field')
        action.prepare(test_db, schema, MigrationPolicy.strict)
        action.run_forward()
        action.cleanup()
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_backward()

        assert dump_db() == dump


class TestAlterFieldCommonRequired:
    def test_forward__for_document_when_default_is_set__should_set_to_default_value(
            self, load_fixture, test_db, dump_db
    ):
        default = 'test!'
        schema = load_fixture('schema1').get_schema()

        dump = dump_db()
        expect = deepcopy(dump)
        parser = jsonpath_rw.parse('schema1_doc1[*]')
        for rec in parser.find(expect):
            rec.value['doc1_str_empty'] = default

        action = AlterField('Schema1Doc1', 'doc1_str_empty', required=True, default=default)
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump_db() == expect

    def test_forward__for_embedded_document__should_make_required(
            self, load_fixture, test_db, dump_db
    ):
        default = 'test!'
        schema = load_fixture('schema1').get_schema()

        dump = dump_db()
        expect = deepcopy(dump)
        parsers = load_fixture('schema1').get_embedded_jsonpath_parsers('~Schema1EmbDoc1')
        for rec in itertools.chain.from_iterable(p.find(expect) for p in parsers):
            rec.value['embdoc1_str_empty'] = default

        action = AlterField('~Schema1EmbDoc1', 'embdoc1_str_empty', required=True, default=default)
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump_db() == expect

    def test_forward_backward__for_document_when_default_is_set__should_leave_set_values(
            self, load_fixture, test_db, dump_db
    ):
        default = 'test!'
        schema = load_fixture('schema1').get_schema()

        dump = dump_db()
        expect = deepcopy(dump)
        parser = jsonpath_rw.parse('schema1_doc1[*]')
        for rec in parser.find(expect):
            rec.value['doc1_str_empty'] = default

        action = AlterField('Schema1Doc1', 'doc1_str_empty', required=True, default=default)
        action.prepare(test_db, schema, MigrationPolicy.strict)
        action.run_forward()
        action.cleanup()
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_backward()

        assert dump_db() == expect

    def test_forward_backward__for_embedded_when_default_is_set__should_leave_set_values(
            self, load_fixture, test_db, dump_db
    ):
        default = 'test!'
        schema = load_fixture('schema1').get_schema()

        dump = dump_db()
        expect = deepcopy(dump)
        parsers = load_fixture('schema1').get_embedded_jsonpath_parsers('~Schema1EmbDoc1')
        for rec in itertools.chain.from_iterable(p.find(expect) for p in parsers):
            rec.value['embdoc1_str_empty'] = default

        action = AlterField('~Schema1EmbDoc1', 'embdoc1_str_empty', required=True, default=default)
        action.prepare(test_db, schema, MigrationPolicy.strict)
        action.run_forward()
        action.cleanup()
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_backward()

        assert dump_db() == expect

    @pytest.mark.parametrize('document_type,field_name', (
        ('Schema1Doc1', 'doc1_str'),
        ('~Schema1EmbDoc1', 'embdoc1_str'),
        ('~Schema1EmbDoc2', 'embdoc2_str')
    ))
    def test_forward__when_default_is_not_set__should_raise_error(
            self, load_fixture, test_db, dump_db, document_type, field_name
    ):
        schema = load_fixture('schema1').get_schema()

        action = AlterField(document_type, field_name, required=True, default=None)
        action.prepare(test_db, schema, MigrationPolicy.strict)

        with pytest.raises(SchemaError):
            action.run_forward()


class TestAlterFieldCommonDefault:
    def test_forward__for_document_when_default_is_set__should_do_nothing(
            self, load_fixture, test_db, dump_db
    ):
        default = 'test!'
        schema = load_fixture('schema1').get_schema()

        dump = dump_db()

        action = AlterField('Schema1Doc1', 'doc1_str_empty', default=default)
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump_db() == dump

    @pytest.mark.parametrize('document_type,field_name', (
        ('Schema1Doc1', 'doc1_str'),
        ('~Schema1EmbDoc1', 'embdoc1_str'),
        ('~Schema1EmbDoc2', 'embdoc2_str')
    ))
    def test_forward_backward__should_do_nothing(
            self, load_fixture, test_db, dump_db, document_type, field_name
    ):
        default = 'test!'
        schema = load_fixture('schema1').get_schema()

        dump = dump_db()

        action = AlterField(document_type, field_name, default=default)
        action.prepare(test_db, schema, MigrationPolicy.strict)
        action.run_forward()
        action.cleanup()
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_backward()

        assert dump_db() == dump


class TestAlterFieldCommonUnique:
    def test_forward__should_do_nothing(
            self, load_fixture, test_db, dump_db
    ):
        schema = load_fixture('schema1').get_schema()

        dump = dump_db()

        action = AlterField('Schema1Doc1', 'doc1_str', unique=True)
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump_db() == dump

    def test_forward_backward__should_do_nothing(self, load_fixture, test_db, dump_db):
        schema = load_fixture('schema1').get_schema()

        dump = dump_db()

        action = AlterField('Schema1Doc1', 'doc1_str', unique=True)
        action.prepare(test_db, schema, MigrationPolicy.strict)
        action.run_forward()
        action.cleanup()
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_backward()

        assert dump_db() == dump


class TestAlterFieldCommonUniqueWith:
    def test_forward__should_do_nothing(
            self, load_fixture, test_db, dump_db
    ):
        schema = load_fixture('schema1').get_schema()

        dump = dump_db()

        action = AlterField('Schema1Doc1', 'doc1_str', unique_with=['doc1_int'])
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump_db() == dump

    def test_forward_backward__should_do_nothing(self, load_fixture, test_db, dump_db):
        schema = load_fixture('schema1').get_schema()

        dump = dump_db()

        action = AlterField('Schema1Doc1', 'doc1_str', unique_with=['doc1_int'])
        action.prepare(test_db, schema, MigrationPolicy.strict)
        action.run_forward()
        action.cleanup()
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_backward()

        assert dump_db() == dump


class TestAlterFieldCommonPrimaryKey:
    def test_forward__if_field_is_filled__do_nothing(self, load_fixture, test_db, dump_db):
        schema = load_fixture('schema1').get_schema()

        dump = dump_db()

        action = AlterField('Schema1Doc1', 'doc1_str', primary_key=True)
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump_db() == dump

    def test_forward__if_field_is_partially_filled__raise_error(self, load_fixture, test_db):
        schema = load_fixture('schema1').get_schema()
        for doc in test_db['schema1_doc1'].find(limit=2):
            test_db['schema1_doc1'].update_one({'_id': doc['_id']},
                                               {'$set': {'doc1_str_empty': 'test!'}})

        action = AlterField('Schema1Doc1', 'doc1_str_empty', primary_key=True)
        action.prepare(test_db, schema, MigrationPolicy.strict)

        with pytest.raises(InconsistencyError):
            action.run_forward()

    def test_forward_backward__should_do_nothing(self, load_fixture, test_db, dump_db):
        schema = load_fixture('schema1').get_schema()

        dump = dump_db()

        action = AlterField('Schema1Doc1', 'doc1_str', primary_key=True)
        action.prepare(test_db, schema, MigrationPolicy.strict)
        action.run_forward()
        action.cleanup()
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_backward()

        assert dump_db() == dump

    def test_forward__for_embedded_document__raise_error(self, load_fixture, test_db):
        schema = load_fixture('schema1').get_schema()

        action = AlterField('~Schema1EmbDoc1', 'embdoc1_str', primary_key=True)
        action.prepare(test_db, schema, MigrationPolicy.strict)

        with pytest.raises(SchemaError):
            action.run_forward()

    def test_backward__for_embedded_document__raise_error(self, load_fixture, test_db):
        schema = load_fixture('schema1').get_schema()

        action = AlterField('~Schema1EmbDoc1', 'embdoc1_str', primary_key=True)
        action.prepare(test_db, schema, MigrationPolicy.strict)

        with pytest.raises(SchemaError):
            action.run_backward()


class TestAlterFieldCommonChoices:
    @pytest.mark.parametrize('document_type,field_name', (
        ('Schema1Doc1', 'doc1_str_ten'),
        ('~Schema1EmbDoc1', 'embdoc1_str_ten'),
        ('~Schema1EmbDoc2', 'embdoc2_str_ten')
    ))
    @pytest.mark.parametrize('old_choices,new_choices', (
        (None, [str(x) for x in range(11)]),  # set up
        (['doesnt', 'match'], [str(x) for x in range(11)]),  # change
        ([], [str(x) for x in range(11)]),  # change
    ))
    def test_forward__on_setup_or_change_choices_and_if_field_values_are_in_choices__do_nothing(
            self, load_fixture, test_db, dump_db, document_type, field_name, old_choices, new_choices,
    ):
        schema = load_fixture('schema1').get_schema()
        schema[document_type][field_name]['choices'] = old_choices

        dump = dump_db()

        action = AlterField(document_type, field_name, choices=new_choices)
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump_db() == dump

    @pytest.mark.parametrize('document_type,field_name', (
        ('Schema1Doc1', 'doc1_str_ten'),
        ('~Schema1EmbDoc1', 'embdoc1_str_ten'),
        ('~Schema1EmbDoc2', 'embdoc2_str_ten')
    ))
    @pytest.mark.parametrize('old_choices,new_choices', (
        (None, ['choices', 'which', 'doesnt', 'match']),  # set up
        ([str(x) for x in range(11)], ['choices', 'which', 'doesnt', 'match'])  # change
    ))
    def test_forward__on_setup_or_change_choices_and_if_some_values_are_not_in_choices__raise_error(
            self, load_fixture, test_db, document_type, field_name, old_choices, new_choices
    ):
        schema = load_fixture('schema1').get_schema()
        schema[document_type][field_name]['choices'] = old_choices

        action = AlterField(document_type, field_name, choices=new_choices)
        action.prepare(test_db, schema, MigrationPolicy.strict)

        with pytest.raises(InconsistencyError):
            action.run_forward()

    @pytest.mark.parametrize('document_type,field_name', (
        ('Schema1Doc1', 'doc1_str_ten'),
        ('~Schema1EmbDoc1', 'embdoc1_str_ten'),
        ('~Schema1EmbDoc2', 'embdoc2_str_ten')
    ))
    @pytest.mark.parametrize('old_choices,new_choices', (
        (None, [str(x) for x in range(11)]),
        ([str(x) for x in range(11)], None)
    ))
    def test_forward_backward__on_setup_or_change_choices_and_values_are_in_choices__do_nothing(
            self, load_fixture, test_db, dump_db, document_type, field_name, old_choices, new_choices
    ):
        schema = load_fixture('schema1').get_schema()
        schema[document_type][field_name]['choices'] = old_choices

        dump = dump_db()

        action = AlterField(document_type, field_name, choices=new_choices)
        action.prepare(test_db, schema, MigrationPolicy.strict)
        action.run_forward()
        action.cleanup()
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_backward()

        assert dump_db() == dump

    def test_forward_backward__for_document_if_values_became_wrong_after_forward_step__raise_error(
            self, load_fixture, test_db
    ):
        schema = load_fixture('schema1').get_schema()

        action = AlterField('Schema1Doc1', 'doc1_str_ten', choices=None)
        action.prepare(test_db, schema, MigrationPolicy.strict)
        action.run_forward()
        action.cleanup()
        for doc in test_db['schema1_doc1'].find(limit=2):
            test_db['schema1_doc1'].update_one({'_id': doc['_id']},
                                               {'$set': {'doc1_str_ten': 'out_of_choices'}})
        action.prepare(test_db, schema, MigrationPolicy.strict)

        with pytest.raises(InconsistencyError):
            action.run_backward()

    def test_forward_backward__for_embedded_document_if_values_corrupted_after_forward_step__raise_error(
            self, load_fixture, test_db
    ):
        schema = load_fixture('schema1').get_schema()

        action = AlterField('~Schema1EmbDoc1', 'embdoc1_str_ten', choices=None)
        action.prepare(test_db, schema, MigrationPolicy.strict)
        action.run_forward()
        action.cleanup()

        # Corrupt data in db
        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000002'))
        doc['doc1_emb_embdoc1']['embdoc1_str_ten'] = 'test!'
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000002')}, doc)
        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000003'))
        doc['doc1_emblist_embdoc1'][0]['embdoc1_str_ten'] = 'test!'
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000003')}, doc)

        action.prepare(test_db, schema, MigrationPolicy.strict)

        with pytest.raises(InconsistencyError):
            action.run_backward()


class TestAlterFieldCommonNull:
    def test_forward__for_document_if_field_is_unset__should_set_unset_fields_to_null(
            self, load_fixture, test_db, dump_db
    ):
        schema = load_fixture('schema1').get_schema()

        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000002'))
        doc['doc1_str_empty'] = 'test!'
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000002')}, doc)

        expect = dump_db()
        parsers = load_fixture('schema1').get_embedded_jsonpath_parsers('Schema1Doc1')
        for doc in itertools.chain.from_iterable(p.find(expect) for p in parsers):
            if 'doc1_str_empty' not in doc.value:
                doc.value['doc1_str_empty'] = None

        action = AlterField('Schema1Doc1', 'doc1_str_empty', null=True)
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump_db() == expect

    def test_forward__for_embedded_if_field_is_unset__should_set_unset_fields_to_null(
            self, load_fixture, test_db, dump_db
    ):
        schema = load_fixture('schema1').get_schema()

        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000002'))
        doc['doc1_emb_embdoc1']['embdoc1_str_empty'] = 'test!'
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000002')}, doc)
        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000003'))
        doc['doc1_emblist_embdoc1'][0]['embdoc1_str_empty'] = 'test!'
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000003')}, doc)

        expect = dump_db()
        parsers = load_fixture('schema1').get_embedded_jsonpath_parsers('~Schema1EmbDoc1')
        for doc in itertools.chain.from_iterable(p.find(expect) for p in parsers):
            if 'embdoc1_str_empty' not in doc.value:
                doc.value['embdoc1_str_empty'] = None

        action = AlterField('~Schema1EmbDoc1', 'embdoc1_str_empty', null=True)
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump_db() == expect

    def test_forward_backward__for_document_if_field_is_unset__should_set_unset_fields_to_null(
            self, load_fixture, test_db, dump_db
    ):
        schema = load_fixture('schema1').get_schema()

        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000002'))
        doc['doc1_str_empty'] = 'test!'
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000002')}, doc)

        expect = dump_db()
        parsers = load_fixture('schema1').get_embedded_jsonpath_parsers('Schema1Doc1')
        for doc in itertools.chain.from_iterable(p.find(expect) for p in parsers):
            if 'doc1_str_empty' not in doc.value:
                doc.value['doc1_str_empty'] = None

        action = AlterField('Schema1Doc1', 'doc1_str_empty', null=True)
        action.prepare(test_db, schema, MigrationPolicy.strict)
        action.run_forward()
        action.cleanup()
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_backward()

        assert dump_db() == expect

    def test_forward_backward__for_embedded_if_field_is_unset__should_set_unset_fields_to_null(
            self, load_fixture, test_db, dump_db
    ):
        schema = load_fixture('schema1').get_schema()

        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000002'))
        doc['doc1_emb_embdoc1']['embdoc1_str_empty'] = 'test!'
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000002')}, doc)
        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000003'))
        doc['doc1_emblist_embdoc1'][0]['embdoc1_str_empty'] = 'test!'
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000003')}, doc)

        expect = dump_db()
        parsers = load_fixture('schema1').get_embedded_jsonpath_parsers('~Schema1EmbDoc1')
        for doc in itertools.chain.from_iterable(p.find(expect) for p in parsers):
            if 'embdoc1_str_empty' not in doc.value:
                doc.value['embdoc1_str_empty'] = None

        action = AlterField('~Schema1EmbDoc1', 'embdoc1_str_empty', null=True)
        action.prepare(test_db, schema, MigrationPolicy.strict)
        action.run_forward()
        action.cleanup()
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_backward()

        assert dump_db() == expect


class TestAlterFieldCommonSparse:
    pass  # TODO


class TestAlterFieldCommonTypeKey:
    pass  # TODO


class TestAlterFieldNumberMinValue:
    @pytest.mark.parametrize('db_value,min_value,expect_value', (
        (123.45, 0, 123.45),
        (2.2, 5, 5)
    ))
    @pytest.mark.parametrize('field_name', ('doc1_float', 'doc1_int_empty', 'doc1_long'))
    def test_forward__for_document__should_set_to_min_value_for_non_empty_fields(
            self, load_fixture, test_db, dump_db, field_name, db_value, min_value, expect_value
    ):
        schema = load_fixture('schema1').get_schema()

        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000001'))
        doc[field_name] = db_value
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000001')}, doc)

        expect = dump_db()
        parser = jsonpath_rw.parse('schema1_doc1[*]')
        for doc in parser.find(expect):
            if doc.value['_id'] == ObjectId(f'000000000000000000000001'):
                doc.value[field_name] = expect_value

        action = AlterField('Schema1Doc1', field_name, min_value=min_value)
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump_db() == expect

    @pytest.mark.parametrize('db_value,min_value,expect_value', (
        (123.45, 0, 123.45),
        (2.2, 5, 5)
    ))
    @pytest.mark.parametrize('field_name', ('embdoc1_float', 'embdoc1_int_empty', 'embdoc1_long'))
    def test_forward__for_embedded_document__should_set_to_min_value_for_non_empty_fields(
            self, load_fixture, test_db, dump_db, field_name, db_value, min_value, expect_value
    ):
        schema = load_fixture('schema1').get_schema()

        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000002'))
        doc['doc1_emb_embdoc1'][field_name] = db_value
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000002')}, doc)
        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000003'))
        doc['doc1_emblist_embdoc1'][0][field_name] = db_value
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000003')}, doc)

        expect = dump_db()
        parser = jsonpath_rw.parse('schema1_doc1[*]')
        for doc in parser.find(expect):
            if doc.value['_id'] == ObjectId(f'000000000000000000000002'):
                doc.value['doc1_emb_embdoc1'][field_name] = expect_value
            if doc.value['_id'] == ObjectId(f'000000000000000000000003'):
                doc.value['doc1_emblist_embdoc1'][0][field_name] = expect_value

        action = AlterField('~Schema1EmbDoc1', field_name, min_value=min_value)
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump_db() == expect

    @pytest.mark.parametrize('db_value,min_value,expect_value', (
        (123.45, 0, 123.45),
        (2.2, 5, 5)
    ))
    @pytest.mark.parametrize('field_name', ('doc1_float', 'doc1_int_empty', 'doc1_long'))
    def test_forward_backward__for_document__should_leave_min_value_for_non_empty_fields(
            self, load_fixture, test_db, dump_db, field_name, db_value, min_value, expect_value
    ):
        schema = load_fixture('schema1').get_schema()

        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000001'))
        doc[field_name] = db_value
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000001')}, doc)

        expect = dump_db()
        parser = jsonpath_rw.parse('schema1_doc1[*]')
        for doc in parser.find(expect):
            if doc.value['_id'] == ObjectId(f'000000000000000000000001'):
                doc.value[field_name] = expect_value

        action = AlterField('Schema1Doc1', field_name, min_value=min_value)
        action.prepare(test_db, schema, MigrationPolicy.strict)
        action.run_forward()
        action.cleanup()
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_backward()

        assert dump_db() == expect

    @pytest.mark.parametrize('db_value,min_value,expect_value', (
        (123.45, 0, 123.45),
        (2.2, 5, 5)
    ))
    @pytest.mark.parametrize('field_name', ('embdoc1_float', 'embdoc1_int_empty', 'embdoc1_long'))
    def test_forward_backward__for_embedded_document__should_set_to_min_value_for_non_empty_fields(
            self, load_fixture, test_db, dump_db, field_name, db_value, min_value, expect_value
    ):
        schema = load_fixture('schema1').get_schema()

        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000002'))
        doc['doc1_emb_embdoc1'][field_name] = db_value
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000002')}, doc)
        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000003'))
        doc['doc1_emblist_embdoc1'][0][field_name] = db_value
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000003')}, doc)

        expect = dump_db()
        parser = jsonpath_rw.parse('schema1_doc1[*]')
        for doc in parser.find(expect):
            if doc.value['_id'] == ObjectId(f'000000000000000000000002'):
                doc.value['doc1_emb_embdoc1'][field_name] = expect_value
            if doc.value['_id'] == ObjectId(f'000000000000000000000003'):
                doc.value['doc1_emblist_embdoc1'][0][field_name] = expect_value

        action = AlterField('~Schema1EmbDoc1', field_name, min_value=min_value)
        action.prepare(test_db, schema, MigrationPolicy.strict)
        action.run_forward()
        action.cleanup()
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_backward()

        assert dump_db() == expect


class TestAlterFieldNumberMaxValue:
    @pytest.mark.parametrize('db_value,max_value,expect_value', (
        (123.45, 200, 123.45),
        (5, 2.2, 2.2)
    ))
    @pytest.mark.parametrize('field_name', ('doc1_float', 'doc1_int_empty', 'doc1_long'))
    def test_forward__for_document__should_set_to_max_value_for_non_empty_fields(
            self, load_fixture, test_db, dump_db, field_name, db_value, max_value, expect_value
    ):
        schema = load_fixture('schema1').get_schema()

        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000001'))
        doc[field_name] = db_value
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000001')}, doc)

        expect = dump_db()
        parser = jsonpath_rw.parse('schema1_doc1[*]')
        for doc in parser.find(expect):
            if doc.value['_id'] == ObjectId(f'000000000000000000000001'):
                doc.value[field_name] = expect_value

        action = AlterField('Schema1Doc1', field_name, max_value=max_value)
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump_db() == expect

    @pytest.mark.parametrize('db_value,max_value,expect_value', (
        (123.45, 200, 123.45),
        (5, 2.2, 2.2)
    ))
    @pytest.mark.parametrize('field_name', ('embdoc1_float', 'embdoc1_int_empty', 'embdoc1_long'))
    def test_forward__for_embedded_document__should_set_to_max_value_for_non_empty_fields(
            self, load_fixture, test_db, dump_db, field_name, db_value, max_value, expect_value
    ):
        schema = load_fixture('schema1').get_schema()

        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000002'))
        doc['doc1_emb_embdoc1'][field_name] = db_value
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000002')}, doc)
        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000003'))
        doc['doc1_emblist_embdoc1'][0][field_name] = db_value
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000003')}, doc)

        expect = dump_db()
        parser = jsonpath_rw.parse('schema1_doc1[*]')
        for doc in parser.find(expect):
            if doc.value['_id'] == ObjectId(f'000000000000000000000002'):
                doc.value['doc1_emb_embdoc1'][field_name] = expect_value
            if doc.value['_id'] == ObjectId(f'000000000000000000000003'):
                doc.value['doc1_emblist_embdoc1'][0][field_name] = expect_value

        action = AlterField('~Schema1EmbDoc1', field_name, max_value=max_value)
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump_db() == expect

    @pytest.mark.parametrize('db_value,max_value,expect_value', (
        (123.45, 200, 123.45),
        (5, 2.2, 2.2)
    ))
    @pytest.mark.parametrize('field_name', ('doc1_float', 'doc1_int_empty', 'doc1_long'))
    def test_forward_backward__for_document__should_leave_max_value_for_non_empty_fields(
            self, load_fixture, test_db, dump_db, field_name, db_value, max_value, expect_value
    ):
        schema = load_fixture('schema1').get_schema()

        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000001'))
        doc[field_name] = db_value
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000001')}, doc)

        expect = dump_db()
        parser = jsonpath_rw.parse('schema1_doc1[*]')
        for doc in parser.find(expect):
            if doc.value['_id'] == ObjectId(f'000000000000000000000001'):
                doc.value[field_name] = expect_value

        action = AlterField('Schema1Doc1', field_name, max_value=max_value)
        action.prepare(test_db, schema, MigrationPolicy.strict)
        action.run_forward()
        action.cleanup()
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_backward()

        assert dump_db() == expect

    @pytest.mark.parametrize('db_value,max_value,expect_value', (
        (123.45, 200, 123.45),
        (5, 2.2, 2.2)
    ))
    @pytest.mark.parametrize('field_name', ('embdoc1_float', 'embdoc1_int_empty', 'embdoc1_long'))
    def test_forward_backward__for_embedded_document__should_set_to_max_value_for_non_empty_fields(
            self, load_fixture, test_db, dump_db, field_name, db_value, max_value, expect_value
    ):
        schema = load_fixture('schema1').get_schema()

        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000002'))
        doc['doc1_emb_embdoc1'][field_name] = db_value
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000002')}, doc)
        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000003'))
        doc['doc1_emblist_embdoc1'][0][field_name] = db_value
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000003')}, doc)

        expect = dump_db()
        parser = jsonpath_rw.parse('schema1_doc1[*]')
        for doc in parser.find(expect):
            if doc.value['_id'] == ObjectId(f'000000000000000000000002'):
                doc.value['doc1_emb_embdoc1'][field_name] = expect_value
            if doc.value['_id'] == ObjectId(f'000000000000000000000003'):
                doc.value['doc1_emblist_embdoc1'][0][field_name] = expect_value

        action = AlterField('~Schema1EmbDoc1', field_name, max_value=max_value)
        action.prepare(test_db, schema, MigrationPolicy.strict)
        action.run_forward()
        action.cleanup()
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_backward()

        assert dump_db() == expect


class TestAlterFieldStringMaxLength:
    @pytest.mark.parametrize('document_type,field_name', (
        ('Schema1Doc1', 'doc1_str'),
        ('~Schema1EmbDoc1', 'embdoc1_str'),
        ('~Schema1EmbDoc2', 'embdoc2_str')
    ))
    def test_forward__if_string_length_less_max_length__should_do_nothing(
            self, load_fixture, test_db, dump_db, document_type, field_name
    ):
        schema = load_fixture('schema1').get_schema()

        expect = dump_db()

        action = AlterField(document_type, field_name, max_length=200)
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump_db() == expect

    def test_forward__for_document_if_string_length_more_max_length__should_cut_off_string(
            self, load_fixture, test_db, dump_db
    ):
        schema = load_fixture('schema1').get_schema()

        expect = dump_db()
        parser = jsonpath_rw.parse('schema1_doc1[*]')
        for doc in parser.find(expect):
            doc.value['doc1_str'] = 'st'

        action = AlterField('Schema1Doc1', 'doc1_str', max_length=2)
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump_db() == expect

    def test_forward__for_embedded_document_if_string_length_more_max_length__should_cut_off_string(
            self, load_fixture, test_db, dump_db
    ):
        schema = load_fixture('schema1').get_schema()

        expect = dump_db()
        parsers = load_fixture('schema1').get_embedded_jsonpath_parsers('~Schema1EmbDoc1')
        for doc in itertools.chain.from_iterable(p.find(expect) for p in parsers):
            doc.value['embdoc1_str'] = 'st'

        action = AlterField('~Schema1EmbDoc1', 'embdoc1_str', max_length=2)
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump_db() == expect

    @pytest.mark.parametrize('document_type,field_name', (
        ('Schema1Doc1', 'doc1_str'),
        ('~Schema1EmbDoc1', 'embdoc1_str'),
        ('~Schema1EmbDoc2', 'embdoc2_str')
    ))
    def test_forward_backward__if_string_length_less_max_length__should_do_nothing(
            self, load_fixture, test_db, dump_db, document_type, field_name
    ):
        schema = load_fixture('schema1').get_schema()

        expect = dump_db()

        action = AlterField(document_type, field_name, max_length=200)
        action.prepare(test_db, schema, MigrationPolicy.strict)
        action.run_forward()
        action.cleanup()
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_backward()

        assert dump_db() == expect

    def test_forward_backward__for_document_if_string_length_more_max_length__should_cut_off_string(
            self, load_fixture, test_db, dump_db
    ):
        schema = load_fixture('schema1').get_schema()

        expect = dump_db()
        parser = jsonpath_rw.parse('schema1_doc1[*]')
        for doc in parser.find(expect):
            doc.value['doc1_str'] = 'st'

        action = AlterField('Schema1Doc1', 'doc1_str', max_length=2)
        action.prepare(test_db, schema, MigrationPolicy.strict)
        action.run_forward()
        action.cleanup()
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_backward()

        assert dump_db() == expect

    def test_forward_backward__for_embedded_if_string_length_more_max_length__should_cut_off_string(
            self, load_fixture, test_db, dump_db
    ):
        schema = load_fixture('schema1').get_schema()

        expect = dump_db()
        parsers = load_fixture('schema1').get_embedded_jsonpath_parsers('~Schema1EmbDoc1')
        for doc in itertools.chain.from_iterable(p.find(expect) for p in parsers):
            doc.value['embdoc1_str'] = 'st'

        action = AlterField('~Schema1EmbDoc1', 'embdoc1_str', max_length=2)
        action.prepare(test_db, schema, MigrationPolicy.strict)
        action.run_forward()
        action.cleanup()
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_backward()

        assert dump_db() == expect


class TestAlterFieldStringMinLength:
    @pytest.mark.parametrize('document_type,field_name', (
        ('Schema1Doc1', 'doc1_str'),
        ('~Schema1EmbDoc1', 'embdoc1_str'),
        ('~Schema1EmbDoc2', 'embdoc2_str')
    ))
    def test_forward__if_string_length_more_min_length__should_do_nothing(
            self, load_fixture, test_db, dump_db, document_type, field_name
    ):
        schema = load_fixture('schema1').get_schema()

        expect = dump_db()

        action = AlterField(document_type, field_name, min_length=2)
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump_db() == expect

    @pytest.mark.parametrize('document_type,field_name', (
        ('Schema1Doc1', 'doc1_str'),
        ('~Schema1EmbDoc1', 'embdoc1_str'),
        ('~Schema1EmbDoc2', 'embdoc2_str')
    ))
    def test_forward__if_string_length_less_min_length__should_raise_error(
            self, load_fixture, test_db, document_type, field_name
    ):
        schema = load_fixture('schema1').get_schema()

        action = AlterField(document_type, field_name, min_length=200)
        action.prepare(test_db, schema, MigrationPolicy.strict)

        with pytest.raises(InconsistencyError):
            action.run_forward()

    @pytest.mark.parametrize('document_type,field_name', (
        ('Schema1Doc1', 'doc1_str'),
        ('~Schema1EmbDoc1', 'embdoc1_str'),
        ('~Schema1EmbDoc2', 'embdoc2_str')
    ))
    def test_forward_backward__if_string_length_more_min_length__should_do_nothing(
            self, load_fixture, test_db, dump_db, document_type, field_name
    ):
        schema = load_fixture('schema1').get_schema()

        expect = dump_db()

        action = AlterField(document_type, field_name, min_length=2)
        action.prepare(test_db, schema, MigrationPolicy.strict)
        action.run_forward()
        action.cleanup()
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_backward()

        assert dump_db() == expect


class TestAlterFieldStringRegex:
    @pytest.mark.parametrize('regex', (re.compile('^str'), '^str'))
    @pytest.mark.parametrize('document_type,field_name', (
        ('Schema1Doc1', 'doc1_str'),
        ('~Schema1EmbDoc1', 'embdoc1_str'),
        ('~Schema1EmbDoc2', 'embdoc2_str')
    ))
    def test_forward__if_field_value_match_regex__should_do_nothing(
            self, load_fixture, test_db, dump_db, document_type, field_name, regex
    ):
        schema = load_fixture('schema1').get_schema()

        expect = dump_db()

        action = AlterField(document_type, field_name, regex=regex)
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump_db() == expect

    @pytest.mark.parametrize('regex', (re.compile('^unknown'), '^unknown'))
    @pytest.mark.parametrize('document_type,field_name', (
        ('Schema1Doc1', 'doc1_str'),
        ('~Schema1EmbDoc1', 'embdoc1_str'),
        ('~Schema1EmbDoc2', 'embdoc2_str')
    ))
    def test_forward__if_field_value_doesnt_match_regex__should_raise_error(
            self, load_fixture, test_db, document_type, field_name, regex
    ):
        schema = load_fixture('schema1').get_schema()

        action = AlterField(document_type, field_name, regex=regex)
        action.prepare(test_db, schema, MigrationPolicy.strict)

        with pytest.raises(InconsistencyError):
            action.run_forward()

    @pytest.mark.parametrize('regex', (re.compile('^str'), '^str'))
    @pytest.mark.parametrize('document_type,field_name', (
        ('Schema1Doc1', 'doc1_str'),
        ('~Schema1EmbDoc1', 'embdoc1_str'),
        ('~Schema1EmbDoc2', 'embdoc2_str')
    ))
    def test_forward_backward__if_field_value_match_regex__should_do_nothing(
            self, load_fixture, test_db, dump_db, document_type, field_name, regex
    ):
        schema = load_fixture('schema1').get_schema()

        expect = dump_db()

        action = AlterField(document_type, field_name, regex=regex)
        action.prepare(test_db, schema, MigrationPolicy.strict)
        action.run_forward()
        action.cleanup()
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_backward()

        assert dump_db() == expect


class TestAlterFieldUrlSchemes:
    pass  # TODO


class TestAlterFieldEmailDomainWhitelist:
    pass  # TODO


class TestAlterFieldEmailAllowUTF8User:
    pass  # TODO


class TestAlterFieldEmailAllowIPDomain:
    pass  # TODO


class TestAlterFieldDecimalForceString:
    @pytest.mark.parametrize('init_value', (3.14, '3.14'))
    def test_forward__for_document__should_cast_to_string(
            self, load_fixture, test_db, dump_db, init_value
    ):
        schema = load_fixture('schema1').get_schema()

        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000001'))
        doc['doc1_decimal'] = init_value
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000001')}, doc)

        expect = dump_db()
        parser = jsonpath_rw.parse('schema1_doc1[*]')
        for doc in parser.find(expect):
            if doc.value['_id'] == ObjectId(f'000000000000000000000001'):
                doc.value['doc1_decimal'] = '3.14'

        action = AlterField('Schema1Doc1', 'doc1_decimal', force_string=True)
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump_db() == expect

    @pytest.mark.parametrize('init_value1,init_value2', ((3.14, 2.17),  ('3.14', '2.17')))
    def test_forward__for_embedded__should_cast_to_string(
            self, load_fixture, test_db, dump_db, init_value1, init_value2
    ):
        schema = load_fixture('schema1').get_schema()

        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000002'))
        doc['doc1_emb_embdoc1']['embdoc1_decimal'] = init_value1
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000002')}, doc)
        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000003'))
        doc['doc1_emblist_embdoc1'][0]['embdoc1_decimal'] = init_value2
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000003')}, doc)

        expect = dump_db()
        parser = jsonpath_rw.parse('schema1_doc1[*]')
        for doc in parser.find(expect):
            if doc.value['_id'] == ObjectId(f'000000000000000000000002'):
                doc.value['doc1_emb_embdoc1']['embdoc1_decimal'] = '3.14'
            if doc.value['_id'] == ObjectId(f'000000000000000000000003'):
                doc.value['doc1_emblist_embdoc1'][0]['embdoc1_decimal'] = '2.17'

        action = AlterField('~Schema1EmbDoc1', 'embdoc1_decimal', force_string=True)
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump_db() == expect

    @pytest.mark.parametrize('init_value', (3.14, '3.14'))
    def test_forward_backward__for_document__should_cast_to_string(
            self, load_fixture, test_db, dump_db, init_value
    ):
        schema = load_fixture('schema1').get_schema()

        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000001'))
        doc['doc1_decimal'] = init_value
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000001')}, doc)

        expect = dump_db()
        parser = jsonpath_rw.parse('schema1_doc1[*]')
        for doc in parser.find(expect):
            if doc.value['_id'] == ObjectId(f'000000000000000000000001'):
                doc.value['doc1_decimal'] = 3.14

        action = AlterField('Schema1Doc1', 'doc1_decimal', force_string=True)
        action.prepare(test_db, schema, MigrationPolicy.strict)
        action.run_forward()
        action.cleanup()
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_backward()

        assert dump_db() == expect

    @pytest.mark.parametrize('init_value1,init_value2', ((3.14, 2.17),  ('3.14', '2.17')))
    def test_forward_backward__for_embedded__should_cast_to_string(
            self, load_fixture, test_db, dump_db, init_value1, init_value2
    ):
        schema = load_fixture('schema1').get_schema()

        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000002'))
        doc['doc1_emb_embdoc1']['embdoc1_decimal'] = init_value1
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000002')}, doc)
        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000003'))
        doc['doc1_emblist_embdoc1'][0]['embdoc1_decimal'] = init_value2
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000003')}, doc)

        expect = dump_db()
        parser = jsonpath_rw.parse('schema1_doc1[*]')
        for doc in parser.find(expect):
            if doc.value['_id'] == ObjectId(f'000000000000000000000002'):
                doc.value['doc1_emb_embdoc1']['embdoc1_decimal'] = 3.14
            if doc.value['_id'] == ObjectId(f'000000000000000000000003'):
                doc.value['doc1_emblist_embdoc1'][0]['embdoc1_decimal'] = 2.17

        action = AlterField('~Schema1EmbDoc1', 'embdoc1_decimal', force_string=True)
        action.prepare(test_db, schema, MigrationPolicy.strict)
        action.run_forward()
        action.cleanup()
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_backward()

        assert dump_db() == expect


class TestAlterFieldComplexDateTimeSeparator:
    @pytest.mark.parametrize('init_value', (
        '2020.04.03.02.01.00.000000', '2020|04|03|02|01|00|000000'
    ))
    def test_forward__for_document__should_change_separator(
            self, load_fixture, test_db, dump_db, init_value
    ):
        schema = load_fixture('schema1').get_schema()

        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000001'))
        doc['doc1_complex_datetime'] = init_value
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000001')}, doc)

        expect = dump_db()
        parser = jsonpath_rw.parse('schema1_doc1[*]')
        for doc in parser.find(expect):
            if doc.value['_id'] == ObjectId(f'000000000000000000000001'):
                doc.value['doc1_complex_datetime'] = '2020|04|03|02|01|00|000000'

        action = AlterField('Schema1Doc1', 'doc1_complex_datetime', separator='|')
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump_db() == expect

    @pytest.mark.parametrize('init_value1,init_value2', (
        ('2020.00.01.02.03.04.000000', '2020.04.03.02.01.00.000000'),
        ('2020|00|01|02|03|04|000000', '2020|04|03|02|01|00|000000')
    ))
    def test_forward__for_embedded__should_change_separator(
            self, load_fixture, test_db, dump_db, init_value1, init_value2
    ):
        schema = load_fixture('schema1').get_schema()

        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000002'))
        doc['doc1_emb_embdoc1']['embdoc1_complex_datetime'] = init_value1
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000002')}, doc)
        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000003'))
        doc['doc1_emblist_embdoc1'][0]['embdoc1_complex_datetime'] = init_value2
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000003')}, doc)

        expect = dump_db()
        parser = jsonpath_rw.parse('schema1_doc1[*]')
        for doc in parser.find(expect):
            if doc.value['_id'] == ObjectId(f'000000000000000000000002'):
                doc.value['doc1_emb_embdoc1']['embdoc1_complex_datetime'] = \
                    '2020|00|01|02|03|04|000000'
            if doc.value['_id'] == ObjectId(f'000000000000000000000003'):
                doc.value['doc1_emblist_embdoc1'][0]['embdoc1_complex_datetime'] = \
                    '2020|04|03|02|01|00|000000'

        action = AlterField('~Schema1EmbDoc1', 'embdoc1_complex_datetime', separator='|')
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump_db() == expect

    @pytest.mark.parametrize('init_value', (
        '2020.04.03.02.01.00.000000', '2020|04|03|02|01|00|000000'
    ))
    def test_forward_backward__for_document__should_change_separator(
            self, load_fixture, test_db, dump_db, init_value
    ):
        schema = load_fixture('schema1').get_schema()

        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000001'))
        doc['doc1_complex_datetime'] = init_value
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000001')}, doc)

        expect = dump_db()
        parser = jsonpath_rw.parse('schema1_doc1[*]')
        for doc in parser.find(expect):
            if doc.value['_id'] == ObjectId(f'000000000000000000000001'):
                doc.value['doc1_complex_datetime'] = '2020.04.03.02.01.00.000000'

        action = AlterField('Schema1Doc1', 'doc1_complex_datetime', separator='|')
        action.prepare(test_db, schema, MigrationPolicy.strict)
        action.run_forward()
        action.cleanup()
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_backward()

        assert dump_db() == expect

    @pytest.mark.parametrize('init_value1,init_value2', (
        ('2020.00.01.02.03.04.000000', '2020.04.03.02.01.00.000000'),
        ('2020|00|01|02|03|04|000000', '2020|04|03|02|01|00|000000')
    ))
    def test_forward_backward__for_embedded__should_change_separator(
            self, load_fixture, test_db, dump_db, init_value1, init_value2
    ):
        schema = load_fixture('schema1').get_schema()

        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000002'))
        doc['doc1_emb_embdoc1']['embdoc1_complex_datetime'] = init_value1
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000002')}, doc)
        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000003'))
        doc['doc1_emblist_embdoc1'][0]['embdoc1_complex_datetime'] = init_value2
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000003')}, doc)

        expect = dump_db()
        parser = jsonpath_rw.parse('schema1_doc1[*]')
        for doc in parser.find(expect):
            if doc.value['_id'] == ObjectId(f'000000000000000000000002'):
                doc.value['doc1_emb_embdoc1']['embdoc1_complex_datetime'] = \
                    '2020.00.01.02.03.04.000000'
            if doc.value['_id'] == ObjectId(f'000000000000000000000003'):
                doc.value['doc1_emblist_embdoc1'][0]['embdoc1_complex_datetime'] = \
                    '2020.04.03.02.01.00.000000'

        action = AlterField('~Schema1EmbDoc1', 'embdoc1_complex_datetime', separator='|')
        action.prepare(test_db, schema, MigrationPolicy.strict)
        action.run_forward()
        action.cleanup()
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_backward()

        assert dump_db() == expect


@pytest.mark.skipif(mongoengine.VERSION < (0, 19, 0), reason='Mongoengine>=0.19.0 is required')
class TestAlterFieldListMaxLength:
    def test_forward__for_document__should_cut_off_a_list(self, load_fixture, test_db, dump_db):
        schema = load_fixture('schema1').get_schema()

        expect = dump_db()
        parser = jsonpath_rw.parse('schema1_doc1[*]')
        for doc in parser.find(expect):
            if 'doc1_list' in doc.value:
                doc.value['doc1_list'] = doc.value['doc1_list'][:2]

        action = AlterField('Schema1Doc1', 'doc1_list', max_length=2)
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump_db() == expect

    def test_forward__for_embedded__should_cut_off_a_list(self, load_fixture, test_db, dump_db):
        schema = load_fixture('schema1').get_schema()

        expect = dump_db()
        parsers = load_fixture('schema1').get_embedded_jsonpath_parsers('~Schema1EmbDoc1')
        for doc in itertools.chain.from_iterable(p.find(expect) for p in parsers):
            if 'embdoc1_list' in doc.value:
                doc.value['embdoc1_list'] = doc.value['embdoc1_list'][:2]

        action = AlterField('~Schema1EmbDoc1', 'embdoc1_list', max_length=2)
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump_db() == expect

    def test_forward_backward__for_document__should_cut_off_a_list(
            self, load_fixture, test_db, dump_db
    ):
        schema = load_fixture('schema1').get_schema()

        expect = dump_db()
        parser = jsonpath_rw.parse('schema1_doc1[*]')
        for doc in parser.find(expect):
            if 'doc1_list' in doc.value:
                doc.value['doc1_list'] = doc.value['doc1_list'][:2]

        action = AlterField('Schema1Doc1', 'doc1_list', max_length=2)
        action.prepare(test_db, schema, MigrationPolicy.strict)
        action.run_forward()
        action.cleanup()
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_backward()

        assert dump_db() == expect

    def test_forward_backward__for_embedded__should_cut_off_a_list(
            self, load_fixture, test_db, dump_db
    ):
        schema = load_fixture('schema1').get_schema()

        expect = dump_db()
        parsers = load_fixture('schema1').get_embedded_jsonpath_parsers('~Schema1EmbDoc1')
        for doc in itertools.chain.from_iterable(p.find(expect) for p in parsers):
            if 'embdoc1_list' in doc.value:
                doc.value['embdoc1_list'] = doc.value['embdoc1_list'][:2]

        action = AlterField('~Schema1EmbDoc1', 'embdoc1_list', max_length=2)
        action.prepare(test_db, schema, MigrationPolicy.strict)
        action.run_forward()
        action.cleanup()
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_backward()

        assert dump_db() == expect


class TestAlterFieldReferenceDbref:
    def test_forward__for_document__should_convert_to_dbref(self, load_fixture, test_db, dump_db):
        schema = load_fixture('schema1').get_schema()

        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000001'))
        doc['doc1_ref_self'] = ObjectId('000000000000000000000002')
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000001')}, doc)

        expect = dump_db()
        parser = jsonpath_rw.parse('schema1_doc1[*]')
        for doc in parser.find(expect):
            if 'doc1_ref_self' in doc.value:
                doc.value['doc1_ref_self'] = bson.DBRef('schema1_doc1',
                                                        ObjectId('000000000000000000000002'))

        action = AlterField('Schema1Doc1', 'doc1_ref_self', dbref=True)
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump_db() == expect

    def test_forward__for_embedded__should_convert_to_dbref(self, load_fixture, test_db, dump_db):
        schema = load_fixture('schema1').get_schema()

        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000002'))
        doc['doc1_emb_embdoc1']['embdoc1_ref_doc1'] = ObjectId('000000000000000000000002')
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000002')}, doc)
        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000003'))
        doc['doc1_emblist_embdoc1'][0]['embdoc1_ref_doc1'] = ObjectId('000000000000000000000002')
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000003')}, doc)

        expect = dump_db()
        parser = jsonpath_rw.parse('schema1_doc1[*]')
        for doc in parser.find(expect):
            if doc.value['_id'] == ObjectId(f'000000000000000000000002'):
                doc.value['doc1_emb_embdoc1']['embdoc1_ref_doc1'] = bson.DBRef(
                    'schema1_doc1', ObjectId(f'000000000000000000000002')
                )
            if doc.value['_id'] == ObjectId(f'000000000000000000000003'):
                doc.value['doc1_emblist_embdoc1'][0]['embdoc1_ref_doc1'] = bson.DBRef(
                    'schema1_doc1', ObjectId(f'000000000000000000000002')
                )

        action = AlterField('~Schema1EmbDoc1', 'embdoc1_ref_doc1', dbref=True)
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump_db() == expect

    @pytest.mark.parametrize('document_type,field_name', (
            ('Schema1Doc1', 'doc1_ref_self'),
            ('~Schema1EmbDoc1', 'embdoc1_ref_doc1')
    ))
    def test_forward_backward__for_document__should_do_nothing(
            self, load_fixture, test_db, dump_db, document_type, field_name
    ):
        schema = load_fixture('schema1').get_schema()

        expect = dump_db()

        action = AlterField(document_type, field_name, dbref=True)
        action.prepare(test_db, schema, MigrationPolicy.strict)
        action.run_forward()
        action.cleanup()
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_backward()

        assert dump_db() == expect


class TestAlterFieldCachedReferenceFields:
    def test_forward__for_document_when_fields_list_become_bigger__should_do_nothing(
            self, load_fixture, test_db, dump_db
    ):
        schema = load_fixture('schema1').get_schema()

        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000001'))
        doc['doc1_cachedref_self'] = {
            '_id': ObjectId('000000000000000000000002'),
            'doc1_int': 2,
            'doc1_str': '2'
        }
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000001')}, doc)

        expect = dump_db()

        action = AlterField('Schema1Doc1',
                            'doc1_cachedref_self',
                            fields=['doc1_int', 'doc1_str', 'another_field'])
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump_db() == expect

    def test_forward__for_document_when_fields_list_become_smaller__should_remove_extra_fields(
            self, load_fixture, test_db, dump_db
    ):
        schema = load_fixture('schema1').get_schema()
        schema['Schema1Doc1']['doc1_cachedref_self']['fields'] = ['doc1_int', 'doc1_str']

        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000001'))
        doc['doc1_cachedref_self'] = \
            {'_id': ObjectId('000000000000000000000002'), 'doc1_int': 2, 'doc1_str': '2'}
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000001')}, doc)

        expect = dump_db()
        parser = jsonpath_rw.parse('schema1_doc1[*]')
        for doc in parser.find(expect):
            if doc.value['_id'] == ObjectId(f'000000000000000000000001'):
                doc.value['doc1_cachedref_self'] = {'_id': ObjectId('000000000000000000000002'),
                                                    'doc1_int': 2}

        action = AlterField('Schema1Doc1', 'doc1_cachedref_self', fields=['doc1_int'])
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_forward()

        assert dump_db() == expect

    def test_forward__for_embedded_document__forbidden_and_should_raise_error(
            self, load_fixture, test_db
    ):
        schema = load_fixture('schema1').get_schema()

        action = AlterField('~Schema1EmbDoc1', 'embdoc1_cachedref_self', fields=['doc1_int'])

        with pytest.raises(SchemaError):
            action.prepare(test_db, schema, MigrationPolicy.strict)

    def test_forward_backward__for_document__should_remove_extra_subfields(
            self, load_fixture, test_db, dump_db
    ):
        schema = load_fixture('schema1').get_schema()

        doc = test_db['schema1_doc1'].find_one(ObjectId(f'000000000000000000000001'))
        doc['doc1_cachedref_self'] = {
            '_id': ObjectId('000000000000000000000002'),
            'doc1_int': 2,
            'doc1_str': '2'
        }
        test_db['schema1_doc1'].replace_one({'_id': ObjectId(f'000000000000000000000001')}, doc)

        expect = dump_db()
        parser = jsonpath_rw.parse('schema1_doc1[*]')
        for doc in parser.find(expect):
            if 'doc1_cachedref_self' in doc.value:
                doc.value['doc1_cachedref_self'] = {'_id': ObjectId('000000000000000000000002'),
                                                    'doc1_int': 2}

        action = AlterField('Schema1Doc1', 'doc1_cachedref_self', fields=['doc1_int'])
        action.prepare(test_db, schema, MigrationPolicy.strict)
        action.run_forward()
        action.cleanup()
        action.prepare(test_db, schema, MigrationPolicy.strict)

        action.run_backward()

        assert dump_db() == expect

    def test_backward__for_embedded__forbidden_and_should_raise_error(
            self, load_fixture, test_db, dump_db
    ):
        schema = load_fixture('schema1').get_schema()

        action = AlterField('~Schema1EmbDoc1', 'embdoc1_cachedref_self', fields=['doc1_int'])

        with pytest.raises(SchemaError):
            action.prepare(test_db, schema, MigrationPolicy.strict)
