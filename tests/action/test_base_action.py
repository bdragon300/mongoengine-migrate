from typing import Type, Optional
from unittest import mock

import pymongo
import pytest

from mongoengine_migrate.actions import base as actions_base
from mongoengine_migrate.actions.base import (
    BaseAction,
    BaseFieldAction,
    BaseDocumentAction,
    BaseCreateDocument,
    BaseDropDocument,
    BaseRenameDocument,
    BaseAlterDocument,
    BaseIndexAction
)
from mongoengine_migrate.exceptions import SchemaError, ActionError
from mongoengine_migrate.graph import MigrationPolicy
from mongoengine_migrate.schema import Schema
from mongoengine_migrate.utils import Diff


@pytest.fixture
def baseaction_stub() -> Type[BaseAction]:
    class StubAction(BaseAction):
        def run_forward(self):
            pass

        def run_backward(self):
            pass

        def to_schema_patch(self, left_schema: Schema):
            pass

        def to_python_expr(self) -> str:
            pass

    return StubAction


@pytest.fixture
def basefieldaction_stub() -> Type[BaseFieldAction]:
    class StubFieldAction(BaseFieldAction):
        def run_forward(self):
            pass

        def run_backward(self):
            pass

        def to_schema_patch(self, left_schema: Schema):
            pass

        @classmethod
        def build_object(cls,
                         document_type: str,
                         field_name: str,
                         left_schema: Schema,
                         right_schema: Schema) -> Optional['BaseFieldAction']:
            pass

    return StubFieldAction


@pytest.fixture
def basedocumentaction_stub() -> Type[BaseDocumentAction]:
    class StubDocumentAction(BaseDocumentAction):
        def run_forward(self):
            pass

        def run_backward(self):
            pass

        def to_schema_patch(self, left_schema: Schema):
            pass

        @classmethod
        def build_object(cls,
                         document_type: str,
                         left_schema: Schema,
                         right_schema: Schema) -> Optional['BaseDocumentAction']:
            pass

    return StubDocumentAction


@pytest.fixture
def basecreatedocumentaction_stub() -> Type[BaseCreateDocument]:
    class StubCreateDocumentAction(BaseCreateDocument):
        def run_forward(self):
            pass

        def run_backward(self):
            pass

    return StubCreateDocumentAction


@pytest.fixture
def basedropdocumentaction_stub() -> Type[BaseDropDocument]:
    class StubDropDocumentAction(BaseDropDocument):
        def run_forward(self):
            pass

        def run_backward(self):
            pass

    return StubDropDocumentAction


@pytest.fixture
def baserenamedocumentaction_stub() -> Type[BaseRenameDocument]:
    class StubRenameDocumentAction(BaseRenameDocument):
        def run_forward(self):
            pass

        def run_backward(self):
            pass

        def to_python_expr(self) -> str:
            pass

    return StubRenameDocumentAction


@pytest.fixture
def basealterdocumentaction_stub() -> Type[BaseAlterDocument]:
    class StubAlterDocumentAction(BaseAlterDocument):
        pass

    return StubAlterDocumentAction


@pytest.fixture
def baseindexaction_stub() -> Type[BaseIndexAction]:
    class StubIndexAction(BaseIndexAction):
        def run_forward(self):
            pass

        def run_backward(self):
            pass

        def to_schema_patch(self, left_schema: Schema):
            pass

        def build_object(cls,
                         document_type: str,
                         index_name: str,
                         left_schema: Schema,
                         right_schema: Schema) -> Optional['BaseIndexAction']:
            pass

    return StubIndexAction


@pytest.fixture
def left_schema():
    return Schema({
        'Document1': Schema.Document(
            {'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'}},
            parameters={'collection': 'document1'},
            indexes={'index1': {'fields': [('field1', pymongo.DESCENDING)], 'sparse': True}}
        ),
        '~Document2': Schema.Document({
            'field1': {'param3': 'schemavalue3'},
            'field2': {'param4': 'schemavalue4'},
        }, parameters={})
    })


class TestBaseActionMeta:
    @pytest.fixture(autouse=True)
    def setup(self):
        actions_base.actions_registry = {}

        yield

    def test_registry__if_only_base_actions_defined__registry_should_be_empty(self):
        assert actions_base.actions_registry == {}

    def test_registry__on_action_was_instantiated__should_be_added_to_registry(
            self, baseaction_stub
    ):
        assert actions_base.actions_registry == {'StubAction': baseaction_stub}

    def test_instantiate__meta_attr_should_point_to_metaclass_instance(self, baseaction_stub):
        assert baseaction_stub._meta.__name__ == 'BaseActionMeta'


class TestBaseAction:
    def test_init__should_set_attributes(self, baseaction_stub):
        obj = baseaction_stub('Document1', dummy_action=False,
                              param1='value1', param2=123)  # type: BaseAction

        assert obj.document_type == 'Document1'
        assert obj.dummy_action is False
        assert obj.parameters == {'param1': 'value1', 'param2': 123}
        assert obj._run_ctx is None

    def test_prepare__if_collection_in_left_schema__should_prepare_run_context(
            self, test_db, left_schema, baseaction_stub
    ):
        obj = baseaction_stub('Document1', dummy_action=False,
                              param1='value1', param2=123)  # type: BaseAction
        policy = MigrationPolicy.relaxed

        obj.prepare(test_db, left_schema, policy)

        assert obj._run_ctx == {
            'left_schema': left_schema,
            'db': test_db,
            'collection': test_db['document1'],
            'migration_policy': policy
        }

    def test_prepare__if_collection_in_parameters__should_pick_it_and_prepare_run_context(
            self, test_db, left_schema, baseaction_stub
    ):
        policy = MigrationPolicy.relaxed
        obj = baseaction_stub('Document1', dummy_action=False, collection='test_collection1')

        obj.prepare(test_db, left_schema, policy)

        assert obj._run_ctx == {
            'left_schema': left_schema,
            'db': test_db,
            'collection': test_db['test_collection1'],
            'migration_policy': policy
        }

    def test_prepare__if_collection_is_omitted__should_use_placeholder_and_prepare_run_context(
            self, test_db, left_schema, baseaction_stub
    ):
        policy = MigrationPolicy.relaxed
        obj = baseaction_stub('~Document2', dummy_action=False)

        obj.prepare(test_db, left_schema, policy)

        assert obj._run_ctx == {
            'left_schema': left_schema,
            'db': test_db,
            'collection': test_db['COLLECTION_PLACEHOLDER'],
            'migration_policy': policy
        }

    def test_prepare__if_document_type_not_in_schema__should_raise_error(
            self, test_db, left_schema, baseaction_stub
    ):
        policy = MigrationPolicy.relaxed
        obj = baseaction_stub('UnknownDocumentType')

        with pytest.raises(SchemaError):
            obj.prepare(test_db, left_schema, policy)

    @pytest.mark.parametrize('args,kwargs,expect', (
        (
            ('Document1',),
            {'param1': 'val1', 'param2': 4},
            "StubAction('Document1', param1='val1', param2=4)"
        ),
        (
            ('Document1',),
            {'param1': 'val1', 'param2': 4, 'dummy_action': True},
            "StubAction('Document1', param1='val1', param2=4, dummy_action=True)"
        ),
    ))
    def test_repr__should_return_repr_string(self, baseaction_stub, args, kwargs, expect):
        obj = baseaction_stub(*args, **kwargs)

        assert repr(obj) == expect

    @pytest.mark.parametrize('args,kwargs,expect', (
        (
            ('Document1',),
            {'param1': 'val1', 'param2': 4},
            "StubAction('Document1', ...)"
        ),
        (
            ('Document1',),
            {'param1': 'val1', 'param2': 4, 'dummy_action': True},
            "StubAction('Document1', dummy_action=True, ...)"
        ),
    ))
    def test_str__should_return_str_string(self, baseaction_stub, args, kwargs, expect):
        obj = baseaction_stub(*args, **kwargs)

        assert str(obj) == expect


class TestBaseFieldAction:
    def test_init__should_set_attributes(self, basefieldaction_stub):
        obj = basefieldaction_stub('Document1', 'field1', dummy_action=False,
                                   param1='value1', param2=123)  # type: BaseFieldAction

        assert obj.document_type == 'Document1'
        assert obj.dummy_action is False
        assert obj.parameters == {'param1': 'value1', 'param2': 123}
        assert obj._run_ctx is None
        assert obj.field_name == 'field1'

    def test_init__if_db_field_param_contains_dots__should_raise_error(self, basefieldaction_stub):
        with pytest.raises(ActionError):
            basefieldaction_stub('Document1', 'field1', db_field='this.db_field1',
                                 param1='value1', param2=123)  # type: BaseFieldAction

    @mock.patch('mongoengine_migrate.actions.base.type_key_registry')
    def test_get_field_handler_cls__should_return_class_from_type_registry(
            self, type_key_registry_mock, basefieldaction_stub
    ):
        type_key_registry_mock.__contains__.return_value = True
        obj = basefieldaction_stub('Document1', 'field1',
                                   param1='value1', param2=123)  # type: BaseFieldAction

        res = obj.get_field_handler_cls('StringField')

        assert res == type_key_registry_mock['StringField'].field_handler_cls

    @mock.patch('mongoengine_migrate.actions.base.type_key_registry')
    def test_get_field_handler_cls__if_type_key_is_unknown__should_raise_error(
            self, type_key_registry_mock, basefieldaction_stub
    ):
        type_key_registry_mock.__contains__.return_value = False
        obj = basefieldaction_stub('Document1', 'field1',
                                   param1='value1', param2=123)  # type: BaseFieldAction

        with pytest.raises(SchemaError):
            obj.get_field_handler_cls('StringField')

    def test_prepare__if_collection_in_left_schema__should_prepare_run_context(
            self, test_db, left_schema, basefieldaction_stub
    ):
        obj = basefieldaction_stub('Document1', 'field1',
                                   param1='value1', param2=123)  # type: BaseFieldAction
        policy = MigrationPolicy.relaxed

        obj.prepare(test_db, left_schema, policy)

        assert obj._run_ctx == {
            'left_schema': left_schema,
            'db': test_db,
            'collection': test_db['document1'],
            'migration_policy': policy,
            'left_field_schema': {'param1': 'schemavalue1', 'param2': 'schemavalue2'}
        }

    def test_prepare__if_collection_is_unknown__should_use_placeholder_and_prepare_run_context(
            self, test_db, left_schema, basefieldaction_stub
    ):
        policy = MigrationPolicy.relaxed
        obj = basefieldaction_stub('~Document2', 'field1')  # type: BaseFieldAction

        obj.prepare(test_db, left_schema, policy)

        assert obj._run_ctx == {
            'left_schema': left_schema,
            'db': test_db,
            'collection': test_db['COLLECTION_PLACEHOLDER'],
            'migration_policy': policy,
            'left_field_schema': {'param3': 'schemavalue3'}
        }

    def test_prepare__if_document_type_not_in_schema__should_raise_error(
            self, test_db, left_schema, basefieldaction_stub
    ):
        policy = MigrationPolicy.relaxed
        obj = basefieldaction_stub('UnknownDocumentType', 'field1')  # type: BaseFieldAction

        with pytest.raises(SchemaError):
            obj.prepare(test_db, left_schema, policy)

    def test_prepare__if_field_not_in_document_schema__should_raise_error(
            self, test_db, left_schema, basefieldaction_stub
    ):
        policy = MigrationPolicy.relaxed
        obj = basefieldaction_stub('Document1', 'field_unknown')  # type: BaseFieldAction

        with pytest.raises(SchemaError):
            obj.prepare(test_db, left_schema, policy)

    @pytest.mark.parametrize('args,kwargs,expect', (
        (
            ('Document1', 'field1'),
            {'param1': 'val1', 'param2': 4},
            "StubFieldAction('Document1', 'field1', param1='val1', param2=4)"
        ),
        (
            ('Document1', 'field1'),
            {'param1': 'val1', 'param2': 4, 'dummy_action': True},
            "StubFieldAction('Document1', 'field1', dummy_action=True, param1='val1', param2=4)"
        ),
    ))
    def test_to_python_expr__should_return_python_expression_which_creates_action_object(
            self, basefieldaction_stub, args, kwargs, expect
    ):
        obj = basefieldaction_stub(*args, **kwargs)  # type: BaseFieldAction

        res = obj.to_python_expr()

        assert res == expect

    def test_to_python_expr__if_parameter_has_its_own_to_python_expr__should_call_it(
            self, basefieldaction_stub
    ):
        param1 = mock.Mock()
        param1.to_python_expr.return_value = "'param1_repr'"
        obj = basefieldaction_stub('Document1', 'field1', param1=param1)  # type: BaseFieldAction
        expect = "StubFieldAction('Document1', 'field1', param1='param1_repr')"

        res = obj.to_python_expr()

        assert res == expect

    def test_get_field_handler__should_return_field_handler_object(
            self, test_db, left_schema, basefieldaction_stub
    ):
        obj = basefieldaction_stub('Document1', 'field1', param1='value1')  # type: BaseFieldAction
        right_schema = left_schema
        obj.prepare(test_db, left_schema, MigrationPolicy.relaxed)

        with mock.patch.object(obj, 'get_field_handler_cls') as get_field_handler_cls_mock:
            handler_cls_mock = get_field_handler_cls_mock.return_value
            res = obj._get_field_handler('StringName',
                                         left_schema['Document1']['field1'],
                                         right_schema['Document1']['field1'])

            assert res == handler_cls_mock.return_value
            handler_cls_mock.assert_called_once_with(test_db,
                                                     'Document1',
                                                     left_schema,
                                                     left_schema['Document1']['field1'],
                                                     right_schema['Document1']['field1'],
                                                     MigrationPolicy.relaxed)

    @pytest.mark.parametrize('args,kwargs,expect', (
        (
            ('Document1', 'field1'),
            {'param1': 'val1', 'param2': 4},
            "StubFieldAction('Document1', 'field1', param1='val1', param2=4)"
        ),
        (
            ('Document1', 'field1'),
            {'param1': 'val1', 'param2': 4, 'dummy_action': True},
            "StubFieldAction('Document1', 'field1', param1='val1', param2=4, dummy_action=True)"
        ),
    ))
    def test_repr__should_return_repr_string(self, basefieldaction_stub, args, kwargs, expect):
        obj = basefieldaction_stub(*args, **kwargs)

        assert repr(obj) == expect

    @pytest.mark.parametrize('args,kwargs,expect', (
        (
            ('Document1', 'field1'),
            {'param1': 'val1', 'param2': 4},
            "StubFieldAction('Document1', 'field1', ...)"
        ),
        (
            ('Document1', 'field1'),
            {'param1': 'val1', 'param2': 4, 'dummy_action': True},
            "StubFieldAction('Document1', 'field1', dummy_action=True, ...)"
        ),
    ))
    def test_str__should_return_str_string(self, basefieldaction_stub, args, kwargs, expect):
        obj = basefieldaction_stub(*args, **kwargs)

        assert str(obj) == expect


class TestBaseDocumentAction:
    @pytest.mark.parametrize('args,kwargs,expect', (
        (
            ('Document1',),
            {'param1': 'val1', 'param2': 4},
            "StubDocumentAction('Document1', param1='val1', param2=4)"
        ),
        (
            ('Document1',),
            {'param1': 'val1', 'param2': 4, 'dummy_action': True},
            "StubDocumentAction('Document1', dummy_action=True, param1='val1', param2=4)"
        ),
    ))
    def test_to_python_expr__should_return_python_expression_which_creates_action_object(
            self, basedocumentaction_stub, args, kwargs, expect
    ):
        obj = basedocumentaction_stub(*args, **kwargs)  # type: BaseDocumentAction

        res = obj.to_python_expr()

        assert res == expect

    def test_to_python_expr__if_parameter_has_its_own_to_python_expr__should_call_it(
            self, basedocumentaction_stub
    ):
        param1 = mock.Mock()
        param1.to_python_expr.return_value = "'param1_repr'"
        obj = basedocumentaction_stub('Document1', param1=param1)  # type: BaseDocumentAction
        expect = "StubDocumentAction('Document1', param1='param1_repr')"

        res = obj.to_python_expr()

        assert res == expect

    def test_is_my_collection_used_by_other_documents__if_not_used__should_return_false(
            self, test_db, left_schema, basedocumentaction_stub
    ):
        obj = basedocumentaction_stub('Document1',
                                      param1='val1', param2=4)  # type: BaseDocumentAction
        obj.prepare(test_db, left_schema, MigrationPolicy.relaxed)

        res = obj._is_my_collection_used_by_other_documents()

        assert res is False

    def test_is_my_collection_used_by_other_documents__if_used_by_document__should_return_true(
            self, test_db, basedocumentaction_stub
    ):
        left_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            'Document1_2': Schema.Document({
                'field2': {'param4': 'schemavalue4'},
            }, parameters={'collection': 'document1'})
        })
        obj = basedocumentaction_stub('Document1',
                                      param1='val1', param2=4)  # type: BaseDocumentAction
        obj.prepare(test_db, left_schema, MigrationPolicy.relaxed)

        res = obj._is_my_collection_used_by_other_documents()

        assert res is True

    def test_is_my_collection_used_by_other_documents__should_exclude_embedded_documents(
            self, test_db, left_schema, basedocumentaction_stub
    ):
        Schema({
            'Document1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            '~Document1_2': Schema.Document({
                'field2': {'param4': 'schemavalue4'},
            }, parameters={'collection': 'document1'})
        })
        obj = basedocumentaction_stub('Document1',
                                      param1='val1', param2=4)  # type: BaseDocumentAction
        obj.prepare(test_db, left_schema, MigrationPolicy.relaxed)

        res = obj._is_my_collection_used_by_other_documents()

        assert res is False


class TestBaseCreateDocument:
    def test_build_object__if_document_is_creating__should_return_object(
            self, left_schema, basecreatedocumentaction_stub
    ):
        test_right_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            'Document_new': Schema.Document({
                'field1': {'param_new': 'schemavalue_new'},
            }, parameters={'collection': 'document_new', 'test_parameter': 'test_value'})
        })

        res = basecreatedocumentaction_stub.build_object('Document_new',
                                                         left_schema,
                                                         test_right_schema)

        assert isinstance(res, BaseCreateDocument)
        assert res.document_type == 'Document_new'
        assert res.parameters == {'collection': 'document_new', 'test_parameter': 'test_value'}

    @pytest.mark.parametrize('document_type', ('Document1', '~Document2', 'Document_unknown'))
    def test_build_object__if_document_is_not_creating_in_schema__should_return_none(
            self, left_schema, basecreatedocumentaction_stub, document_type
    ):
        test_right_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            'Document_new': Schema.Document({
                'field1': {'param_new': 'schemavalue_new'},
            }, parameters={'collection': 'document_new', 'test_parameter': 'test_value'})
        })

        res = basecreatedocumentaction_stub.build_object(document_type,
                                                         left_schema,
                                                         test_right_schema)

        assert res is None

    def test_to_schema_patch__should_return_dictdiffer_diff(
            self, left_schema, basecreatedocumentaction_stub
    ):
        obj = basecreatedocumentaction_stub('Document_new',
                                            collection='document_new',
                                            param1='value1')
        item = Schema.Document()
        item.parameters.update(dict(collection='document_new', param1='value1'))

        expect = [(
            'add',
            '',
            [(
                'Document_new',
                item
            )]
        )]

        res = obj.to_schema_patch(left_schema)

        assert res == expect

    def test_prepare__if_collection_in_parameters__should_pick_it_and_prepare_run_context(
            self, test_db, left_schema, basecreatedocumentaction_stub
    ):
        policy = MigrationPolicy.relaxed
        obj = basecreatedocumentaction_stub('Document_new', dummy_action=False,
                                            collection='test_collection1')

        obj.prepare(test_db, left_schema, policy)

        assert obj._run_ctx == {
            'left_schema': left_schema,
            'db': test_db,
            'collection': test_db['test_collection1'],
            'migration_policy': policy
        }

    def test_prepare__if_collection_is_omitted__should_use_placeholder_and_prepare_run_context(
            self, test_db, left_schema, basecreatedocumentaction_stub
    ):
        policy = MigrationPolicy.relaxed
        obj = basecreatedocumentaction_stub('Document_new', dummy_action=False)

        obj.prepare(test_db, left_schema, policy)

        assert obj._run_ctx == {
            'left_schema': left_schema,
            'db': test_db,
            'collection': test_db['COLLECTION_PLACEHOLDER'],
            'migration_policy': policy
        }

    def test_prepare__if_document_type_already_in_left_schema__should_raise_error(
            self, test_db, left_schema, basecreatedocumentaction_stub
    ):
        policy = MigrationPolicy.relaxed
        obj = basecreatedocumentaction_stub('Document1', collection='test_collection')

        with pytest.raises(SchemaError):
            obj.prepare(test_db, left_schema, policy)


class TestBaseDropDocument:
    def test_build_object__if_document_is_dropping__should_return_object(
            self, left_schema, basedropdocumentaction_stub
    ):
        test_right_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            'Document_new': Schema.Document({
                'field1': {'param_new': 'schemavalue_new'},
            }, parameters={'collection': 'document_new', 'test_parameter': 'test_value'})
        })

        res = basedropdocumentaction_stub.build_object('~Document2',
                                                       left_schema,
                                                       test_right_schema)

        assert isinstance(res, BaseDropDocument)
        assert res.document_type == '~Document2'
        assert res.parameters == {}

    @pytest.mark.parametrize('document_type', ('Document1', 'Document_new', 'Document_unknown'))
    def test_build_object__if_document_is_not_dropping_in_schema__should_return_none(
            self, left_schema, basedropdocumentaction_stub, document_type
    ):
        test_right_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            'Document_new': Schema.Document({
                'field1': {'param_new': 'schemavalue_new'},
            }, parameters={'collection': 'document_new', 'test_parameter': 'test_value'})
        })

        res = basedropdocumentaction_stub.build_object(document_type,
                                                       left_schema,
                                                       test_right_schema)

        assert res is None

    def test_to_schema_patch__should_return_dictdiffer_diff(
            self, left_schema, basedropdocumentaction_stub
    ):
        obj = basedropdocumentaction_stub('Document1',
                                          collection='document1',
                                          param1='value1')
        expect = [(
            'remove',
            '',
            [(
                'Document1',
                Schema.Document(
                    {'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'}},
                    parameters=dict(collection='document1'),
                    indexes={'index1': {'fields': [('field1', pymongo.DESCENDING)], 'sparse': True}}
                )
            )]
        )]

        res = obj.to_schema_patch(left_schema)

        assert res == expect


class TestBaseRenameDocument:
    def test_init__should_set_attributes(self, baserenamedocumentaction_stub):
        obj = baserenamedocumentaction_stub('Document1', new_name='Document11', dummy_action=False,
                                            param1='value1', param2=123)  # type: BaseRenameDocument

        assert obj.document_type == 'Document1'
        assert obj.dummy_action is False
        assert obj.parameters == {'param1': 'value1', 'param2': 123}
        assert obj._run_ctx is None
        assert obj.new_name == 'Document11'

    def test_build_object__if_document_was_just_renamed__should_return_object(
            self, left_schema, baserenamedocumentaction_stub
    ):
        test_right_schema = Schema({
            'Document11': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            'Document_new': Schema.Document({
                'field1': {'param_new': 'schemavalue_new'},
            }, parameters={'collection': 'document_new', 'test_parameter': 'test_value'})
        })

        res = baserenamedocumentaction_stub.build_object('Document1',
                                                         left_schema,
                                                         test_right_schema)

        assert isinstance(res, BaseRenameDocument)
        assert res.document_type == 'Document1'
        assert res.new_name == 'Document11'
        assert res.parameters == {}

    @pytest.mark.parametrize('new_schema', (
        Schema.Document({
            'field_changed': {'param11': 'schemavalue11', 'param12': 'schemavalue21'},
            'field12': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
            'field13': {'param31': 'schemavalue31', 'param32': 'schemavalue32'},
            'field14': {'param41': 'schemavalue41', 'param42': 'schemavalue42'},
            'field15': {'param51': 'schemavalue51', 'param52': 'schemavalue52'},
        }, parameters={'collection': 'document1'}),
        Schema.Document({
            'field11': {'param11': 'schemavalue11', 'param12': 'schemavalue21'},
            'field12': {'param_changed': 'schemavalue21', 'param22': 'schemavalue22'},
            'field13': {'param31': 'schemavalue31', 'param32': 'schemavalue32'},
            'field14': {'param41': 'schemavalue41', 'param42': 'schemavalue42'},
            'field15': {'param51': 'schemavalue51', 'param52': 'schemavalue52'},
        }, parameters={'collection': 'document1'}),
        Schema.Document({
            'field11': {'param11': 'schemavalue11', 'param12': 'schemavalue21'},
            'field12': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
            'field13': {'param31': 'schemavalue_changed', 'param32': 'schemavalue32'},
            'field14': {'param41': 'schemavalue41', 'param42': 'schemavalue42'},
            'field15': {'param51': 'schemavalue51', 'param52': 'schemavalue52'},
        }, parameters={'collection': 'document1'}),
        Schema.Document({
            'field11': {'param11': 'schemavalue11', 'param12': 'schemavalue21'},
            'field12': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
            'field13': {'param31': 'schemavalue31', 'param32': 'schemavalue32'},
            'field14': {'param41': 'schemavalue41', 'param42': 'schemavalue42'},
            'field15': {'param51': 'schemavalue51', 'param52': 'schemavalue52'},
        }, parameters={'collection': 'document_changed'}),
    ))
    def test_build_object__if_changes_similarity_more_than_threshold__should_return_object(
            self, baserenamedocumentaction_stub, new_schema
    ):
        left_schema = Schema({
            'Document1': new_schema,
            'Document2': Schema.Document({
                'field1': {'param123': 'schemavalue123'},
            }, parameters={'collection': 'document123', 'test_parameter': 'test_value'}),
            'Document3': Schema.Document({
                'field11': {'param11': 'schemavalue11', 'param12': 'schemavalue21'},
                'field14': {'param41': 'schemavalue41', 'param42': 'schemavalue42'},
            })
        })
        
        right_schema = Schema({
            'Document11': Schema.Document({
                'field11': {'param11': 'schemavalue11', 'param12': 'schemavalue21'},
                'field12': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
                'field13': {'param31': 'schemavalue31', 'param32': 'schemavalue32'},
                'field14': {'param41': 'schemavalue41', 'param42': 'schemavalue42'},
                'field15': {'param51': 'schemavalue51', 'param52': 'schemavalue52'},
            }, parameters={'collection': 'document1'}),
            'Document2': Schema.Document({
                'field1': {'param123': 'schemavalue123'},
            }, parameters={'collection': 'document123', 'test_parameter': 'test_value'}),
            'Document31': Schema.Document({
                'field11': {'param11': 'schemavalue11', 'param12': 'schemavalue21'},
                'field14': {'param41': 'schemavalue41', 'param42': 'schemavalue42'},
            })
        })

        res = baserenamedocumentaction_stub.build_object('Document1', left_schema, right_schema)

        assert isinstance(res, BaseRenameDocument)
        assert res.document_type == 'Document1'
        assert res.new_name == 'Document11'
        assert res.parameters == {}

    def test_build_object__if_there_are_several_rename_candidates__should_return_none(
            self, baserenamedocumentaction_stub
    ):
        left_schema = Schema({
            'Document1': Schema.Document({
                'field11': {'param11': 'schemavalue11', 'param12': 'schemavalue21'},
                'field12': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
                'field13': {'param31': 'schemavalue31', 'param32': 'schemavalue32'},
                'field14': {'param41': 'schemavalue41', 'param42': 'schemavalue42'},
                'field15': {'param51': 'schemavalue51', 'param52': 'schemavalue52'},
            }, parameters={'collection': 'document1'}),
            'Document2': Schema.Document({
                'field1': {'param123': 'schemavalue123'},
            }, parameters={'collection': 'document123', 'test_parameter': 'test_value'}),
            'Document3': Schema.Document({
                'field11': {'param11': 'schemavalue11', 'param12': 'schemavalue21'},
                'field14': {'param41': 'schemavalue41', 'param42': 'schemavalue42'},
            })
        })
        
        right_schema = Schema({
            'Document11': Schema.Document({
                'field_changed': {'param11': 'schemavalue11', 'param12': 'schemavalue21'},
                'field12': {'param21': 'schemavalue21', 'param22': 'schemavalue22'},
                'field13': {'param31': 'schemavalue31', 'param32': 'schemavalue32'},
                'field14': {'param41': 'schemavalue41', 'param42': 'schemavalue42'},
                'field15': {'param51': 'schemavalue51', 'param52': 'schemavalue52'},
            }, parameters={'collection': 'document1'}),
            'Document111': Schema.Document({
                'field11': {'param11': 'schemavalue11', 'param12': 'schemavalue21'},
                'field12': {'param21': 'schemavalue_changed', 'param22': 'schemavalue22'},
                'field13': {'param31': 'schemavalue31', 'param32': 'schemavalue32'},
                'field14': {'param41': 'schemavalue41', 'param42': 'schemavalue42'},
                'field15': {'param51': 'schemavalue51', 'param52': 'schemavalue52'},
            }, parameters={'collection': 'document1'}),
            'Document2': Schema.Document({
                'field1': {'param123': 'schemavalue123'},
            }, parameters={'collection': 'document123', 'test_parameter': 'test_value'}),
            'Document31': Schema.Document({
                'field11': {'param11': 'schemavalue11', 'param12': 'schemavalue21'},
                'field14': {'param41': 'schemavalue41', 'param42': 'schemavalue42'},
            }),
        })
        
        res = baserenamedocumentaction_stub.build_object('Document1', left_schema, right_schema)

        assert res is None

    @pytest.mark.parametrize('document_type', ('Document1', 'Document_new', 'Document_unknown'))
    def test_build_object__if_document_is_not_disappears_in_right_schema__should_return_none(
            self, left_schema, baserenamedocumentaction_stub, document_type
    ):
        test_right_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            'Document_new': Schema.Document({
                'field1': {'param_new': 'schemavalue_new'},
            }, parameters={'collection': 'document_new', 'test_parameter': 'test_value'})
        })

        res = baserenamedocumentaction_stub.build_object(document_type,
                                                         left_schema,
                                                         test_right_schema)

        assert res is None

    def test_build_object__if_changes_similarity_less_than_threshold__should_return_object(
            self, left_schema, baserenamedocumentaction_stub
    ):
        test_right_schema = Schema({
            'Document11': Schema.Document({
                'field1': {'param3': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document12'}),
            'Document_new': Schema.Document({
                'field1': {'param_new': 'schemavalue_new'},
            }, parameters={'collection': 'document_new', 'test_parameter': 'test_value'})
        })

        res = baserenamedocumentaction_stub.build_object('Document1',
                                                         left_schema,
                                                         test_right_schema)

        assert res is None

    def test_to_schema_patch__should_return_dictdiffer_diff(
            self, left_schema, baserenamedocumentaction_stub
    ):
        obj = baserenamedocumentaction_stub('Document1', new_name='Document11', param1='value1')
        expect = [
            ('remove', '',
                [(
                    'Document1',
                    Schema.Document(
                        {'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'}},
                        parameters={'collection': 'document1'},
                        indexes={
                            'index1': {'fields': [('field1', pymongo.DESCENDING)], 'sparse': True}
                        }
                    )
                )]
            ),
            ('add', '',
                [(
                    'Document11',
                    Schema.Document(
                        {'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'}},
                        parameters={'collection': 'document1'},
                        indexes={
                            'index1': {'fields': [('field1', pymongo.DESCENDING)], 'sparse': True}
                        }
                    )
                )]
            )
        ]

        res = obj.to_schema_patch(left_schema)

        assert res == expect


class TestBaseAlterDocument:
    def test_build_object__if_document_in_schema_has_changed__should_return_object(
            self, left_schema, basealterdocumentaction_stub
    ):
        test_right_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param3': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document11'}),
            'Document_new': Schema.Document({
                'field1': {'param_new': 'schemavalue_new'},
            }, parameters={'collection': 'document_new', 'test_parameter': 'test_value'})
        })

        res = basealterdocumentaction_stub.build_object('Document1', left_schema, test_right_schema)

        assert isinstance(res, BaseAlterDocument)
        assert res.document_type == 'Document1'
        assert res.parameters == {'collection': 'document11'}

    @pytest.mark.parametrize('document_type', ('~Document2', 'Document_new', 'Document_unknown'))
    def test_build_object__if_document_is_not_in_both_schemas__should_return_none(
            self, left_schema, basealterdocumentaction_stub, document_type
    ):
        test_right_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document11'}),
            'Document_new': Schema.Document({
                'field1': {'param_new': 'schemavalue_new'},
            }, parameters={'collection': 'document_new', 'test_parameter': 'test_value'})
        })

        res = basealterdocumentaction_stub.build_object(document_type,
                                                        left_schema,
                                                        test_right_schema)

        assert res is None

    def test_build_object__if_document_parameters_has_not_changed__should_return_none(
            self, left_schema, basealterdocumentaction_stub
    ):
        test_right_schema = Schema({
            'Document1': Schema.Document({
                'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'},
            }, parameters={'collection': 'document1'}),
            'Document_new': Schema.Document({
                'field1': {'param_new': 'schemavalue_new'},
            }, parameters={'collection': 'document_new', 'test_parameter': 'test_value'})
        })

        res = basealterdocumentaction_stub.build_object('Document1', left_schema, test_right_schema)

        assert res is None

    def test_to_schema_patch__should_return_dictdiffer_diff_with_all_new_parameters(
            self, left_schema, basealterdocumentaction_stub
    ):
        obj = basealterdocumentaction_stub('Document1', param1='new_value1', param2='new_param2')
        expect_left_docschema = left_schema['Document1']
        expect_right_docschema = Schema.Document(
            {'field1': {'param1': 'schemavalue1', 'param2': 'schemavalue2'}},
            parameters={'param1': 'new_value1', 'param2': 'new_param2'},
            indexes={'index1': {'fields': [('field1', pymongo.DESCENDING)], 'sparse': True}}
        )
        expect = [(
            'change',
            'Document1',
            (expect_left_docschema, expect_right_docschema)
        )]

        res = obj.to_schema_patch(left_schema)

        assert res == expect

    @pytest.mark.parametrize('diff,can_be_none,check_type', (
        (Diff(1, 2, "key"), True, None),
        (Diff(1, None, "key"), True, None),
        (Diff(None, 1, "key"), True, None),
        (Diff(1, 2, "key"), True, int),
        (Diff(1, "2", "key"), True, (int, str)),
        (Diff("1", 2, "key"), True, (int, str)),
    ))
    def test_check_diff__if_check_passed__should_not_raise_error(
            self, basealterdocumentaction_stub, diff, can_be_none, check_type
    ):
        res = basealterdocumentaction_stub._check_diff(diff, can_be_none, check_type)

        assert res is None

    @pytest.mark.parametrize('diff,can_be_none,check_type', (
        (Diff(1, 1, "key"), True, None),  # old == new
        (Diff(None, None, "key"), True, None),  # old == new
        (Diff(None, None, "key"), False, None),  # old == new
        (Diff(1, None, "key"), False, None),  # Can not be None
        (Diff(None, 1, "key"), False, None),  # Can not be None
        (Diff(1, "2", "key"), True, int),  # Wrong type
        (Diff("1", 2, "key"), True, int)   # Wrong type
    ))
    def test_check_diff__if_check_failed__should_raise_error(
            self, basealterdocumentaction_stub, diff, can_be_none, check_type
    ):
        with pytest.raises(SchemaError):
            res = basealterdocumentaction_stub._check_diff(diff, can_be_none, check_type)


class TestBaseIndex:
    def test_init__should_set_attributes(self, baseindexaction_stub):
        obj = baseindexaction_stub(
            'Document1', 'index1', fields=[('field1', pymongo.DESCENDING)], dummy_action=False,
            param1='value1', param2=123
        )  # type: BaseIndexAction

        assert obj.document_type == 'Document1'
        assert obj.index_name == 'index1'
        assert obj.dummy_action is False
        assert obj.parameters == {
            'param1': 'value1',
            'param2': 123,
            'fields': [('field1', pymongo.DESCENDING)]
        }
        assert obj._run_ctx is None

    def test_prepare__if_collection_in_left_schema__should_prepare_run_context(
            self, test_db, left_schema, baseindexaction_stub
    ):
        obj = baseindexaction_stub(
            'Document1', 'index1', fields=[('field1', pymongo.DESCENDING)],
            param1='value1', param2=123
        )  # type: BaseIndexAction
        policy = MigrationPolicy.relaxed

        obj.prepare(test_db, left_schema, policy)

        assert obj._run_ctx == {
            'left_schema': left_schema,
            'db': test_db,
            'collection': test_db['document1'],
            'migration_policy': policy,
            'left_index_schema': {'fields': [('field1', pymongo.DESCENDING)], 'sparse': True}
        }

    @pytest.mark.parametrize('args,kwargs,expect', (
        (
            ('Document1', 'index1'),
            {'param1': 'val1', 'param2': 4, 'fields': [('field1', pymongo.DESCENDING)]},
            "StubIndexAction('Document1', 'index1', fields=[('field1', pymongo.DESCENDING)], param1='val1', param2=4)"
        ),
        (
            ('Document1', 'index1'),
            {
                'param1': 'val1',
                'param2': 4,
                'dummy_action': True,
                'fields': [('field1', pymongo.DESCENDING)]
            },
            "StubIndexAction('Document1', 'index1', fields=[('field1', pymongo.DESCENDING)], dummy_action=True, param1='val1', param2=4)"
        ),
    ))
    def test_to_python_expr__should_return_python_expression_which_creates_action_object(
            self, baseindexaction_stub, args, kwargs, expect
    ):
        obj = baseindexaction_stub(*args, **kwargs)  # type: BaseIndexAction

        res = obj.to_python_expr()

        assert res == expect

    def test_to_python_expr__if_parameter_has_its_own_to_python_expr__should_call_it(
            self, baseindexaction_stub
    ):
        param1 = mock.Mock()
        param1.to_python_expr.return_value = "'param1_repr'"
        obj = baseindexaction_stub(
            'Document1', 'index1', fields=[('field1', pymongo.DESCENDING)], param1=param1
        )  # type: BaseIndexAction
        expect = "StubIndexAction('Document1', 'index1', fields=[('field1', pymongo.DESCENDING)], param1='param1_repr')"

        res = obj.to_python_expr()

        assert res == expect
