from unittest.mock import Mock

import pytest

from mongoengine_migrate.actions import RunPython
from mongoengine_migrate.exceptions import ActionError
from mongoengine_migrate.graph import MigrationPolicy
from mongoengine_migrate.schema import Schema


function_mock = Mock()
function_mock.__str__ = lambda self: 'function_mock'
function_mock.__name__ = 'function_mock'


@pytest.fixture
def left_schema():
    return Schema({'Document1': Schema.Document(
        {'field1': {'param1': 'val1'}},
        parameters={'collection': 'document1'}
    )})


class TestRunPython:
    def test_init__should_set_attributes(self):
        forward_func = Mock()
        backward_func = Mock()

        obj = RunPython('Document1',
                        forward_func=forward_func, backward_func=backward_func,
                        param1='value1', param2=123)

        assert obj.forward_func is forward_func
        assert obj.backward_func is backward_func
        assert obj.document_type == 'Document1'
        assert obj.dummy_action is False
        assert obj.parameters == {'param1': 'value1', 'param2': 123}
        assert obj._run_ctx is None

    @pytest.mark.parametrize('forward_func,backward_func', (
            (function_mock, None),
            (None, function_mock)
    ))
    def test_init__if_one_function_specified__should_set_attributes(
            self, forward_func, backward_func
    ):
        obj = RunPython('Document1', forward_func=forward_func, backward_func=backward_func)

        assert obj.forward_func is forward_func
        assert obj.backward_func is backward_func

    def test_init__if_no_both_functions_are_omitted__should_raise_error(self):
        with pytest.raises(ActionError):
            RunPython('Document1')

    def test_forward__should_call_forward_func(self, test_db, left_schema):
        forward_func = Mock()
        obj = RunPython('Document1', forward_func=forward_func)
        obj.prepare(test_db, left_schema, MigrationPolicy.relaxed)

        obj.run_forward()

        forward_func.assert_called_once_with(test_db, test_db['document1'], left_schema)

    def test_backward__should_call_backward_func(self, test_db, left_schema):
        backward_func = Mock()
        obj = RunPython('Document1', backward_func=backward_func)
        obj.prepare(test_db, left_schema, MigrationPolicy.relaxed)

        obj.run_backward()

        backward_func.assert_called_once_with(test_db, test_db['document1'], left_schema)

    def test_to_schema_patch__should_return_empty_schema(self, left_schema):
        forward_func = Mock()
        obj = RunPython('Document1', forward_func=forward_func)

        res = obj.to_schema_patch(left_schema)

        assert res == []

    @pytest.mark.parametrize('args,kwargs,expect', (
        (
            ('Document1', ),
            {'forward_func': function_mock, 'backward_func': function_mock, 'param1': 'val1', 'param2': 4},
            "RunPython('Document1', forward_func=function_mock, backward_func=function_mock, param1='val1', param2=4)"
        ),
        (
            ('Document1', ),
            {'forward_func': function_mock, 'param1': 'val1', 'param2': 4, 'dummy_action': True},
            "RunPython('Document1', forward_func=function_mock, dummy_action=True, param1='val1', param2=4)"
        ),
        (
            ('Document1', ),
            {'backward_func': function_mock, 'param1': 'val1', 'param2': 4, 'dummy_action': True},
            "RunPython('Document1', backward_func=function_mock, dummy_action=True, param1='val1', param2=4)"
        ),
    ))
    def test_to_python_expr__should_return_python_expression_which_creates_action_object(
            self, args, kwargs, expect
    ):
        obj = RunPython(*args, **kwargs)

        res = obj.to_python_expr()

        assert res == expect

    def test_to_python_expr__if_parameter_has_its_own_to_python_expr__should_call_it(self):
        param1 = Mock()
        param1.to_python_expr.return_value = "'param1_repr'"
        obj = RunPython('Document1', forward_func=function_mock, param1=param1)
        expect = "RunPython('Document1', forward_func=function_mock, param1='param1_repr')"

        res = obj.to_python_expr()

        assert res == expect
