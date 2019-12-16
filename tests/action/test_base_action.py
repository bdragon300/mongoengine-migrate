import pytest
import itertools
from unittest.mock import Mock
from mongoengine_migrate.action import BaseAction
from mongoengine_migrate.command import BaseCommand
from mongoengine_migrate.exceptions import ActionError


class BaseActionStub(BaseAction):
    COMMANDS_CHAIN = []
    STATE = {}

    def as_schema_patch(self):
        return self.STATE

    def _build_commands_chain(self, old_schema, new_schema):
        return self.COMMANDS_CHAIN


class TestBaseAction:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.new_state = {'new': 'schema'}
        command_attrs = {'forward.return_value': True, 'backward.return_value': True}
        BaseActionStub.COMMANDS_CHAIN = [
            Mock(spec=BaseCommand, **command_attrs),
            Mock(spec=BaseCommand, **command_attrs),
            Mock(spec=BaseCommand, **command_attrs),
            Mock(spec=BaseCommand, **command_attrs)
        ]
        BaseActionStub.STATE = self.new_state
        self.db = Mock()
        self.collection = Mock()
        self.old_state = {'old': 'schema'}

        self.obj = BaseActionStub('base_collection', 'test', 123, test_param=789)

    def test_init__should_set_constructor_params(self):
        assert self.obj.collection_name == 'base_collection'
        assert list(self.obj._init_args) == ['test', 123]
        assert self.obj._init_kwargs == {'test_param': 789}

    def test_prepare__should_store_old_state(self):
        self.obj.prepare(self.old_state)

        assert self.obj.current_schema == self.old_state

    def test_run_forward__on_all_command_successful__should_return_empty_list(self):
        res = self.obj.run_forward(self.db, self.collection)

        assert res == []

    def test_run_forward__with_default_params__should_call_every_command(self):
        self.obj.prepare(self.old_state)
        res = self.obj.run_forward(self.db, self.collection)

        [cmd_mock.forward.assert_called_once_with(self.db,
                                                  self.collection,
                                                  self.old_state,
                                                  self.new_state)
         for cmd_mock in self.obj.COMMANDS_CHAIN]

    @pytest.mark.parametrize('side_effect', (False, None, ActionError))
    def test_run_forward__on_command_fail__should_raise_action_error_and_return_cmd_index(
            self, side_effect
    ):
        failed_index = 1
        self.obj.COMMANDS_CHAIN[failed_index].side_effect = side_effect

        self.obj.prepare(self.old_state)

        with pytest.raises(ActionError) as e:
            self.obj.run_forward(self.db, self.collection)

        assert e.value.args[1] == failed_index

    @pytest.mark.parametrize('side_effect', (False, None, ActionError))
    def test_run_forward__on_command_fail__should_stop_on_failed_command(self, side_effect):
        failed_index = 2
        self.obj.COMMANDS_CHAIN[failed_index].side_effect = side_effect

        self.obj.prepare(self.old_state)

        with pytest.raises(ActionError) as e:
            self.obj.run_forward(self.db, self.collection)

        [cmd_mock.forward.assert_called_once_with(self.db,
                                                  self.collection,
                                                  self.old_state,
                                                  self.new_state)
         for cmd_mock in self.obj.COMMANDS_CHAIN[:failed_index + 1]]
        [cmd_mock.forward.assert_not_called()
         for cmd_mock in self.obj.COMMANDS_CHAIN[failed_index + 1:]]

    @pytest.mark.parametrize('side_effect', (False, None, ActionError))
    def test_run_forward__on_commands_fail_and_ignore_failed__should_return_exceptions(self,
                                                                                       side_effect):
        failed_indexes = (1, 2)
        test_exception = RuntimeError('Something')
        self.obj.COMMANDS_CHAIN[failed_indexes[0]].side_effect = side_effect
        self.obj.COMMANDS_CHAIN[failed_indexes[1]].side_effect = test_exception
        expect = [
            (1, side_effect),
            (2, test_exception)
        ]

        self.obj.prepare(self.old_state)
        res = self.obj.run_forward(self.db, self.collection, ignore_failed=True)

        assert res == expect

    @pytest.mark.parametrize('side_effect', (False, None, ActionError))
    def test_run_forward__on_commands_fail_and_ignore_failed__should_call_all_commands_whatever(
            self,
            side_effect
    ):
        failed_indexes = (1, 2)
        test_exception = RuntimeError('Something')
        self.obj.COMMANDS_CHAIN[failed_indexes[0]].side_effect = side_effect
        self.obj.COMMANDS_CHAIN[failed_indexes[1]].side_effect = test_exception

        self.obj.prepare(self.old_state)
        res = self.obj.run_forward(self.db, self.collection, ignore_failed=True)

        [cmd_mock.forward.assert_called_once_with(self.db,
                                                  self.collection,
                                                  self.old_state,
                                                  self.new_state)
         for cmd_mock in self.obj.COMMANDS_CHAIN]

    def test_run_forward__on_start_from_is_passed__should_run_command_chain_from_this_index(self):
        start_from = 2
        new_state = {'test': 'schema'}

        self.obj.prepare(self.old_state)
        res = self.obj.run_forward(self.db, self.collection, start_from=start_from)

        assert res == []
        [cmd_mock.forward.assert_not_called()
         for cmd_mock in self.obj.COMMANDS_CHAIN[:start_from]]
        [cmd_mock.forward.assert_called_once_with(self.db,
                                                  self.collection,
                                                  self.old_state,
                                                  new_state)
         for cmd_mock in self.obj.COMMANDS_CHAIN[start_from:]]

    @pytest.mark.parametrize('start_from', (-1, 4, 100))
    def test_run_forward__on_wrong_start_from_param__should_raise_value_error(self, start_from):
        self.obj.prepare(self.old_state)
        with pytest.raises(ValueError):
            self.obj.run_forward(self.db, self.collection, start_from=start_from)

    def test_run_backward__on_all_command_successful__should_return_empty_list(self):
        res = self.obj.run_backward(self.db, self.collection)

        assert res == []

    def test_run_backward__with_default_params__should_call_every_command(self):
        self.obj.prepare(self.old_state)
        res = self.obj.run_backward(self.db, self.collection)

        [cmd_mock.backward.assert_called_once_with(self.db,
                                                   self.collection,
                                                   self.old_state,
                                                   self.new_state)
         for cmd_mock in self.obj.COMMANDS_CHAIN]

    @pytest.mark.parametrize('side_effect', (False, None, ActionError))
    def test_run_backward__on_command_fail__should_raise_action_error_and_return_cmd_index(
            self, side_effect
    ):
        failed_index = 1
        self.obj.COMMANDS_CHAIN[failed_index].side_effect = side_effect

        self.obj.prepare(self.old_state)

        with pytest.raises(ActionError) as e:
            self.obj.run_backward(self.db, self.collection)

        assert e.value.args[1] == failed_index

    @pytest.mark.parametrize('side_effect', (False, None, ActionError))
    def test_run_backward__on_command_fail__should_stop_on_failed_command(self, side_effect):
        failed_index = 1
        self.obj.COMMANDS_CHAIN[failed_index].side_effect = side_effect

        self.obj.prepare(self.old_state)

        with pytest.raises(ActionError) as e:
            self.obj.run_backward(self.db, self.collection)

        [cmd_mock.backward.assert_called_once_with(self.db,
                                                   self.collection,
                                                   self.old_state,
                                                   self.new_state)
         for cmd_mock in self.obj.COMMANDS_CHAIN[failed_index:]]
        [cmd_mock.backward.assert_not_called()
         for cmd_mock in self.obj.COMMANDS_CHAIN[:failed_index]]

    @pytest.mark.parametrize('side_effect', (False, None, ActionError))
    def test_run_backward__on_commands_fail_and_ignore_failed__should_return_exceptions(self,
                                                                                       side_effect):
        failed_indexes = (1, 2)
        test_exception = RuntimeError('Something')
        self.obj.COMMANDS_CHAIN[failed_indexes[0]].side_effect = side_effect
        self.obj.COMMANDS_CHAIN[failed_indexes[1]].side_effect = test_exception
        expect = [
            (2, test_exception),
            (1, side_effect)
        ]

        self.obj.prepare(self.old_state)
        res = self.obj.run_backward(self.db, self.collection, ignore_failed=True)

        assert res == expect

    @pytest.mark.parametrize('side_effect', (False, None, ActionError))
    def test_run_backward__on_commands_fail_and_ignore_failed__should_call_all_commands_whatever(
            self,
            side_effect
    ):
        failed_indexes = (1, 2)
        test_exception = RuntimeError('Something')
        self.obj.COMMANDS_CHAIN[failed_indexes[0]].side_effect = side_effect
        self.obj.COMMANDS_CHAIN[failed_indexes[1]].side_effect = test_exception

        self.obj.prepare(self.old_state)
        res = self.obj.run_backward(self.db, self.collection, ignore_failed=True)

        [cmd_mock.backward.assert_called_once_with(self.db,
                                                   self.collection,
                                                   self.old_state,
                                                   self.new_state)
         for cmd_mock in self.obj.COMMANDS_CHAIN]

    def test_run_backward__on_start_from_is_passed__should_run_command_chain_from_this_index(self):
        start_from = 2
        new_state = {'test': 'schema'}

        self.obj.prepare(self.old_state)
        res = self.obj.run_backward(self.db, self.collection, start_from=start_from)

        assert res == []
        [cmd_mock.backward.assert_not_called()
         for cmd_mock in self.obj.COMMANDS_CHAIN[start_from + 1:]]
        [cmd_mock.backward.assert_called_once_with(self.db,
                                                  self.collection,
                                                  self.old_state,
                                                  new_state)
         for cmd_mock in self.obj.COMMANDS_CHAIN[:start_from + 1]]

    @pytest.mark.parametrize('start_from', (-1, 4, 100))
    def test_run_backward__on_wrong_start_from_param__should_raise_value_error(self, start_from):
        self.obj.prepare(self.old_state)
        with pytest.raises(ValueError):
            self.obj.run_backward(self.db, self.collection, start_from=start_from)
