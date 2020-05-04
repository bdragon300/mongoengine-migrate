import weakref
from abc import ABCMeta, abstractmethod
from typing import Dict, Type, Optional, List

from pymongo.database import Database

from mongoengine_migrate.fields.registry import type_key_registry
import mongoengine_migrate.flags as runtime_flags
from mongoengine_migrate.query_tracer import QueryTracer, HistoryCall

#: Migration Actions registry. Mapping of class name and its class
actions_registry: Dict[str, Type['BaseAction']] = {}


class BaseActionMeta(ABCMeta):
    def __new__(mcs, name, bases, attrs):
        attrs['_meta'] = weakref.proxy(mcs)

        c = super(BaseActionMeta, mcs).__new__(mcs, name, bases, attrs)
        if not name.startswith('Base'):
            actions_registry[name] = c

        return c


class BaseAction(metaclass=BaseActionMeta):
    """Base class for migration actions

    Action represents one change within migration such as field
    altering, collection renaming, collection dropping, etc.

    Migration file typically consists of actions following by each
    other. Every action accepts collection name and other parameters
    (if any) which describes change.

    Action also can be represented as dictdiff diff in order to apply
    schema changes.
    """

    #: `higher_priority = True` means that the action has high
    #: priority in test for applicability for schema change.
    #: This flag is suitable for rename actions which should get tested
    #: before create/drop actions
    higher_priority = False

    def __init__(self, collection_name: str, **kwargs):
        """
        :param collection_name: Name of collection where the migration
         will be performed on
        :param kwargs: Action keyword parameters
        """
        self.collection_name = collection_name
        self.parameters = kwargs

        self.current_schema = None
        self.db = None
        self.collection = None

    def prepare(self, db: Database, current_schema: dict):
        """
        Prepare action before Action run (both forward and backward)
        :param db: pymongo.Database object
        :param current_schema: db schema which is before migration
        :return:
        """
        self.current_schema = current_schema
        self.db = db
        self.collection = db[self.collection_name]
        if runtime_flags.dry_run:
            self.collection = QueryTracer(self.collection)

    def cleanup(self):
        """Cleanup after Action run (both forward and backward)"""
        if runtime_flags.dry_run:
            self.collection.call_history.clear()

    @abstractmethod
    def run_forward(self):
        """DB commands to be run in forward direction"""

    @abstractmethod
    def run_backward(self):
        """DB commands to be run in backward direction"""

    @abstractmethod
    def to_schema_patch(self, current_schema: dict):
        """
        Return dictdiff patch should get applied in a forward direction
        run, but it runs in both directions. So the patch typically
        should be the same no matter which the `current_schema`
        structure has. In other words, `current_schema` has
        the schema before action would run in a forward direction and
        the schema after action would run in a backward direction.
        :param current_schema: schema state before method call
        :return: dictdiffer diff
        """

    @abstractmethod
    def to_python_expr(self) -> str:
        """
        Return string of python code which creates current object with
        the same state
        """

    def get_call_history(self) -> List[HistoryCall]:
        """Return call history of collection modification methods"""
        if runtime_flags.dry_run:
            return self.collection.call_history

        return []


# TODO: add to prepare() checking if db_field param has not dots
class BaseFieldAction(BaseAction):
    """
    Base class for action which affects on one field in a collection
    """

    def __init__(self, collection_name: str, field_name: str, **kwargs):
        """
        :param collection_name: collection name to be affected
        :param field_name: changing mongoengine document field name
        """
        super().__init__(collection_name, **kwargs)
        self.field_name = field_name

    @property
    def field_handler_cls(self):
        """Concrete FieldHandler class for a current field type"""
        return type_key_registry[self.parameters.get('type_key')].field_handler_cls

    @classmethod
    @abstractmethod
    def build_object(cls,
                     collection_name: str,
                     field_name: str,
                     old_schema: dict,
                     new_schema: dict) -> Optional['BaseFieldAction']:
        """
        Factory method which tests if current action type could process
        schema changes for a given collection and field. If yes then
        it produces object of current action type with filled out
        perameters. If no then it returns None.

        This method is used to guess which action is suitable to
        reflect schema change. It's called for several times for each
        field which was modified by a user in mongoengine documents.

        For example, on field deleting the method defined in
        CreateField action should return None, but those one in
        DropField action should return DeleteField object with
        filled out parameters of change (type of field, required flag,
        etc.)

        :param collection_name: collection name to consider
        :param field_name: field name to consider
        :param old_schema: database schema before a migration
         would get applied
        :param new_schema: database schema after a migration
         would get applied
        :return: object of self type or None
        """
        pass

    def to_python_expr(self) -> str:
        parameters = {
            name: getattr(val, 'to_python_expr', lambda: repr(val))()
            for name, val in self.parameters.items()
        }
        kwargs_str = ''.join(f", {name}={val}" for name, val in sorted(parameters.items()))
        return f'{self.__class__.__name__}({self.collection_name!r}, {self.field_name!r}' \
               f'{kwargs_str})'


class BaseCollectionAction(BaseAction):
    """
    Base class for actions which change a collection at whole such as
    renaming, creating, dropping, etc.
    """

    #: Empty collection schema contents skeleton
    COLLECTION_SCHEMA_SKEL = {}

    @classmethod
    @abstractmethod
    def build_object(cls,
                     collection_name: str,
                     old_schema: dict,
                     new_schema: dict) -> Optional['BaseCollectionAction']:
        """
        Factory method which tests if current action type could process
        schema changes for a given collection at whole. If yes then
        it produces object of current action type with filled out
        perameters. If no then it returns None.

        This method is used to guess which action is suitable to
        reflect schema change. It's called for several times for each
        collection which was modified by a user in mongoengine
        documents.

        For example, on collection deleting the method defined in
        CreateCollection action should return None, but those one in
        DropCollection action should return DropCollection object with
        filled out parameters of change (collection name, indexes, etc.)

        :param collection_name: collection name to consider
        :param old_schema: database schema before a migration
         would get applied
        :param new_schema: database schema after a migration
         would get applied
        :return: object of self type or None
        """
        pass

    def to_python_expr(self) -> str:
        parameters = {
            name: getattr(val, 'to_python_expr', lambda: repr(val))()
            for name, val in self.parameters.items()
        }
        kwargs_str = ''.join(f", {name}={val}" for name, val in sorted(parameters.items()))
        return f'{self.__class__.__name__}({self.collection_name!r}{kwargs_str})'
