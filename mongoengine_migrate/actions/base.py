import weakref
from abc import ABCMeta, abstractmethod
from typing import Dict, Type, Optional, List

from pymongo.database import Database

import mongoengine_migrate.flags as runtime_flags
from mongoengine_migrate.exceptions import MigrationError, ActionError
from mongoengine_migrate.fields.registry import type_key_registry
from mongoengine_migrate.query_tracer import HistoryCall
from mongoengine_migrate.schema import Schema

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

    #: Priority which this action will be tested with. The smaller
    #: priority number, the higher priority this action has.
    #: This flag is suitable for rename actions which should get tested
    #: before create/drop actions. Default is 5 which means normal
    priority = 12

    def __init__(self, document_type: str, *, dummy_action: bool = False, **kwargs):
        """
        :param document_type: Document type in schema which will
         Action will use to make changes
        :param dummy_action: If True then the action will not
         perform any queries on db during migration, but still used
         for changing the db schema
        :param kwargs: Action keyword parameters
        """
        self.tumblr = document_type
        # Could contain collection name or embedded document name
        self.document_type = document_type
        self.dummy_action = dummy_action
        self.parameters = kwargs
        self._run_ctx = None  # Run context, filled by `prepare()`

        _prefix = runtime_flags.EMBEDDED_DOCUMENT_NAME_PREFIX
        if document_type.startswith(_prefix):
            self.tumblr = document_type[len(_prefix):]

    def prepare(self, db: Database, left_schema: Schema):
        """
        Prepare action before Action run (both forward and backward)
        :param db: pymongo.Database object
        :param left_schema: db schema before migration (left side)
        :return:
        """
        collection = db[self.tumblr]
        self._run_ctx = {
            'left_schema': left_schema,
            'db': db,
            'collection': collection
        }

    def cleanup(self):
        """Cleanup after Action run (both forward and backward)"""
        if runtime_flags.dry_run:
            self._run_ctx['collection'].call_history.clear()

    @abstractmethod
    def run_forward(self):
        """
        DB commands to be run in forward direction.

        All queries executed here must be idempotental, i.e. give the
        same result after repeated execution. This is because if any
        query would fail then migration process will be aborted, and
        repeated migration run will execute the same commands in this
        case until the migration will get finished.
        """

    @abstractmethod
    def run_backward(self):
        """
        DB commands to be run in backward direction

        All queries executed here must be idempotental, i.e. give the
        same result after repeated execution. This is because if any
        query would fail then migration process will be aborted, and
        repeated migration run will execute the same commands in this
        case until the migration will get finished.
        """

    @abstractmethod
    def to_schema_patch(self, left_schema: Schema):
        """
        Return dictdiff patch should get applied in a forward direction
        run
        :param left_schema: schema state before the Action would get
         applied (left side)
        :return: schema diff
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
            return self._run_ctx['collection'].call_history

        return []


class BaseFieldAction(BaseAction):
    """
    Base class for action which affects on one field in a collection
    """

    def __init__(self, document_type: str, field_name: str, **kwargs):
        """
        :param document_type: collection name to be affected
        :param field_name: changing mongoengine document field name
        """
        super().__init__(document_type, **kwargs)
        self.field_name = field_name

        db_field = kwargs.get('db_field')
        if db_field and '.' in db_field:
            raise ActionError("Field name must not contain dots")

    def get_field_handler_cls(self, type_key: str):
        """Concrete FieldHandler class for a given type key"""
        if type_key not in type_key_registry:
            raise MigrationError(f'Could not find field {type_key!r} or one of its base classes '
                                 f'in type_key registry')

        return type_key_registry[type_key].field_handler_cls

    @classmethod
    @abstractmethod
    def build_object(cls,
                     document_type: str,
                     field_name: str,
                     left_schema: Schema,
                     right_schema: Schema) -> Optional['BaseFieldAction']:
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

        :param document_type: document type in schema to consider
        :param field_name: field name to consider
        :param left_schema: database schema before a migration
         would get applied (left side)
        :param right_schema: database schema after a migration
         would get applied (right side)
        :return: object of self type or None
        """
        pass

    def prepare(self, db: Database, left_schema: Schema):
        super().prepare(db, left_schema)

        self._run_ctx['left_field_schema'] = \
            left_schema[self.document_type].get(self.field_name, {})

    def to_python_expr(self) -> str:
        parameters = {
            name: getattr(val, 'to_python_expr', lambda: repr(val))()
            for name, val in self.parameters.items()
        }
        if self.dummy_action:
            parameters['dummy_action'] = True

        kwargs_str = ''.join(f", {name}={val}" for name, val in sorted(parameters.items()))
        return f'{self.__class__.__name__}({self.document_type!r}, {self.field_name!r}' \
               f'{kwargs_str})'

    def _get_field_handler(self, type_key: str, left_field_schema: dict, right_field_schema: dict):
        """
        Return FieldHandler object by type_key
        :param type_key: field type_key string
        :param left_field_schema: left schema which will be passed to
         a field
        :param right_field_schema: right schema which will be passed to
         a field
        :return: concrete FieldHandler object
        """
        handler_cls = self.get_field_handler_cls(type_key)
        handler = handler_cls(self._run_ctx['db'],
                              self.document_type,
                              self._run_ctx['left_schema'],
                              left_field_schema,
                              right_field_schema)
        return handler


class BaseDocumentAction(BaseAction):
    """
    Base class for actions which change a document (collection or
    embedded document) at whole such as renaming, creating, dropping, etc.
    """
    @classmethod
    @abstractmethod
    def build_object(cls,
                     document_type: str,
                     left_schema: Schema,
                     right_schema: Schema) -> Optional['BaseDocumentAction']:
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

        :param document_type: document type in schema to consider
        :param left_schema: database schema before a migration
         would get applied (left side)
        :param right_schema: database schema after a migration
         would get applied (right side)
        :return: object of self type or None
        """
        pass

    def to_python_expr(self) -> str:
        parameters = {
            name: getattr(val, 'to_python_expr', lambda: repr(val))()
            for name, val in self.parameters.items()
        }
        if self.dummy_action:
            parameters['dummy_action'] = True

        kwargs_str = ''.join(f", {name}={val}" for name, val in sorted(parameters.items()))
        return f'{self.__class__.__name__}({self.document_type!r}{kwargs_str})'


class BaseCreateDocument(BaseDocumentAction):
    @classmethod
    def build_object(cls, document_type: str, left_schema: Schema, right_schema: Schema):
        if document_type not in left_schema and document_type in right_schema:
            return cls(document_type=document_type)

    def to_schema_patch(self, left_schema: Schema):
        return [('add', '', [(self.document_type, Schema.Document())])]


class BaseDropDocument(BaseDocumentAction):
    @classmethod
    def build_object(cls, document_type: str, left_schema: Schema, right_schema: Schema):
        if document_type in left_schema and document_type not in right_schema:
            return cls(document_type=document_type)  # FIXME: parameters (indexes, acl, etc.)

    def to_schema_patch(self, left_schema: Schema):
        item = left_schema[self.document_type]
        return [('remove', '', [(self.document_type, item)])]


class BaseRenameDocument(BaseDocumentAction):
    priority = 3

    #: How much percent of items in schema diff of two collections
    #: should be equal to consider such change as collection rename
    #: instead of drop/create
    similarity_threshold = 70

    def __init__(self, document_type: str, *, new_name, **kwargs):
        super().__init__(document_type, new_name=new_name, **kwargs)
        self.new_name = new_name

    @classmethod
    def build_object(cls, document_type: str, left_schema: Schema, right_schema: Schema):
        # Check if field exists under different name in schema.
        # Field also can have small schema changes in the same time
        # So we try to get similarity percentage and if it more than
        # threshold then we're consider such change as rename/alter.
        # Otherwise it is drop/create
        match = document_type in left_schema and document_type not in right_schema
        if not match:
            return

        left_document_schema = left_schema[document_type]
        candidates = []
        matches = 0
        compares = 0
        for right_document_type, right_document_schema in right_schema.items():
            # Skip collections which was not renamed
            if right_document_type in left_schema:
                continue

            # Exact match, collection was just renamed
            if left_document_schema == right_document_schema:
                candidates = [(right_document_type, right_document_schema)]
                break

            # Try to find collection by its schema similarity
            # Compares are counted as every field schema comparing
            common_fields = left_document_schema.keys() | right_document_schema.keys()
            for field_name in common_fields:
                left_field_schema = left_document_schema.get(field_name, {})
                right_field_schema = right_document_schema.get(field_name, {})
                common_keys = left_field_schema.keys() & right_field_schema.keys()
                compares += len(common_keys)
                matches += sum(
                    left_field_schema[k] == right_field_schema[k]
                    for k in common_keys
                )

            if compares > 0 and (matches / compares * 100) >= cls.similarity_threshold:
                candidates.append((right_document_type, right_document_schema))

        if len(candidates) == 1:
            return cls(document_type=document_type, new_name=candidates[0][0])

    def to_schema_patch(self, left_schema: Schema):
        item = left_schema[self.document_type]
        return [
            ('remove', '', [(self.document_type, item)]),
            ('add', '', [(self.new_name, item)])
        ]
