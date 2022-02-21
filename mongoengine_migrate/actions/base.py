__all__ = [
    'actions_registry',
    'BaseActionMeta',
    'BaseAction',
    'BaseFieldAction',
    'BaseDocumentAction',
    'BaseCreateDocument',
    'BaseDropDocument',
    'BaseRenameDocument',
    'BaseAlterDocument',
    'BaseIndexAction'
]

import logging
import weakref
from abc import ABCMeta, abstractmethod
from copy import deepcopy
from typing import Dict, Type, Optional, Mapping, Any, Iterable, Tuple

from bson import SON
from pymongo.database import Database, Collection
import pymongo.errors

import mongoengine_migrate.flags as flags
from mongoengine_migrate.exceptions import ActionError, SchemaError, MigrationError
from mongoengine_migrate.fields.registry import type_key_registry
from mongoengine_migrate.graph import MigrationPolicy
from mongoengine_migrate.schema import Schema
from mongoengine_migrate.updater import DocumentUpdater
from mongoengine_migrate.utils import Diff, UNSET, document_type_to_class_name

#: Migration Actions registry. Mapping of class name and its class
actions_registry: Dict[str, Type['BaseAction']] = {}


log = logging.getLogger('mongoengine-migrate')


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
    #: before create/drop actions. Default is below which means normal
    #: priority
    priority = 100

    def __init__(self, document_type: str, *, dummy_action: bool = False, **kwargs):
        """
        :param document_type: Document type in schema which will
         Action will use to make changes
        :param dummy_action: If True then the action will not
         perform any queries on db during migration, but still used
         for changing the db schema
        :param kwargs: Action keyword parameters
        """
        self.document_type = document_type
        self.dummy_action = dummy_action
        self.parameters = kwargs
        self._run_ctx = None  # Run context, filled by `prepare()`

    def prepare(self, db: Database, left_schema: Schema, migration_policy: MigrationPolicy):
        """
        Prepare action before Action run (both forward and backward)
        :param db: pymongo.Database object
        :param left_schema: db schema before migration (left side)
        :param migration_policy:
        :return:
        """
        self._prepare(db, left_schema, migration_policy, True)

    def _prepare(self,
                 db: Database,
                 left_schema: Schema,
                 migration_policy: MigrationPolicy,
                 ensure_existence: bool):
        if ensure_existence and self.document_type not in left_schema:
            raise SchemaError(f'Document {self.document_type} does not exist in schema')
        elif not ensure_existence and self.document_type in left_schema:
            raise SchemaError(f'Document {self.document_type} already exists in schema')

        collection_name = self.parameters.get('collection')
        if not collection_name:
            docschema = left_schema.get(self.document_type)
            if docschema:
                collection_name = docschema.parameters.get('collection')

        collection = db[collection_name] if collection_name else db['COLLECTION_PLACEHOLDER']

        self._run_ctx = {
            'left_schema': left_schema,
            'db': db,
            'collection': collection,
            'migration_policy': migration_policy
        }

    def cleanup(self):
        """Cleanup after Action run (both forward and backward)"""

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

    def __repr__(self):
        params_str = ', '.join(f'{k!s}={v!r}' for k, v in sorted(self.parameters.items()))
        args_str = repr(self.document_type)
        if self.dummy_action:
            params_str += f', dummy_action={self.dummy_action}'
        return f'{self.__class__.__name__}({args_str}, {params_str})'

    def __str__(self):
        args_str = repr(self.document_type)
        if self.dummy_action:
            args_str += f', dummy_action={self.dummy_action}'
        return f'{self.__class__.__name__}({args_str}, ...)'


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
            raise ActionError(f"db_field must not contain dots "
                              f"{self.document_type}.{self.field_name}")

    def get_field_handler_cls(self, type_key: str):
        """Concrete FieldHandler class for a given type key"""
        if type_key not in type_key_registry:
            raise SchemaError(f'Unknown type_key in {self.document_type}.{self.field_name}: '
                              f'{type_key}')

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

        For example, on field deletion the method defined in
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

    def prepare(self, db: Database, left_schema: Schema, migration_policy: MigrationPolicy):
        self._prepare(db, left_schema, migration_policy, True)

        self._run_ctx['left_field_schema'] = \
            left_schema[self.document_type].get(self.field_name, {})

    def _prepare(self,
                 db: Database,
                 left_schema: Schema,
                 migration_policy: MigrationPolicy,
                 ensure_existence: bool):
        super()._prepare(db, left_schema, migration_policy, True)

        if ensure_existence and self.field_name not in left_schema[self.document_type]:
            raise SchemaError(f'Field {self.document_type}.{self.field_name} '
                              f'does not exist in schema')
        elif not ensure_existence and self.field_name in left_schema[self.document_type]:
            raise SchemaError(f'Field {self.document_type}.{self.field_name} '
                              f'already exists in schema')

    def to_python_expr(self) -> str:
        # `to_python_expr` must return repr() string
        parameters = {
            name: getattr(val, 'to_python_expr', lambda: repr(val))()
            for name, val in self.parameters.items()
        }
        if self.dummy_action:
            parameters['dummy_action'] = True

        kwargs_str = ''.join(f", {name!s}={val!s}" for name, val in sorted(parameters.items()))
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
                              right_field_schema,
                              self._run_ctx['migration_policy'])
        return handler

    def __repr__(self):
        params_str = ', '.join(f'{k!s}={v!r}' for k, v in self.parameters.items())
        args_str = f'{self.document_type!r}, {self.field_name!r}'
        if self.dummy_action:
            params_str += f', dummy_action={self.dummy_action}'
        return f'{self.__class__.__name__}({args_str}, {params_str})'

    def __str__(self):
        args_str = f'{self.document_type!r}, {self.field_name!r}'
        if self.dummy_action:
            args_str += f', dummy_action={self.dummy_action}'
        return f'{self.__class__.__name__}({args_str}, ...)'


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

        For example, on collection deletion the method defined in
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

        kwargs_str = ''.join(f", {name!s}={val!s}" for name, val in sorted(parameters.items()))
        return f'{self.__class__.__name__}({self.document_type!r}{kwargs_str})'

    def _is_my_collection_used_by_other_documents(self) -> bool:
        """Return True if some of documents uses the same collection"""
        docschema = self._run_ctx['left_schema'].get(self.document_type)
        if docschema:
            collection_name = docschema.parameters.get('collection')
        else:
            collection_name = self.parameters.get('collection')

        return collection_name and any(
            v.parameters.get('collection') == collection_name
            for k, v in self._run_ctx['left_schema'].items()
            if k != self.document_type and not k.startswith(flags.EMBEDDED_DOCUMENT_NAME_PREFIX)
        )


class BaseCreateDocument(BaseDocumentAction):
    @classmethod
    def build_object(cls, document_type: str, left_schema: Schema, right_schema: Schema):
        if document_type not in left_schema and document_type in right_schema:
            return cls(document_type=document_type, **right_schema[document_type].parameters)

    def to_schema_patch(self, left_schema: Schema):
        item = Schema.Document()
        item.parameters.update(self.parameters)
        return [('add', '', [(self.document_type, item)])]

    def prepare(self, db: Database, left_schema: Schema, migration_policy: MigrationPolicy):
        self._prepare(db, left_schema, migration_policy, False)


class BaseDropDocument(BaseDocumentAction):
    @classmethod
    def build_object(cls, document_type: str, left_schema: Schema, right_schema: Schema):
        if document_type in left_schema and document_type not in right_schema:
            return cls(document_type=document_type)

    def to_schema_patch(self, left_schema: Schema):
        item = left_schema[self.document_type]
        return [('remove', '', [(self.document_type, item)])]


class BaseRenameDocument(BaseDocumentAction):
    #: How much percent of items in schema diff of two collections
    #: should be equal to consider such change as collection rename
    #: instead of drop/create
    similarity_threshold = 70

    def __init__(self, document_type: str, *, new_name, **kwargs):
        super().__init__(document_type, **kwargs)
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

        is_left_embedded = document_type.startswith(flags.EMBEDDED_DOCUMENT_NAME_PREFIX)
        left_document_schema = left_schema[document_type]
        candidates = []
        for right_document_type, right_document_schema in right_schema.items():
            matches = 0
            compares = 0

            # Skip collections which apparently was not renamed
            if right_document_type in left_schema:
                continue

            # Prevent adding to 'candidates' a right document, which
            # could have same/similar schema but has another type
            # (embedded and usual and vice versa)
            is_right_embedded = right_document_type.startswith(flags.EMBEDDED_DOCUMENT_NAME_PREFIX)
            if is_left_embedded != is_right_embedded:
                continue

            # Exact match, collection was just renamed. We found it
            if left_document_schema == right_document_schema:
                candidates = [(right_document_type, right_document_schema)]
                break

            # Count of equal fields and parameters items and then
            # divide it on whole compared fields/parameters count
            items = ((left_document_schema, right_document_schema),
                     (left_document_schema.parameters, right_document_schema.parameters))
            for left, right in items:
                all_keys = left.keys() | right.keys()
                compares += len(all_keys)
                # FIXME: keys can be functions (default for instance)
                #        they will not be equal then dispite they hasn't change
                matches += sum(left.get(k) == right.get(k) for k in all_keys)

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


class BaseAlterDocument(BaseDocumentAction):
    @classmethod
    def build_object(cls, document_type: str, left_schema: Schema, right_schema: Schema):
        match = document_type in left_schema \
                and document_type in right_schema \
                and left_schema[document_type].parameters != right_schema[document_type].parameters
        if match:
            return cls(document_type=document_type, **right_schema[document_type].parameters)

    def to_schema_patch(self, left_schema: Schema):
        # Constructor args are written to parameters directly, not as
        # diff. Unlike the AlterField there are no schema skel here,
        # so we can't delete a parameter implicitly
        left_item = left_schema[self.document_type]
        right_item = deepcopy(left_item)
        right_item.parameters.clear()
        right_item.parameters.update(self.parameters)

        return [('change', self.document_type, (left_item, right_item))]

    def run_forward(self):
        self._run_migration(self._run_ctx['left_schema'][self.document_type],
                            self.parameters,
                            swap=False)

    def run_backward(self):
        self._run_migration(self._run_ctx['left_schema'][self.document_type],
                            self.parameters,
                            swap=True)

    def _run_migration(self,
                       self_schema: Schema.Document,
                       parameters: Mapping[str, Any],
                       swap: bool = False):
        # Try to process all parameters on same order to avoid
        # potential problems on repeated launches if some query on
        # previous launch was failed
        for name in sorted(parameters.keys() | self_schema.parameters.keys()):
            left_value = self_schema.parameters.get(name, UNSET)
            right_value = parameters.get(name, UNSET)
            if left_value == right_value:
                continue

            diff = Diff(
                old=right_value if swap else left_value,
                new=left_value if swap else right_value,
                key=name
            )

            log.debug(">> Change %s: %s => %s", repr(name), repr(diff.old), repr(diff.new))
            try:
                method = getattr(self, f'change_{name}')
            except AttributeError as e:
                raise SchemaError(f'Unknown document parameter: {name}') from e

            inherit = self._run_ctx['left_schema'][self.document_type].parameters.get('inherit')
            document_cls = document_type_to_class_name(self.document_type) if inherit else None
            updater = DocumentUpdater(self._run_ctx['db'], self.document_type,
                                      self._run_ctx['left_schema'], '',
                                      self._run_ctx['migration_policy'], document_cls)

            method(updater, diff)

    @staticmethod
    def _check_diff(diff: Diff, can_be_none=True, check_type=None):
        if diff.new == diff.old:
            raise SchemaError(f'Parameter {diff.key} does not changed from previous Action')

        if check_type is not None:
            if diff.old not in (UNSET, None) and not isinstance(diff.old, check_type) \
                    or diff.new not in (UNSET, None) and not isinstance(diff.new, check_type):
                raise SchemaError(f'{diff.key} must have type {check_type!r}')

        if not can_be_none:
            if diff.old is None or diff.new is None:
                raise SchemaError(f'{diff.key} could not be None')


class BaseIndexAction(BaseAction):
    def __init__(self, document_type: str, index_name: str, **kwargs):
        super().__init__(document_type, **kwargs)

        self.index_name = index_name

    @classmethod
    @abstractmethod
    def build_object(cls,
                     document_type: str,
                     index_name: str,
                     left_schema: Schema,
                     right_schema: Schema) -> Optional['BaseIndexAction']:
        """
        Factory method which tests if current action type could process
        schema changes for a given collection and fields. If yes then
        it produces object of current action type with filled out
        perameters. If no then it returns None.

        This method is used to guess which action is suitable to
        reflect schema change. It's called for several times for each
        field which was modified by a user in mongoengine documents.

        For example, on index of particular fields deletion, the
        method defined in CreateIndex action should return None, but
        those one in DropIndex action should return DropIndex object
        with filled out parameters of change (document type, indexed
        fields)

        :param document_type: document type in schema to consider
        :param index_name: index name
        :param left_schema: database schema before a migration
         would get applied (left side)
        :param right_schema: database schema after a migration
         would get applied (right side)
        :return: object of self type or None
        """

    def prepare(self, db: Database, left_schema: Schema, migration_policy: MigrationPolicy):
        self._prepare(db, left_schema, migration_policy, True)

        self._run_ctx['left_index_schema'] = \
            left_schema[self.document_type].indexes.get(self.index_name, {})

    def _prepare(self,
                 db: Database,
                 left_schema: Schema,
                 migration_policy: MigrationPolicy,
                 ensure_existence: bool):
        super()._prepare(db, left_schema, migration_policy, True)

        if ensure_existence and self.index_name not in left_schema[self.document_type].indexes:
            raise SchemaError(f'Index {self.index_name} does not exist in schema of {self.document_type}')
        elif not ensure_existence and self.index_name in left_schema[self.document_type].indexes:
            raise SchemaError(f'Index {self.index_name} already exists in schema of {self.document_type}')

    def to_python_expr(self) -> str:
        # `to_python_expr` must return repr() string
        parameters = {
            name: getattr(val, 'to_python_expr', lambda: repr(val))()
            for name, val in self.parameters.items()
        }
        if self.dummy_action:
            parameters['dummy_action'] = True

        fields_str = ''
        if 'fields' in self.parameters:  # DropIndex has no 'fields'
            class ReprStr(str):
                """str type with repr() without single quotes"""
                def __repr__(self): return self.__str__()

            del parameters['fields']
            index_types = (
                'ASCENDING', 'DESCENDING', 'GEO2D', 'GEOSPHERE', 'HASHED', 'TEXT'
            )
            if int(pymongo.__version__.split(".")[0]) < 4:
                index_types += ('GEOHAYSTACK',)
            index_type_map = {getattr(pymongo, name): ReprStr(f'pymongo.{name}')
                              for name in index_types}
            fields = [(field, index_type_map.get(typ, typ))
                      for field, typ in self.parameters.get('fields', ())]
            fields_str = f', fields={str(fields)}'

        kwargs_str = ''.join(f", {name!s}={val!s}" for name, val in sorted(parameters.items()))
        return f'{self.__class__.__name__}({self.document_type!r}, {self.index_name!r}' \
               f'{fields_str}{kwargs_str})'

    @staticmethod
    def _find_index(collection: Collection,
                    name: str,
                    fields_spec: Iterable[Iterable[Any]]) -> Optional[Tuple[str, dict]]:
        """
        Find index in db which has either given name or fields spec
        (key).
        :param collection: pymongo.Collection object
        :param name: name to search
        :param fields_spec: fields spec to search
        :return: index name and description tuple or None if not found
        """
        for iname, ispec in collection.index_information().items():
            if iname == name or ispec['key'] == fields_spec:
                return iname, ispec

    def _drop_index(self, parameters: dict) -> None:
        """
        Drop current index by name.

        If name was not specified in parameters (the most often
        scenario), we search for an index with given fields, obtain
        its name and drop by name

        :param parameters: index parameters
        :return:
        """
        left_index_schema = deepcopy(parameters)
        fields = left_index_schema.pop('fields')  # Key must be present

        # Drop all indexes by name since some of index types
        # (text ones, for instance) are require to be dropped by name
        name = left_index_schema.get('name')
        found_index = self._find_index(self._run_ctx['collection'], name, fields)
        if found_index is None:
            log.warning("Index %s was already dropped, ignoring", fields)
            return

        iname, idesc = found_index
        if not self._is_my_index_used_by_other_documents():
            self._run_ctx['collection'].drop_index(iname)

    def _create_index(self, parameters: dict) -> None:
        """
        Create index with given parameters
        :param parameters:
        :return:
        """
        left_index_schema = deepcopy(parameters)
        fields = left_index_schema.pop('fields')  # Key must be present
        name = left_index_schema.get('name')

        # Check if index with such name\parameters exists in db
        found_index = self._find_index(self._run_ctx['collection'], name, fields)
        if found_index:
            iname, ispec = found_index
            # Exclude index desc keys which does not get to index schema
            compare_keys = (ispec.keys() | left_index_schema.keys()) - {'v', 'ns', 'key'}
            if all(ispec.get(k) == left_index_schema.get(k) for k in compare_keys):
                log.warning('Index %s already exists, ignore', fields)
                return
            else:
                raise MigrationError(
                    'Index {} already exists with other parameters. Please drop it before '
                    'applying the migration'.format(fields)
                )

        try:
            self._run_ctx['collection'].create_index(fields, **left_index_schema)
        except pymongo.errors.OperationFailure as e:
            index_id = left_index_schema.get('name', fields)
            raise MigrationError('Could not create index {}'.format(index_id)) from e

    def _is_my_index_used_by_other_documents(self) -> bool:
        """
        Return True if current index is declared in another document
        for the same collection
        :return:
        """
        my_document = self._run_ctx['left_schema'].get(self.document_type)  # type: Schema.Document
        my_collection = my_document.parameters.get('collection')
        if my_collection is None:
            raise SchemaError(
                f'No collection name in {self.document_type} schema parameters. Schema is corrupted'
            )

        for name, schema in self._run_ctx['left_schema'].items():
            if name == self.document_type or name.startswith(flags.EMBEDDED_DOCUMENT_NAME_PREFIX):
                continue

            col = schema.parameters.get('collection')
            if col == my_collection and self.index_name in schema.indexes:
                return True

        return False

    def __repr__(self):
        args_str = f'{self.document_type!r}, {self.index_name!r}'

        fields_str = ''
        if 'fields' in self.parameters:
            fields_str = f'fields={self.parameters["fields"]!r}'
        params_str = ', '.join(f'{k!s}={v!r}' for k, v in self.parameters.items() if k != 'fields')
        if self.dummy_action:
            params_str += f', dummy_action={self.dummy_action}'

        return f'{self.__class__.__name__}({args_str}, {fields_str}, {params_str})'

    def __str__(self):
        args_str = f'{self.document_type!r}, {self.index_name!r}'

        fields_str = ''
        if 'fields' in self.parameters:
            fields_str = f'fields={self.parameters["fields"]!r}'
        if self.dummy_action:
            args_str += f', dummy_action={self.dummy_action}'

        return f'{self.__class__.__name__}({args_str}, {fields_str} ...)'
