import weakref
from abc import ABCMeta, abstractmethod
from typing import Dict, Type, Optional, List, Tuple, Iterable, Any

from pymongo.database import Database
from pymongo.collection import Collection

import mongoengine_migrate.flags as runtime_flags
from mongoengine_migrate.fields.registry import type_key_registry
from mongoengine_migrate.query_tracer import CollectionQueryTracer, HistoryCall
from mongoengine_migrate.exceptions import MigrationError, ActionError
from mongoengine_migrate.mongo import mongo_version

#: Migration Actions registry. Mapping of class name and its class
actions_registry: Dict[str, Type['BaseAction']] = {}


_sentinel = object()


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

    def __init__(self, collection_name: str, dummy_action: bool = False, **kwargs):
        """
        :param collection_name: Name of collection where the migration
         will be performed on
        :param dummy_action: If True then the action will not
         perform any queries on db during migration, but still used
         for changing the db schema
        :param kwargs: Action keyword parameters
        """
        self.collection_name = collection_name
        self.orig_collection_name = collection_name
        self.dummy_action = dummy_action
        self.parameters = kwargs
        self.is_embedded = False
        self._run_ctx = None  # Run context, filled by `prepare()`

        _prefix = runtime_flags.EMBEDDED_DOCUMENT_NAME_PREFIX
        if collection_name.startswith(_prefix):
            self.collection_name = collection_name[len(_prefix):]
            self.is_embedded = True

    def prepare(self, db: Database, left_schema: dict):
        """
        Prepare action before Action run (both forward and backward)
        :param db: pymongo.Database object
        :param left_schema: db schema before migration (left side)
        :return:
        """
        collection = db[self.collection_name]
        if runtime_flags.dry_run:
            collection = CollectionQueryTracer(collection)

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
    def to_schema_patch(self, left_schema: dict):
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

    def _find_embedded_fields(self,
                              collection: Collection,
                              document_type: str,
                              db_schema: dict,
                              _base_path: Optional[list] = None,
                              _document_name: Optional[str] = None) -> Iterable[list]:
        """
        Perform recursive search for embedded document fields of given
        type in given collection and return key paths to them. Paths
        for fields which are contained objects and arrays in database
        are returned separately: ['a', 'b', 'c'] and
        ['a', 'b', '$[]', 'c', '$[]'] (b and c are arrays) appropriately

        Each key path is returned if it actually exists in db. This
        check is needed to break recursion since embedded documents
        may refer to each other or even themselves.
        :param collection: collection object where to search given
         embedded document
        :param document_type: embedded document name to search
        :param db_schema: db schema
        :return:
        """
        if _base_path is None:
            _base_path = []

        # Restrict recursion depth
        max_path_len = 64
        if len(_base_path) >= max_path_len:
            return

        # Begin the search from a passed collection
        if _document_name is None:
            _document_name = collection.name

        # Return every field nested path if it has a needed type_key.
        # Next also overlook in depth to each embedded document field
        # (including fields with another type_keys) if they have nested
        # embedded documents which we also should to check. Recursion
        # stops when we found that nested field is not exists in db.
        # Keep in mind that embedded documents could refer to each
        # other or even to itself.
        #
        # Fields may contain embedded docs and/or array of embedded docs
        # Because of limitations of MongoDB we're checking type
        # (object/array) and update a field further separately.
        for field, field_schema in db_schema.get(_document_name, {}).items():
            path = _base_path + [field]
            filter_path = [p for p in path if p != '$[]']

            # Check if field is EmbeddedField or EmbeddedFieldList
            doc_name = field_schema.get('document_type')
            if doc_name and doc_name.startswith(runtime_flags.EMBEDDED_DOCUMENT_NAME_PREFIX):
                # Check if field type is object or array.
                # Dotpath field resolving always takes the first
                # element type if it is an array
                # So do the {"field.path.0": {$exists: true}} in
                # order to ensure that field contains array (non-empty)
                array_dotpath = '.'.join(filter_path + ['.0'])

                is_object = collection.find(
                    {
                        array_dotpath: {'$exists': False},
                        '.'.join(filter_path): {'$type': "object"}
                    },
                    limit=1
                )
                if is_object.retrieved > 0:
                    if doc_name == document_type:
                        yield path
                    yield from self._find_embedded_fields(collection,
                                                          document_type,
                                                          db_schema,
                                                          path,
                                                          doc_name)
                    # Return if field contains objects
                    # It's better to have ability to handle situation
                    # when the same field has both array and object
                    # values at the same time.
                    # But this function tests field existence using
                    # dotpath. Dotpath resolving makes no distinction
                    # between array and object, so it can be generated
                    # extra paths.
                    # For example, a field could contain array which
                    # contains objects only and also could contain
                    # object which contains arrays only. Function must
                    # return two paths: object->arrays, array->objects.
                    # But because of dotpath resolving thing we'll
                    # got all 4 path combinations.
                    # For a while I leave return here. It's better to
                    # remove it and solve the problem somehow.
                    # TODO: I'll be back
                    return

                # TODO: return also empty array fields
                is_nonempty_array = collection.find(
                    {array_dotpath: {'$exists': True}},
                    limit=1
                )
                if is_nonempty_array.retrieved > 0:
                    if doc_name == document_type:
                        yield path + ['$[]']
                    yield from self._find_embedded_fields(collection,
                                                          document_type,
                                                          db_schema,
                                                          path + ['$[]'],
                                                          doc_name)

    # TODO: move method to Schema class
    @staticmethod
    def _filter_collection_items(db_schema: dict) -> Iterable[Tuple[str, dict]]:
        """
        Return collection names only from db schema
        :param db_schema:
        :return:
        """
        for colname, colschema in db_schema.items():
            if not colname.startswith(runtime_flags.EMBEDDED_DOCUMENT_NAME_PREFIX):
                yield colname, colschema

    @mongo_version(min_version='3.6')
    def _update_embedded_doc_field(self,
                                   document_type: str,
                                   field_name: str,
                                   db_schema: dict,
                                   set_to: Any = _sentinel,
                                   unset=False):
        """
        Recursively perform update a field in embedded documents of
        given document type in all collections
        :param document_type: embedded document name to be updated
        :param field_name: field name to be updated
        :param db_schema:
        :param set_to: set field value to this value
        :param unset: unset field
        :return:
        """
        def _new_collection(x): return self._run_ctx['db'][x]
        if runtime_flags.dry_run:
            def _new_collection(x): return CollectionQueryTracer(self._run_ctx['db'][x])

        for collection_name, collection_schema in self._filter_collection_items(db_schema):
            collection = _new_collection(collection_name)
            for path in self._find_embedded_fields(collection, document_type, db_schema):
                update_path = path + [field_name]
                filter_path = [p for p in path if p != '$[]']

                # Inject array filters for each array field path
                array_filters = {}
                for num, item in enumerate(update_path):
                    if item == '$[]':
                        update_path[num] = f'$[elem{num}]'
                        array_filters[f'elem{num}.{update_path[num + 1]}'] = {"$exists": True}

                array_filters = [{k: v} for k, v in array_filters] or None

                update_dotpath = '.'.join(update_path)
                if set_to is not _sentinel:
                    # Check if we deal with object where we are
                    # supposed to set a field (both field value and
                    # array item)
                    filter_expr = {'.'.join(filter_path[:-1]): {"$type": "object"}}
                    update_expr = {"$set": {update_dotpath: set_to}},
                elif unset:
                    filter_expr = {'.'.join(filter_path): {"$exists": True}}
                    update_expr = {"$unset": {update_dotpath: ''}}
                else:
                    raise ValueError("No update command was specified in function parameters")

                collection.update_many(
                    filter_expr,
                    update_expr,
                    array_filters=array_filters
                )


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
        self.left_field_schema = None

        db_field = kwargs.get('db_field')
        if db_field and '.' in db_field:
            raise ActionError("'db_field' parameter could not contain dots")

    def get_field_handler_cls(self, type_key: str):
        """Concrete FieldHandler class for a given type key"""
        if type_key not in type_key_registry:
            raise MigrationError(f'Could not find field {type_key!r} or one of its base classes '
                                 f'in type_key registry')

        return type_key_registry[type_key].field_handler_cls

    @classmethod
    @abstractmethod
    def build_object(cls,
                     collection_name: str,
                     field_name: str,
                     left_schema: dict,
                     right_schema: dict) -> Optional['BaseFieldAction']:
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
        :param left_schema: database schema before a migration
         would get applied (left side)
        :param right_schema: database schema after a migration
         would get applied (right side)
        :return: object of self type or None
        """
        pass

    def prepare(self, db: Database, left_schema: dict):
        super().prepare(db, left_schema)
        self.left_field_schema = left_schema[self.orig_collection_name].get(self.field_name, {})

    def to_python_expr(self) -> str:
        parameters = {
            name: getattr(val, 'to_python_expr', lambda: repr(val))()
            for name, val in self.parameters.items()
        }
        if self.dummy_action:
            parameters['dummy_action'] = True

        kwargs_str = ''.join(f", {name}={val}" for name, val in sorted(parameters.items()))
        return f'{self.__class__.__name__}({self.orig_collection_name!r}, {self.field_name!r}' \
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
        handler = handler_cls(self._run_ctx['collection'], left_field_schema, right_field_schema)
        return handler


class BaseDocumentAction(BaseAction):
    """
    Base class for actions which change a document (collection or
    embedded document) at whole such as renaming, creating, dropping, etc.
    """

    #: Empty document schema contents skeleton
    DOCUMENT_SCHEMA_SKEL = {}

    @classmethod
    @abstractmethod
    def build_object(cls,
                     collection_name: str,
                     left_schema: dict,
                     right_schema: dict) -> Optional['BaseDocumentAction']:
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
        return f'{self.__class__.__name__}({self.orig_collection_name!r}{kwargs_str})'


class BaseCreateDocument(BaseDocumentAction):
    @classmethod
    def build_object(cls, collection_name: str, left_schema: dict, right_schema: dict):
        if collection_name not in left_schema and collection_name in right_schema:
            return cls(collection_name=collection_name)

    def to_schema_patch(self, left_schema: dict):
        return [('add', '', [(self.orig_collection_name, self.DOCUMENT_SCHEMA_SKEL)])]


class BaseDropDocument(BaseDocumentAction):
    @classmethod
    def build_object(cls, collection_name: str, left_schema: dict, right_schema: dict):
        if collection_name in left_schema and collection_name not in right_schema:
            return cls(collection_name=collection_name)  # FIXME: parameters (indexes, acl, etc.)

    def to_schema_patch(self, left_schema: dict):
        item = left_schema[self.orig_collection_name]
        return [('remove', '', [(self.orig_collection_name, item)])]


class BaseRenameDocument(BaseDocumentAction):
    priority = 3

    #: How much percent of items in schema diff of two collections
    #: should be equal to consider such change as collection rename
    #: instead of drop/create
    similarity_threshold = 70

    def __init__(self, collection_name: str, new_name, **kwargs):
        super().__init__(collection_name, new_name=new_name, **kwargs)
        self.new_name = new_name

    @classmethod
    def build_object(cls, collection_name: str, left_schema: dict, right_schema: dict):
        # Check if field exists under different name in schema.
        # Field also can have small schema changes in the same time
        # So we try to get similarity percentage and if it more than
        # threshold then we're consider such change as rename/alter.
        # Otherwise it is drop/create
        match = collection_name in left_schema and collection_name not in right_schema
        if not match:
            return

        left_collection_schema = left_schema[collection_name]
        candidates = []
        matches = 0
        compares = 0
        for right_collection_name, right_collection_schema in right_schema.items():
            # Skip collections which was not renamed
            if right_collection_name in left_schema:
                continue

            # Exact match, collection was just renamed
            if left_collection_schema == right_collection_schema:
                candidates = [(right_collection_name, right_collection_schema)]
                break

            # Try to find collection by its schema similarity
            # Compares are counted as every field schema comparing
            common_fields = left_collection_schema.keys() | right_collection_schema.keys()
            for field_name in common_fields:
                left_field_schema = left_collection_schema.get(field_name, {})
                right_field_schema = right_collection_schema.get(field_name, {})
                common_keys = left_field_schema.keys() & right_field_schema.keys()
                compares += len(common_keys)
                matches += sum(
                    left_field_schema[k] == right_field_schema[k]
                    for k in common_keys
                )

            if compares > 0 and (matches / compares * 100) >= cls.similarity_threshold:
                candidates.append((right_collection_name, right_collection_schema))

        if len(candidates) == 1:
            return cls(collection_name=collection_name, new_name=candidates[0][0])

    def to_schema_patch(self, left_schema: dict):
        item = left_schema[self.orig_collection_name]
        return [
            ('remove', '', [(self.orig_collection_name, item)]),
            ('add', '', [(self.new_name, item)])
        ]
