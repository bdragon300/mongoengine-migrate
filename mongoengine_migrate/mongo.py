__all__ = [
    'check_empty_result',
    'mongo_version',
    'ByPathContext',
    'ByDocContext',
    'DocumentUpdater'
]

import logging
import functools
from typing import Optional, Callable, Tuple, Generator, NamedTuple, List

import jsonpath_rw
from pymongo import ReplaceOne
from pymongo.collection import Collection
from pymongo.database import Database

from mongoengine_migrate.exceptions import MigrationError, InconsistencyError
from mongoengine_migrate.schema import Schema
from . import flags


log = logging.getLogger('mongoengine-migrate')


def check_empty_result(collection: Collection, db_field: str, find_filter: dict) -> None:
    """
    Find records in collection satisfied to a given filter expression
    and raise error if anything found
    :param collection: pymongo collection object to find in
    :param db_field: collection field name
    :param find_filter: collection.find() method filter argument
    :raises MigrationError: if any records found
    """
    bad_records = collection.find(find_filter, limit=3)
    if bad_records.retrieved:
        examples = (
            f'{{_id: {x.get("_id", "unknown")},...{db_field}: {x.get(db_field, "unknown")}}}'
            for x in bad_records
        )
        raise InconsistencyError(f"Field {collection.name}.{db_field} in some records "
                                 f"has wrong values. First several examples: "
                                 f"{','.join(examples)}")


def mongo_version(min_version: str = None, max_version: str = None, throw_error: bool = False):
    """
    Decorator restrict decorated function execution by MongoDB version.

    If current db version is out of specified range then the function
    either won't get executed or error will be raised, depending on
    `throw_error` parameter
    :param min_version: Minimum MongoDB version (including)
    :param max_version: Maximum MongoDB version (excluding)
    :param throw_error: If False then function call will just silently
     skipped on version mismatch. If True then `MigrationError`
     exception will be raised then.
    :return:
    """
    assert min_version or max_version
    # TODO: add warning if monge version is not in range

    def dec(f):
        @functools.wraps(f)
        def w(*args, **kwargs):
            invalid = min_version and flags.mongo_version < min_version \
                      or max_version and flags.mongo_version >= max_version

            if invalid and throw_error:
                version_msg = ', '.join([
                    (">=" + min_version if min_version else ""),
                    ("<" + max_version if max_version else "")
                ])
                raise MigrationError(
                    f'Commands are valid only for MongoDB version {version_msg}'
                )
            elif not invalid:
                return f(*args, **kwargs)

        return w
    return dec


class ByPathContext(NamedTuple):
    """Context of `by_path` callback"""
    collection: Collection
    filter_dotpath: str
    update_dotpath: str
    array_filters: Optional[List[dict]]
    extra_filter: dict


class ByDocContext(NamedTuple):
    """Context of `by_document` callback"""
    collection: Optional[Collection]
    document: dict
    filter_dotpath: str


class DocumentUpdater:
    """Document updater class. Used to update certain field in
    collection or embedded document
    """
    def __init__(self, db: Database,
                 document_type: str,
                 field_name: str,
                 db_schema: Schema,
                 document_cls: Optional[str] = None):
        """
        :param db: pymongo database object
        :param document_type: document name
        :param field_name: field to work with
        :param db_schema: current db schema
        :param document_cls: if given then we ignore those documents
         and embedded documents whose '_cls' field is not equal to this
         parameter value. Documents with no '_cls' field and fields
         with types other than object will not be ignored. This
         parameter uses for Document inheritance support
        """
        self.db = db
        self.document_type = document_type
        self.field_name = field_name
        self.db_schema = db_schema
        self.document_cls = document_cls

    @property
    def document_type(self):
        return self._document_type

    @document_type.setter
    def document_type(self, val):
        self._document_type = val
        self.is_embedded = self.document_type.startswith(flags.EMBEDDED_DOCUMENT_NAME_PREFIX)

    def update_by_path(self, callback: Callable) -> None:
        """
        Call the given callback for every path to a field contained
        document with needed type.

        For a field in collection it will be called once.

        The same embedded document could be nested or be included to
        many collections -- the callback will be called for each of
        these fields. Returned value is ignored

        Callback parameters are:
        * collection -- pymongo collection object
        * filter_expr -- string dotpath of field which is used in
          filter expressions
        * update_expr -- string dotpath of field (with $[]) which
          is used for pointing
        * array_filters - dict with array filters which could be
          passed to update_many method for instance
        :param callback:
        :return:
        """
        class_fltr = {'_cls': self.document_cls} if self.document_cls else {}
        if not self.document_type.startswith(flags.EMBEDDED_DOCUMENT_NAME_PREFIX):
            collection_name = self.db_schema[self.document_type].parameters['collection']
            ctx = ByPathContext(collection=self.db[collection_name],
                                filter_dotpath=self.field_name,
                                update_dotpath=self.field_name,
                                array_filters=None,
                                extra_filter=class_fltr)
            callback(ctx)
            return

        for collection, update_path, filter_path in self._get_update_paths():
            self._update_by_path(callback, collection, filter_path, update_path)

    def _update_by_path(self, callback, collection, filter_path, update_path) -> None:
        update_path, array_filters = self._inject_array_filters(update_path)
        filter_dotpath = '.'.join(filter_path)
        update_dotpath = '.'.join(update_path)
        class_fltr = {'_cls': self.document_cls} if self.document_cls else {}
        ctx = ByPathContext(collection=collection,
                            filter_dotpath=filter_dotpath,
                            update_dotpath=update_dotpath,
                            array_filters=array_filters,
                            extra_filter=class_fltr)
        callback(ctx)

    def update_by_document(self, callback: Callable) -> None:
        """
        Call the given callback for every document of needed
        type found in db. If field contains array of documents then
        callback will be called for each of them.

        Callback function could modify a next field value in-place.
        Returned value is ignored

        Callback parameters are:
        * collection -- pymongo Collection object
        * embedded_document -- field contents, which should contain
          embedded document
        * filter_path -- dotpath of field
        :param callback:
        :return:
        """
        class_fltr = {'_cls': self.document_cls} if self.document_cls else {}
        collection_name = self.db_schema[self.document_type].parameters.get('collection', '*')

        if not self.document_type.startswith(flags.EMBEDDED_DOCUMENT_NAME_PREFIX):
            if flags.dry_run:
                msg = '* db.%s.find(%s) -> [Loop](%s) -> db.%s.bulk_write(...)'
                log.info(msg, collection_name, class_fltr, self.field_name, collection_name)
                return

            collection_name = self.db_schema[self.document_type].parameters['collection']
            collection = self.db[collection_name]
            for doc in collection.find(class_fltr):
                ctx = ByDocContext(collection=collection,
                                   document=doc,
                                   filter_dotpath=self.field_name)
                callback(ctx)
                # FIXME: where update_many?
            return

        for collection, update_path, filter_path in self._get_update_paths():
            self._update_by_document(callback, collection, filter_path, update_path)

    def _update_by_document(self, callback, collection, filter_path, update_path) -> None:
        json_path = '.'.join(f.replace('$[]', '[*]') for f in update_path)
        json_path = json_path.replace('.[*]', '[*]')
        parser = jsonpath_rw.parse(json_path)
        find_fltr = {'.'.join(filter_path): {'$exists': True}}

        if flags.dry_run:
            msg = '* db.%s.find(%s) -> [Loop](%s) -> db.%s.bulk_write(...)'
            log.info(msg, collection.name, find_fltr, filter_path, collection.name)
            return

        buf = []
        for doc in collection.find(find_fltr):
            # Recursively apply the callback to every embedded doc
            for embedded_doc in parser.find(doc):
                if self.document_cls:
                    if (isinstance(embedded_doc, dict)
                            and embedded_doc.get('_cls', self.document_cls) != self.document_cls):
                        continue
                ctx = ByDocContext(collection=collection,
                                   document=embedded_doc,
                                   filter_dotpath=filter_path)
                callback(ctx)
            buf.append(ReplaceOne({'_id': doc['_id']}, doc, upsert=False))

            # Flush buffer
            if len(buf) >= flags.BULK_BUFFER_LENGTH:
                collection.bulk_write(buf, ordered=False)  # FIXME: separate db session?
                buf.clear()
        if buf:
            collection.bulk_write(buf, ordered=False)
            buf.clear()

    def update_combined(self,
                        document_by_path_cb: Callable,
                        embedded_array_by_document_cb: Callable,
                        embedded_noarray_by_path_cb: Optional[Callable] = None,
                        embedded_noarray_by_document_cb: Optional[Callable] = None) -> None:
        """
        Perform an update if an concrete query/pipeline is unable to
        be executed over embedded field dotpath. For example,
        `$rename` does not accept dotpath if it contains "$[]",
        but accepts dotpath without "$[]". Some queries
        (`$set` for example) accept both variants.

        In order to manage with this restrictions we could update
        documents by hand in python if the query is limited to do this.

        In the same time mongo queries work with document field without
        restrictions.

        By default we accept 2 callbacks for document update (by_path)
        and for embedded document update (by_document). They will be
        used both for array and not-array dotpaths.

        Two optional callbacks are used for array dotpaths only in
        embedded documents (if set)

        :param document_by_path_cb: "by_path" callback which is always
         called for non-embedded document
        :param embedded_array_by_document_cb: "by_document" callback
         which is called always for non-array dotpaths.
        :param embedded_noarray_by_path_cb: "by_path" callback which is
         called for non-array dotpath to embedded document field
        :param embedded_noarray_by_document_cb: "by_document" callback
         which is called for non-array dotpath to embedded doc field
        """
        assert embedded_noarray_by_path_cb or embedded_noarray_by_document_cb, \
            'You must give one of non-array dotpath callbacks'
        assert not(embedded_noarray_by_document_cb and embedded_noarray_by_path_cb), \
            'You must give only one non-array dotpath callback, not both'

        if self.is_embedded:
            for collection, update_path, filter_path in self._get_update_paths():
                is_array_update = bool('$[]' in update_path)

                if not is_array_update and embedded_noarray_by_path_cb:
                    return self._update_by_path(embedded_noarray_by_path_cb,
                                                collection,
                                                filter_path,
                                                update_path)
                elif not is_array_update and embedded_noarray_by_document_cb:
                    return self._update_by_document(embedded_noarray_by_document_cb,
                                                    collection,
                                                    filter_path,
                                                    update_path)
                else:
                    return self._update_by_document(embedded_array_by_document_cb,
                                                    collection,
                                                    filter_path,
                                                    update_path)
        else:
            return self.update_by_path(document_by_path_cb)

    def _get_update_paths(self) -> Generator[Tuple[Collection, list, list], None, None]:
        """
        Return dotpaths to fields of embedded documents found in db and
        collection object where they was found.
        Returned dotpaths are filter expressions (pure dotpath)
        and update expressions (dotpath with `$[]` expressions)
        :return: tuple(collection_object, update_dotpath, filter_dotpath)
        """
        # Document types of non-embedded documents
        document_types = ((name, schema) for name, schema in self.db_schema.items()
                          if not name.startswith(flags.EMBEDDED_DOCUMENT_NAME_PREFIX))

        for document_type, document_schema in document_types:
            collection = self.db[document_schema.parameters['collection']]
            for path in self._find_embedded_fields(collection,
                                                   document_type,
                                                   self.document_type,  # FIXME: could not be an embedded!
                                                   self.db_schema):
                update_path = path + [self.field_name]
                filter_path = [p for p in path if p != '$[]']

                yield collection, update_path, filter_path

    def _find_embedded_fields(self,
                              collection: Collection,
                              root_doctype: str,
                              search_doctype: str,
                              db_schema: Schema,
                              _base_path: Optional[list] = None) -> Generator[list, None, None]:
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
        :param root_doctype: document type name where to perform
         recursive search
        :param search_doctype: embedded document name to search
        :param db_schema: db schema
        :return:
        """
        if _base_path is None:
            _base_path = []

        # Restrict recursion depth
        max_path_len = 64
        if len(_base_path) >= max_path_len:
            return

        # Return every field nested path if it has a needed type_key.
        # Next also overlook in depth to each embedded document field
        # (including fields with another type_keys) if they have nested
        # embedded documents which we also should to check. Recursion
        # stops when we found that nested field which does not exist
        # in db. Keep in mind that embedded documents could refer to
        # each other or even to itself.
        #
        # Fields may contain embedded docs and/or array of embedded docs
        # Because of limitations of MongoDB we're checking type
        # (object/array) and update a field further separately.
        for field, field_schema in db_schema.get(root_doctype, {}).items():
            path = _base_path + [field]
            filter_path = [p for p in path if p != '$[]']

            # Check if field is EmbeddedField or EmbeddedFieldList
            ref = field_schema.get('target_doctype')
            if ref is None or not ref.startswith(flags.EMBEDDED_DOCUMENT_NAME_PREFIX):
                # Skip fields which don't point to embedded document
                continue

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
                if ref == search_doctype:
                    yield path
                yield from self._find_embedded_fields(collection,
                                                      ref,
                                                      search_doctype,
                                                      db_schema,
                                                      path)
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
                if ref == search_doctype:
                    yield path + ['$[]']
                yield from self._find_embedded_fields(collection,
                                                      ref,
                                                      search_doctype,
                                                      db_schema,
                                                      path + ['$[]'])

    def _inject_array_filters(self, update_path: list) -> Tuple[list, Optional[list]]:
        """
        Inject array filters for each array field path.
        Return modified update path and appropriate array filters
        :param update_path:
        :return: tuple(update_path, array_filters)
        """
        #
        array_filters = {}
        update_path = update_path.copy()
        for num, item in enumerate(update_path):
            if item == '$[]':
                update_path[num] = f'$[elem{num}]'
                array_filters[f'elem{num}.{update_path[num + 1]}'] = {"$exists": True}

        return update_path, [{k: v} for k, v in array_filters] or None
