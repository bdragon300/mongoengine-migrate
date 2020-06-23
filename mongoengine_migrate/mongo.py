import functools
from typing import Iterable, Optional, Callable, Tuple

import jsonpath_rw
from pymongo import ReplaceOne
from pymongo.collection import Collection
from pymongo.database import Database

from mongoengine_migrate.exceptions import MigrationError
from . import flags
from .query_tracer import CollectionQueryTracer


def check_empty_result(collection: Collection, db_field: str, find_filter: dict):
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
        raise MigrationError(f"Field {collection.name}.{db_field} in some records "
                             f"has wrong values. First several examples: "
                             f"{','.join(examples)}")


def mongo_version(min_version: str = None, max_version: str = None, throw_error: bool = False):
    """
    Restrict the decorated function execution by MongoDB version.

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
                raise MigrationError(f'Commands are valid only for MongoDB version {version_msg}')
            elif not invalid:
                return f(*args, **kwargs)

        return w
    return dec


class EmbeddedDocumentUpdater:
    """Class used to update certain field of embedded documents in
    all documents or other embedded documents which use it
    """
    def __init__(self, db: Database, document_type: str, field_name: str, db_schema: dict):
        """
        :param db: pymongo database object
        :param document_type: embedded document name
        :param field_name: field to work with
        :param db_schema: current db schema
        """
        self.db = db
        self.document_type = document_type
        self.field_name = field_name
        self.db_schema = db_schema

    def update_by_path(self, callback: Callable):
        """
        Call the given callback for every dotpath to a field contained
        embedded document with needed type.

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
        for collection, update_path, filter_path in self._get_update_paths():
            update_path, array_filters = self._inject_array_filters(update_path)
            filter_dotpath = '.'.join(filter_path)
            update_dotpath = '.'.join(update_path)

            callback(collection, filter_dotpath, update_dotpath, array_filters)

    def update_by_document(self, callback: Callable):
        """
        Call the given callback for every embedded document of needed
        type found in db.

        If callback was modified the passed field value then it will
        be updated in db. Returned value is ignored

        Callback parameters are:
        * collection -- pymongo Collection object
        * embedded_document -- field contents, which should contain
          embedded document
        * filter_path -- dotpath of field
        :param callback:
        :return:
        """
        for collection, update_path, filter_path in self._get_update_paths():
            json_path = '.'.join(f.replace('$[]', '[*]') for f in update_path)
            json_path = json_path.replace('.[*]', '[*]')
            parser = jsonpath_rw.parse(json_path)

            buf = []
            for doc in collection.find({'.'.join(filter_path): {'$exists': True}}):
                # Recursively apply the callback to every embedded doc
                for embedded_doc in parser.find(doc):
                    callback(collection, embedded_doc, filter_path)
                buf.append(ReplaceOne({'_id': doc['_id']}, doc, upsert=False))

                # Flush buffer
                if len(buf) >= flags.BULK_BUFFER_LENGTH:
                    collection.bulk_write(buf, ordered=False)  # FIXME: separate db session?
                    buf.clear()

            if buf:
                collection.bulk_write(buf, ordered=False)
                buf.clear()

    def _get_update_paths(self):
        """
        Return dotpaths to fields of embedded documents found in db and
        collection object where they was found.
        Returned dotpaths are filter expressions (pure dotpath)
        and update expressions (dotpath with `$[]` expressions)
        :return: tuple(collection_object, update_dotpath, filter_dotpath)
        """
        collections = ((k, v) for k, v in self.db_schema.items()
                       if not k.startswith(flags.EMBEDDED_DOCUMENT_NAME_PREFIX))

        for collection_name, collection_schema in collections:
            collection = self._get_collection(collection_name)
            for path in self._find_embedded_fields(collection, self.document_type, self.db_schema):
                update_path = path + [self.field_name]
                filter_path = [p for p in path if p != '$[]']

                yield collection, update_path, filter_path

    def _get_collection(self, name: str) -> Collection:
        if flags.dry_run:
            return CollectionQueryTracer(self.db[name])
        return self.db[name]

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
            if doc_name and doc_name.startswith(flags.EMBEDDED_DOCUMENT_NAME_PREFIX):
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
