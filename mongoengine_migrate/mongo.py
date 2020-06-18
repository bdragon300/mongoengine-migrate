import functools
from typing import Iterable, Optional, Any, Tuple

from pymongo.collection import Collection
from pymongo.database import Database

from mongoengine_migrate.exceptions import MigrationError
from . import flags
from .query_tracer import CollectionQueryTracer


_sentinel = object()


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


def mongo_version(min_version: str = None, max_version:str = None, throw_error: bool = False):
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


def find_embedded_fields(collection: Collection,
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
                yield from find_embedded_fields(collection,
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
                yield from find_embedded_fields(collection,
                                                document_type,
                                                db_schema,
                                                path + ['$[]'],
                                                doc_name)


@mongo_version(min_version='3.6')
def update_embedded_doc_field(db: Database,
                              document_type: str,
                              field_name: str,
                              db_schema: dict,
                              set_to: Any = _sentinel,
                              unset=False):
    """
    Recursively perform update a field in embedded documents of
    given document type in all collections
    :param db: database object
    :param document_type: embedded document name to be updated
    :param field_name: field name to be updated
    :param db_schema:
    :param set_to: set field value to this value
    :param unset: unset field
    :return:
    """
    def _new_collection(x): return db[x]
    if flags.dry_run:
        def _new_collection(x): return CollectionQueryTracer(db[x])

    for collection_name, collection_schema in get_collections_only(db_schema):
        collection = _new_collection(collection_name)
        for path in find_embedded_fields(collection, document_type, db_schema):
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


# TODO: move method to Schema class
def get_collections_only(db_schema: dict) -> Iterable[Tuple[str, dict]]:
    """
    Return collection names only from db schema
    :param db_schema:
    :return:
    """
    for colname, colschema in db_schema.items():
        if not colname.startswith(flags.EMBEDDED_DOCUMENT_NAME_PREFIX):
            yield colname, colschema
