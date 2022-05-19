__all__ = [
    'nothing',
    'deny',
    'drop_field',
    'item_to_list',
    'extract_from_list',
    'remove_cls_key',
    'to_manual_ref',
    'to_dbref',
    'to_dynamic_ref',
    'to_string',
    'to_int',
    'to_long',
    'to_double',
    'to_decimal',
    'to_date',
    'to_bool',
    'to_binary',
    'to_object',
    'to_object_id',
    'to_uuid_str',
    'to_uuid_bin',
    'to_url_string',
    'to_email_string',
    'to_complex_datetime',
]

import re
import uuid

import bson
from dateutil.parser import parse as dateutil_parse

from mongoengine_migrate.exceptions import MigrationError, InconsistencyError
from mongoengine_migrate.mongo import (
    check_empty_result
)
from mongoengine_migrate.updater import ByPathContext, ByDocContext, DocumentUpdater


def nothing(*args, **kwargs):
    """Converter which does nothing"""
    pass


def deny(updater: DocumentUpdater):
    """Convertion is denied"""
    raise MigrationError(f"Such type_key convertion for field "
                         f"{updater.document_type}.{updater.field_name} is forbidden")


def drop_field(updater: DocumentUpdater):
    """Drop field"""
    def by_path(ctx: ByPathContext):
        ctx.collection.update_many(
            {ctx.filter_dotpath: {'$exists': True}, **ctx.extra_filter},
            {'$unset': {ctx.update_dotpath: ''}},
            array_filters=ctx.build_array_filters()
        )

    updater.update_by_path(by_path)


def item_to_list(updater: DocumentUpdater, remove_cls_key=False):
    """Make a list with single element from every non-array value"""
    def by_doc(ctx: ByDocContext):
        if updater.field_name in ctx.document:
            f = ctx.document[updater.field_name]
            if f is not None:
                if remove_cls_key and isinstance(f, dict) and '_cls' in f:
                    del f['_cls']
                if not isinstance(f, (list, tuple)):
                    ctx.document[updater.field_name] = [f]
            else:
                ctx.document[updater.field_name] = []  # null -> []

    updater.update_by_document(by_doc)


def extract_from_list(updater: DocumentUpdater, item_type, remove_cls_key=False):
    """
    Replace every list which was met with its first element with
    checking item type. If type is other than `item_type` then
    the error will be raised
    :param updater:
    :param item_type: python type(s) to check the element
    :param remove_cls_key: if True then '_cls' keys will be removed
     from dict items if any
    :return:
    """
    def by_doc(ctx: ByDocContext):
        doc = ctx.document
        if updater.field_name not in doc or doc[updater.field_name] is None:
            return

        f = doc[updater.field_name]
        if isinstance(f, (list, tuple)):
            if f:
                f = f[0]
                if remove_cls_key and isinstance(f, dict) and '_cls' in f:
                    del f['_cls']
                if not isinstance(f, item_type) and updater.migration_policy.name == 'strict':
                    raise InconsistencyError(f"Field {updater.field_name} has wrong value {f!r} "
                                             f"(should be {item_type}) in record {doc}")
            else:
                f = None
            doc[updater.field_name] = f
        elif f is not None and updater.migration_policy.name == 'strict':
            raise MigrationError(f'Could not extract item from non-list value '
                                 f'{updater.field_name}: {doc[updater.field_name]}')

    updater.update_by_document(by_doc)


def remove_cls_key(updater: DocumentUpdater):
    """Unset '_cls' key in documents if any"""
    def by_path(ctx: ByPathContext):
        ctx.collection.update_many(
            {ctx.filter_dotpath + '._cls': {'$exists': True}, **ctx.extra_filter},
            {'$unset': {ctx.update_dotpath + '._cls': ''}},
            array_filters=ctx.build_array_filters()
        )

    updater.update_by_path(by_path)


def to_object_id(updater: DocumentUpdater):
    def by_doc(ctx: ByDocContext):
        doc = ctx.document
        if updater.field_name not in doc or doc[updater.field_name] is None:
            return

        f = doc[updater.field_name]
        is_dict = isinstance(f, dict)
        if isinstance(f, str):  # ObjectId as string
            try:
                f = bson.ObjectId(f)
            except bson.errors.BSONError:
                pass

        if isinstance(f, bson.ObjectId):
            return
        elif is_dict and isinstance(f.get('_ref'), bson.DBRef):  # Already dynamic ref
            doc[updater.field_name] = f['_ref'].id
        elif isinstance(f, bson.DBRef):
            doc[updater.field_name] = f.id
        elif is_dict and isinstance(f.get('_id'), bson.ObjectId):  # manual ref
            doc[updater.field_name] = f['_id']

        elif updater.migration_policy.name == 'strict':  # Other data type
            raise InconsistencyError(f"Field {updater.field_name} has wrong value {f!r} "
                                     f"(should be DBRef, ObjectId, manual ref, dynamic ref, "
                                     f"ObjectId string) in record {doc}")

    # TODO: precheck if field actually contains value other than ObjectId
    updater.update_by_document(by_doc)


def to_manual_ref(updater: DocumentUpdater):
    """Convert references (ObjectId, DBRef, dynamic ref) to manual ref
    """
    def by_doc(ctx: ByDocContext):
        doc = ctx.document
        if updater.field_name not in doc or doc[updater.field_name] is None:
            return

        f = doc[updater.field_name]
        if isinstance(f, str):  # ObjectId as string
            try:
                f = bson.ObjectId(f)
            except bson.errors.BSONError:
                pass

        is_dict = isinstance(f, dict)
        if is_dict and isinstance(f.get('_id'), bson.ObjectId):  # Already manual ref
            return
        elif is_dict and isinstance(f.get('_ref'), bson.DBRef):  # dynamic ref
            doc[updater.field_name] = {'_id': f['_ref'].id}
        elif isinstance(f, bson.DBRef):
            doc[updater.field_name] = {'_id': f.id}
        elif isinstance(f, bson.ObjectId):
            doc[updater.field_name] = {'_id': f}
        elif updater.migration_policy.name == 'strict':  # Other data type
            raise InconsistencyError(f"Field {updater.field_name} has wrong value {f!r} "
                                     f"(should be DBRef, ObjectId, manual ref, dynamic ref, "
                                     f"ObjectId string) in record {doc}")

    # TODO: precheck if field actually contains value other than manual ref
    updater.update_by_document(by_doc)


def to_dbref(updater: DocumentUpdater):
    """Convert references (ObjectId, manual ref, dynamic ref) to dbref
    """
    def by_doc(ctx: ByDocContext):
        doc = ctx.document
        if updater.field_name not in doc or doc[updater.field_name] is None:
            return

        f = doc[updater.field_name]
        if isinstance(f, bson.DBRef):  # Already DBRef
            return
        elif isinstance(f, str):  # ObjectId as string
            try:
                f = bson.ObjectId(f)
            except bson.errors.BSONError:
                pass

        collection_name = ctx.collection.name if ctx.collection is not None else None
        is_dict = isinstance(f, dict)
        if is_dict and isinstance(f.get('_id'), bson.ObjectId):  # manual ref
            doc[updater.field_name] = bson.DBRef(collection_name, f['_id'])
        elif is_dict and isinstance(f.get('_ref'), bson.DBRef):  # dynamic ref
            doc[updater.field_name] = f['_ref']
        elif isinstance(f, bson.ObjectId):
            doc[updater.field_name] = bson.DBRef(collection_name, f)
        elif updater.migration_policy.name == 'strict':  # Other data type
            raise InconsistencyError(f"Field {updater.field_name} has wrong value {f!r} "
                                     f"(should be DBRef, ObjectId, manual ref, dynamic ref, "
                                     f"ObjectId string) in record {doc}")

    # TODO: precheck if field actually contains value other than DBRef
    updater.update_by_document(by_doc)


def to_dynamic_ref(updater: DocumentUpdater):
    """Convert references (ObjectId, DBRef, manual ref) to dynamic ref
    """
    def by_doc(ctx: ByDocContext):
        doc = ctx.document
        if updater.field_name not in doc or doc[updater.field_name] is None:
            return

        f = doc[updater.field_name]
        is_dict = isinstance(f, dict)
        collection_name = ctx.collection.name if ctx.collection is not None else None

        if isinstance(f, str):  # ObjectId as string
            try:
                f = bson.ObjectId(f)
            except bson.errors.BSONError:
                pass

        # We cannot get dynamic ref from other types of refs because
        # of lack of '_cls' value. Mongoengine fields which use this
        # converter can keep DBRef. So return DBRef instead
        if is_dict and isinstance(f.get('_ref'), bson.DBRef):  # Already dynamic ref
            return
        elif isinstance(f, bson.DBRef):
            return
        elif is_dict and isinstance(f.get('_id'), bson.ObjectId):  # manual ref
            doc[updater.field_name] = bson.DBRef(collection_name, f['_id'])
        elif isinstance(f, bson.ObjectId):
            doc[updater.field_name] = bson.DBRef(collection_name, f)
        elif updater.migration_policy.name == 'strict':  # Other data type
            raise InconsistencyError(f"Field {updater.field_name} has wrong value {f!r} "
                                     f"(should be DBRef, ObjectId, manual ref, dynamic ref) "
                                     f"in record {doc}")

    # TODO: precheck if field actually contains value other than dynamic ref or DBRef
    updater.update_by_document(by_doc)


def to_string(updater: DocumentUpdater):
    def by_doc(ctx: ByDocContext):
        doc = ctx.document
        if updater.field_name not in doc or doc[updater.field_name] is None:
            return

        f = doc[updater.field_name]
        is_dict = isinstance(f, dict)
        if is_dict and isinstance(f.get('_ref'), bson.DBRef):  # dynamic ref
            doc[updater.field_name] = str(f['_ref'].id)
        elif is_dict and isinstance(f.get('_id'), bson.ObjectId):  # manual ref
            doc[updater.field_name] = str(f['_id'])
        elif isinstance(f, bson.DBRef):
            doc[updater.field_name] = str(f.id)
        else:
            try:
                doc[updater.field_name] = str(f)
            except (TypeError, ValueError) as e:
                if updater.migration_policy.name == 'strict':
                    raise MigrationError(f'Cannot convert value {updater.field_name}: '
                                         f'{doc[updater.field_name]} to string') from e

    # TODO: precheck if field actually contains value other than string
    updater.update_by_document(by_doc)


def to_int(updater: DocumentUpdater):
    __mongo_convert(updater, 'int')


def to_long(updater: DocumentUpdater):
    __mongo_convert(updater, 'long')


def to_double(updater: DocumentUpdater):
    __mongo_convert(updater, 'double')


def to_decimal(updater: DocumentUpdater):
    __mongo_convert(updater, 'decimal')


def to_date(updater: DocumentUpdater):
    __mongo_convert(updater, 'date')


def to_bool(updater: DocumentUpdater):
    __mongo_convert(updater, 'bool')


def to_binary(updater: DocumentUpdater):
    __mongo_convert(updater, 'binary')


def to_object(updater: DocumentUpdater):
    __mongo_convert(updater, 'object')


def to_uuid_str(updater: DocumentUpdater):
    """Convert binData with UUID to string with UUID"""
    def by_doc(ctx: ByDocContext):
        doc = ctx.document
        if updater.field_name not in doc or doc[updater.field_name] is None:
            return

        f = doc[updater.field_name]
        if isinstance(f, str) and uuid_pattern.match(f):
            return
        elif isinstance(f, uuid.UUID):
            doc[updater.field_name] = str(f)
        elif updater.migration_policy.name == 'strict':
            raise InconsistencyError(f"Field {updater.field_name} has wrong value {f!r} "
                                     f"(should be UUID string or UUID Binary data) in record {doc}")

    uuid_pattern = re.compile(r'\A[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}\Z',
                              re.IGNORECASE)
    updater.update_by_document(by_doc)


def to_uuid_bin(updater: DocumentUpdater):
    """Convert strings with UUID to binData with UUID"""
    def by_doc(ctx: ByDocContext):
        doc = ctx.document
        if updater.field_name not in doc or doc[updater.field_name] is None:
            return

        f = doc[updater.field_name]
        if isinstance(f, uuid.UUID):
            return
        elif isinstance(f, str) and uuid_pattern.match(f):
            doc[updater.field_name] = uuid.UUID(f)
        elif updater.migration_policy.name == 'strict':
            raise InconsistencyError(f"Field {updater.field_name} has wrong value {f!r} "
                                     f"(should be UUID string or UUID Binary data) in record {doc}")

    uuid_pattern = re.compile(r'\A[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}\Z',
                              re.IGNORECASE)
    updater.update_by_document(by_doc)


def to_url_string(updater: DocumentUpdater, check_only=False):
    """Cast fields to string and then verify if they contain URLs"""
    def by_path(ctx: ByPathContext):
        fltr = {ctx.filter_dotpath: {'$not': url_regex, '$ne': None}, **ctx.extra_filter}
        check_empty_result(ctx.collection, ctx.filter_dotpath, fltr)

    to_string(updater)

    url_regex = re.compile(
        r"\A[A-Z]{3,}://[A-Z0-9\-._~:/?#\[\]@!$&'()*+,;%=]\Z",
        re.IGNORECASE
    )
    if updater.migration_policy.name == 'strict':
        updater.update_by_path(by_path)


def to_email_string(updater: DocumentUpdater):
    def by_path(ctx: ByPathContext):
        email_regex = r"\A[^\W][A-Z0-9._%+-]+@[\p{L}0-9.-]+\.\p{L}+\Z"
        fltr = {ctx.filter_dotpath: {'$not': {'$regex': email_regex, '$options': 'i'}, '$ne': None}, **ctx.extra_filter}
        check_empty_result(ctx.collection, ctx.filter_dotpath, fltr)

    to_string(updater)

    if updater.migration_policy.name == 'strict':
        updater.update_by_path(by_path)


def to_complex_datetime(updater: DocumentUpdater):
    def by_path(ctx: ByPathContext):
        fltr = {ctx.filter_dotpath: {'$not': regex, '$ne': None}, **ctx.extra_filter}
        check_empty_result(ctx.collection, ctx.filter_dotpath, fltr)

    to_string(updater)

    # We should not know which separator is used, so use '.+'
    # Separator change is handled by appropriate field method
    regex = r'\A' + str('.+'.join([r"\d{4}"] + [r"\d{2}"] * 5 + [r"\d{6}"])) + r'\Z'
    if updater.migration_policy.name == 'strict':
        updater.update_by_path(by_path)


def __mongo_convert(updater: DocumentUpdater, target_type: str):
    """
    Convert field to a given type in a given collection. `target_type`
    contains MongoDB type name, such as 'string', 'decimal', etc.

    https://docs.mongodb.com/manual/reference/operator/aggregation/convert/
    :param updater: DocumentUpdater object
    :param target_type: MongoDB type name
    :return:
    """
    def by_doc(ctx: ByDocContext):
        # https://docs.mongodb.com/manual/reference/operator/aggregation/convert/
        type_map = {
            'double': float,
            'string': str,
            'objectId': bson.ObjectId,
            'bool': bool,
            'date': lambda x: dateutil_parse(str(x)),
            'int': int,
            'long': bson.Int64,
            'decimal': float,
            'binary': bson.Binary,
            'object': dict
        }
        assert target_type in type_map

        doc = ctx.document
        field_name = updater.field_name
        if field_name in doc:
            t = type_map[target_type]
            if not isinstance(doc[field_name], t) and doc[field_name] is not None:
                try:
                    doc[field_name] = type_map[target_type](doc[field_name])
                except (TypeError, ValueError) as e:
                    if updater.migration_policy.name == 'strict':
                        raise MigrationError(f'Cannot convert value '
                                             f'{field_name}: {doc[field_name]} to type {t}') from e

    updater.update_by_document(by_doc)
