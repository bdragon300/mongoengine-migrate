import re
from datetime import date
from decimal import Decimal

import bson
from dateutil.parser import parse as dateutil_parse

from mongoengine_migrate.exceptions import MigrationError
from mongoengine_migrate.mongo import check_empty_result, mongo_version, DocumentUpdater


def nothing(*args, **kwargs):
    """Converter which does nothing"""
    pass


def deny(updater: DocumentUpdater):
    """Convertion is denied"""
    raise MigrationError(f"Convertion of field {updater.document_type}.{updater.field_name} "
                         f"is forbidden")


def drop_field(updater: DocumentUpdater):
    """Drop field"""
    def by_path(col, filter_dotpath, update_dotpath, array_filters):
        col.update_many({filter_dotpath: {'$exists': True}}, {'$unset': {update_dotpath: ''}})

    updater.update_by_path(by_path)


@mongo_version(min_version='3.6')
def item_to_list(updater: DocumentUpdater):
    """Make a list with single element from every non-array value"""
    def by_path(col, filter_dotpath, update_dotpath, array_filters):
        col.aggregate([
            {'$match': {
                filter_dotpath: {"$exists": True},
                # $expr >= 3.6, $type >= 3.4
                "$expr": {"$ne": [{"$type": f'${filter_dotpath}'}, 'array']}
            }},
            {'$addFields': {filter_dotpath: [f"${filter_dotpath}"]}},  # >=3.4
            {'$out': col.name}  # >= 2.6
        ])

    def by_doc(col, doc, filter_dotpath):
        if isinstance(doc, dict) and updater.field_name in doc:
            doc[updater.field_name] = [doc[updater.field_name]]

    updater.update_combined(by_path, by_doc, embedded_noarray_by_path_cb=by_path)


@mongo_version(min_version='3.6')
def extract_from_list(updater: DocumentUpdater):
    """Replace every list which was met with its first element"""
    def by_path(col, filter_dotpath, update_dotpath, array_filters):
        col.aggregate([
            {'$match': {
                filter_dotpath: {"$ne": None},
                # $expr >= 3.6, $type >= 3.4
                # FIXME: what if nested list (not idempotent query)
                "$expr": {"$eq": [{"$type": f'${filter_dotpath}'}, 'array']}
            }},
            {'$addFields': {filter_dotpath: {"$arrayElemAt": [f"${filter_dotpath}", 0]}}},  # >=3.4
            {'$out': col.name}  # >= 2.6
        ])

    def by_doc(col, doc, filter_dotpath):
        if isinstance(doc, dict) and isinstance(doc.get(updater.field_name), (list, tuple)):
            doc[updater.field_name] = doc[updater.field_name][0]

    updater.update_combined(by_path, by_doc, embedded_noarray_by_path_cb=by_path)


def to_string(updater: DocumentUpdater):
    __mongo_convert(updater, 'string')


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


def to_object_id(updater: DocumentUpdater):
    __mongo_convert(updater, 'objectId')


@mongo_version(min_version='4.0')
def to_uuid(updater: DocumentUpdater):
    """Don't touch fields with 'binData' type. Convert values with
    other types to a string. Then verify if these strings contain
    UUIDs. Raise error if not
    """
    def post_check(col, filter_dotpath):
        # Verify strings. There are only binData and string values now in db
        fltr = {
            filter_dotpath: {
                '$not': re.compile(r'\A[0-9a-f]{8}-([0-9a-f]{4}-){3}[0-9a-f]{12}\Z'),
                '$ne': None,
                '$type': "string"
            }
        }
        check_empty_result(col, filter_dotpath, fltr)

    def by_path(col, filter_dotpath, update_dotpath, array_filters):
        # Convert fields to string where value has type other than binData
        col.aggregate([
            {'$match': {
                filter_dotpath: {'$ne': None}, # Field exists and not null
                '$expr': {  # >= 3.6
                    '$not': [
                        # $type >= 3.4, $in >= 3.4
                        {'$in': [{'$type': f'${filter_dotpath}'}, ['binData', 'string']]}
                    ]
                }
            }},
            {'$addFields': {  # >= 3.4
                '$convert': {  # >= 4.0
                    'input': f'${filter_dotpath}',
                    'to': 'string'
                }
            }},
            {'$out': col.name}  # >= 2.6
        ])

    def by_doc(col, doc, filter_dotpath):
        if not isinstance(doc, dict):
            return
        item = doc.get(updater.field_name)

        if item is not None and not isinstance(item, (bson.Binary, str)):
            doc[updater.field_name] = str(doc)
        # FIXME: call post_check for every filter_dotpath, not for doc

    updater.update_combined(by_path, by_doc, embedded_noarray_by_path_cb=by_path)
    updater.update_by_path(post_check)


def to_url_string(updater: DocumentUpdater):
    """Cast fields to string and then verify if they contain URLs"""
    def by_path(col, filter_dotpath, update_dotpath, array_filters):
        fltr = {filter_dotpath: {'$not': url_regex, '$ne': None}}
        check_empty_result(col, filter_dotpath, fltr)

    to_string(updater)

    url_regex = re.compile(
        r"\A[A-Z]{3,}://[A-Z0-9\-._~:/?#\[\]@!$&'()*+,;%=]\Z",
        re.IGNORECASE
    )
    updater.update_by_path(by_path)


def to_complex_datetime(updater: DocumentUpdater):
    def by_path(col, filter_dotpath, update_dotpath, array_filters):
        fltr = {filter_dotpath: {'$not': regex, '$ne': None}}
        check_empty_result(col, filter_dotpath, fltr)

    to_string(updater)

    # We should not know which separator is used, so use '.+'
    # Separator change is handled by appropriate field method
    regex = r'\A' + str('.+'.join([r"\d{4}"] + [r"\d{2}"] * 5 + [r"\d{6}"])) + r'\Z'
    updater.update_by_path(by_path)


@mongo_version(min_version='3.6')
def ref_to_cached_reference(updater: DocumentUpdater):
    """Convert ObjectId values to Manual Reference SON object.
    Leave DBRef objects as is.
    """
    def post_check(col, filter_dotpath, update_dotpath, array_filters):
        # Check if all values in collection are DBRef or Manual reference
        # objects because we could miss other value types on a previous step
        fltr = {
            filter_dotpath: {"$ne": None},
            f'{filter_dotpath}.$id': {"$exists": False},  # Exclude DBRef objects
            f'{filter_dotpath}._id': {"$exists": False},  # Exclude Manual refs
        }
        check_empty_result(col, filter_dotpath, fltr)

    def by_path(col, filter_dotpath, update_dotpath, array_filters):
        col.aggregate([
            {'$match': {
                filter_dotpath: {"$ne": None},
                # $expr >= 3.6, $type >= 3.4
                "$expr": {"$eq": [{"$type": f'${filter_dotpath}'}, 'objectId']}
            }},
            {'$addFields': {filter_dotpath: {'_id': f"${filter_dotpath}"}}},  # >= 3.4
            {'$out': col.name}  # >= 2.6
        ])

    def by_doc(col, doc, filter_dotpath):
        if isinstance(doc, dict) and isinstance(doc.get(updater.field_name), bson.ObjectId):
            doc[updater.field_name] = {'_id': doc[updater.field_name]}

    updater.update_combined(by_path, by_doc, embedded_noarray_by_path_cb=by_path)
    updater.update_by_path(post_check)


@mongo_version(min_version='3.6')
def cached_reference_to_ref(updater: DocumentUpdater):
    """Convert Manual Reference SON object to ObjectId value.
    Leave DBRef objects as is.
    """
    def post_check(col, filter_dotpath, update_dotpath, array_filters):
        # Check if all values in collection are DBRef or ObjectId because
        # we could miss other value types on a previous step
        fltr = {
            filter_dotpath: {"$ne": None},
            f'{filter_dotpath}.$id': {"$exists": False},  # Exclude DBRef objects
            "$expr": {  # >= 3.6
                "$ne": [{"$type": "$key"}, 'objectId']
            }
        }
        check_empty_result(col, filter_dotpath, fltr)

    def by_path(col, filter_dotpath, update_dotpath, array_filters):
        col.aggregate([
            {'$match': {
                f'{filter_dotpath}._id': {"$ne": None},
                # $expr >= 3.6, $type >= 3.4
                "$expr": {"$eq": [{"$type": f'${filter_dotpath}'}, 'object']}
            }},
            {'$addFields': {filter_dotpath: f"${filter_dotpath}._id"}},  # >= 3.4
            {'$out': col.name}  # >= 2.6
        ])

    def by_doc(col, doc, filter_dotpath):
        if isinstance(doc, dict) and isinstance(doc.get(updater.field_name), dict):
            doc[updater.field_name] = doc[updater.field_name].get('_id')

    updater.update_combined(by_path, by_doc, embedded_noarray_by_path_cb=by_path)
    updater.update_by_path(post_check)


@mongo_version(min_version='4.0')
def __mongo_convert(updater: DocumentUpdater, target_type: str):
    """
    Convert field to a given type in a given collection. `target_type`
    contains MongoDB type name, such as 'string', 'decimal', etc.

    https://docs.mongodb.com/manual/reference/operator/aggregation/convert/
    :param updater: DocumentUpdater object
    :param target_type: MongoDB type name
    :return:
    """
    def by_path(col, filter_dotpath, update_dotpath, array_filters):
        # TODO: implement also for mongo 3.x
        # TODO: use $convert with onError and onNull
        col.aggregate([
            # Field exists and not null
            {'$match': {
                filter_dotpath: {'$ne': None},  # Field exists and not null
                # $expr >= 3.6, $type >= 3.4
                "$expr": {"$ne": [{"$type": f'${filter_dotpath}'}, target_type]}
            }},
            {'$addFields': {
                '$convert': {  # >= 4.0
                    'input': f'${filter_dotpath}',
                    'to': target_type
                }
            }},
            {'$out': col.name}  # >= 2.6
        ])

    def by_doc(col, doc, filter_dotpath):
        # https://docs.mongodb.com/manual/reference/operator/aggregation/convert/
        type_map = {
            'double': float,
            'string': str,
            'objectId': bson.ObjectId,
            'bool': bool,
            'date': date,
            'int': int,
            'long': int,
            'decimal': Decimal
        }
        assert target_type in type_map

        if isinstance(doc, dict) \
                and not isinstance(doc.get(updater.field_name), type_map[target_type]):
            if target_type == 'date':
                doc[updater.field_name] = dateutil_parse(str(doc[updater.field_name]))
            else:
                doc[updater.field_name] = type_map[target_type]()

    updater.update_combined(by_path, by_doc, embedded_noarray_by_path_cb=by_path)
