import re
from typing import Type

from mongoengine.fields import BaseField
from pymongo.collection import Collection

from mongoengine_migrate.exceptions import MigrationError
from mongoengine_migrate.mongo import check_empty_result


def nothing(*args, **kwargs):
    """Converter which does nothing"""
    pass


def deny(collection: Collection, db_field: str, from_cls: Type[BaseField], to_cls: Type[BaseField]):
    """Convertion is denied"""
    raise MigrationError(f"Unable to convert field {collection.name}.{db_field} "
                         f"from {from_cls.__name__!r} to {to_cls.__name__!r}. You can use "
                         f"error_policy for 'type_key' diff to override this")


def drop_field(collection: Collection, db_field: str):
    """Drop field"""
    collection.update_many({db_field: {'$exists': True}}, {'$unset': {db_field: ''}})


def item_to_list(collection: Collection, db_field: str):
    """Make list with a single element from an value"""
    collection.aggregate([
        {'$match': {db_field: {"$exists": True}}},
        {'$addFields': {db_field: [f"${db_field}"]}},
        {'$out': collection.name}
    ])


def extract_from_list(collection: Collection, db_field: str):
    """Everwrite list with its first element"""
    collection.aggregate([
        {'$match': {db_field: {"$ne": None}}},
        {'$addFields': {db_field: {"$arrayElemAt": [f"${db_field}", 0]}}},
        {'$out': collection.name}
    ])


def to_string(collection: Collection, db_field: str):
    __mongo_convert(collection, db_field, 'string')


def to_int(collection: Collection, db_field: str):
    __mongo_convert(collection, db_field, 'int')


def to_long(collection: Collection, db_field: str):
    __mongo_convert(collection, db_field, 'long')


def to_double(collection: Collection, db_field: str):
    __mongo_convert(collection, db_field, 'double')


def to_decimal(collection: Collection, db_field: str):
    __mongo_convert(collection, db_field, 'decimal')


def to_date(collection: Collection, db_field: str):
    __mongo_convert(collection, db_field, 'date')


def to_bool(collection: Collection, db_field: str):
    __mongo_convert(collection, db_field, 'bool')


def to_object_id(collection: Collection, db_field: str):
    __mongo_convert(collection, db_field, 'objectId')


def to_uuid(collection: Collection, db_field: str):
    """Don't touch fields with 'binData' type. Convert values with
    other types to a string. Then verify if these strings contain
    UUIDs. Raise error if not
    """
    # Convert fields to string where value has type other than binData
    collection.aggregate([
        {'$match': {
            db_field: {'$ne': None}, # Field exists and not null
            '$expr': {'$not': [{'$type': f'${db_field}'}, 'binData']}
        }},
        {'$addFields': {
            '$convert': {
                'input': f'${db_field}',
                'to': 'string'
            }
        }},
        {'$out': collection.name}
    ])

    # Verify strings. There are only binData and string values now in db
    fltr = {
        db_field: {
            '$not': re.compile(r'\A[0-9a-f]{8}-([0-9a-f]{4}-){3}[0-9a-f]{12}\Z'),
            '$ne': None,
            '$type': "string"
        }
    }
    check_empty_result(collection, db_field, fltr)


def to_url_string(collection: Collection, db_field: str):
    """Cast fields to string and then verify if they contain URLs"""
    to_string(collection, db_field)

    url_regex = re.compile(
        r"\A[A-Z]{3,}://[A-Z0-9\-._~:/?#\[\]@!$&'()*+,;%=]\Z",
        re.IGNORECASE
    )
    fltr = {db_field: {'$not': url_regex, '$ne': None}}
    check_empty_result(collection, db_field, fltr)


def to_complex_datetime(collection: Collection, db_field: str):
    # We should not know which separator is used, so use '.+'
    # Separator change is handled by appropriate method
    to_string(collection, db_field)

    regex = r'\A' + str('.+'.join([r"\d{4}"] + [r"\d{2}"] * 5 + [r"\d{6}"])) + r'\Z'
    fltr = {db_field: {'$not': regex, '$ne': None}}
    check_empty_result(collection, db_field, fltr)


def ref_to_cached_reference(collection: Collection, db_field: str):
    """Make SON object (dict) from ObjectID/DBRef object"""
    collection.aggregate([
        {'$match': {db_field: {"$exists": True}}},
        {'$addFields': {db_field: {'_id': f"${db_field}"}}},
        {'$out': collection.name}
    ])


def cached_reference_to_ref(collection: Collection, db_field: str):
    """Extract ObjectID/DBRef reference object from SON object (dict)"""
    collection.aggregate([
        {'$match': {db_field: {"$exists": True}}},
        {'$addFields': {db_field: f"${db_field}._id"}},
        {'$out': collection.name}
    ])


def __mongo_convert(collection: Collection, db_field: str, target_type: str):
    """
    Convert field to a given type in a given collection. `target_type`
    contains MongoDB type name, such as 'string', 'decimal', etc.

    https://docs.mongodb.com/manual/reference/operator/aggregation/convert/
    :param collection: pymongo collection object
    :param db_field: field name
    :param target_type: MongoDB type name
    :return:
    """
    # TODO: implement also for mongo 3.x
    # TODO: use $convert with onError and onNull
    collection.aggregate([
        # Field exists and not null
        {'$match': {db_field: {"$ne": None}}},
        {'$addFields': {
            '$convert': {
                'input': f'${db_field}',
                'to': target_type
            }
        }},
        {'$out': collection.name}
    ])
