import re
from typing import Type

from mongoengine.fields import BaseField
from pymongo.collection import Collection

from mongoengine_migrate.exceptions import ActionError, MigrationError


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
    bad_records = collection.find(
        {db_field: {
            '$not': re.compile(r'\A[0-9a-f]{8}-([0-9a-f]{4}-){3}[0-9a-f]{12}\Z')},
            '$ne': None,
            '$type': "string"
        },
        limit=3
    )
    if bad_records.retrieved:
        examples = (
            f'{{_id: {x.get("_id", "unknown")},...{db_field}: ' \
            f'{x.get(db_field, "unknown")}}}'
            for x in bad_records
        )
        raise MigrationError(f"Some of records in {collection.name}.{db_field} "
                             f"contain values which are not UUID. This cannot be converted. "
                             f"First several examples {','.join(examples)}")


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
