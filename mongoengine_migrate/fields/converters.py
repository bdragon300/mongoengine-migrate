import re
from typing import Type

from mongoengine.fields import BaseField
from pymongo.collection import Collection

from mongoengine_migrate.exceptions import MigrationError
from mongoengine_migrate.mongo import check_empty_result
from ..mongo import mongo_version


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


@mongo_version(min_version='3.6')
def item_to_list(collection: Collection, db_field: str):
    """Make a list with single element from every non-array value"""
    collection.aggregate([
        {'$match': {
            db_field: {"$exists": True},
            "$expr": {"$ne": [{"$type": f'${db_field}'}, 'array']}  # $expr >= 3.6, $type >= 3.4
        }},
        {'$addFields': {db_field: [f"${db_field}"]}},  # >=3.4
        {'$out': collection.name}  # >= 2.6
    ])


@mongo_version(min_version='3.6')
def extract_from_list(collection: Collection, db_field: str):
    """Replace every list which was met with its first element"""
    collection.aggregate([
        {'$match': {
            db_field: {"$ne": None},
            # FIXME: what if nested list (not idempotent query)
            "$expr": {"$eq": [{"$type": f'${db_field}'}, 'array']}  # $expr >= 3.6, $type >= 3.4
        }},
        {'$addFields': {db_field: {"$arrayElemAt": [f"${db_field}", 0]}}},  # >=3.4
        {'$out': collection.name}  # >= 2.6
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


@mongo_version(min_version='4.0')
def to_uuid(collection: Collection, db_field: str):
    """Don't touch fields with 'binData' type. Convert values with
    other types to a string. Then verify if these strings contain
    UUIDs. Raise error if not
    """
    # Convert fields to string where value has type other than binData
    collection.aggregate([
        {'$match': {
            db_field: {'$ne': None}, # Field exists and not null
            '$expr': {  # >= 3.6
                '$not': [
                    # $type >= 3.4, $in >= 3.4
                    {'$in': [{'$type': f'${db_field}'}, ['binData', 'string']]}
                ]
            }
        }},
        {'$addFields': {  # >= 3.4
            '$convert': {  # >= 4.0
                'input': f'${db_field}',
                'to': 'string'
            }
        }},
        {'$out': collection.name}  # >= 2.6
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


@mongo_version(min_version='3.6')
def ref_to_cached_reference(collection: Collection, db_field: str):
    """Convert ObjectId values to Manual Reference SON object.
    Leave DBRef objects as is.
    """
    collection.aggregate([
        {'$match': {
            db_field: {"$ne": None},
            # $expr >= 3.6, $type >= 3.4
            "$expr": {"$eq": [{"$type": f'${db_field}'}, 'objectId']}
        }},
        {'$addFields': {db_field: {'_id': f"${db_field}"}}},  # >= 3.4
        {'$out': collection.name}  # >= 2.6
    ])

    # Check if all values in collection are DBRef or Manual reference
    # objects because we could miss other value types on a previous step
    fltr = {
        db_field: {"$ne": None},
        f'{db_field}.$id': {"$exists": False},  # Exclude DBRef objects
        f'{db_field}._id': {"$exists": False},  # Exclude Manual refs
    }
    check_empty_result(collection, db_field, fltr)


@mongo_version(min_version='3.6')
def cached_reference_to_ref(collection: Collection, db_field: str):
    """Convert Manual Reference SON object to ObjectId value.
    Leave DBRef objects as is.
    """
    collection.aggregate([
        {'$match': {
            f'{db_field}._id': {"$ne": None},
            # $expr >= 3.6, $type >= 3.4
            "$expr": {"$eq": [{"$type": f'${db_field}'}, 'object']}
        }},
        {'$addFields': {db_field: f"${db_field}._id"}},  # >= 3.4
        {'$out': collection.name}  # >= 2.6
    ])

    # Check if all values in collection are DBRef or ObjectId because
    # we could miss other value types on a previous step
    fltr = {
        db_field: {"$ne": None},
        f'{db_field}.$id': {"$exists": False},  # Exclude DBRef objects
        "$expr": {  # >= 3.6
            "$ne": [{"$type": "$key"}, 'objectId']
        }
    }
    check_empty_result(collection, db_field, fltr)


@mongo_version(min_version='3.6')
def geojson_to_legacy_coordinate_pairs(collection: Collection, db_field: str):
    """
    Convert GeoJSON Point field to a legacy geo coordinates
    representation. Such field contains array of 2 elements: longitude,
    latitude
    https://docs.mongodb.com/manual/geospatial-queries/#legacy-coordinate-pairs
    :param collection: pymongo collection object
    :param db_field: field name
    :return:
    """
    __check_geo_points(collection, db_field)

    fltr = {
        db_field: {"$ne": None},
        f'{db_field}.type': "Point"
    }
    collection.update_many(fltr, {'$set': f'${db_field}.coordinates'})


@mongo_version(min_version='3.6')
def legacy_coordinate_pairs_to_geojson(collection: Collection, db_field: str):
    """
    Convert legacy geo coordinates representation to GeoJSON Point field
    :param collection: pymongo collection object
    :param db_field: field name
    :return:
    """
    __check_geo_points(collection, db_field)

    fltr = {"$and": [
        {db_field: {"$ne": None}},
        {"$expr": {"$eq": [{"$isArray": f"${db_field}"}, True]}}
    ]}
    collection.update_many(
        fltr,
        {'$set': {
            db_field: {
                "type": "Point",
                "coordinates": f'${db_field}'
            }
        }}
    )


@mongo_version(min_version='3.6', throw_error=True)
def __check_geo_points(collection: Collection, db_field: str):
    """
    Check if given collection contains GeoJSON Point objects or
    legacy coordinates pairs or NULLS in given field. Raise
    MigrationError if something another found
    :param collection:
    :param db_field:
    :return:
    """
    # Check for objects other than GeoJSON Point
    fltr = {"$and": [
        {db_field: {"$ne": None}},
        {f'{db_field}.type': {'$ne': "Point"}},
        # $expr >= 3.6
        {"$expr": {"$eq": [{"$type": f'${db_field}'}, 'object']}}
    ]}
    check_empty_result(collection, db_field, fltr)

    # Check for not 2-element arrays
    fltr = {"$and": [
        {db_field: {"$ne": None}},
        # $expr >= 3.6, $isArray >= 3.2
        {"$expr": {"$eq": [{"$isArray": f"${db_field}"}, True]}},
        {"$expr": {"$ne": [{"$size": f"${db_field}"}, 2]}},  # $expr >= 3.6
    ]}
    check_empty_result(collection, db_field, fltr)

    # Check for data types other than objects or arrays
    fltr = {"$and": [
        {db_field: {"$ne": None}},
        # $expr >= 3.6, $type >= 3.4
        {"$expr": {"$not": [{"$in": [{"$type": f'${db_field}'}, ['object', 'array']]}]}}
    ]}
    check_empty_result(collection, db_field, fltr)


@mongo_version(min_version='4.0')
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
        {'$match': {
            db_field: {'$ne': None},  # Field exists and not null
            # $expr >= 3.6, $type >= 3.4
            "$expr": {"$ne": [{"$type": f'${db_field}'}, target_type]}
        }},
        {'$addFields': {
            '$convert': {  # >= 4.0
                'input': f'${db_field}',
                'to': target_type
            }
        }},
        {'$out': collection.name}  # >= 2.6
    ])
