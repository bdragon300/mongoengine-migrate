from typing import List
from pymongo.collection import Collection

from mongoengine_migrate.mongo import check_empty_result, mongo_version


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
    __check_geojson_objects(collection, db_field, ['Point'])
    __check_legacy_point_coordinates(collection, db_field)
    __check_value_types(collection, db_field, ['object', 'array'])

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
    __check_geojson_objects(collection, db_field, ['Point'])
    __check_legacy_point_coordinates(collection, db_field)
    __check_value_types(collection, db_field, ['object', 'array'])

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
def __check_geojson_objects(collection: Collection, db_field: str, geojson_types: List[str]):
    """
    Check if all object values in field are GeoJSON objects of given
    types. Raise MigrationError if other objects found
    :param collection:
    :param db_field:
    :param geojson_types:
    :return:
    """
    fltr = {"$and": [
        {db_field: {"$ne": None}},
        {f'{db_field}.type': {'$nin': geojson_types}},
        # $expr >= 3.6
        {"$expr": {"$eq": [{"$type": f'${db_field}'}, 'object']}}
    ]}
    check_empty_result(collection, db_field, fltr)


@mongo_version(min_version='3.6', throw_error=True)
def __check_legacy_point_coordinates(collection: Collection, db_field: str):
    """
    Check if all array values in field has legacy geo point
    coordinates type. Raise MigrationError if other arrays was found
    :param collection:
    :param db_field:
    :return:
    """
    fltr = {"$and": [
        {db_field: {"$ne": None}},
        # $expr >= 3.6, $isArray >= 3.2
        {"$expr": {"$eq": [{"$isArray": f"${db_field}"}, True]}},
        {"$expr": {"$ne": [{"$size": f"${db_field}"}, 2]}},  # $expr >= 3.6
        # TODO: add element type check
    ]}
    check_empty_result(collection, db_field, fltr)


@mongo_version(min_version='3.6', throw_error=True)
def __check_value_types(collection: Collection, db_field: str, allowed_types: List[str]):
    """
    Check if given field contains only given types of value.
    Raise if other value types was found
    :param collection:
    :param db_field:
    :param allowed_types:
    :return:
    """
    # Check for data types other than objects or arrays
    fltr = {"$and": [
        {db_field: {"$ne": None}},
        # $expr >= 3.6, $type >= 3.4
        {"$expr": {"$not": [{"$in": [{"$type": f'${db_field}'}, allowed_types]}]}}
    ]}
    check_empty_result(collection, db_field, fltr)
