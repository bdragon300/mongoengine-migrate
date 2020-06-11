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
