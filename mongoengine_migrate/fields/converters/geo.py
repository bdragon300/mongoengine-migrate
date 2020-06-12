from typing import List

from pymongo.collection import Collection

from mongoengine_migrate.mongo import check_empty_result, mongo_version

#: GeoJSON field convertions in order of increasing the nested array
#  depth in `coordinates` subfield.
#: The simplest is Point (`coordinates` contains array of numbers),
#: the most complex is MultiPolygon (`coordinates` consists of array
#: of arrays of arrays of arrays of numbers).
__CONVERTIONS = (
    ("Point", ),
    ("MultiPoint", "LineString"),
    ("Polygon", "MultiLineString"),
    ("MultiPolygon", )
)


@mongo_version(min_version='3.6')
def convert_geojson(collection: Collection, db_field: str, from_type: str, to_type: str):
    """Convert GeoJSON object from one type to another"""
    from_ind, to_ind = None, None
    __check_geojson_objects(
        collection,
        db_field,
        [from_type, to_type]
    )
    # There could be legacy coordinate pair arrays or GeoJSON objects
    __check_legacy_point_coordinates(collection, db_field)
    __check_value_types(collection, db_field, ['object', 'array'])

    if from_type == to_type:
        return

    for ind, convertion in __CONVERTIONS:
        if from_type in convertion:
            from_ind = ind
        if to_type in convertion:
            to_ind = ind

    if from_ind is None or to_ind is None:
        raise ValueError(f"Unknown geo field type. Was requested: {from_type}, {to_type}")

    depth = abs(from_ind - to_ind)
    if from_ind <= to_ind:
        __increase_geojson_nesting(collection, db_field, from_type, to_type, depth)
    else:
        __decrease_geojson_nesting(collection, db_field, from_type, to_type, depth)


@mongo_version(min_version='3.6')
def legacy_pairs_to_geojson(collection: Collection, db_field: str, to_type: str):
    """Convert legacy coordinate pairs to GeoJSON objects of given type"""
    __check_geojson_objects(
        collection,
        db_field,
        ['Point', to_type]
    )
    __check_legacy_point_coordinates(collection, db_field)
    __check_value_types(collection, db_field, ['object', 'array'])

    # Convert to GeoJSON Point object
    collection.aggregate([
        {'$match': {
            "$and": [
                {db_field: {"$ne": None}},
                # $expr >= 3.6
                {"$expr": {"$eq": [{"$isArray": f"${db_field}"}, True]}}
            ]}
        },
        {'$addFields': {  # >= 3.4
            db_field: {
                'type': 'Point',
                'coordinates': f'${db_field}'
            }
        }},
        {'$out': collection.name}  # >= 2.6
    ])

    convert_geojson(collection, db_field, 'Point', to_type)


@mongo_version(min_version='3.6')
def geojson_to_legacy_pairs(collection: Collection, db_field: str, from_type: str):
    """Convert GeoJSON objects of given type to legacy coordinate pairs"""
    __check_geojson_objects(
        collection,
        db_field,
        ["Point", from_type]
    )
    __check_legacy_point_coordinates(collection, db_field)
    __check_value_types(collection, db_field, ['object', 'array'])

    convert_geojson(collection, db_field, from_type, 'Point')

    collection.aggregate([
        {'$match': {
            db_field: {"$ne": None},
            f'{db_field}.type': "Point"
        }},
        {'$addFields': {  # >= 3.4
            db_field: f'${db_field}.coordinates'
        }},
        {'$out': collection.name}  # >= 2.6
    ])


@mongo_version(min_version='3.4', throw_error=True)
def __increase_geojson_nesting(collection: Collection,
                               db_field: str,
                               from_type: str,
                               to_type: str,
                               depth: int = 1):
    """
    Wraps `coordinates` field into nested array on GeoJSON fields
    with given type.
    :param collection: collection object
    :param db_field: collection field
    :param from_type: GeoJSON type to change
    :param to_type: this GeoJSON type will be set in changed records
    :param depth: nested array depth to wrap in
    :return:
    """
    assert depth > 0

    add_fields = [
        {'$addFields': {  # >= 3.4
            f'{db_field}.coordinates': [f'${db_field}.coordinates']
        }}
    ] * depth

    collection.aggregate([
        {'$match': {
            db_field: {"$ne": None},
            f'{db_field}.type': from_type
        }},
        *add_fields,
        {'$addFields': {  # >= 3.4
            f'{db_field}.type': to_type
        }},
        {'$out': collection.name}  # >= 2.6
    ])


@mongo_version(min_version='3.4', throw_error=True)
def __decrease_geojson_nesting(collection: Collection,
                               db_field: str,
                               from_type: str,
                               to_type: str,
                               depth: int = 1):
    """
    Extract the first element from nested arrays in `coordinates` field
    on GeoJSON fields with given type
    :param collection: collection object
    :param db_field: collection field
    :param from_type: GeoJSON type to change
    :param to_type: this GeoJSON type will be set in changed records
    :param depth: nested array depth to extract from
    :return:
    """
    assert depth > 0

    add_fields = [
        {'$addFields': {  # >= 3.4
            # $arrayElemAt >= 3.2
            f'{db_field}.coordinates': {"$arrayElemAt": [f'${db_field}.coordinates', 0]}
        }},
    ] * depth

    collection.aggregate([
        {'$match': {
            db_field: {"$ne": None},
            f'{db_field}.type': from_type
        }},
        *add_fields,
        {'$addFields': {  # >= 3.4
            f'{db_field}.type': to_type
        }},
        {'$out': collection.name}  # >= 2.6
    ])


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
