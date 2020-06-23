import functools
from typing import List

from mongoengine_migrate.mongo import check_empty_result, mongo_version, DocumentUpdater

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
def convert_geojson(updater: DocumentUpdater, from_type: str, to_type: str):
    """Convert GeoJSON object from one type to another"""
    from_ind, to_ind = None, None
    __check_geojson_objects(updater, [from_type, to_type])
    # There could be legacy coordinate pair arrays or GeoJSON objects
    __check_legacy_point_coordinates(updater)
    __check_value_types(updater, ['object', 'array'])

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
        __increase_geojson_nesting(updater, from_type, to_type, depth)
    else:
        __decrease_geojson_nesting(updater, from_type, to_type, depth)


@mongo_version(min_version='3.6')
def legacy_pairs_to_geojson(updater: DocumentUpdater, to_type: str):
    """Convert legacy coordinate pairs to GeoJSON objects of given type"""
    def upd_doc(col, filter_dotpath, update_dotpath, array_filters):
        # Convert to GeoJSON Point object
        col.aggregate([
            {'$match': {
                "$and": [
                    {filter_dotpath: {"$ne": None}},
                    # $expr >= 3.6
                    {"$expr": {"$eq": [{"$isArray": f"${filter_dotpath}"}, True]}}
                ]}
            },
            {'$addFields': {  # >= 3.4
                filter_dotpath: {
                    'type': 'Point',
                    'coordinates': f'${filter_dotpath}'
                }
            }},
            {'$out': col.name}  # >= 2.6
        ])

    def upd_embedded(col, doc, filter_dotpath):
        if isinstance(doc, dict) and isinstance(doc.get(updater.field_name), (list, tuple)):
            doc[updater.field_name] = {'type': 'Point', 'coordinates': doc[updater.field_name]}

    __check_geojson_objects(updater, ['Point', to_type])
    __check_legacy_point_coordinates(updater)
    __check_value_types(updater, ['object', 'array'])

    updater.update_by_path_or_by_document(upd_doc, upd_embedded)
    convert_geojson(updater, 'Point', to_type)


@mongo_version(min_version='3.6')
def geojson_to_legacy_pairs(updater: DocumentUpdater, from_type: str):
    """Convert GeoJSON objects of given type to legacy coordinate pairs"""
    def upd_doc(col, filter_dotpath, update_dotpath, array_filters):
        col.aggregate([
            {'$match': {
                filter_dotpath: {"$ne": None},
                f'{filter_dotpath}.type': "Point"
            }},
            {'$addFields': {  # >= 3.4
                filter_dotpath: f'${filter_dotpath}.coordinates'
            }},
            {'$out': col.name}  # >= 2.6
        ])

    def upd_embedded(col, doc, filter_dotpath):
        if isinstance(doc, dict) and isinstance(doc.get(updater.field_name), dict):
            if 'Point' in doc[updater.field_name]:
                doc[updater.field_name] = doc[updater.field_name].get('coordinates')

    __check_geojson_objects(updater, ["Point", from_type])
    __check_legacy_point_coordinates(updater)
    __check_value_types(updater, ['object', 'array'])

    convert_geojson(updater, from_type, 'Point')

    updater.update_by_path_or_by_document(upd_doc, upd_embedded)


@mongo_version(min_version='3.4', throw_error=True)
def __increase_geojson_nesting(updater: DocumentUpdater,
                               from_type: str,
                               to_type: str,
                               depth: int = 1):
    """
    Wraps `coordinates` field into nested array on GeoJSON fields
    with given type.
    :param updater: DocumentUpdater object
    :param from_type: GeoJSON type to change
    :param to_type: this GeoJSON type will be set in changed records
    :param depth: nested array depth to wrap in
    :return:
    """
    assert depth > 0

    def upd_doc(col, filter_dotpath, update_dotpath, array_filters):
        add_fields = [
            {'$addFields': {  # >= 3.4
                f'{filter_dotpath}.coordinates': [f'${filter_dotpath}.coordinates']
            }}
        ] * depth

        col.aggregate([
            {'$match': {
                filter_dotpath: {"$ne": None},
                f'{filter_dotpath}.type': from_type
            }},
            *add_fields,
            {'$addFields': {  # >= 3.4
                f'{filter_dotpath}.type': to_type
            }},
            {'$out': col.name}  # >= 2.6
        ])

    def upd_embedded(col, doc, filter_dotpath):
        if isinstance(doc, dict) and isinstance(doc.get(updater.field_name), dict):
            match = doc[updater.field_name].get('type') == from_type \
                    and doc[updater.field_name].get('coordinates')
            if match:
                doc[updater.field_name]['coordinates'] = functools.reduce(
                    lambda x, y: [x],
                    range(depth),
                    doc[updater.field_name].get('coordinates', [.0, .0])
                )

    updater.update_by_path_or_by_document(upd_doc, upd_embedded)


@mongo_version(min_version='3.4', throw_error=True)
def __decrease_geojson_nesting(updater: DocumentUpdater,
                               from_type: str,
                               to_type: str,
                               depth: int = 1):
    """
    Extract the first element from nested arrays in `coordinates` field
    on GeoJSON fields with given type
    :param updater: DocumentUpdater object
    :param from_type: GeoJSON type to change
    :param to_type: this GeoJSON type will be set in changed records
    :param depth: nested array depth to extract from
    :return:
    """
    assert depth > 0

    def upd_doc(col, filter_dotpath, update_dotpath, array_filters):
        add_fields = [
            {'$addFields': {  # >= 3.4
                # $arrayElemAt >= 3.2
                f'{filter_dotpath}.coordinates': {
                    "$arrayElemAt": [f'${filter_dotpath}.coordinates', 0]
                }
            }},
        ] * depth

        col.aggregate([
            {'$match': {
                filter_dotpath: {"$ne": None},
                f'{filter_dotpath}.type': from_type
            }},
            *add_fields,
            {'$addFields': {  # >= 3.4
                f'{filter_dotpath}.type': to_type
            }},
            {'$out': col.name}  # >= 2.6
        ])

    def upd_embedded(col, doc, filter_dotpath):
        if isinstance(doc, dict) and isinstance(doc.get(updater.field_name), dict):
            match = doc[updater.field_name].get('type') == from_type \
                    and doc[updater.field_name].get('coordinates')
            if match:
                doc[updater.field_name]['coordinates'] = functools.reduce(
                    lambda x, y: x[0] if x and isinstance(x, (list, tuple)) else None,
                    range(depth),
                    doc[updater.field_name].get('coordinates', [.0, .0])
                )

    updater.update_by_path_or_by_document(upd_doc, upd_embedded)


@mongo_version(min_version='3.6', throw_error=True)
def __check_geojson_objects(updater: DocumentUpdater, geojson_types: List[str]):
    """
    Check if all object values in field are GeoJSON objects of given
    types. Raise MigrationError if other objects found
    :param updater:
    :param geojson_types:
    :return:
    """
    def upd(col, filter_dotpath, update_dotpath, array_filters):
        fltr = {"$and": [
            {filter_dotpath: {"$ne": None}},
            {f'{filter_dotpath}.type': {'$nin': geojson_types}},
            # $expr >= 3.6
            {"$expr": {"$eq": [{"$type": f'${filter_dotpath}'}, 'object']}}
        ]}
        check_empty_result(col, filter_dotpath, fltr)

    updater.update_by_path(upd)


@mongo_version(min_version='3.6', throw_error=True)
def __check_legacy_point_coordinates(updater: DocumentUpdater):
    """
    Check if all array values in field has legacy geo point
    coordinates type. Raise MigrationError if other arrays was found
    :param updater:
    :return:
    """
    def upd(col, filter_dotpath, update_dotpath, array_filters):
        fltr = {"$and": [
            {filter_dotpath: {"$ne": None}},
            # $expr >= 3.6, $isArray >= 3.2
            {"$expr": {"$eq": [{"$isArray": f"${filter_dotpath}"}, True]}},
            {"$expr": {"$ne": [{"$size": f"${filter_dotpath}"}, 2]}},  # $expr >= 3.6
            # TODO: add element type check
        ]}
        check_empty_result(col, filter_dotpath, fltr)

    updater.update_by_path(upd)


@mongo_version(min_version='3.6', throw_error=True)
def __check_value_types(updater: DocumentUpdater, allowed_types: List[str]):
    """
    Check if given field contains only given types of value.
    Raise if other value types was found
    :param updater:
    :param allowed_types:
    :return:
    """
    def upd(col, filter_dotpath, update_dotpath, array_filters):
        # Check for data types other than objects or arrays
        fltr = {"$and": [
            {filter_dotpath: {"$ne": None}},
            # $expr >= 3.6, $type >= 3.4
            {"$expr": {"$not": [{"$in": [{"$type": f'${filter_dotpath}'}, allowed_types]}]}}
        ]}
        check_empty_result(col, filter_dotpath, fltr)

    updater.update_by_path(upd)
