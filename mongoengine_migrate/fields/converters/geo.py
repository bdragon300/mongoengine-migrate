__all__ = [
    'convert_geojson',
    'legacy_pairs_to_geojson',
    'geojson_to_legacy_pairs'
]

import functools
from datetime import datetime, date, time
from typing import List

import bson

from mongoengine_migrate.exceptions import MigrationError, InconsistencyError
from mongoengine_migrate.mongo import (
    check_empty_result,
    mongo_version
)
from mongoengine_migrate.updater import ByPathContext, ByDocContext, DocumentUpdater

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


def convert_geojson(updater: DocumentUpdater, from_type: str, to_type: str):
    """Convert GeoJSON object from one type to another"""
    from_ind, to_ind = None, None
    if updater.migration_policy.name == 'strict':
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
        raise MigrationError(f"Unknown geo field type. Was requested: {from_type}, {to_type}")

    depth = abs(from_ind - to_ind)
    if from_ind <= to_ind:
        __increase_geojson_nesting(updater, from_type, to_type, depth)
    else:
        __decrease_geojson_nesting(updater, from_type, to_type, depth)


def legacy_pairs_to_geojson(updater: DocumentUpdater, to_type: str):
    """Convert legacy coordinate pairs to GeoJSON objects of given type"""
    def by_doc(ctx: ByDocContext):
        doc = ctx.document
        if isinstance(doc.get(updater.field_name), (list, tuple)):
            doc[updater.field_name] = {'type': 'Point', 'coordinates': doc[updater.field_name]}

    if updater.migration_policy.name == 'strict':
        __check_geojson_objects(updater, ['Point', to_type])
        __check_legacy_point_coordinates(updater)
        __check_value_types(updater, ['object', 'array'])

    updater.update_by_document(by_doc)
    convert_geojson(updater, 'Point', to_type)


def geojson_to_legacy_pairs(updater: DocumentUpdater, from_type: str):
    """Convert GeoJSON objects of given type to legacy coordinate pairs"""
    def by_doc(ctx: ByDocContext):
        doc = ctx.document
        if isinstance(doc.get(updater.field_name), dict):
            if 'Point' in doc[updater.field_name]:
                doc[updater.field_name] = doc[updater.field_name].get('coordinates')

    if updater.migration_policy.name == 'strict':
        __check_geojson_objects(updater, ["Point", from_type])
        __check_legacy_point_coordinates(updater)
        __check_value_types(updater, ['object', 'array'])

    convert_geojson(updater, from_type, 'Point')

    updater.update_by_document(by_doc)


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

    def by_doc(ctx: ByDocContext):
        doc = ctx.document
        if isinstance(doc.get(updater.field_name), dict):
            match = doc[updater.field_name].get('type') == from_type \
                    and doc[updater.field_name].get('coordinates')
            if match:
                doc[updater.field_name]['coordinates'] = functools.reduce(
                    lambda x, y: [x],
                    range(depth),
                    doc[updater.field_name].get('coordinates', [.0, .0])
                )

    updater.update_by_document(by_doc)


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

    def by_doc(ctx: ByDocContext):
        doc = ctx.document
        if isinstance(doc.get(updater.field_name), dict):
            match = doc[updater.field_name].get('type') == from_type \
                    and doc[updater.field_name].get('coordinates')
            if match:
                doc[updater.field_name]['coordinates'] = functools.reduce(
                    lambda x, y: x[0] if x and isinstance(x, (list, tuple)) else None,
                    range(depth),
                    doc[updater.field_name].get('coordinates', [.0, .0])
                )

    updater.update_by_document(by_doc)


@mongo_version(min_version='3.6')
def __check_geojson_objects(updater: DocumentUpdater, geojson_types: List[str]):
    """
    Check if all object values in field are GeoJSON objects of given
    types. Raise InconsistencyError if other objects found
    :param updater:
    :param geojson_types:
    :return:
    """
    def by_path(ctx: ByPathContext):
        fltr = {"$and": [
            {ctx.filter_dotpath: {"$ne": None}},
            *[{k: v} for k, v in ctx.extra_filter.items()],
            {f'{ctx.filter_dotpath}.type': {'$nin': geojson_types}},
            # $expr >= 3.6
            {"$expr": {"$eq": [{"$type": f'${ctx.filter_dotpath}'}, 'object']}}
        ]}
        check_empty_result(ctx.collection, ctx.filter_dotpath, fltr)

    def by_doc(ctx: ByDocContext):
        doc = ctx.document
        if updater.field_name in doc:
            f = doc[updater.field_name]
            valid = f is None or (isinstance(f, dict) and f.get('type') in geojson_types)
            if not valid:
                raise InconsistencyError(f"Field {updater.field_name} has wrong value {f!r} "
                                         f"(should be GeoJSON) in record {doc}")

    updater.update_combined(by_path, by_doc, False, False)


@mongo_version(min_version='3.6')
def __check_legacy_point_coordinates(updater: DocumentUpdater):
    """
    Check if all array values in field has legacy geo point
    coordinates type. Raise InconsistencyError if other arrays was found
    :param updater:
    :return:
    """
    def by_path(ctx: ByPathContext):
        fltr = {"$and": [
            {ctx.filter_dotpath: {"$ne": None}},
            *[{k: v} for k, v in ctx.extra_filter.items()],
            # $expr >= 3.6, $isArray >= 3.2
            {"$expr": {"$eq": [{"$isArray": f"${ctx.filter_dotpath}"}, True]}},
            {"$expr": {"$ne": [{"$size": f"${ctx.filter_dotpath}"}, 2]}},  # $expr >= 3.6
            # TODO: add element type check
        ]}
        check_empty_result(ctx.collection, ctx.filter_dotpath, fltr)

    def by_doc(ctx: ByDocContext):
        doc = ctx.document
        if updater.field_name in doc:
            f = doc[updater.field_name]
            valid = f is None or (isinstance(f, (list, tuple)) and len(f) == 2)
            if not valid:
                raise InconsistencyError(f"Field {updater.field_name} has wrong value {f!r} "
                                         f"(should be legacy geo point) in record {doc}")

    updater.update_combined(by_path, by_doc, False, False)


@mongo_version(min_version='3.6')
def __check_value_types(updater: DocumentUpdater, allowed_types: List[str]):
    """
    Check if given field contains only given types of value.
    Raise InconsistencyError if other value types was found
    :param updater:
    :param allowed_types:
    :return:
    """
    def by_path(ctx: ByPathContext):
        # Check for data types other than objects or arrays
        fltr = {"$and": [
            {ctx.filter_dotpath: {"$ne": None}},
            *[{k: v} for k, v in ctx.extra_filter.items()],
            # $expr >= 3.6, $type >= 3.4
            {"$expr": {"$not": [{"$in": [{"$type": f'${ctx.filter_dotpath}'}, allowed_types]}]}}
        ]}
        check_empty_result(ctx.collection, ctx.filter_dotpath, fltr)

    def by_doc(ctx: ByDocContext):
        # https://docs.mongodb.com/manual/reference/operator/aggregation/convert/
        type_map = {
            'double': float,
            'string': str,
            'objectId': bson.ObjectId,
            'bool': bool,
            'date': datetime,
            'int': int,
            'long': int,
            'decimal': float
        }
        assert set(allowed_types) < type_map.keys()

        doc = ctx.document
        if updater.field_name in doc:
            f = doc[updater.field_name]
            valid_types = tuple(type_map[t] for t in allowed_types)
            valid = f is None or isinstance(f, valid_types)
            if not valid:
                raise InconsistencyError(f"Field {updater.field_name} has wrong type of value "
                                         f"{f!r} (should be any of {valid_types}) in record {doc}")

    updater.update_combined(by_path, by_doc, False, False)
