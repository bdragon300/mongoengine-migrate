__all__ = [
    'TypeKeyRegistryItem',
    'type_key_registry',
    'add_type_key',
    'add_field_handler',
    'CONVERTION_MATRIX'
]

import inspect
from functools import partial
from typing import Dict, Type, Optional, NamedTuple

from mongoengine import fields

from . import converters

#
# Description of data kept in MongoDB fields which operated by
# appropriate mongoengine fields
#
# A field can be null which is valid value for any type
#
# Definitions (SON==dict):
# * manual ref: dict({'_id': ObjectId(), **other_keys})
#   https://docs.mongodb.com/manual/reference/database-references/#document-references
# * dynamic ref: dict({'_cls': _class_name, '_ref': DBRef({"$ref" : collection_name, "$id" : id})})
# * dynamic document: dict({'_cls': _class_name, **other_keys})
#   used for document inheritance
# * GeoJSON: dict('type': '<type>', 'coordinates': [list, with, coordinates])
#   'type' can be: Point, MultiPoint, LineString, Polygon, MultiLineString, MultiPolygon
#    https://docs.mongodb.com/manual/reference/geojson/
#    https://docs.mongodb.com/manual/geospatial-queries/
#
# ObjectIdField:
#   ObjectId
# StringField:
#   string
# URLField(StringField):
#   string, matched to url pattern which is set by field params
# EmailField(StringField):
#   string, matched to email pattern which is set by field params
# IntField:
#   32-bit integer
#   Acceptable: 64-bit integer
#   Acceptable: double (mongo shell treats integer as double by default)
# LongField:
#   64-bit integer
#   Acceptable: 32-bit integer
#   Acceptable: double (mongo shell treats integer as double by default)
# FloatField:
#   double
#   Acceptable: 32-bit integer
#   Acceptable: 64-bit integer
# DecimalField:
#   double (if force_string=False)
#   string (string representation with dot, if force_string=True)
#   Acceptable: 32-bit integer
#   Acceptable: 64-bit integer
# BooleanField:
#   boolean
# DateTimeField:
#   date (ISODate)
#   Acceptable: string with datetime
# DateField(DateTimeField):
#   date (ISODate)
#   Acceptable: string with datetime
# ComplexDateTimeField(StringField)
#   string with datetime which is set by field params
# EmbeddedDocumentField:
#   dict (any keys without '_id')
#   dynamic document (any keys without '_id')
# GenericEmbeddedDocumentField:
#   dynamic document
# DynamicField:
#   any type (including dynamic ref, dynamic document, manual ref)
# ListField:
#   list
# EmbeddedDocumentListField(ListField)
#   list
# SortedListField(ListField)
#   list
# DictField
#   dict
# MapField(DictField):
#   dict (with restriction to dict values type)
# ReferenceField:
#   ObjectId (if dbref=False)
#   DBRef (if dbref=True)
#   DBRef with `cls` (without underscore) (if dbref=True)
# CachedReferenceField:
#   manual ref
# GenericReferenceField:
#   ObjectId
#   DBRef
#   dynamic ref
# BinaryField:
#   Binary
# FileField:
#   ObjectId (id to `files` collection entry)
# ImageField(FileField):
#   ObjectId (id to `files` collection entry)
# SequenceField (virtual field, it points to a collection with counters)
#   nothing
# UUIDField:
#   Binary (binary=True)
#   string with uuid (binary=False)
# GeoPointField:
#   2-element list with doubles or 32-bit integers
# PointField:
#   GeoJSON(Point)
# LineStringField:
#   GeoJSON(LineString)
# PolygonField:
#   GeoJSON(Polygon)
# MultiPointField:
#   GeoJSON(MultiPoint)
# MultiLineStringField:
#   GeoJSON(MultiLineString)
# MultiPolygonField:
#   GeoJSON(MultiPolygon)
# LazyReferenceField
#   ObjectId (if dbref=False)
#   DBRef (if dbref=True)
# GenericLazyReferenceField(GenericReferenceField):
#   ObjectId
#   DBRef
#   dynamic ref


class TypeKeyRegistryItem(NamedTuple):
    """
    Information of classes for a type key
    """
    field_cls: Type[fields.BaseField]
    field_handler_cls: Optional[Type['CommonFieldHandler']]


#: Registry of available `type_key` values which can be used in schema.
#: And appropriate mongoengine field class and handler class
#:
#: This registry is filled out with all available mongoengine field
#: classes. Type key is a name of mongoengine field class. As a handler
#: there is used handler associated with this field or CommonFieldHander
type_key_registry: Dict[str, TypeKeyRegistryItem] = {}


def add_type_key(field_cls: Type[fields.BaseField]):
    """
    Add mongoengine field to type_key registry
    :param field_cls: mongoengine field class
    :return:
    """
    assert inspect.isclass(field_cls) and issubclass(field_cls, fields.BaseField), \
        f'{field_cls!r} is not a class derived from BaseField'

    type_key_registry[field_cls.__name__] = TypeKeyRegistryItem(field_cls=field_cls,
                                                                field_handler_cls=None)


def add_field_handler(field_cls: Type[fields.BaseField], handler_cls: Type['CommonFieldHandler']):
    """
    Add field handler to the type_key registry for a given field
    :param field_cls:
    :param handler_cls:
    :return:
    """
    if field_cls.__name__ not in type_key_registry:
        raise ValueError(f'Could not add handler {handler_cls!r} for unknown mongoengine field '
                         f'class {field_cls!r}')

    # Handlers can be added in any order
    # So set a handler only on those registry items where no handler
    # was set or where handler is a base class of given one
    for type_key, registry_item in type_key_registry.items():
        current_handler = registry_item.field_handler_cls
        if current_handler is None or issubclass(registry_item.field_cls, field_cls):
            type_key_registry[type_key] = TypeKeyRegistryItem(
                field_cls=registry_item.field_cls,
                field_handler_cls=handler_cls
            )


# Fill out the type key registry with all mongoengine fields
for name, member in inspect.getmembers(fields):
    if not inspect.isclass(member) or not issubclass(member, fields.BaseField):
        continue

    add_type_key(member)


COMMON_CONVERTERS = {
    fields.StringField: converters.to_string,
    fields.URLField: converters.to_url_string,
    fields.IntField: converters.to_int,
    fields.LongField: converters.to_long,
    fields.FloatField: converters.to_double,
    fields.DecimalField: converters.to_decimal,
    fields.BooleanField: converters.to_bool,
    fields.DateTimeField: converters.to_date,
    fields.ListField: converters.item_to_list,
    fields.EmbeddedDocumentListField: converters.deny,
    fields.ComplexDateTimeField: converters.to_complex_datetime,
    fields.GenericEmbeddedDocumentField: converters.deny,
    fields.GenericReferenceField: converters.deny
}

OBJECTID_CONVERTERS = {
    fields.StringField: converters.to_string,
    fields.URLField: converters.to_url_string,
    fields.EmbeddedDocumentField: converters.deny,
    fields.ListField: converters.item_to_list,
    fields.EmbeddedDocumentListField: converters.deny,
    fields.ReferenceField: converters.nothing,  # FIXME: it could be dbref
    fields.LazyReferenceField: converters.nothing,  # FIXME: it could be dbref
    fields.ObjectIdField: converters.nothing,
    fields.FileField: converters.nothing,
    fields.ImageField: converters.nothing,
    fields.CachedReferenceField: converters.ref_to_cached_reference,
    fields.GenericEmbeddedDocumentField: converters.deny,
    fields.GenericReferenceField: converters.deny
}


def get_geojson_converters(from_type):
    return {
        fields.DictField: converters.nothing,  # Also for MapField
        fields.GeoPointField: partial(converters.geojson_to_legacy_pairs, from_type=from_type),
        fields.PointField: partial(
            converters.convert_geojson,
            from_type=from_type,
            to_type='Point'
        ),
        fields.LineStringField: partial(
            converters.convert_geojson,
            from_type=from_type,
            to_type='LineString'
        ),
        fields.PolygonField: partial(
            converters.convert_geojson,
            from_type=from_type,
            to_type='Polygon'
        ),
        fields.MultiPointField: partial(
            converters.convert_geojson,
            from_type=from_type,
            to_type='MultiPoint'
        ),
        fields.MultiLineStringField: partial(
            converters.convert_geojson,
            from_type=from_type,
            to_type='MultiLineString'
        ),
        fields.MultiPolygonField: partial(
            converters.convert_geojson,
            from_type=from_type,
            to_type='MultiPolygon'
        ),
        fields.GenericEmbeddedDocumentField: converters.deny,
        fields.GenericReferenceField: converters.deny
    }


#: Field type convertion matrix
#: Contains information which converter must be used to convert one
#: mongoengine field type to another. Used by field handlers when
#: 'type_key' schema parameter changes
#:
#: Types are searched here either as exact class equality or the nearest
#: parent. If type pair is not exists in matrix, then such convertion
#: is denied.
#:
#: Format: {field_type1: {field_type2: converter_function, ...}, ...}
#:
CONVERTION_MATRIX = {
    fields.StringField: {
        **COMMON_CONVERTERS,
        fields.ObjectIdField: converters.to_object_id,
        fields.ReferenceField: converters.to_object_id,
        # fields.CachedReferenceField: converters.to_object_id,  -- dict???
        fields.LazyReferenceField: converters.to_object_id,
        fields.UUIDField: converters.to_uuid
    },
    fields.IntField: COMMON_CONVERTERS.copy(),
    fields.LongField: COMMON_CONVERTERS.copy(),
    fields.FloatField: COMMON_CONVERTERS.copy(),
    fields.DecimalField: COMMON_CONVERTERS.copy(),
    fields.BooleanField: COMMON_CONVERTERS.copy(),
    fields.DateTimeField: COMMON_CONVERTERS.copy(),
    fields.DateField: COMMON_CONVERTERS.copy(),
    fields.EmbeddedDocumentField: {
        fields.DictField: converters.nothing,  # Also for MapField
        fields.ReferenceField: converters.deny,  # TODO: implement convert reference-like fields from/to embedded-like
        fields.ListField: converters.item_to_list,
        fields.EmbeddedDocumentListField: converters.item_to_list,
        fields.CachedReferenceField: converters.nothing,
        fields.GenericEmbeddedDocumentField: converters.deny,  # requires '_cls' dict field
        fields.GenericReferenceField: converters.deny  # requires '_cls' dict field
        # TODO: to DynamicField: val["_cls"] = cls.__name__
        # fields.GeoJsonBaseField: converters.dict_to_geojson,
    },
    # DynamicField can contain any type, so no convertation is requried
    fields.DynamicField: {
        fields.BaseField: converters.nothing,
        fields.UUIDField: converters.to_uuid,
        fields.URLField: converters.to_url_string,
        fields.ComplexDateTimeField: converters.to_complex_datetime,
        fields.GenericEmbeddedDocumentField: converters.deny,
        fields.GenericReferenceField: converters.deny
    },
    fields.ListField: {
        fields.EmbeddedDocumentField: converters.deny,  # TODO: implement convert to item with embedded docs check with schema
        fields.EmbeddedDocumentListField: converters.deny,  # TODO: implement embedded docs check with schema
        fields.DictField: converters.extract_from_list,  # FIXME: it's may be not a dict after extraction
        fields.GenericEmbeddedDocumentField: converters.deny,
        fields.GenericReferenceField: converters.deny
        # fields.GeoJsonBaseField: converters.list_to_geojson
    },
    fields.EmbeddedDocumentListField: {
        fields.EmbeddedDocumentField: converters.nothing,
        fields.ListField: converters.nothing,
        fields.DictField: converters.extract_from_list,
        fields.CachedReferenceField: converters.extract_from_list,
        fields.GenericEmbeddedDocumentField: converters.deny,
        fields.GenericReferenceField: converters.deny
        # fields.GeoJsonBaseField: converters.list_to_geojson
    },
    fields.DictField: {
        fields.EmbeddedDocumentField: converters.deny,  # TODO: implement embedded docs check with schema
        fields.ListField: converters.item_to_list,
        fields.EmbeddedDocumentListField: converters.deny,  # TODO: implement convert to list with embedded docs check with schema
        fields.CachedReferenceField: converters.deny,
        fields.GenericEmbeddedDocumentField: converters.deny,
        fields.GenericReferenceField: converters.deny
        # fields.GeoJsonBaseField: converters.dict_to_geojson,
    },
    fields.ReferenceField: OBJECTID_CONVERTERS.copy(),  # TODO: to DynamicField: val = {"_ref": value.to_dbref(), "_cls": cls.__name__}
    fields.LazyReferenceField: OBJECTID_CONVERTERS.copy(),  # TODO: to DynamicField: val = {"_ref": value.to_dbref(), "_cls": cls.__name__}
    fields.ObjectIdField: OBJECTID_CONVERTERS.copy(),
    fields.FileField: OBJECTID_CONVERTERS.copy(),
    fields.ImageField: OBJECTID_CONVERTERS.copy(),
    fields.CachedReferenceField: {  # TODO: to DynamicField: val = {"_ref": value.to_dbref(), "_cls": cls.__name__}
        fields.EmbeddedDocumentField: converters.nothing,
        fields.ListField: converters.item_to_list,
        fields.ReferenceField: converters.cached_reference_to_ref,
        fields.LazyReferenceField: converters.cached_reference_to_ref,
        fields.DictField: converters.nothing,
        fields.GenericEmbeddedDocumentField: converters.deny,  # requires '_cls' dict field
        fields.GenericReferenceField: converters.deny  # requires '_cls' dict field
    },
    fields.BinaryField: {
        fields.UUIDField: converters.to_uuid,
        fields.GenericEmbeddedDocumentField: converters.deny,
        fields.GenericReferenceField: converters.deny
        # TODO: image field, file field
    },
    # Sequence field just points to another counter field, so do nothing
    fields.SequenceField: {
        fields.BaseField: converters.nothing
    },
    # Generic* fields
    fields.GenericEmbeddedDocumentField: {
        fields.EmbeddedDocumentField: converters.nothing,
        fields.GenericReferenceField: converters.deny,
        fields.GenericLazyReferenceField: converters.deny,
        fields.DictField: converters.nothing,
        fields.BaseField: converters.deny  # FIXME: replace BaseField to many denies,
    },
    fields.GenericReferenceField: {
        fields.StringField: converters.to_string,
        fields.DynamicField: converters.nothing,
        fields.ReferenceField: converters.to_object_id,
        fields.DictField: converters.deny,
        fields.EmbeddedDocumentField: converters.deny,
        fields.GenericEmbeddedDocumentField: converters.nothing,
        fields.CachedReferenceField: converters.to_manual_ref,
        fields.LazyReferenceField: converters.to_object_id,
        fields.GenericLazyReferenceField: converters.nothing,
        fields.ObjectIdField: converters.to_object_id,
        fields.BaseField: converters.deny
    },
    fields.GenericLazyReferenceField: {
        fields.StringField: converters.to_string,
        fields.DynamicField: converters.nothing,
        fields.ReferenceField: converters.to_object_id,
        fields.DictField: converters.deny,
        fields.EmbeddedDocumentField: converters.deny,
        fields.GenericEmbeddedDocumentField: converters.nothing,
        fields.GenericReferenceField: converters.nothing,
        fields.CachedReferenceField: converters.to_manual_ref,
        fields.LazyReferenceField: converters.to_object_id,
        fields.GenericLazyReferenceField: converters.nothing,
        fields.ObjectIdField: converters.to_object_id,
        fields.BaseField: converters.deny
    },
    # Geo fields
    fields.GeoPointField: {
        fields.PointField: partial(converters.legacy_pairs_to_geojson, to_type='Point'),
        fields.LineStringField: partial(converters.legacy_pairs_to_geojson, to_type='LineString'),
        fields.PolygonField: partial(converters.legacy_pairs_to_geojson, to_type='Polygon'),
        fields.MultiPointField: partial(converters.legacy_pairs_to_geojson, to_type='MultiPoint'),
        fields.MultiLineStringField: partial(converters.legacy_pairs_to_geojson,
                                             to_type='MultiLineString'),
        fields.MultiPolygonField: partial(converters.legacy_pairs_to_geojson,
                                          to_type='MultiPolygon'),
        fields.EmbeddedDocumentField: converters.deny,
        fields.GenericReferenceField: converters.deny,
        fields.GenericLazyReferenceField: converters.deny,
    },
    fields.PointField: get_geojson_converters('Point'),
    fields.LineStringField: get_geojson_converters('LineString'),
    fields.PolygonField: get_geojson_converters('Polygon'),
    fields.MultiPointField: get_geojson_converters('MultiPoint'),
    fields.MultiLineStringField: get_geojson_converters('MultiLineString'),
    fields.MultiPolygonField: get_geojson_converters('MultiPolygonField'),
    # Leave field as is if field type is unknown
    fields.BaseField: {}
}


for klass, converters_mapping in CONVERTION_MATRIX.items():
    # Add fallback converter for unknown fields
    CONVERTION_MATRIX[klass].setdefault(fields.BaseField, converters.nothing)

    # Add boolean converter for all fields
    CONVERTION_MATRIX[klass].setdefault(fields.BooleanField, converters.to_bool)

    # Drop field during convertion to SequenceField
    CONVERTION_MATRIX[klass].setdefault(fields.SequenceField, converters.drop_field)

    # Force set convertion between class and its parent/child class
    CONVERTION_MATRIX[klass][klass] = converters.nothing
