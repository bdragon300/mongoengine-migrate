__all__ = [
    'TypeKeyRegistryItem',
    'type_key_registry',
    'add_type_key',
    'add_field_handler',
    'CONVERTION_MATRIX'
]

import decimal
import inspect
from datetime import datetime, date
from functools import partial
from typing import Dict, Type, Optional, NamedTuple

import bson
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
#   any type (including dynamic ref, dynamic document, manual ref.
#     Embedded documents always are kept as dynamic document)
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
# LazyReferenceField:
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


DENY_BASES = {
    # Do not set converters for BooleanField, DynamicField,
    # SequenceField since they are set below
    fields.ObjectIdField: converters.deny,
    fields.StringField: converters.deny,   # + URLField, EmailField, ComplexDateTimeField
    fields.IntField: converters.deny,
    fields.LongField: converters.deny,
    fields.FloatField: converters.deny,
    fields.DecimalField: converters.deny,
    fields.DateTimeField: converters.deny,  # + DateField, ComplexDateTimeField
    fields.EmbeddedDocumentField: converters.deny,
    fields.GenericEmbeddedDocumentField: converters.deny,
    fields.ListField: converters.deny,  # + EmbeddedDocumentListField, SortedListField
    fields.DictField: converters.deny,  # + MapField
    fields.ReferenceField: converters.deny,
    fields.CachedReferenceField: converters.deny,
    fields.GenericReferenceField: converters.deny,  # + GenericLazyReferenceField
    fields.BinaryField: converters.deny,
    fields.FileField: converters.deny,  # + ImageField
    fields.UUIDField: converters.deny,
    fields.GeoPointField: converters.deny,
    fields.GeoJsonBaseField: converters.deny,  # All geo fields except GeoPointField
    fields.LazyReferenceField: converters.deny,
}


COMMON_CONVERTERS = {
    **DENY_BASES,
    fields.StringField: converters.to_string,
    fields.IntField: converters.to_int,
    fields.LongField: converters.to_long,
    fields.FloatField: converters.to_double,
    fields.DecimalField: converters.to_decimal,
    fields.DateTimeField: converters.to_date,  # + DateField
    fields.ComplexDateTimeField: converters.to_complex_datetime,
    fields.ListField: converters.item_to_list,  # + SortedListField
    fields.EmbeddedDocumentListField: converters.deny,  # Override ListField converter
}

OBJECTID_CONVERTERS = {
    **DENY_BASES,
    fields.ObjectIdField: converters.to_object_id,
    fields.StringField: converters.to_string,
    fields.URLField: converters.deny,  # Override StringField converter
    fields.EmailField: converters.deny,  # Override StringField converter
    fields.ComplexDateTimeField: converters.deny,  # Override StringField converter
    fields.ListField: converters.item_to_list,
    fields.ReferenceField: converters.to_dbref,
    fields.CachedReferenceField: converters.to_manual_ref,
    fields.FileField: converters.to_object_id,
    fields.LazyReferenceField: converters.to_dbref,
    fields.GenericReferenceField: converters.to_dynamic_ref  # + GenericLazyReferenceField
}


def get_geojson_converters(from_type):
    return {
        **DENY_BASES,
        fields.ListField: converters.item_to_list,
        fields.EmbeddedDocumentListField: converters.deny,  # Override ListField converter
        fields.DictField: converters.nothing,  # + MapField
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
#: Deny convertion from unknown general dicts to embedded documents,
#: manual ref, dynamic document, geojson fields because these dicts
#: have unknown structure. Also deny convertion general lists to
#: legacy geo fields, embedded document list fields for the same reason
#:
#: Format: {field_type1: {field_type2: converter_function, ...}, ...}
#:
CONVERTION_MATRIX = {
    fields.ObjectIdField: OBJECTID_CONVERTERS.copy(),
    fields.StringField: {
        **COMMON_CONVERTERS,
        fields.ObjectIdField: converters.to_object_id,
        fields.URLField: partial(converters.to_url_string, check_only=True),
        fields.EmailField: partial(converters.to_email_string, check_only=True),
        fields.ReferenceField: converters.to_dbref,
        fields.GenericReferenceField: converters.to_dbref,  # + GenericLazyReferenceField
        fields.FileField: converters.to_object_id,  # + ImageField
        fields.UUIDField: converters.to_uuid_str,
        fields.LazyReferenceField: converters.to_dbref,
    },
    fields.IntField: COMMON_CONVERTERS.copy(),
    fields.LongField: COMMON_CONVERTERS.copy(),
    fields.FloatField: COMMON_CONVERTERS.copy(),
    fields.DecimalField: COMMON_CONVERTERS.copy(),
    fields.BooleanField: {
        **COMMON_CONVERTERS,
        fields.DateTimeField: converters.deny,
        fields.ComplexDateTimeField: converters.deny
    },
    fields.DateTimeField: COMMON_CONVERTERS.copy(),

    fields.EmbeddedDocumentField: {
        # Forbid convertion to DynamicField since it requires
        # to be dynamic document (with '_cls' dict key) if it contains
        # embedded document
        **DENY_BASES,
        fields.DynamicField: converters.deny,
        fields.ListField: converters.item_to_list,
        fields.EmbeddedDocumentListField: converters.item_to_list,
        fields.DictField: converters.nothing,  # + MapField
        fields.ReferenceField: converters.deny,  # TODO: convert reference to embedded
    },
    fields.GenericEmbeddedDocumentField: {
        **DENY_BASES,
        fields.EmbeddedDocumentField: converters.remove_cls_key,
        fields.ListField: converters.item_to_list,
        fields.EmbeddedDocumentListField: partial(converters.item_to_list, remove_cls_key=True),
        fields.DictField: converters.nothing,  # + MapField
    },

    fields.DynamicField: {
        **COMMON_CONVERTERS,
        fields.ObjectIdField: converters.to_object_id,
        fields.URLField: converters.to_url_string,
        fields.EmailField: converters.to_email_string,
        fields.ComplexDateTimeField: converters.to_complex_datetime,
        fields.DictField: converters.to_object,  # + MapField
        fields.ReferenceField: converters.to_dbref,
        fields.GenericReferenceField: converters.to_dbref,  # + GenericLazyReferenceField
        fields.BinaryField: converters.to_binary,
        fields.FileField: converters.to_object_id,
        fields.UUIDField: converters.to_uuid_str,
        fields.LazyReferenceField: converters.to_dbref,
    },
    fields.ListField: {
        **DENY_BASES,
        fields.ObjectIdField: partial(converters.extract_from_list, bson.ObjectId),
        fields.StringField: partial(converters.extract_from_list, str),
        fields.IntField: partial(converters.extract_from_list, int),
        fields.LongField: partial(converters.extract_from_list, int),  # or bson.Int64
        fields.FloatField: partial(converters.extract_from_list, float),
        fields.DecimalField: partial(converters.extract_from_list, decimal.Decimal),
        fields.DateTimeField: partial(converters.extract_from_list, (datetime, date)),
        # Override ListField
        fields.EmbeddedDocumentListField: converters.deny,
        fields.DictField: partial(converters.extract_from_list, dict),  # + MapField
        fields.ReferenceField: partial(converters.extract_from_list, (bson.ObjectId, bson.DBRef)),
        fields.BinaryField: partial(converters.extract_from_list, bson.Binary),
        fields.FileField: partial(converters.extract_from_list, bson.ObjectId),
        fields.LazyReferenceField: partial(converters.extract_from_list,
                                           (bson.ObjectId, bson.DBRef)),
    },
    fields.EmbeddedDocumentListField: {
        **DENY_BASES,
        fields.EmbeddedDocumentField: partial(converters.extract_from_list, dict),
        fields.ListField: converters.nothing,
        fields.DictField: partial(converters.extract_from_list, dict),  # + MapField
    },
    # Forbid convertion for DictField almost everywhere because
    # typically a dict has unknown structure
    fields.DictField: {
        **DENY_BASES,
        fields.ListField: converters.item_to_list,
        # Override ListField
        fields.EmbeddedDocumentListField: converters.deny,
    },

    fields.ReferenceField: OBJECTID_CONVERTERS.copy(),
    fields.CachedReferenceField: {
        **DENY_BASES,
        fields.ObjectIdField: converters.to_object_id,
        fields.ListField: converters.item_to_list,
        # Override ListField
        fields.EmbeddedDocumentListField: converters.deny,
        fields.DictField: converters.nothing,  # + MapField
        fields.ReferenceField: converters.to_dbref,
        fields.GenericReferenceField: converters.to_dbref,  # + GenericLazyReferenceField
        fields.FileField: converters.to_object_id,
        fields.LazyReferenceField: converters.to_dbref,
    },
    fields.GenericReferenceField: OBJECTID_CONVERTERS.copy(),  # + GenericLazyReferenceField

    # BinaryField has unknown binary data which more likely
    # will not be decoded into one of value
    # User can write his own action using RunPython to convert it
    fields.BinaryField: {
        **DENY_BASES,
    },
    fields.FileField: OBJECTID_CONVERTERS.copy(),
    # Sequence field just points to another counter field, so do nothing
    fields.SequenceField: {
        fields.BaseField: converters.nothing
    },
    fields.UUIDField: {
        **DENY_BASES,
        fields.StringField: converters.to_string,
        fields.URLField: converters.deny,  # Override StringField converter
        fields.EmailField: converters.deny,  # Override StringField converter
        fields.ComplexDateTimeField: converters.deny,  # Override StringField converter
        fields.ListField: converters.item_to_list,
        fields.EmbeddedDocumentListField: converters.deny,   # Override ListField converter
        fields.BinaryField: converters.to_uuid_bin,
    },

    # Geo fields
    fields.GeoPointField: {
        **DENY_BASES,
        fields.ListField: converters.item_to_list,
        fields.EmbeddedDocumentListField: converters.deny,   # Override ListField converter
        fields.DictField: converters.nothing,
        fields.PointField: partial(converters.legacy_pairs_to_geojson, to_type='Point'),
        fields.LineStringField: partial(converters.legacy_pairs_to_geojson, to_type='LineString'),
        fields.PolygonField: partial(converters.legacy_pairs_to_geojson, to_type='Polygon'),
        fields.MultiPointField: partial(converters.legacy_pairs_to_geojson, to_type='MultiPoint'),
        fields.MultiLineStringField: partial(converters.legacy_pairs_to_geojson,
                                             to_type='MultiLineString'),
        fields.MultiPolygonField: partial(converters.legacy_pairs_to_geojson,
                                          to_type='MultiPolygon'),
    },
    fields.PointField: get_geojson_converters('Point'),
    fields.LineStringField: get_geojson_converters('LineString'),
    fields.PolygonField: get_geojson_converters('Polygon'),
    fields.MultiPointField: get_geojson_converters('MultiPoint'),
    fields.MultiLineStringField: get_geojson_converters('MultiLineString'),
    fields.MultiPolygonField: get_geojson_converters('MultiPolygonField'),

    fields.LazyReferenceField: OBJECTID_CONVERTERS.copy(),

    # Leave field as is if field type is unknown
    fields.BaseField: {}
}


for klass, converters_mapping in CONVERTION_MATRIX.items():
    # Add fallback converter for unknown fields
    CONVERTION_MATRIX[klass].setdefault(fields.BaseField, converters.nothing)

    # Add boolean converter for all fields
    # Any db value can get casted to boolean
    CONVERTION_MATRIX[klass].setdefault(fields.BooleanField, converters.to_bool)

    # Drop field during convertion to SequenceField
    CONVERTION_MATRIX[klass].setdefault(fields.SequenceField, converters.drop_field)

    # Add DynamicField converter for all fields since it can keep any
    # type of value
    CONVERTION_MATRIX[klass].setdefault(fields.DynamicField, converters.nothing)

    # Force set convertion between class and its parent/child class
    CONVERTION_MATRIX[klass][klass] = converters.nothing
