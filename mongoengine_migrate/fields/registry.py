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


# StringField
# URLField
# EmailField  -- implement
# IntField
# LongField
# FloatField
# DecimalField
# BooleanField
# DateTimeField
# DateField
# ComplexDateTimeField -- implement
# EmbeddedDocumentField
# GenericEmbeddedDocumentField -- ???
# DynamicField
# ListField
# EmbeddedDocumentListField
# -SortedListField
# DictField
# -MapField
# ReferenceField
# CachedReferenceField
# GenericReferenceField -- ???
# BinaryField
# SequenceField -- implement
# UUIDField -- implement. May be either binary or text
# LazyReferenceField
# GenericLazyReferenceField -- ???
#
# ObjectIdField
#
#
# GeoPointField
# PointField
# LineStringField
# PolygonField
# MultiPointField
# MultiLineStringField
# MultiPolygonField
#
#
# FileField
# ImageField
#


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
    fields.ComplexDateTimeField: converters.to_complex_datetime
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
    fields.CachedReferenceField: converters.ref_to_cached_reference
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
        fields.CachedReferenceField: converters.nothing
        # TODO: to DynamicField: val["_cls"] = cls.__name__
        # fields.GeoJsonBaseField: converters.dict_to_geojson,
    },
    # DynamicField can contain any type, so no convertation is requried
    fields.DynamicField: {
        fields.BaseField: converters.nothing,
        fields.UUIDField: converters.to_uuid,
        fields.URLField: converters.to_url_string,
        fields.ComplexDateTimeField: converters.to_complex_datetime
    },
    fields.ListField: {
        fields.EmbeddedDocumentField: converters.deny,  # TODO: implement convert to item with embedded docs check with schema
        fields.EmbeddedDocumentListField: converters.deny,  # TODO: implement embedded docs check with schema
        fields.DictField: converters.extract_from_list,  # FIXME: it's may be not a dict after extraction
        # fields.GeoJsonBaseField: converters.list_to_geojson
    },
    fields.EmbeddedDocumentListField: {
        fields.EmbeddedDocumentField: converters.nothing,
        fields.ListField: converters.nothing,
        fields.DictField: converters.extract_from_list,
        fields.CachedReferenceField: converters.extract_from_list
        # fields.GeoJsonBaseField: converters.list_to_geojson
    },
    fields.DictField: {
        fields.EmbeddedDocumentField: converters.deny,  # TODO: implement embedded docs check with schema
        fields.ListField: converters.item_to_list,
        fields.EmbeddedDocumentListField: converters.deny,  # TODO: implement convert to list with embedded docs check with schema
        fields.CachedReferenceField: converters.deny
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
        fields.DictField: converters.nothing
    },
    fields.BinaryField: {
        fields.UUIDField: converters.to_uuid
        # TODO: image field, file field
    },
    # Sequence field just points to another counter field, so do nothing
    fields.SequenceField: {
        fields.BaseField: converters.nothing
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
