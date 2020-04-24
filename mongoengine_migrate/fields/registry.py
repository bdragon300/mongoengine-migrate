import inspect
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


# TODO: description
class TypeKeyRegistryItem(NamedTuple):
    field_cls: Type[fields.BaseField]
    field_handler_cls: Optional[Type['CommonFieldHandler']]


type_key_registry: Dict[str, TypeKeyRegistryItem] = {}


def add_type_key(field_cls: Type[fields.BaseField]):
    """
    Add mongoengine field to type_key registry
    :param field_cls: mongoengine field class
    :return:
    """
    assert (
        inspect.isclass(field_cls) and issubclass(field_cls, fields.BaseField),
        f'{field_cls!r} is not derived from BaseField'
    )

    type_key_registry[field_cls.__name__] = TypeKeyRegistryItem(field_cls=field_cls,
                                                                field_handler_cls=None)


def add_field_handler(field_cls: Type[fields.BaseField], handler_cls):
    """
    # TODO: fix func doc here
    Add field handler to registry of appropriate mongoengine field class
    :param field_cls:
    :param handler_cls:
    :return:
    """
    if field_cls not in type_key_registry:
        raise ValueError(f'Could not find {field_cls!r} or one of its base classes '
                         f'in type_key registry')

    # TODO: comment what going on here
    for fname, (fcls, ftypecls) in type_key_registry.items():
        if ftypecls is None or issubclass(field_cls, fcls):
            type_key_registry[fname].field_handler_cls = handler_cls


# Fill out the type key registry with mongoengine fields
for name, member in inspect.getmembers(fields):
    if not inspect.isclass(member) or not issubclass(member, fields.BaseField):
        continue

    add_type_key(member)


# TODO: description required
COMMON_CONVERTERS = {
    fields.StringField: converters.to_string,
    fields.IntField: converters.to_int,
    fields.LongField: converters.to_long,
    fields.FloatField: converters.to_double,
    fields.DecimalField: converters.to_decimal,
    fields.BooleanField: converters.to_bool,
    fields.DateTimeField: converters.to_date,
    fields.ListField: converters.item_to_list,
    fields.EmbeddedDocumentListField: converters.deny,
}

OBJECTID_CONVERTERS = {
    fields.StringField: converters.to_string,
    fields.EmbeddedDocumentField: converters.deny,  # TODO: implement embedded documents
    fields.ListField: converters.item_to_list,
    fields.EmbeddedDocumentListField: converters.deny,  # TODO: implement embedded documents
    fields.ReferenceField: converters.nothing,
    fields.LazyReferenceField: converters.nothing,
    fields.ObjectIdField: converters.nothing
}


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
        # fields.GeoJsonBaseField: converters.dict_to_geojson,
    },
    # DynamicField can contain any type, so no convertation is requried
    fields.DynamicField: {
        fields.BaseField: converters.nothing,
        fields.UUIDField: converters.to_uuid
    },
    fields.ListField: {
        fields.EmbeddedDocumentField: converters.deny,  # TODO: implement embedded documents
        fields.EmbeddedDocumentListField: converters.deny,  # TODO: implement embedded documents
        fields.DictField: converters.extract_from_list,
        # fields.GeoJsonBaseField: converters.list_to_geojson
    },
    fields.EmbeddedDocumentListField: {
        fields.EmbeddedDocumentField: converters.nothing,
        fields.ListField: converters.nothing,
        fields.DictField: converters.extract_from_list,
        # fields.GeoJsonBaseField: converters.list_to_geojson
    },
    fields.DictField: {
        fields.EmbeddedDocumentField: converters.deny,  # TODO: implement embedded documents
        fields.ListField: converters.item_to_list,
        fields.EmbeddedDocumentListField: converters.deny,  # TODO: implement embedded documents
        # fields.GeoJsonBaseField: converters.dict_to_geojson,
    },
    fields.ReferenceField: OBJECTID_CONVERTERS.copy(),
    fields.LazyReferenceField: OBJECTID_CONVERTERS.copy(),
    fields.ObjectIdField: OBJECTID_CONVERTERS.copy(),
    fields.CachedReferenceField: {
        # TODO
    },
    fields.BinaryField: {
        fields.UUIDField: converters.to_uuid
        # TODO: image field, file field
    },
    # Sequence field just points to another counter field, so do nothing
    fields.SequenceField: {
        fields.BaseField: converters.nothing
    },
    # Leave field as is if field type is unknown
    fields.BaseField: {}
}


for klass, converters_mapping in CONVERTION_MATRIX.items():
    # Add fallback converter for unknown fields
    CONVERTION_MATRIX[klass][fields.BaseField] = converters.nothing

    # Add boolean converter for all fields
    CONVERTION_MATRIX[klass][fields.BooleanField] = converters.to_bool

    # Drop field during convertion to SequenceField
    CONVERTION_MATRIX[klass][fields.SequenceField] = converters.drop_field

    # Add convertion between class and its parent/child class
    CONVERTION_MATRIX[klass][klass] = converters.nothing
