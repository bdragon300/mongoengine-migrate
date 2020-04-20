from . import converters
from mongoengine import fields

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
