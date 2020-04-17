from . import converters
from mongoengine import fields

# StringField
# URLField
# EmailField
# IntField
# LongField
# FloatField
# DecimalField
# BooleanField
# DateTimeField
# DateField -- implement
# ComplexDateTimeField -- implement
# EmbeddedDocumentField
# GenericEmbeddedDocumentField -- ???
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


COMMON_CONVERTERS = {
    fields.StringField: converters.to_string,
    fields.URLField: converters.deny,
    fields.EmailField: converters.deny,
    fields.IntField: converters.to_int,
    fields.LongField: converters.to_long,
    fields.FloatField: converters.to_double,
    fields.DecimalField: converters.to_decimal,
    fields.BooleanField: converters.to_bool,
    fields.DateTimeField: converters.to_date,
    fields.ListField: converters.item_to_list,
    fields.EmbeddedDocumentListField: converters.deny,
}


CONVERTION_MATRIX = {
    fields.StringField: {
        **COMMON_CONVERTERS,
        fields.StringField: converters.nothing,
        fields.URLField: converters.to_url_string,
        fields.EmailField: converters.to_email_string,
        fields.ObjectIdField: converters.to_object_id,
        fields.ReferenceField: converters.to_object_id,
        # fields.CachedReferenceField: converters.to_object_id,  -- dict???
        fields.LazyReferenceField: converters.to_object_id,
    },
    fields.IntField: {
        **COMMON_CONVERTERS,
        fields.IntField: converters.nothing,
    },
    fields.LongField: {
        **COMMON_CONVERTERS,
        fields.LongField: converters.nothing,
    },
    fields.FloatField: {
        **COMMON_CONVERTERS,
        fields.FloatField: converters.nothing,
    },
    fields.DecimalField: {
        **COMMON_CONVERTERS,
        fields.DecimalField: converters.nothing,
    },
    fields.BooleanField: {
        **COMMON_CONVERTERS,
        fields.BooleanField: converters.nothing,
    },
    fields.DateTimeField: {
        **COMMON_CONVERTERS,
        fields.DateTimeField: converters.nothing,
    },
    fields.EmbeddedDocumentField: {
        fields.BooleanField: converters.cast_to_bool,
        fields.EmbeddedDocumentField: converters.nothing,
        fields.DictField: converters.nothing,  # Also for MapField
        fields.ReferenceField: converters.deny,  # TODO: implement convert reference-like fields from/to embedded-like
        fields.ListField: converters.item_to_list,
        fields.EmbeddedDocumentListField: converters.item_to_list,
        fields.GeoJsonBaseField: converters.dict_to_geojson,
    },
    fields.ListField: {
        fields.BooleanField: converters.cast_to_bool,
        fields.EmbeddedDocumentField: converters.deny,  # TODO: implement embedded documents
        fields.ListField: converters.nothing,
        fields.EmbeddedDocumentListField: converters.deny,  # TODO: implement embedded documents
        fields.DictField: converters.extract_from_list,
        fields.GeoJsonBaseField: converters.list_to_geojson
    },
    fields.EmbeddedDocumentListField: {
        fields.BooleanField: converters.cast_to_bool,
        fields.EmbeddedDocumentField: converters.nothing,
        fields.ListField: converters.nothing,
        fields.EmbeddedDocumentListField: converters.nothing,
        fields.DictField: converters.extract_from_list,
        fields.GeoJsonBaseField: converters.list_to_geojson
    },
    fields.DictField: {
        fields.BooleanField: converters.cast_to_bool,
        fields.EmbeddedDocumentField: converters.deny,  # TODO: implement embedded documents
        fields.ListField: converters.item_to_list,
        fields.EmbeddedDocumentListField: converters.deny,  # TODO: implement embedded documents
        fields.DictField: converters.nothing,
        fields.GeoJsonBaseField: converters.dict_to_geojson,
    },
    (fields.ReferenceField, fields.LazyReferenceField, fields.ObjectId): {
        fields.StringField: converters.to_string,
        fields.BooleanField: converters.cast_to_bool,
        fields.EmbeddedDocumentField: converters.deny,  # TODO: implement embedded documents
        fields.ListField: converters.item_to_list,
        fields.EmbeddedDocumentListField: converters.deny,  # TODO: implement embedded documents
        fields.ReferenceField: converters.nothing,
        fields.LazyReferenceField: converters.nothing,
        fields.ObjectIdField: converters.nothing
    },
    fields.CachedReferenceField: {},  # TODO
    fields.BinaryField: {
        fields.BooleanField: converters.cast_to_bool,
        # TODO: image field, file field
    },
}