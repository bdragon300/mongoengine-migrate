from mongoengine import (
    StringField,
    IntField,
)

from .base import CommonFieldType


class StringFieldType(CommonFieldType):
    mongoengine_field_cls = StringField

    @classmethod
    def schema_skel(cls):
        params = {'max_length', 'min_length', 'regex'}
        skel = CommonFieldType.schema_skel()
        skel.update({f: None for f in params})
        return skel


class IntFieldType(CommonFieldType):
    mongoengine_field_cls = IntField

    @classmethod
    def schema_skel(cls):
        params = {'max_value', 'max_value'}
        skel = CommonFieldType.schema_skel()
        skel.update({f: None for f in params})
        return skel
