from mongoengine import (
    StringField,
    IntField,
)

from .base import CommonFieldType


class StringFieldType(CommonFieldType):
    mongoengine_field_cls = StringField
    type_key = 'string'

    @classmethod
    def schema_skel(cls):
        params = {'max_length', 'min_length', 'regex'}
        skel = super(cls).schema_skel()
        skel.update({f: None for f in params})
        return skel


class IntFieldType(CommonFieldType):
    mongoengine_field_cls = IntField
    type_key = 'int'

    @classmethod
    def schema_skel(cls):
        params = {'max_value', 'max_value'}
        skel = super(cls).schema_skel()
        skel.update({f: None for f in params})
        return skel

