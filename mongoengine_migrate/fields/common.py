from mongoengine import (
    StringField,
    IntField,
)

from .base import BaseFieldType


class StringFieldType(BaseFieldType):
    mongoengine_field_cls = StringField
    type_key = 'string'

    @classmethod
    def schema_skel(cls):
        fields = {'db_field', 'required', 'unique', 'unique_with', 'primary_key', 'choices',
                  'null', 'sparse', 'max_length', 'min_length', 'regex', 'default'}
        res = {f: None for f in fields}
        res['type_key'] = cls.type_key

        return res


class IntFieldType(BaseFieldType):
    mongoengine_field_cls = IntField
    type_key = 'int'

    @classmethod
    def schema_skel(cls):
        fields = {'db_field', 'required', 'unique', 'unique_with', 'primary_key', 'choices',
                  'null', 'sparse', 'max_value', 'max_value', 'default'}
        res = {f: None for f in fields}
        res['type_key'] = cls.type_key

        return res
