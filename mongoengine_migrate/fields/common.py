from .base import BaseFieldType
from mongoengine import (
    StringField,
    IntField,
)


class StringFieldType(BaseFieldType):
    mongoengine_field_cls = StringField
    key = 'string'

    @classmethod
    def schema_skel(cls):
        fields = {'db_field', 'required', 'unique', 'unique_with', 'primary_key', 'choices',
                  'null', 'sparse', 'max_length', 'min_length', 'regex'}
        res = {f: None for f in fields}
        res['type_key'] = cls.key

        return res


class IntFieldType(BaseFieldType):
    mongoengine_field_cls = IntField
    key = 'int'

    @classmethod
    def schema_skel(cls):
        fields = {'db_field', 'required', 'unique', 'unique_with', 'primary_key', 'choices',
                  'null', 'sparse', 'max_value', 'max_value'}
        res = {f: None for f in fields}
        res['type_key'] = cls.key

        return res
