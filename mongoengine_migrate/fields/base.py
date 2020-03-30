import weakref
from abc import ABCMeta, abstractmethod
from typing import Type

import mongoengine.fields

# Mongoengine field type mapping to appropriate FieldType class
# {mongoengine_field_cls: field_type_cls}
mongoengine_fields_mapping = {}

# Schema "type_key" mapping to appropriate FieldType class
# {type_key_str: field_type_cls}
schema_fields_mapping = {}


class FieldTypeMeta(ABCMeta):
    def __new__(mcs, name, bases, attrs):
        is_baseclass = name == 'BaseFieldType'
        me_cls_attr = 'mongoengine_field_cls'
        me_cls = attrs.get(me_cls_attr)
        if me_cls is None:
            me_classes = [getattr(b, me_cls_attr) for b in bases if hasattr(b, me_cls_attr)]
            if me_classes:
                me_cls = me_classes[-1]
        if me_cls is None and not is_baseclass:
            raise ValueError(f'{me_cls_attr} attribute is not set in {name}')

        attrs['_meta'] = weakref.proxy(mcs)

        klass = super(FieldTypeMeta, mcs).__new__(mcs, name, bases, attrs)
        mongoengine_fields_mapping[me_cls] = klass

        type_key = attrs.get('type_key')
        if type_key is None and not is_baseclass:
            raise ValueError(f'type_key attribute is not set in {name}')
        schema_fields_mapping[type_key] = klass

        return klass


class BaseFieldType(metaclass=FieldTypeMeta):
    mongoengine_field_cls: Type[mongoengine.fields.BaseField] = None
    type_key: str = None

    @classmethod
    @abstractmethod
    def schema_skel(cls) -> dict:
        """Return db schema skeleton dict for concrete field type"""
        pass

    @classmethod
    def build_schema(cls, field_obj: mongoengine.fields.BaseField) -> dict:
        """
        Return db schema from a given mongoengine field object
        :param field_obj: mongoengine field object
        :return: schema dict
        """
        schema_skel = cls.schema_skel()
        return {f: getattr(field_obj, f, val) for f, val in schema_skel.items()}
