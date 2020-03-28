import weakref
from abc import ABCMeta, abstractmethod

# {mongoengine_field_cls: migration_field_cls}
mongoengine_fields_mapping = {}
schema_fields_mapping = {}


class FieldTypeMeta(ABCMeta):
    def __new__(mcs, name, bases, attrs):
        me_cls_attr = 'mongoengine_field_cls'
        me_cls = attrs.get(me_cls_attr)
        if me_cls is None:
            me_classes = [getattr(b, me_cls_attr) for b in bases if hasattr(b, me_cls_attr)]
            if me_classes:
                me_cls = me_classes[-1]

        attrs['_meta'] = weakref.proxy(mcs)

        c = super(FieldTypeMeta, mcs).__new__(mcs, name, bases, attrs)
        if me_cls is not None:
            mongoengine_fields_mapping[me_cls] = c

        type_key = attrs.get('key')
        if type_key is not None:
            schema_fields_mapping[type_key] = c

        return c


class BaseFieldType(metaclass=FieldTypeMeta):
    mongoengine_field_cls = None
    key = None

    @classmethod
    @abstractmethod
    def schema_skel(cls):
        """Returns {attr_name: default_value}"""
        pass

    @classmethod
    def field_to_schema(cls, field_obj):
        schema_skel = cls.schema_skel()
        return {f: getattr(field_obj, f, val) for f, val in schema_skel.items()}

    # @abstractmethod
    # def convert_from_type(self, from_type):
    #     """Returns pipeline or function or both??????"""
    #     pass
