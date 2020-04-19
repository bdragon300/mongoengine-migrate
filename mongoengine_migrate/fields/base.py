import inspect
import weakref
from typing import Type, Iterable, List, Tuple

import mongoengine.fields
from pymongo.collection import Collection

from mongoengine_migrate.actions.diff import AlterDiff
from mongoengine_migrate.exceptions import SchemaError, MigrationError
from .convertion_matrix import CONVERTION_MATRIX

# Mongoengine field type mapping to appropriate FieldType class
# {mongoengine_field_name: field_type_cls}
mongoengine_fields_mapping = {}


class FieldTypeMeta(type):
    def __new__(mcs, name, bases, attrs):
        is_baseclass = name == 'CommonFieldType'
        me_classes_attr = 'mongoengine_field_classes'
        # Mongoengine field classes should be defined explicitly to
        # get to the global mapping
        me_classes = attrs.get(me_classes_attr)

        assert isinstance(me_classes, (List, Tuple)) or me_classes is None, \
            f'{me_classes_attr} must be mongoengine field classes list'

        attrs['_meta'] = weakref.proxy(mcs)

        klass = super(FieldTypeMeta, mcs).__new__(mcs, name, bases, attrs)
        if me_classes:
            mapping = {c.__name__: klass for c in me_classes}
            assert not (mapping.keys() & mongoengine_fields_mapping.keys()), \
                f'FieldType classes has duplicated mongoengine class defined in {me_classes_attr}'
            mongoengine_fields_mapping.update(mapping)
        elif is_baseclass:
            # Base FieldType class as fallback variant
            mongoengine_fields_mapping[None] = klass
        return klass


class CommonFieldType(metaclass=FieldTypeMeta):
    """FieldType used as default for mongoengine fields which does
    not have special FieldType since this class implements behavior for
    mongoengine.fields.BaseField

    Special FieldTypes should be derived from this class
    """
    mongoengine_field_classes: Iterable[Type[mongoengine.fields.BaseField]] = None

    def __init__(self,
                 collection: Collection,
                 field_schema: dict):
        self.field_schema = field_schema
        self.collection = collection
        self.db_field = field_schema.get('db_field')
        if self.db_field is None:
            raise SchemaError(f"Missed 'db_field' key in schema of collection {collection.name}")

    @classmethod
    def schema_skel(cls) -> dict:
        """
        Return db schema skeleton dict for concrete field type. This
        dict must include all keys in schema with default value (usually
        None)
        """
        # TODO: move skel to class variable
        params = {'db_field', 'required', 'default', 'unique', 'unique_with', 'primary_key',
                  'choices', 'null', 'sparse', 'type_key'}
        return {f: None for f in params}

    @classmethod
    def build_schema(cls, field_obj: mongoengine.fields.BaseField) -> dict:
        """
        Return db schema from a given mongoengine field object

        As for 'type_key' item it fills mongoengine field class name
        :param field_obj: mongoengine field object
        :return: schema dict
        """
        schema_skel = cls.schema_skel()
        schema = {f: getattr(field_obj, f, val) for f, val in schema_skel.items()}
        schema['type_key'] = field_obj.__class__.__name__

        return schema

    def change_param(self, name: str, diff: AlterDiff):
        """
        Return MongoDB pipeline which makes change of given
        parameter.

        This is a facade method which calls concrete method which
        changes given parameter. Such methods should be called as
        'change_NAME' where NAME is a parameter name.
        :param name: parameter name to change
        :param diff: AlterDiff object
        :return:
        """
        # FIXME: process UNSET value in diff
        # TODO: make change_x methods return three functions for different policies
        # FIXME: exclude 'param' from search to avoid endless recursion
        method_name = f'change_{name}'
        if hasattr(self, method_name):
            return getattr(self, method_name)(diff)
        # FIXME: change self.field_schema with diff

    # TODO: make arguments checking and that old != new
    # TODO: consider renaming before other ones
    def change_db_field(self, diff: AlterDiff):
        """
        Change db field name for a field. Simply rename this field
        :param diff:
        :return:
        """
        self.collection.update_many(
            {diff.old: {'$exists': True}},
            {'$rename': {diff.old: diff.new}}
        )
        self.db_field = diff.new

    def change_required(self, diff: AlterDiff):
        """
        Make field required, which means to add this field to all
        documents. Reverting of this doesn't require smth to do
        :param diff:
        :return:
        """
        # FIXME: consider diff.policy
        if diff.old is False and diff.new is True:
            if diff.default is None:
                raise MigrationError(f'Cannot mark field {self.collection.name}.{self.db_field} '
                                     f'as required because default value is not set')
            self.collection.update_many(
                {self.db_field: {'$exists': False}},
                {'$set': {self.db_field: diff.default}}
            )

    def change_unique(self, diff: AlterDiff):
        # TODO
        pass

    def change_unique_with(self, diff: AlterDiff):
        # TODO
        pass

    def change_primary_key(self, diff: AlterDiff):
        """
        Setting field as primary key means to set it required and unique
        :param diff:
        :return:
        """
        self.change_required(diff),
        # self.change_unique([], []) or []  # TODO

    # TODO: consider Document, EmbeddedDocument as choices
    # TODO: parameter what to do with documents where choices are not met
    def change_choices(self, diff: AlterDiff):
        """
        Set choices for a field
        :param diff:
        :return:
        """
        choices = diff.new
        if isinstance(next(iter(choices)), (list, tuple)):
            # next(iter) is useful for sets
            choices = [k for k, _ in choices]

        if diff.error_policy == 'modify':
            wrong_count = self.collection.find({self.db_field: {'$nin': choices}}).retrieved
            if wrong_count:
                raise MigrationError(f'Cannot migrate choices for '
                                     f'{self.collection.name}.{self.db_field} because '
                                     f'{wrong_count} documents with field values not in choices')
        if diff.error_policy == 'replace':
            if diff.default not in choices:
                raise MigrationError(f'Cannot set new choices for '
                                     f'{self.collection.name}.{self.db_field} because default value'
                                     f'{diff.default} does not listed in choices')
            self.collection.update_many(
                {self.db_field: {'$nin': choices}},
                {'$set': {self.db_field: diff.default}}
            )

    def change_null(self, diff: AlterDiff):
        pass

    def change_sparse(self, diff: AlterDiff):
        pass

    def change_type_key(self, diff: AlterDiff):
        """
        Change type of field. Try to convert value
        :param diff:
        :return:
        """
        def find_field_class(class_name: str,
                             field_type: CommonFieldType) -> Type[mongoengine.fields.BaseField]:
            """
            Find mongoengine field class by its name
            Return None if not class was not found
            """
            # Search in given FieldType
            if field_type.mongoengine_field_classes is not None:
                me_field_cls = [c for c in field_type.mongoengine_field_classes
                                if c.__name__ == class_name]  # FIXME: search also for user-defined fields derived from standard ones
                if me_field_cls:
                    return me_field_cls[-1]

            # Search in mongoengine itself
            klass = getattr(mongoengine.fields, class_name, None)
            if klass is not None:
                return klass

            # Search in documents retrieved from global registry
            from mongoengine.base import _document_registry
            for model_cls in _document_registry.values():
                for field_obj in model_cls._fields.values():
                    if field_obj.__class__.__name__ == class_name:
                        return field_obj.__class__

            # Cannot find anything. Return default
            return mongoengine.fields.BaseField

        old_fieldtype_cls = mongoengine_fields_mapping.get(diff.old, CommonFieldType)
        new_fieldtype_cls = mongoengine_fields_mapping.get(diff.new, CommonFieldType)

        old_field_cls = find_field_class(diff.old, old_fieldtype_cls)
        new_field_cls = find_field_class(diff.new, new_fieldtype_cls)
        if new_field_cls is mongoengine.fields.BaseField:
            raise MigrationError(f'Cannot migrate field type because cannot find {diff.new} class')

        # TODO: use diff.policy
        new_fieldtype = new_fieldtype_cls(self.collection, self.field_schema)
        new_fieldtype.convert_type(old_field_cls, new_field_cls)

    def convert_type(self,
                     from_field_cls: Type[mongoengine.fields.BaseField],
                     to_field_cls: Type[mongoengine.fields.BaseField]):
        """
        Convert field type from another to a current one. This method
        is called only if such change was requested in a migration.

        We use convertion matrix here. It contains mapping between
        old and new types and appropriate converter function which
        is called to perform such convertion.

        Old field can be either actual field type which used before.
        But in case if that field was a user defined class and does
        not exist already, the BaseField will be sent.

        New field will always have target mongoengine field type
        :param from_field_cls: mongoengine field class which was used
         before or BaseField
        :param to_field_cls: mongoengine field class which will be used
         further
        :return:
        """

        type_converters = CONVERTION_MATRIX.get(from_field_cls) or \
            CONVERTION_MATRIX.get(self._closest_parent(from_field_cls, CONVERTION_MATRIX.keys()))

        if type_converters is None:
            raise MigrationError(f'Type converter not found for convertion '
                                 f'{from_field_cls!r} -> {to_field_cls!r}')

        type_converter = type_converters.get(to_field_cls) or \
            type_converters.get(self._closest_parent(to_field_cls, type_converters))

        if type_converter is None:
            raise MigrationError(f'Type converter not found for convertion '
                                 f'{from_field_cls!r} -> {to_field_cls!r}')

        type_converter(self.collection, self.db_field, from_field_cls, to_field_cls)

    @staticmethod
    def _closest_parent(target: Type, classes: Iterable[Type]) -> Type:
        """
        Find which class in given list is the closest parent to
        a target class.
        :param target: class which we are comparing of
        :param classes:
        :return: the closest parent or None if not found
        """
        target_mro = inspect.getmro(target)
        res = None
        min_distance = float('inf')
        for parent in classes:
            found = [x for x in enumerate(target_mro) if x[1] == parent]
            if found:
                distance, klass = found[0]
                if distance == 0:  # Skip if klass is target
                    continue
                if distance < min_distance:
                    res = klass
                    min_distance = distance

        return res
