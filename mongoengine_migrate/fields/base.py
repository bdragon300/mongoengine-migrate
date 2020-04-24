import inspect
import weakref
from typing import Type, Iterable, List, Tuple, Collection

import mongoengine.fields
from pymongo.collection import Collection as MongoCollection

from mongoengine_migrate.actions.diff import AlterDiff, UNSET
from mongoengine_migrate.exceptions import SchemaError, MigrationError
from mongoengine_migrate.fields.registry import type_key_registry, add_field_handler
from mongoengine_migrate.utils import get_closest_parent
from .registry import CONVERTION_MATRIX


class FieldHandlerMeta(type):
    def __new__(mcs, name, bases, attrs):
        me_classes_attr = 'mongoengine_field_classes'
        me_classes = attrs.get(me_classes_attr)

        assert isinstance(me_classes, (List, Tuple)) or me_classes is None, \
            f'{me_classes_attr} must be mongoengine field classes list'

        attrs['_meta'] = weakref.proxy(mcs)

        klass = super(FieldHandlerMeta, mcs).__new__(mcs, name, bases, attrs)
        if me_classes:
            for me_class in me_classes:
                add_field_handler(me_class, klass)

        return klass


class CommonFieldHandler(metaclass=FieldHandlerMeta):
    """FieldHandler used as default for mongoengine fields which does
    not have special FieldHandler since this class implements behavior
    for mongoengine.fields.BaseField

    Special FieldHandler should be derived from this class
    """
    # TODO: doc
    # TODO: rename
    mongoengine_field_classes: Iterable[Type[mongoengine.fields.BaseField]] = [
        mongoengine.fields.BaseField
    ]

    # TODO: doc
    schema_skel_keys: Iterable[str] = {'db_field', 'required', 'default', 'unique', 'unique_with',
                                       'primary_key', 'choices', 'null', 'sparse', 'type_key'}

    def __init__(self,
                 collection: MongoCollection,
                 field_schema: dict):
        self.field_schema = field_schema
        self.collection = collection
        self.db_field = field_schema.get('db_field')
        if self.db_field is None:
            raise SchemaError(f"Missed 'db_field' key in schema of collection {collection.name}")

    @classmethod
    def schema_skel(cls) -> dict:
        """Return db schema skeleton dict for concrete field type"""
        keys = []
        for klass in reversed(inspect.getmro(cls)):
            keys.extend(getattr(klass, 'schema_skel_keys', []))

        return {f: None for f in keys}

    @classmethod
    def build_schema(cls, field_obj: mongoengine.fields.BaseField) -> dict:
        """
        Return db schema from a given mongoengine field object

        'type_key' schema item will get filled with the closest
        mongoengine field class from the type key registry
        :param field_obj: mongoengine field object
        :return: schema dict
        """
        schema_skel = cls.schema_skel()
        schema = {f: getattr(field_obj, f, val) for f, val in schema_skel.items()}
        field_class = field_obj.__class__
        if field_class.__name__ in type_key_registry:
            schema['type_key'] = field_class.__name__
        else:
            registry_field_cls = get_closest_parent(
                field_class,
                (x.field_cls for x in type_key_registry.values())
            )
            if registry_field_cls is None:
                raise SchemaError(f'Could not find {field_class!r} or one of its base classes '
                                  f'in type_key registry')

            schema['type_key'] = registry_field_cls.__name__

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
        self._check_diff(diff, False, False, str)
        if not diff.new or not diff.old:
            raise MigrationError("db_field must be a non-empty string")

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
        self._check_diff(diff, False, False, bool)
        # FIXME: consider diff.policy
        if diff.old is not True and diff.new is True:
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
        self._check_diff(diff, False, False, bool)
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
        self._check_diff(diff, False, True, Collection)
        choices = diff.new
        if isinstance(next(iter(choices)), (list, tuple)):
            # next(iter) is useful for sets
            choices = [k for k, _ in choices]

        if diff.error_policy == 'raise':
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
        self._check_diff(diff, False, False, str)
        if not diff.old or not diff.new:
            raise MigrationError(f"'type_key' has empty values: {diff!r}")

        field_classes = []
        for val in (diff.new, diff.old):
            if val not in type_key_registry:
                raise MigrationError(f'Could not find {val!r} or one of its base classes '
                                     f'in type_key registry')
            field_classes.append(type_key_registry[val].field_cls)

        # TODO: use diff.policy
        new_handler_cls = type_key_registry[diff.new].field_handler_cls
        new_handler = new_handler_cls(self.collection, self.field_schema)
        new_handler.convert_type(*field_classes)

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
            CONVERTION_MATRIX.get(get_closest_parent(from_field_cls, CONVERTION_MATRIX.keys()))

        if type_converters is None:
            raise MigrationError(f'Type converter not found for convertion '
                                 f'{from_field_cls!r} -> {to_field_cls!r}')

        type_converter = type_converters.get(to_field_cls) or \
            type_converters.get(get_closest_parent(to_field_cls, type_converters))

        if type_converter is None:
            raise MigrationError(f'Type converter not found for convertion '
                                 f'{from_field_cls!r} -> {to_field_cls!r}')

        # FIXME: remove from_field_cls, to_field_cls. Also from current function
        type_converter(self.collection, self.db_field, from_field_cls, to_field_cls)

    def _check_diff(self, diff: AlterDiff, can_be_unset=True, can_be_none=True, check_type=None):
        if diff.new == diff.old:
            raise MigrationError(f'Diff of field {self.db_field} has the equal old and new values')

        if not can_be_unset:
            if diff.new == UNSET or diff.old == UNSET:
                raise MigrationError(f'{self.db_field} field cannot be UNSET')

        if check_type is not None:
            if diff.old not in (UNSET, None) and not isinstance(diff.old, check_type) \
                    or diff.new not in (UNSET, None) and not isinstance(diff.new, check_type):
                raise MigrationError(f'Field {self.db_field}, diff {diff!s} values must be of type '
                                     f'{check_type!r}')

        if not can_be_none:
            if diff.old is None or diff.new is None:
                raise MigrationError(f'{self.db_field} could not be None')
