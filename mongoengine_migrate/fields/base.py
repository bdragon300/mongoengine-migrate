import inspect
import weakref
from typing import Type, Iterable, List, Tuple, Collection

import mongoengine.fields
from pymongo.database import Database

import mongoengine_migrate.flags as flags
from mongoengine_migrate.actions.diff import AlterDiff, UNSET
from mongoengine_migrate.exceptions import SchemaError, MigrationError
from mongoengine_migrate.fields.registry import type_key_registry, add_field_handler
from mongoengine_migrate.mongo import (
    check_empty_result,
    DocumentUpdater
)
from mongoengine_migrate.utils import get_closest_parent
from .registry import CONVERTION_MATRIX


class FieldHandlerMeta(type):
    def __new__(mcs, name, bases, attrs):
        me_classes_attr = 'field_classes'
        me_classes = attrs.get(me_classes_attr)

        assert not {'field_name', 'collection_name'} & attrs.get('schema_skel_keys', set()), \
            "Handler schema_skel_keys shouldn't have keys matched with BaseAction parameters"

        assert isinstance(me_classes, (List, Tuple)) or me_classes is None, \
            f'{me_classes_attr} must be mongoengine field classes list'

        attrs['_meta'] = weakref.proxy(mcs)

        klass = super(FieldHandlerMeta, mcs).__new__(mcs, name, bases, attrs)
        if me_classes:
            for me_class in me_classes:
                add_field_handler(me_class, klass)

        return klass


class CommonFieldHandler(metaclass=FieldHandlerMeta):
    """
    Default handler for all mongoengine fields. Used for fields which
    does not have specific handler.

    Also this is base class for other field-specific handlers
    """
    #: Mongoengine field classes which concrete handler can be used for
    field_classes: Iterable[Type[mongoengine.fields.BaseField]] = [
        mongoengine.fields.BaseField
    ]

    #: Schema keys used for all fields listed in `field_classes`
    #: This variable inherits, i.e. keys defined in current class is
    #: appended to keys defined in parent classes
    schema_skel_keys: Iterable[str] = {
        'db_field', 'required', 'default', 'unique', 'unique_with', 'primary_key', 'choices',
        'null', 'sparse', 'type_key'
    }

    def __init__(self,
                 db: Database,
                 collection_name: str,
                 left_schema: dict,
                 left_field_schema: dict,  # FIXME: get rid of
                 right_field_schema: dict):
        """
        :param db: pymongo Database object
        :param collection_name: collection name. Could contain
         collection name or embedded document name
        :param left_field_schema:
        :param right_field_schema:
        :param left_schema:
        """
        self.db = db
        self.collection_name = collection_name
        self.left_field_schema = left_field_schema
        self.right_field_schema = right_field_schema
        self.left_schema = left_schema

        self.is_embedded = self.collection_name.startswith(flags.EMBEDDED_DOCUMENT_NAME_PREFIX)
        self.collection = None if self.is_embedded else db[collection_name]  # FIXME: make db query tracer

    @classmethod
    def schema_skel(cls) -> dict:
        """
        Return db schema skeleton dict, which contains keys taken from
        `schema_skel_keys` and Nones as values
        """
        keys = []
        for klass in reversed(inspect.getmro(cls)):
            keys.extend(getattr(klass, 'schema_skel_keys', []))

        return {f: None for f in keys}

    @classmethod
    def build_schema(cls, field_obj: mongoengine.fields.BaseField) -> dict:
        """
        Return db schema from a given mongoengine field object

        'type_key' schema item will get filled with a mongoengine field
        class name or one of its parents which have its own type key
        in registry
        :param field_obj: mongoengine field object
        :return: schema dict
        """
        schema_skel = cls.schema_skel()
        schema = {f: getattr(field_obj, f, val) for f, val in schema_skel.items()}

        if 'default' in schema:
            schema['default'] = cls._normalize_default(schema['default'])

        if 'choices' in schema:
            schema['choices'] = cls._normalize_choices(schema['choices'])

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

    def change_param(self, db_field: str, name: str):
        """
        DB commands to be run in order to change given parameter

        This is a facade method which calls concrete method which
        changes given parameter. Such methods should have name
        'change_NAME' where NAME is a parameter name.
        :param db_field: db field name to change
        :param name: parameter name to change
        :return:
        """
        assert name != 'param', "Schema key could not be 'param'"

        diff = AlterDiff(
            self.left_field_schema.get(name, UNSET),
            self.right_field_schema.get(name, UNSET)
        )

        method = getattr(self, f'change_{name}')
        updater = DocumentUpdater(self.db, self.collection_name, db_field, self.left_schema)
        return method(updater, diff)

    def change_db_field(self, updater: DocumentUpdater, diff: AlterDiff):
        """
        Change db field name of a field. Simply rename this field
        :param updater:
        :param diff:
        :return:
        """
        def by_path(col, filter_dotpath, update_dotpath, array_filters):
            col.update_many(
                {filter_dotpath: {'$exists': True}},
                {'$rename': {filter_dotpath: diff.new}}
            )

        def by_doc(col, doc, path):
            if isinstance(doc, dict) and diff.old in doc:
                doc[diff.new] = doc.pop(diff.old)

        self._check_diff(updater.field_name, diff, False, str)
        if not diff.new or not diff.old:
            raise MigrationError("db_field must be a non-empty string")

        updater.update_combined(by_path, by_doc, embedded_noarray_by_path_cb=by_path)

    def change_required(self, updater: DocumentUpdater, diff: AlterDiff):
        """
        Make field required, which means to add this field to all
        documents. Reverting of this doesn't require smth to do
        :param updater:
        :param diff:
        :return:
        """
        def by_path(col, filter_dotpath, update_dotpath, array_filters):
            col.update_many(
                {filter_dotpath: None},  # Both null and nonexistent field
                {'$set': {update_dotpath: default}},
                array_filters=array_filters
            )

        self._check_diff(updater.field_name, diff, False, bool)

        if diff.old is not True and diff.new is True:
            default = self.right_field_schema.get('default')
            # None and UNSET default has the same meaning here
            if default is None:
                raise MigrationError(f'Cannot mark field '
                                     f'{self.collection.name}.{updater.field_name} '
                                     f'as required because default value is not set')

            updater.update_by_path(by_path)

    def change_default(self, updater: DocumentUpdater, diff: AlterDiff):
        """Stub method. No need to do smth on default change"""
        pass

    def change_unique(self, updater: DocumentUpdater, diff: AlterDiff):
        # TODO
        pass

    def change_unique_with(self, updater: DocumentUpdater, diff: AlterDiff):
        # TODO
        pass

    def change_primary_key(self, updater: DocumentUpdater, diff: AlterDiff):
        """
        Setting field as primary key means to set it required and unique
        :param updater:
        :param diff:
        :return:
        """
        self._check_diff(updater.field_name, diff, False, bool)
        self.change_required(updater, diff),  # FIXME: should not consider default value, but check if field is required
        # self.change_unique([], []) or []  # TODO

    # TODO: consider Document, EmbeddedDocument as choices
    def change_choices(self, updater: DocumentUpdater, diff: AlterDiff):
        """
        Set choices for a field
        :param updater:
        :param diff:
        :return:
        """
        def by_path(col, filter_dotpath, update_dotpath, array_filters):
            choices = diff.new
            check_empty_result(col, filter_dotpath, {filter_dotpath: {'$nin': choices}})

        self._check_diff(updater.field_name, diff, True, Collection)

        updater.update_by_path(by_path)

    def change_null(self,updater: DocumentUpdater, diff: AlterDiff):
        pass

    def change_sparse(self, updater: DocumentUpdater, diff: AlterDiff):
        pass

    def change_type_key(self, updater: DocumentUpdater, diff: AlterDiff):
        """
        Change type of field. Try to convert value in db
        :param updater:
        :param diff:
        :return:
        """
        self._check_diff(updater.field_name, diff, False, str)
        if not diff.old or not diff.new:
            raise MigrationError(f"'type_key' has empty values: {diff!r}")

        field_classes = []
        for val in (diff.old, diff.new):
            if val not in type_key_registry:
                raise MigrationError(f'Could not find {val!r} or one of its base classes '
                                     f'in type_key registry')
            field_classes.append(type_key_registry[val].field_cls)

        new_handler_cls = type_key_registry[diff.new].field_handler_cls
        new_handler = new_handler_cls(self.db,
                                      self.collection_name,
                                      self.left_schema,
                                      self.left_field_schema,
                                      self.right_field_schema)
        new_handler.convert_type(updater, *field_classes)  # FIXME: embedded

    def convert_type(self,
                     updater: DocumentUpdater,
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
        :param updater:
        :param from_field_cls: mongoengine field class which was used
         before
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

        type_converter(updater)

    def _check_diff(self, db_field: str, diff: AlterDiff, can_be_none=True, check_type=None):
        if diff.new == diff.old:
            raise MigrationError(f'Diff of field {db_field} has the equal old and new values')

        if check_type is not None:
            if diff.old not in (UNSET, None) and not isinstance(diff.old, check_type) \
                    or diff.new not in (UNSET, None) and not isinstance(diff.new, check_type):
                raise MigrationError(f'Field {db_field}, diff {diff!s} values must be of type '
                                     f'{check_type!r}')

        if not can_be_none:
            if diff.old is None or diff.new is None:
                raise MigrationError(f'{db_field} could not be None')

    @classmethod
    def _normalize_default(cls, default):
        if callable(default):
            default = default()

        # Check if the expression repr produces correct python expr
        try:
            eval(repr(default), {}, {})
        except:
            # FIXME: with required=True this leads to error (mongoengine will not raise error since default is specified)
            default = None

        return default

    @classmethod
    def _normalize_choices(cls, choices):
        if not isinstance(choices, Iterable):
            return None

        if isinstance(next(iter(choices)), (list, tuple)):
            # next(iter) is useful for sets
            choices = [k for k, _ in choices]
        else:
            # Make a tuple from any iterable type (e.g. dict_keys)
            choices = tuple(choices)

        return choices
