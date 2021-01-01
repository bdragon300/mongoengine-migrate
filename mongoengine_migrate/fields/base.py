__all__ = [
    'FieldHandlerMeta',
    'CommonFieldHandler',
]

import inspect
import weakref
from typing import Type, Iterable, List, Tuple, Collection, Any

import mongoengine.fields
from pymongo.database import Database

import mongoengine_migrate.flags as flags
from mongoengine_migrate.exceptions import SchemaError, MigrationError, ActionError
from mongoengine_migrate.fields.registry import type_key_registry, add_field_handler
from mongoengine_migrate.graph import MigrationPolicy
from mongoengine_migrate.mongo import check_empty_result
from mongoengine_migrate.schema import Schema
from mongoengine_migrate.utils import get_closest_parent, document_type_to_class_name, Diff, UNSET
from .registry import CONVERTION_MATRIX
from ..updater import ByPathContext, ByDocContext, DocumentUpdater


class FieldHandlerMeta(type):
    def __new__(mcs, name, bases, attrs):
        me_classes_attr = 'field_classes'
        me_classes = attrs.get(me_classes_attr)

        assert not {'field_name', 'document_type'} & attrs.get('schema_skel_keys', set()), \
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
                 document_type: str,
                 left_schema: Schema,
                 left_field_schema: dict,  # FIXME: get rid of
                 right_field_schema: dict,
                 migration_policy: MigrationPolicy):
        """
        :param db: pymongo Database object
        :param document_type: collection name. Could contain
         collection name or embedded document name
        :param left_schema:
        :param left_field_schema:
        :param right_field_schema:
        :param migration_policy:
        """
        self.db = db
        self.document_type = document_type
        self.left_schema = left_schema
        self.left_field_schema = left_field_schema
        self.right_field_schema = right_field_schema
        self.migration_policy = migration_policy

        self.is_embedded = self.document_type.startswith(flags.EMBEDDED_DOCUMENT_NAME_PREFIX)
        self.collection = None
        if not self.is_embedded:
            collection_name = left_schema[document_type].parameters['collection']
            self.collection = None if self.is_embedded else db[collection_name]

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
        Return schema of a given mongoengine field object

        'type_key' schema item will get filled with a mongoengine field
        class name or one of its parents which have its own type key
        in registry
        :param field_obj: mongoengine field object
        :return: field schema dict
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
                raise ActionError(f'Could not find {field_class!r} or one of its base classes '
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

        diff = Diff(
            old=self.left_field_schema.get(name, UNSET),
            new=self.right_field_schema.get(name, UNSET),
            key=name
        )

        method = getattr(self, f'change_{name}')
        inherit = self.left_schema[self.document_type].parameters.get('inherit')
        document_cls = document_type_to_class_name(self.document_type) if inherit else None
        updater = DocumentUpdater(self.db, self.document_type, self.left_schema, db_field,
                                  self.migration_policy, document_cls)
        return method(updater, diff)

    def change_db_field(self, updater: DocumentUpdater, diff: Diff):
        """
        Change db field name of a field. Simply rename this field
        :param updater:
        :param diff:
        :return:
        """
        def by_path(ctx: ByPathContext):
            path = ctx.filter_dotpath.split('.')[:-1]
            ctx.collection.update_many(
                {ctx.filter_dotpath: {'$exists': True}, **ctx.extra_filter},
                {'$rename': {ctx.filter_dotpath: '.'.join(path + [diff.new])}}
            )

        def by_doc(ctx: ByDocContext):
            doc = ctx.document
            if diff.old in doc:
                doc[diff.new] = doc.pop(diff.old)

        self._check_diff(updater, diff, False, str)
        if not diff.new or not diff.old:
            raise SchemaError(f"{updater.document_type}{updater.field_name}.db_field "
                              f"must be a non-empty string")

        updater.update_combined(by_path, by_doc, False)

    def change_required(self, updater: DocumentUpdater, diff: Diff):
        """
        Make field required, which means to add this field to all
        documents. Reverting of this doesn't require smth to do
        :param updater:
        :param diff:
        :return:
        """
        self._check_diff(updater, diff, False, bool)
        if diff.old is not True and diff.new is True:
            default = self.right_field_schema.get('default')
            # None and UNSET default has the same meaning here
            if default is None:
                raise SchemaError(f'{updater.document_type}{updater.field_name}.default is not '
                                  f'set for required field')

            self._set_default_value(updater, default)

    def change_default(self, updater: DocumentUpdater, diff: Diff):
        """Stub method. No need to do smth on default change"""
        pass

    def change_unique(self, updater: DocumentUpdater, diff: Diff):
        """This parameter is about indexes. Handled by index actions
        No need to do anything here.
        """
        pass

    def change_unique_with(self, updater: DocumentUpdater, diff: Diff):
        """This parameter is about indexes. Handled by index actions
        No need to do anything here.
        """
        pass

    def change_primary_key(self, updater: DocumentUpdater, diff: Diff):
        """
        Setting field as primary key means to set it required and unique
        :param updater:
        :param diff:
        :return:
        """
        def by_path(ctx: ByPathContext):
            fltr = {ctx.filter_dotpath: {'$exists': False}, **ctx.extra_filter}
            check_empty_result(ctx.collection, ctx.filter_dotpath, fltr)

        self._check_diff(updater, diff, False, bool)

        if updater.is_embedded:
            raise SchemaError(f'Embedded document {updater.document_type} cannot have primary key')
        if self.migration_policy.name == 'strict':
            updater.update_by_path(by_path)

    # TODO: consider Document, EmbeddedDocument as choices
    def change_choices(self, updater: DocumentUpdater, diff: Diff):
        """
        Set choices for a field
        :param updater:
        :param diff:
        :return:
        """
        def by_path(ctx: ByPathContext):
            choices = diff.new
            fltr = {
                ctx.filter_dotpath: {'$nin': choices, '$exists': True},
                **ctx.extra_filter
            }
            check_empty_result(ctx.collection, ctx.filter_dotpath, fltr)

        self._check_diff(updater, diff, True, Collection)

        if diff.new is not None and self.migration_policy.name == 'strict':
            updater.update_by_path(by_path)

    def change_null(self, updater: DocumentUpdater, diff: Diff):
        """Force set db fields to `null` if they are was not set and
        they defined with `null=True` in mongoengine
        """
        self._check_diff(updater, diff, False, bool)
        if diff.new is True:
            self._set_default_value(updater, None)

    def change_sparse(self, updater: DocumentUpdater, diff: Diff):
        pass

    def change_type_key(self, updater: DocumentUpdater, diff: Diff):
        """
        Change type of field. Try to convert value in db
        :param updater:
        :param diff:
        :return:
        """
        self._check_diff(updater, diff, False, str)
        if not diff.old or not diff.new:
            raise SchemaError(f"Old or new {updater.document_type}{updater.field_name}.type_key "
                              f"values are not set")

        field_classes = []
        for val in (diff.old, diff.new):
            if val not in type_key_registry:
                raise SchemaError(f'Unknown type_key {updater.document_type}{updater.field_name}: '
                                  f'{val!r}')
            field_classes.append(type_key_registry[val].field_cls)

        new_handler_cls = type_key_registry[diff.new].field_handler_cls
        new_handler = new_handler_cls(self.db,
                                      self.document_type,
                                      self.left_schema,
                                      self.left_field_schema,
                                      self.right_field_schema,
                                      self.migration_policy)
        new_handler.convert_type(updater, *field_classes)

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

    @staticmethod
    def _check_diff(updater: DocumentUpdater, diff: Diff, can_be_none=True, check_type=None):
        if diff.new == diff.old:
            raise SchemaError(f'{updater.document_type}.{updater.field_name}.{diff.key} '
                              f'does not changed from previous Action')

        if check_type is not None:
            if diff.old not in (UNSET, None) and not isinstance(diff.old, check_type) \
                    or diff.new not in (UNSET, None) and not isinstance(diff.new, check_type):
                raise SchemaError(f'{updater.document_type}.{updater.field_name}.{diff.key} '
                                  f'must have type {check_type!r}')

        if not can_be_none:
            if diff.old is None or diff.new is None:
                raise SchemaError(f'{updater.document_type}.{updater.field_name}.{diff.key} '
                                  f'could not be None')

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

    @staticmethod
    def _set_default_value(updater: DocumentUpdater, value: Any):
        """Set a given value only for unset fields"""
        def by_path(ctx: ByPathContext):
            # Update documents only
            ctx.collection.update_many(
                {ctx.filter_dotpath: {'$exists': False}, **ctx.extra_filter},
                {'$set': {ctx.update_dotpath: value}},
                array_filters=ctx.build_array_filters()
            )

        def by_doc(ctx: ByDocContext):
            # Update embedded documents
            if updater.field_name not in ctx.document:
                ctx.document.setdefault(updater.field_name, value)

        updater.with_missed_fields().update_combined(by_path, by_doc)
