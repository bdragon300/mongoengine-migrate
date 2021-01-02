__all__ = [
    'CreateField',
    'DropField',
    'AlterField',
    'RenameField'
]

import logging
from typing import Mapping, Any

from pymongo.database import Database

from mongoengine_migrate.exceptions import SchemaError
from ..updater import ByPathContext, ByDocContext, DocumentUpdater
from mongoengine_migrate.schema import Schema
from mongoengine_migrate.utils import document_type_to_class_name
from mongoengine_migrate.graph import MigrationPolicy
from .base import BaseFieldAction

log = logging.getLogger('mongoengine-migrate')


class CreateField(BaseFieldAction):
    """Create field in a given collection"""
    @classmethod
    def build_object(cls,
                     document_type: str,
                     field_name: str,
                     left_schema: Schema,
                     right_schema: Schema):
        match = document_type in left_schema \
                and document_type in right_schema \
                and field_name not in left_schema[document_type] \
                and field_name in right_schema[document_type]
        if match:
            field_params = right_schema[document_type][field_name]
            return cls(document_type=document_type,
                       field_name=field_name,
                       **field_params)

    def to_schema_patch(self, left_schema: Schema):
        keys_to_check = {'type_key', 'db_field'}
        missed_keys = keys_to_check - self.parameters.keys()
        if missed_keys:
            raise SchemaError(f"Missed required parameters in "
                              f"CreateField({self.document_type}...): {missed_keys}")

        # Get schema skeleton for field type
        field_handler_cls = self.get_field_handler_cls(self.parameters['type_key'])
        right_field_schema_skel = field_handler_cls.schema_skel()
        extra_keys = self.parameters.keys() - right_field_schema_skel.keys()
        if extra_keys:
            raise SchemaError(f'Unknown CreateField({self.document_type}...) parameters: '
                              f'{extra_keys}')

        field_params = {
            **right_field_schema_skel,
            **self.parameters
        }
        return [(
            'add',
            self.document_type,
            [(self.field_name, field_params)]
        )]

    def run_forward(self):
        """
        If field is defined as required then force create it with
        default value. Otherwise do nothing since mongoengine creates
        fields automatically on value set
        """
        def by_path(ctx: ByPathContext):
            # Update documents only
            ctx.collection.update_many(
                {ctx.filter_dotpath: {'$exists': False}, **ctx.extra_filter},
                {'$set': {ctx.update_dotpath: default}}
            )

        def by_doc(ctx: ByDocContext):
            # Update embedded documents
            if db_field not in ctx.document:
                ctx.document[db_field] = default

        db_field = self.parameters['db_field']
        default = self.parameters.get('default')
        is_required = self.parameters.get('required') or self.parameters.get('primary_key')
        if is_required:
            inherit = self._run_ctx['left_schema'][self.document_type].parameters.get('inherit')
            document_cls = document_type_to_class_name(self.document_type) if inherit else None
            updater = DocumentUpdater(self._run_ctx['db'], self.document_type,
                                      self._run_ctx['left_schema'], db_field,
                                      self._run_ctx['migration_policy'], document_cls)
            updater.with_missed_fields().update_combined(by_path, by_doc)

    def run_backward(self):
        """Drop field"""
        def by_path(ctx: ByPathContext):
            ctx.collection.update_many(
                {ctx.filter_dotpath: {'$exists': True}, **ctx.extra_filter},
                {'$unset': {ctx.update_dotpath: ''}},
                array_filters=ctx.build_array_filters()
            )

        db_field = self.parameters['db_field']
        inherit = self._run_ctx['left_schema'][self.document_type].parameters.get('inherit')
        document_cls = document_type_to_class_name(self.document_type) if inherit else None
        updater = DocumentUpdater(self._run_ctx['db'], self.document_type,
                                  self._run_ctx['left_schema'], db_field,
                                  self._run_ctx['migration_policy'], document_cls)
        updater.update_by_path(by_path)

    def prepare(self, db: Database, left_schema: Schema, migration_policy: MigrationPolicy):
        self._prepare(db, left_schema, migration_policy, False)


class DropField(BaseFieldAction):
    """Drop field in a given collection"""
    @classmethod
    def build_object(cls,
                     document_type: str,
                     field_name: str,
                     left_schema: Schema,
                     right_schema: Schema):
        match = document_type in left_schema \
                and document_type in right_schema \
                and field_name in left_schema[document_type] \
                and field_name not in right_schema[document_type]
        if match:
            return cls(document_type=document_type, field_name=field_name)

    def to_schema_patch(self, left_schema: Schema):
        if self.document_type not in left_schema:
            raise SchemaError(f'Document {self.document_type!r} is not in schema')
        if self.field_name not in left_schema[self.document_type]:
            raise SchemaError(f'Field {self.document_type}.{self.field_name} is not in schema')

        left_field_schema = left_schema[self.document_type][self.field_name]

        return [(
            'remove',
            self.document_type,
            [(self.field_name, left_field_schema)]
        )]

    def run_forward(self):
        """Drop field"""
        def by_path(ctx: ByPathContext):
            ctx.collection.update_many(
                {ctx.filter_dotpath: {'$exists': True}, **ctx.extra_filter},
                {'$unset': {ctx.update_dotpath: ''}},
                array_filters=ctx.build_array_filters()
            )

        db_field = self._run_ctx['left_field_schema']['db_field']
        inherit = self._run_ctx['left_schema'][self.document_type].parameters.get('inherit')
        document_cls = document_type_to_class_name(self.document_type) if inherit else None
        updater = DocumentUpdater(self._run_ctx['db'], self.document_type,
                                  self._run_ctx['left_schema'], db_field,
                                  self._run_ctx['migration_policy'], document_cls)
        updater.update_by_path(by_path)

    def run_backward(self):
        """
        If field is defined as required then force create it with
        default value. Otherwise do nothing since mongoengine creates
        fields automatically on value set
        """
        def by_path(ctx: ByPathContext):
            # Update documents only
            ctx.collection.update_many(
                {ctx.filter_dotpath: {'$exists': False}, **ctx.extra_filter},
                {'$set': {ctx.update_dotpath: default}}
            )

        def by_doc(ctx: ByDocContext):
            # Update embedded documents
            if db_field not in ctx.document:
                ctx.document[db_field] = default

        db_field = self._run_ctx['left_field_schema']['db_field']
        default = self._run_ctx['left_field_schema'].get('default')
        is_required = self._run_ctx['left_field_schema'].get('required')
        if is_required:
            inherit = self._run_ctx['left_schema'][self.document_type].parameters.get('inherit')
            document_cls = document_type_to_class_name(self.document_type) if inherit else None
            updater = DocumentUpdater(self._run_ctx['db'], self.document_type,
                                      self._run_ctx['left_schema'], db_field,
                                      self._run_ctx['migration_policy'], document_cls)
            updater.with_missed_fields().update_combined(by_path, by_doc)


class AlterField(BaseFieldAction):
    """Change field parameters or its type, i.e. altering"""
    @classmethod
    def build_object(cls,
                     document_type: str,
                     field_name: str,
                     left_schema: Schema,
                     right_schema: Schema):
        # Check if field still here but its schema has changed
        match = document_type in left_schema \
                and document_type in right_schema \
                and field_name in left_schema[document_type] \
                and field_name in right_schema[document_type] \
                and left_schema[document_type][field_name] != right_schema[document_type][field_name]
        if match:
            right_field_schema = right_schema[document_type][field_name]
            left_field_schema = left_schema[document_type][field_name]
            # Consider items which was changed or added, skip those
            # ones which was unchanged or was removed
            # NOTE: `r.items() - l.items()` doesn't work since this
            # requires dict values to be hashable
            action_params = {
                k: right_field_schema[k] for k in right_field_schema.keys()
                if k not in left_field_schema or left_field_schema[k] != right_field_schema[k]
            }
            # FIXME: use function below
            # field_params = cls._fix_field_params(document_type,
            #                                      field_name,
            #                                      field_params,
            #                                      old_schema,
            #                                      new_schema)
            return cls(document_type=document_type, field_name=field_name, **action_params)

    def to_schema_patch(self, left_schema: Schema):
        if self.document_type not in left_schema:
            raise SchemaError(f'Document {self.document_type!r} is not in schema')
        if self.field_name not in left_schema[self.document_type]:
            raise SchemaError(f'Field {self.document_type}.{self.field_name} is not in schema')

        left_field_schema = left_schema[self.document_type][self.field_name]

        # Get schema skeleton for field type
        field_handler_cls = self.get_field_handler_cls(
            self.parameters.get('type_key', left_field_schema['type_key'])
        )
        right_schema_skel = field_handler_cls.schema_skel()
        extra_keys = self.parameters.keys() - right_schema_skel.keys()
        if extra_keys:
            raise SchemaError(f'Unknown keys in schema of field '
                              f'{self.document_type}.{self.field_name}: {extra_keys}')

        # Shortcuts
        left = left_field_schema
        params = self.parameters

        # Remove params
        d = [('remove', f'{self.document_type}.{self.field_name}', [(key, ())])
             for key in sorted(left.keys() - right_schema_skel.keys())]
        # Add new params
        d += [('add', f'{self.document_type}.{self.field_name}', [(key, params[key])])
              for key in sorted(params.keys() - left.keys())]
        # Change params if they are requested to be changed
        d += [('change',
               f'{self.document_type}.{self.field_name}.{key}',
               (left[key], params[key]))
              for key in sorted(params.keys() & left.keys())
              if left[key] != params[key]]

        return d

    def run_forward(self):
        self._run_migration(self._run_ctx['left_field_schema'], self.parameters, swap=False)

    def run_backward(self):
        self._run_migration(self._run_ctx['left_field_schema'], self.parameters, swap=True)

    def _run_migration(self, left_field_schema: dict, parameters: Mapping[str, Any], swap=False):
        type_key = left_field_schema['type_key']
        field_handler_cls = self.get_field_handler_cls(type_key)
        skel = field_handler_cls.schema_skel()
        right_field_schema = {k: parameters.get(k, left_field_schema.get(k, v))
                              for k, v in skel.items()}

        if swap:
            left_field_schema, right_field_schema = right_field_schema, left_field_schema
            type_key = left_field_schema['type_key']

        field_handler = self._get_field_handler(type_key, left_field_schema, right_field_schema)
        db_field = left_field_schema['db_field']

        # Change field type first, obtain new field handler object
        # and process other parameters with it
        if type_key != right_field_schema['type_key']:
            log.debug(">> Change 'type_key': %s => %s",
                      repr(type_key),
                      repr(right_field_schema['type_key']))
            field_handler.change_param(db_field, 'type_key')
            field_handler = self._get_field_handler(right_field_schema['type_key'],
                                                    left_field_schema,
                                                    right_field_schema)

        # Try to process all parameters on same order to avoid
        # potential problems on repeated launches if some query on
        # previous launch was failed
        unset = object()
        for name, new_value in sorted(right_field_schema.items()):
            old_value = left_field_schema.get(name, unset)
            if name == 'type_key' or new_value == old_value:
                continue

            log.debug(">> Change %s: %s => %s", repr(name), repr(old_value), repr(new_value))
            field_handler.change_param(db_field, name)

            # If `db_field` was changed then work with new name further
            if name == 'db_field':
                db_field = right_field_schema['db_field']

    @classmethod
    def _fix_field_params(cls,
                          document_type: str,
                          field_name: str,
                          field_params: Mapping[str, Any],
                          old_schema: Schema,
                          new_schema: Schema) -> Mapping[str, Any]:
        """
        Search for potential problems which could be happened during
        migration and return fixed field schema. If such problem
        could not be resolved only by changing parameters then raise
        an SchemaError
        :param document_type:
        :param field_name:
        :param field_params:
        :param old_schema:
        :param new_schema:
        :raises SchemaError: when some problem found
        :return:
        """
        # TODO: Check all defaults in diffs against choices, required, etc.
        # TODO: check nones for type_key, etc.
        new_changes = {k: v.new for k, v in field_params.items()}

        # Field becomes required or left as required without changes
        become_required = new_changes.get('required',
                                          new_schema.get(field_name, {}).get('required'))

        # 'default' diff object has default parameter or field become
        # field with default or field has default value already
        default = (field_params.get('required') and field_params['required'].default) \
            or new_changes.get('default') \
            or new_schema.get(field_name, {}).get('default')
        if become_required and default is None:
            # TODO: replace following error on interactive mode
            raise SchemaError(f'Field {document_type}.{field_name} could not be '
                              f'created since it defined as required but has not a default value')

        return field_params


class RenameField(BaseFieldAction):
    """Rename field"""
    priority = 80

    #: How much percent of items in schema diff of two fields in the
    #: same collection should be equal to consider such change as
    #: field rename instead of drop/create
    similarity_threshold = 70

    def __init__(self, document_type: str, field_name: str, *, new_name, **kwargs):
        super().__init__(document_type, field_name, new_name=new_name, **kwargs)
        self.new_name = new_name

    @classmethod
    def build_object(cls,
                     document_type: str,
                     field_name: str,
                     left_schema: Schema,
                     right_schema: Schema):
        # Check if field exists under different name in schema
        # Field also can have small schema changes in the same time
        # So we try to get similarity percentage and if it more than
        # threshold then we're consider such change as rename/alter.
        # Otherwise it is drop/create
        match = document_type in left_schema \
                and document_type in right_schema \
                and field_name in left_schema[document_type] \
                and field_name not in right_schema[document_type]
        if not match:
            return

        left_field_schema = left_schema[document_type][field_name]
        db_field = left_field_schema.get('db_field')
        candidates = []
        for right_field_name, right_field_schema in right_schema[document_type].items():
            # Skip fields which was not renamed
            # Changing 'db_field' parameter is altering, not renaming
            if right_field_name in left_schema[document_type]:
                continue

            # Model field renamed, but db field is the same
            if db_field == right_field_schema.get('db_field') and db_field is not None:
                candidates = [(right_field_name, right_field_schema)]
                break

            # Take only common keys to estimate similarity
            # 'type_key' may get changed which means that change of one
            # key leads to many changes in schema. These changes
            # should not be considered as valueable
            keys = left_field_schema.keys() & right_field_schema.keys() - {'db_field'}
            if keys:
                p = sum(left_field_schema[k] == right_field_schema[k] for k in keys) / len(keys)
                if p * 100 >= cls.similarity_threshold:
                    candidates.append((right_field_name, right_field_schema))

        if len(candidates) == 1:
            return cls(document_type=document_type,
                       field_name=field_name,
                       new_name=candidates[0][0])

    def to_schema_patch(self, left_schema: Schema):
        if self.document_type not in left_schema:
            raise SchemaError(f'Document {self.document_type!r} is not in schema')
        if self.field_name not in left_schema[self.document_type]:
            raise SchemaError(f'Field {self.document_type}.{self.field_name} is not in schema')

        left_field_schema = left_schema[self.document_type][self.field_name]

        return [
            ('remove', f'{self.document_type}', [(self.field_name, left_field_schema)]),
            ('add', f'{self.document_type}', [(self.new_name, left_field_schema)])
        ]

    def run_forward(self):
        """Renaming mongoengine model field does not required some
        db changes. It's supposed that possible `db_field` modification,
        which could exist during field renaming, is handled by
        AlterField action
        """

    def run_backward(self):
        """Renaming mongoengine model field does not required some
        db changes. It's supposed that possible `db_field` modification,
        which could exist during field renaming, is handled by
        AlterField action
        """
