from typing import Mapping

from mongoengine_migrate.exceptions import ActionError
from .base import BaseFieldAction
from mongoengine_migrate.fields.base import mongoengine_fields_mapping
from .diff import AlterDiff


class CreateField(BaseFieldAction):
    @classmethod
    def build_object_if_applicable(cls,
                                   collection_name: str,
                                   field_name: str,
                                   old_schema: dict,
                                   new_schema: dict):
        match = collection_name in old_schema \
                and collection_name in new_schema \
                and field_name not in old_schema[collection_name] \
                and field_name in new_schema[collection_name]
        if match:
            field_params = new_schema[collection_name][field_name]
            return cls(collection_name=collection_name,
                       field_name=field_name,
                       **field_params
                       )

    def to_schema_patch(self, current_schema: dict):
        if self.collection_name not in current_schema:
            raise ActionError(f'Cannot create field {self.collection_name}.{self.field_name} '
                              f'since collection {self.collection_name} was not created in schema')
        field_params = {
            **self.field_type_cls.schema_skel(),
            **self._init_kwargs
        }
        return [(
            'add',
            self.collection_name,
            [(self.field_name, field_params)]
        )]

    def run_forward(self):
        """
        FIXME: parameters (indexes, acl, etc.)
        """
        is_required = self._init_kwargs.get('required') or self._init_kwargs.get('primary_key')
        default = self._init_kwargs.get('default')
        if is_required:
            self.collection.update_many(
                {self.field_name: {'$exists': False}}, {'$set': {self.field_name: default}}
            )

    def run_backward(self):
        self.collection.update_many(
            {self.field_name: {'$exists': True}}, {'$unset': {self.field_name: ''}}
        )


class DropField(BaseFieldAction):
    @classmethod
    def build_object_if_applicable(cls,
                                   collection_name: str,
                                   field_name: str,
                                   old_schema: dict,
                                   new_schema: dict):
        match = collection_name in old_schema \
                and collection_name in new_schema \
                and field_name in old_schema[collection_name] \
                and field_name not in new_schema[collection_name]
        if match:
            field_params = new_schema[collection_name][field_name]
            return cls(collection_name=collection_name,
                       field_name=field_name,
                       **field_params
                       )

    def to_schema_patch(self, current_schema: dict):
        if self.collection_name not in current_schema:
            raise ActionError(f'Cannot create field {self.collection_name}.{self.field_name} '
                              f'since collection {self.collection_name} was not created in schema')
        field_params = {
            **self.field_type_cls.schema_skel(),
            **self._init_kwargs
        }
        return [(
            'remove',
            self.collection_name,
            [(self.field_name, field_params)]
        )]

    def run_forward(self):
        self.collection.update_many(
            {self.field_name: {'$exists': True}}, {'$unset': {self.field_name: ''}}
        )

    def run_backward(self):
        is_required = self._init_kwargs.get('required') or self._init_kwargs.get('primary_key')
        default = self._init_kwargs.get('default')
        if is_required:
            self.collection.update_many(
                {self.field_name: {'$exists': False}}, {'$set': {self.field_name: default}}
            )


class AlterField(BaseFieldAction):
    def __init__(self,
                 collection_name: str,
                 field_name: str,
                 *args,
                 **kwargs):
        super().__init__(collection_name, field_name, *args, **kwargs)
        if not all(isinstance(v, AlterDiff) for v in self._init_kwargs.values()):
            raise ActionError(f'Keyword parameters must be AlterDiff objects')

    @classmethod
    def build_object_if_applicable(cls,
                                   collection_name: str,
                                   field_name: str,
                                   old_schema: dict,
                                   new_schema: dict):
        # Check that field still here, but its schema is differ
        match = collection_name in old_schema \
                and collection_name in new_schema \
                and field_name in old_schema[collection_name] \
                and field_name in new_schema[collection_name] \
                and old_schema[collection_name][field_name] != new_schema[collection_name][field_name]
        if match:
            new_params = new_schema[collection_name][field_name]
            old_params = old_schema[collection_name][field_name]
            # Get only those schema keys which have changed
            # If keys are not equal between schemas then consider all
            # of them
            field_params = {k: AlterDiff(old_params.get(k), new_params.get(k))
                            for k in new_params.keys() | old_params.keys()
                            if old_params.get(k) != new_params.get(k)}
            field_params = cls._fix_field_params(collection_name,
                                                 field_name,
                                                 field_params,
                                                 old_schema,
                                                 new_schema)
            return cls(collection_name=collection_name,
                       field_name=field_name,
                       **field_params
                       )

    # TODO: drop current_schema, use self.current_schema instead
    def to_schema_patch(self, current_schema: dict):
        if self.collection_name not in current_schema:
            raise ActionError(f'Cannot create field {self.collection_name}.{self.field_name} '
                              f'since collection {self.collection_name} was not created in schema')
        # TODO raise если param не в skel нового типа при изменении type_key
        # TODO а что делать если параметр появляется или изчезает, но не указан в kwargs
        p = [('change', f'{self.collection_name}.{self.field_name}.{k}', v.diff)
             for k, v in self._init_kwargs.items()]
        return p

    def run_forward(self):
        self._run_migration(self._init_kwargs)

    def run_backward(self):
        reversed_field_params = {k: v.swap() for k, v in self._init_kwargs.items()}
        self._run_migration(reversed_field_params)

    def _run_migration(self, field_params: Mapping[str, AlterDiff]):
        # Take field type from schema. If that field was user-defined
        # and does not exist anymore then we use CommonFieldType as
        # fallback variant
        field_type_cls = mongoengine_fields_mapping(self.current_schema.get('type_key'))
        field_type = field_type_cls(self.collection, self.current_schema)

        # Change field type if requested. Then trying to obtain new
        # FieldType class and process the rest
        if 'type_key' in field_params:
            field_type.change_param('type_key', field_params['type_key'].new)

            field_type_cls = mongoengine_fields_mapping(field_params['type_key'].new)
            field_type = field_type_cls(
                self.collection,
                self.current_schema.get(self.field_name, {})
            )

        for name, diff in field_params.items():
            if name == 'type_key':
                continue

            field_type.change_param(name, diff)

    @classmethod
    def _fix_field_params(cls,
                          collection_name: str,
                          field_name: str,
                          field_params: Mapping[str, AlterDiff],
                          old_schema: dict,
                          new_schema: dict) -> Mapping[str, AlterDiff]:
        """
        Search for potential problems which could be happened during
        migration and return fixed field schema. If such problem
        could not be resolved only by changing parameters then it
        raises error.

        :param collection_name:
        :param field_name:
        :param field_params:
        :param old_schema:
        :param new_schema:
        :raises ActionError: when some problem found
        :return:
        """
        # TODO: Check all defaults in diffs against choices, required, etc.
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
            raise ActionError(f'Field {collection_name}.{field_name} could not be '
                              f'created since it defined as required but has not a default value')

        return field_params
