from typing import Mapping

from mongoengine_migrate.exceptions import ActionError
from .base import BaseFieldAction
from mongoengine_migrate.fields.base import mongoengine_fields_mapping
from .diff import AlterDiff, UNSET


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
                              f'since the collection {self.collection_name} is not in schema')
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
            raise ActionError(f'Cannot drop field {self.collection_name}.{self.field_name} '
                              f'since the collection {self.collection_name} is not in schema')
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
            field_params = {k: AlterDiff(old_params.get(k, UNSET), new_params.get(k, UNSET))
                            for k in new_params.keys() | old_params.keys()
                            if old_params.get(k, object()) != new_params.get(k, object())}
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
            raise ActionError(f'Cannot alter field {self.collection_name}.{self.field_name} '
                              f'since the collection {self.collection_name} is not in schema')
        # TODO raise если param не в skel нового типа при изменении type_key
        # TODO а что делать если параметр появляется или изчезает, но не указан в kwargs
        p = []
        for param, diff in self._init_kwargs.items():
            if diff.old == UNSET or diff.new == UNSET:
                if diff.old == UNSET:
                    p.append(
                        ('add', f'{self.collection_name}.{self.field_name}', [(param, diff.new)])
                    )
                if diff.new == UNSET:
                    p.append(('remove', f'{self.collection_name}.{self.field_name}', [(param, ())]))
            else:
                p.append(('change', f'{self.collection_name}.{self.field_name}.{param}', diff.diff))
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
        field_schema = self.current_schema.get(self.collection_name, {}).get(self.field_name, {})
        field_type = self._get_field_type_cls(field_schema.get('type_key'))

        # Change field type if requested. Then trying to obtain new
        # FieldType class and process the rest
        if 'type_key' in field_params:
            try:  # FIXME: remove try
                field_type.change_param('type_key', field_params['type_key'])
            except:
                pass
            field_type = self._get_field_type_cls(field_params['type_key'].new)

        for name, diff in field_params.items():
            if name == 'type_key':
                continue
            print(name, diff)
            try:  # FIXME: remove try
                field_type.change_param(name, diff)
            except:
                pass

    def _get_field_type_cls(self, type_name: str):
        # TODO: raise if "not type_name"
        field_type_cls = mongoengine_fields_mapping.get(type_name)
        field_type = field_type_cls(
            self.collection,
            self.current_schema.get(self.collection_name, {}).get(self.field_name, {})
        )
        return field_type

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
            raise ActionError(f'Field {collection_name}.{field_name} could not be '
                              f'created since it defined as required but has not a default value')

        return field_params


class RenameField(BaseFieldAction):
    similarity_threshold = 70

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if 'new_name' not in kwargs:
            raise ActionError("'new_name' keyword parameter is not specified")

    @classmethod
    def build_object_if_applicable(cls,
                                   collection_name: str,
                                   field_name: str,
                                   old_schema: dict,
                                   new_schema: dict):
        # Check if field exists under different name in schema
        # Field also can have small schema changes in the same time
        # So we try to get similarity percentage and if it more than
        # threshold then we're consider such change as rename/alter.
        # Otherwise it is drop/create
        match = collection_name in old_schema \
                and collection_name in new_schema \
                and field_name in old_schema[collection_name] \
                and field_name not in new_schema[collection_name]
        if not match:
            return

        old_field_schema = old_schema[collection_name][field_name]
        candidates = []
        for field_name, field_schema in new_schema[collection_name]:
            # Skip fields which was not renamed
            # Changing 'db_field' parameter is altering, not renaming
            if field_name not in old_schema[collection_name]:
                continue

            db_field = field_schema.get('db_field')
            if db_field == field_name:
                candidates = [(field_name, field_schema)]
                break

            # Take only common keys to estimate similarity
            # 'type_key' may get changed which means that change of one
            # key leads to many changes in schema. These changes
            # should not be considered as valueable
            keys = old_field_schema.keys() & field_schema.keys() - {'db_field'}
            percent = sum(old_field_schema[k] == field_schema[k] for k in keys) / len(keys)
            if percent >= cls.similarity_threshold:
                candidates.append((field_name, field_schema))

        if len(candidates) == 1:
            return cls(collection_name=collection_name,
                       field_name=field_name,
                       new_name=candidates[0][0])

    def to_schema_patch(self, current_schema: dict):
        if self.collection_name not in current_schema:
            raise ActionError(f'Cannot rename field {self.collection_name}.{self.field_name} '
                              f'since the collection {self.collection_name} is not in schema')
        if self.field_name not in current_schema[self.collection_name]:
            raise ActionError(f'Cannot rename field {self.collection_name}.{self.field_name} '
                              f'since the field {self.collection_name} is not in collection schema')

        item = current_schema[self.collection_name][self.field_name]
        return [
            ('remove', f'{self.collection_name}', [(self.field_name, ())]),
            ('add', f'{self.collection_name}', [(self.field_name, item)])
        ]

    def run_forward(self):
        db_field = self.current_schema['db_field']
        self.collection.aggregate([
            {'$rename': {db_field: self._init_kwargs['new_name']}}
        ])

    def run_backward(self):
        db_field = self.current_schema['db_field']
        self.collection.aggregate([
            {'$rename': {self._init_kwargs['new_name']: db_field}}
        ])
