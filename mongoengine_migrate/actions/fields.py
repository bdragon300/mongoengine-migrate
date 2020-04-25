from typing import Mapping

from mongoengine_migrate.exceptions import ActionError, MigrationError
from mongoengine_migrate.fields.registry import type_key_registry
from .base import BaseFieldAction
from .diff import AlterDiff, UNSET


class CreateField(BaseFieldAction):
    """Create field in a given collection"""
    @classmethod
    def build_object(cls,
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
            **self.field_handler_cls.schema_skel(),
            **self.parameters
        }
        return [(
            'add',
            self.collection_name,
            [(self.field_name, field_params)]
        )]

    def run_forward(self):
        """
        If field is defined as required then force create it with
        default value. Otherwise do nothing since mongoengine creates
        fields automatically on value set
        FIXME: parameters (indexes, acl, etc.)
        """
        is_required = self.parameters.get('required') or self.parameters.get('primary_key')
        default = self.parameters.get('default')
        if is_required:
            self.collection.update_many(
                {self.field_name: {'$exists': False}}, {'$set': {self.field_name: default}}
            )

    def run_backward(self):
        """Drop field"""
        self.collection.update_many(
            {self.field_name: {'$exists': True}}, {'$unset': {self.field_name: ''}}
        )


class DropField(BaseFieldAction):
    """Drop field in a given collection"""
    @classmethod
    def build_object(cls,
                     collection_name: str,
                     field_name: str,
                     old_schema: dict,
                     new_schema: dict):
        match = collection_name in old_schema \
                and collection_name in new_schema \
                and field_name in old_schema[collection_name] \
                and field_name not in new_schema[collection_name]
        if match:
            field_params = old_schema[collection_name][field_name]
            return cls(collection_name=collection_name,
                       field_name=field_name,
                       **field_params)

    def to_schema_patch(self, current_schema: dict):
        if self.collection_name not in current_schema:
            raise ActionError(f'Cannot drop field {self.collection_name}.{self.field_name} '
                              f'since the collection {self.collection_name} is not in schema')
        field_params = {
            **self.field_handler_cls.schema_skel(),
            **self.parameters
        }
        return [(
            'remove',
            self.collection_name,
            [(self.field_name, field_params)]
        )]

    def run_forward(self):
        """Drop field"""
        self.collection.update_many(
            {self.field_name: {'$exists': True}}, {'$unset': {self.field_name: ''}}
        )

    def run_backward(self):
        """
        If field is defined as required then force create it with
        default value. Otherwise do nothing since mongoengine creates
        fields automatically on value set
        """
        is_required = self.parameters.get('required') or self.parameters.get('primary_key')
        default = self.parameters.get('default')
        if is_required:
            self.collection.update_many(
                {self.field_name: {'$exists': False}}, {'$set': {self.field_name: default}}
            )


class AlterField(BaseFieldAction):
    """Change field parameters or its type, i.e. altering"""
    def __init__(self, collection_name: str, field_name: str, **kwargs):
        super().__init__(collection_name, field_name, **kwargs)
        if not all(isinstance(v, AlterDiff) for v in self.parameters.values()):
            raise ActionError(f'Keyword parameters must be AlterDiff objects')

    @classmethod
    def build_object(cls,
                     collection_name: str,
                     field_name: str,
                     old_schema: dict,
                     new_schema: dict):
        # Check if field still here but its schema has changed
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
        for param, diff in self.parameters.items():
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
        self._run_migration(self.parameters)

    def run_backward(self):
        reversed_field_params = {k: v.swap() for k, v in self.parameters.items()}
        self._run_migration(reversed_field_params)

    def _run_migration(self, field_params: Mapping[str, AlterDiff]):
        """
        Iterates over action parameters (AlterDiff objects) and
        executes handler for each one
        """
        # Take field type from schema. If that field was user-defined
        # and does not exist anymore then we use CommonFieldHandler as
        # fallback variant
        # FIXME: raise if self.collection_name/self.field_name not in schema
        field_schema = self.current_schema.get(self.collection_name, {}).get(self.field_name, {})
        field_handler = self._get_field_handler(field_schema.get('type_key'))

        # Change field type if requested. Then trying to obtain new
        # FieldHandler class and process the rest
        if 'type_key' in field_params:
            try:  # FIXME: remove try
                field_handler.change_param('type_key', field_params['type_key'])
            except:
                pass
            field_handler = self._get_field_handler(field_params['type_key'].new)

        for name, diff in field_params.items():
            if name == 'type_key':
                continue

            try:  # FIXME: remove try
                field_handler.change_param(name, diff)
            except:
                pass

    def _get_field_handler(self, type_key: str):
        """
        Return FieldHandler object by type_key
        :param type_key: `type_key` item of schema
        :return: concrete FieldHandler object
        """
        # TODO: raise if "not type_name"
        if type_key not in type_key_registry:
            raise MigrationError(f'Could not find field {type_key!r} or one of its base classes '
                                 f'in type_key registry')

        handler_cls = type_key_registry[type_key].field_handler_cls
        handler = handler_cls(
            self.collection,
            self.current_schema.get(self.collection_name, {}).get(self.field_name, {})
        )
        return handler

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
        could not be resolved only by changing parameters then raise
        an ActionError
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
    """Rename field"""
    higher_priority = True

    #: How much percent of items in schema diff of two fields in the
    #: same collection should be equal to consider such change as
    #: field rename instead of drop/create
    similarity_threshold = 70

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if 'new_name' not in kwargs:
            raise ActionError("'new_name' keyword parameter is not specified")

    @classmethod
    def build_object(cls,
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
        for name, schema in new_schema[collection_name].items():
            # Skip fields which was not renamed
            # Changing 'db_field' parameter is altering, not renaming
            if name in old_schema[collection_name]:
                continue

            # Model field renamed, but db field is the same
            db_field = schema.get('db_field')
            if db_field == name:
                candidates = [(name, schema)]
                break

            # Take only common keys to estimate similarity
            # 'type_key' may get changed which means that change of one
            # key leads to many changes in schema. These changes
            # should not be considered as valueable
            keys = old_field_schema.keys() & schema.keys() - {'db_field'}
            percent = sum(old_field_schema[k] == schema[k] for k in keys) / len(keys) * 100
            if percent >= cls.similarity_threshold:
                candidates.append((name, schema))

        if len(candidates) == 1:
            return cls(collection_name=collection_name,
                       field_name=field_name,
                       new_name=candidates[0][0])

    def to_schema_patch(self, current_schema: dict):
        if self.collection_name not in current_schema:
            raise ActionError(f'Cannot rename field {self.collection_name}.{self.field_name} '
                              f'since the collection {self.collection_name} is not in schema')
        new_name = self.parameters['new_name']
        item = current_schema[self.collection_name].get(
            self.field_name,
            current_schema[self.collection_name].get(new_name)
        )
        return [
            ('remove', f'{self.collection_name}', [(self.field_name, item)]),
            ('add', f'{self.collection_name}', [(new_name, item)])
        ]

    def run_forward(self):
        """Renaming mongoengine model field does not required some
        db changes. It's supposed that possible `db_field` modification,
        which could exist during field renaming, is handled by
        AlterField action
        """
        pass

    def run_backward(self):
        """Renaming mongoengine model field does not required some
        db changes. It's supposed that possible `db_field` modification,
        which could exist during field renaming, is handled by
        AlterField action
        """
        pass
