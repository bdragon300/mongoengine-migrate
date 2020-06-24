from typing import Mapping, Any

from mongoengine_migrate.exceptions import ActionError
from mongoengine_migrate.mongo import DocumentUpdater
from .base import BaseFieldAction
from .diff import AlterDiff, UNSET


class CreateField(BaseFieldAction):
    """Create field in a given collection"""
    @classmethod
    def build_object(cls,
                     collection_name: str,
                     field_name: str,
                     left_schema: dict,
                     right_schema: dict):
        match = collection_name in left_schema \
                and collection_name in right_schema \
                and field_name not in left_schema[collection_name] \
                and field_name in right_schema[collection_name]
        if match:
            field_params = right_schema[collection_name][field_name]
            return cls(collection_name=collection_name,
                       field_name=field_name,
                       **field_params
                       )

    def to_schema_patch(self, left_schema: dict):
        keys_to_check = {'type_key', 'db_field'}
        if not(keys_to_check < self.parameters.keys()):
            raise ActionError(f"{keys_to_check} parameters are required in CreateField action")

        # Get schema skeleton for field type
        field_handler_cls = self.get_field_handler_cls(self.parameters['type_key'])
        right_field_schema_skel = field_handler_cls.schema_skel()
        extra_keys = self.parameters.keys() - right_field_schema_skel.keys()
        if extra_keys:
            raise ActionError(f'Unknown schema parameters: {extra_keys}')

        field_params = {
            **right_field_schema_skel,
            **self.parameters
        }
        return [(
            'add',
            self.orig_collection_name,
            [(self.field_name, field_params)]
        )]

    def run_forward(self):
        """
        If field is defined as required then force create it with
        default value. Otherwise do nothing since mongoengine creates
        fields automatically on value set
        FIXME: parameters (indexes, acl, etc.)
        """
        def by_path(col, filter_dotpath, update_dotpath, array_filters):
            col.update_many(
                {filter_dotpath: {'$exists': False}},
                {'$set': {update_dotpath: default}},
                array_filters=array_filters
            )

        is_required = self.parameters.get('required') or self.parameters.get('primary_key')
        default = self.parameters.get('default')
        if is_required:
            db_field = self.parameters['db_field']
            updater = DocumentUpdater(self._run_ctx['db'],
                                      self.orig_collection_name,
                                      db_field,
                                      self._run_ctx['left_schema'])
            updater.update_by_path(by_path)

    def run_backward(self):
        """Drop field"""
        def by_path(col, filter_dotpath, update_dotpath, array_filters):
            col.update_many(
                {filter_dotpath: {'$exists': True}},
                {'$unset': update_dotpath}
            )

        db_field = self.parameters['db_field']
        updater = DocumentUpdater(self._run_ctx['db'],
                                  self.orig_collection_name,
                                  db_field,
                                  self._run_ctx['left_schema'])
        updater.update_by_path(by_path)


class DropField(BaseFieldAction):
    """Drop field in a given collection"""
    @classmethod
    def build_object(cls,
                     collection_name: str,
                     field_name: str,
                     left_schema: dict,
                     right_schema: dict):
        match = collection_name in left_schema \
                and collection_name in right_schema \
                and field_name in left_schema[collection_name] \
                and field_name not in right_schema[collection_name]
        if match:
            return cls(collection_name=collection_name, field_name=field_name)

    def to_schema_patch(self, left_schema: dict):
        try:
            left_field_schema = left_schema[self.orig_collection_name][self.field_name]
        except KeyError:
            raise ActionError(f'Cannot alter field {self.orig_collection_name}.{self.field_name} '
                              f'since the collection {self.orig_collection_name} is not in schema')

        return [(
            'remove',
            self.orig_collection_name,
            [(self.field_name, left_field_schema)]
        )]

    def run_forward(self):
        """Drop field"""
        def by_path(col, filter_dotpath, update_dotpath, array_filters):
            col.update_many(
                {filter_dotpath: {'$exists': True}},
                {'$unset': update_dotpath}
            )

        db_field = self._run_ctx['left_field_schema']['db_field']
        updater = DocumentUpdater(self._run_ctx['db'],
                                  self.orig_collection_name,
                                  db_field,
                                  self._run_ctx['left_schema'])
        updater.update_by_path(by_path)

    def run_backward(self):
        """
        If field is defined as required then force create it with
        default value. Otherwise do nothing since mongoengine creates
        fields automatically on value set
        """
        def by_path(col, filter_dotpath, update_dotpath, array_filters):
            col.update_many(
                {filter_dotpath: {'$exists': False}},
                {'$set': {update_dotpath: default}},
                array_filters=array_filters
            )

        is_required = self._run_ctx['left_field_schema'].get('required')
        default = self._run_ctx['left_field_schema'].get('default')
        if is_required:
            db_field = self._run_ctx['left_field_schema']['db_field']
            updater = DocumentUpdater(self._run_ctx['db'],
                                      self.orig_collection_name,
                                      db_field,
                                      self._run_ctx['left_schema'])
            updater.update_by_path(by_path)


class AlterField(BaseFieldAction):
    """Change field parameters or its type, i.e. altering"""
    @classmethod
    def build_object(cls,
                     collection_name: str,
                     field_name: str,
                     left_schema: dict,
                     right_schema: dict):
        # Check if field still here but its schema has changed
        match = collection_name in left_schema \
                and collection_name in right_schema \
                and field_name in left_schema[collection_name] \
                and field_name in right_schema[collection_name] \
                and left_schema[collection_name][field_name] != right_schema[collection_name][field_name]
        if match:
            # Consider items which was changed and added, skip those
            # ones which was unchanged or was removed
            right_field_schema = right_schema[collection_name][field_name]
            left_field_schema = left_schema[collection_name][field_name]
            action_params = dict(right_field_schema.items() - left_field_schema.items())
            # FIXME: use function below
            # field_params = cls._fix_field_params(collection_name,
            #                                      field_name,
            #                                      field_params,
            #                                      old_schema,
            #                                      new_schema)
            return cls(collection_name=collection_name, field_name=field_name, **action_params)

    def to_schema_patch(self, left_schema: dict):
        try:
            left_field_schema = left_schema[self.orig_collection_name][self.field_name]
        except KeyError:
            raise ActionError(f'Cannot alter field {self.orig_collection_name}.{self.field_name} '
                              f'since the collection {self.orig_collection_name} is not in schema')

        # Get schema skeleton for field type
        field_handler_cls = self.get_field_handler_cls(
            self.parameters.get('type_key', left_field_schema['type_key'])
        )
        right_schema_skel = field_handler_cls.schema_skel()
        extra_keys = self.parameters.keys() - right_schema_skel.keys()
        if extra_keys:
            raise ActionError(f'Unknown schema parameters: {extra_keys}')

        # Shortcuts
        left = left_field_schema
        params = self.parameters

        # Remove params
        d = [('remove', f'{self.orig_collection_name}.{self.field_name}', [(key, ())])
             for key in left.keys() - right_schema_skel.keys()]
        # Add new params
        d += [('add', f'{self.orig_collection_name}.{self.field_name}', [(key, params[key])])
              for key in params.keys() - left.keys()]
        # Change params if they are requested to be changed
        d += [('change',
               f'{self.orig_collection_name}.{self.field_name}.{key}',
               (left[key], params[key]))
              for key in params.keys() & left.keys()
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
            field_handler.change_param(db_field, 'type_key')
            field_handler = self._get_field_handler(right_field_schema['type_key'],
                                                    left_field_schema,
                                                    right_field_schema)

        # Try to process all parameters on same order to avoid
        # potential problems on repeated launches if some query on
        # previous lauch was failed
        for name, new_value in sorted(right_field_schema.items()):
            old_value = left_field_schema.get(name, UNSET)
            if name == 'type_key' or new_value == old_value:
                continue

            field_handler.change_param(db_field, name)

            # If `db_field` was changed then work with new name further
            if name == 'db_field':
                db_field = right_field_schema['db_field']

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
    priority = 10

    #: How much percent of items in schema diff of two fields in the
    #: same collection should be equal to consider such change as
    #: field rename instead of drop/create
    similarity_threshold = 70

    def __init__(self, collection_name: str, field_name: str, new_name, **kwargs):
        super().__init__(collection_name, field_name, new_name=new_name, **kwargs)
        self.new_name = new_name

    @classmethod
    def build_object(cls,
                     collection_name: str,
                     field_name: str,
                     left_schema: dict,
                     right_schema: dict):
        # Check if field exists under different name in schema
        # Field also can have small schema changes in the same time
        # So we try to get similarity percentage and if it more than
        # threshold then we're consider such change as rename/alter.
        # Otherwise it is drop/create
        match = collection_name in left_schema \
                and collection_name in right_schema \
                and field_name in left_schema[collection_name] \
                and field_name not in right_schema[collection_name]
        if not match:
            return

        left_field_schema = left_schema[collection_name][field_name]
        candidates = []
        for right_field_name, right_field_schema in right_schema[collection_name].items():
            # Skip fields which was not renamed
            # Changing 'db_field' parameter is altering, not renaming
            if right_field_name in left_schema[collection_name]:
                continue

            # Model field renamed, but db field is the same
            db_field = right_field_schema.get('db_field')
            if db_field == right_field_name:
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
            return cls(collection_name=collection_name,
                       field_name=field_name,
                       new_name=candidates[0][0])

    def to_schema_patch(self, left_schema: dict):
        try:
            left_field_schema = left_schema[self.orig_collection_name][self.field_name]
        except KeyError:
            raise ActionError(f'Cannot alter field {self.orig_collection_name}.{self.field_name} '
                              f'since the collection {self.orig_collection_name} is not in schema')

        return [
            ('remove', f'{self.orig_collection_name}', [(self.field_name, left_field_schema)]),
            ('add', f'{self.orig_collection_name}', [(self.new_name, left_field_schema)])
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
