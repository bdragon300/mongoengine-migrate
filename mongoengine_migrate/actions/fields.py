from mongoengine_migrate.exceptions import ActionError
from .base import BaseFieldAction


class CreateField(BaseFieldAction):
    @classmethod
    def build_object_if_applicable(cls,
                                   collection_name: str,
                                   field_name: str,
                                   old_schema: dict,
                                   new_schema: dict):
        match = all((collection_name in old_schema,
                     collection_name in new_schema,
                     field_name not in old_schema[collection_name],
                     field_name in new_schema[collection_name]))
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

    # @classmethod
    # def _fix_field_schema(cls,
    #                       collection_name: str,
    #                       field_name: str,
    #                       field_params: dict,
    #                       old_schema: dict,
    #                       new_schema: dict) -> dict:
    #     #FIXME: move to AlterField
    #     """
    #     Search for potential problems which could be happened during
    #     migration and return fixed field schema. If such problem
    #     could not be resolved only by changing parameters then it
    #     raises error.
    #
    #     :param collection_name:
    #     :param field_name:
    #     :param field_params:
    #     :param old_schema:
    #     :param new_schema:
    #     :raises ActionError: when some problem found
    #     :return:
    #     """
    #     is_required = field_params.get('required')
    #     default = field_params.get('default')
    #     if is_required and default is None:
    #         # TODO: replace following error on interactive mode
    #         raise ActionError(f'Field {collection_name}.{field_name} could not be created '
    #                           f'since it defined as required but has not a default value')
    #
    #     return field_params


class DropField(BaseFieldAction):
    @classmethod
    def build_object_if_applicable(cls,
                                   collection_name: str,
                                   field_name: str,
                                   old_schema: dict,
                                   new_schema: dict):
        match = all((collection_name in old_schema,
                     collection_name in new_schema,
                     field_name in old_schema[collection_name],
                     field_name not in new_schema[collection_name]))
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
