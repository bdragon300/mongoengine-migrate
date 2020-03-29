from .base import BaseFieldAction
from mongoengine_migrate.fields.base import schema_fields_mapping
from mongoengine_migrate.exceptions import SchemaError


class CreateField(BaseFieldAction):
    @classmethod
    def build_object_if_applicable(cls, collection_name, field_name, old_schema, new_schema):
        if collection_name in new_schema and field_name in new_schema[collection_name]:
            field_schema = new_schema[collection_name][field_name]
            type_key = field_schema['type_key']
            return cls(collection_name=collection_name,
                       field_name=field_name,
                       field_type_cls=schema_fields_mapping[type_key],
                       **field_schema
                       )

    def to_schema_patch(self, current_schema):
        return [('add', '', [(
            self.collection_name,
            {self.field_name: self.field_type_cls.schema_skel()}
        )])]

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
    pass
