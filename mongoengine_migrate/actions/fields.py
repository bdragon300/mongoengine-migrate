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

    def as_schema_patch(self, current_schema):
        return [('add', '', [(
            self.collection_name,
            {self.field_name: self.field_type_cls.schema_skel()}
        )])]

    def run_forward(self, db, collection):
        """
        FIXME: default
        So, do nothing
        FIXME: parameters (indexes, acl, etc.)
        """

    def run_backward(self, db, collection):
        collection.drop()


class DropField(BaseFieldAction):
    pass
