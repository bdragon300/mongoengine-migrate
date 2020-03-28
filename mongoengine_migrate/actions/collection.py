from .base import BaseCollectionAction
from dictdiffer import diff
from mongoengine_migrate.exceptions import ActionError


# Empty collection schema contents skeleton
collection_schema_skel = {}


class CreateCollection(BaseCollectionAction):
    """
    Action which creates new collection
    """
    @classmethod
    def build_object_if_applicable(cls, collection_name, old_schema, new_schema):
        if collection_name not in old_schema and collection_name in new_schema:
            return cls(collection_name=collection_name)  # FIXME: parameters (indexes, acl, etc.)

    def as_schema_patch(self):
        return [('add', '', [(self.collection_name, collection_schema_skel)])]

    def run_forward(self, db, collection):
        """
        Mongodb automatically creates collection on the first insert
        So, do nothing
        FIXME: parameters (indexes, acl, etc.)
        """

    def run_backward(self, db, collection):
        collection.drop()


class DropCollection(BaseCollectionAction):
    """Action which drops existing collection"""
    @classmethod
    def build_object_if_applicable(cls, collection_name, old_schema, new_schema):
        if collection_name in old_schema and collection_name not in new_schema:
            return cls(collection_name=collection_name)  # FIXME: parameters (indexes, acl, etc.)

    def as_schema_patch(self):
        if self.current_schema is None:
            raise ActionError('Action was not initialized with a current schema')
        if self.collection_name not in self.current_schema:
            raise ActionError(f'Schema does not contain collection {self.collection_name!r}')
        new_schema = self.current_schema.copy()
        del new_schema[self.collection_name]

        return diff(self.current_schema, new_schema)

    def run_forward(self, db, collection):
        """
        Mongodb automatically creates collection on the first insert
        So, do nothing
        """
        collection.drop()

    def run_backward(self, db, collection):
        """
        Mongodb automatically creates collection on the first insert
        So, do nothing
        FIXME: parameters (indexes, acl, etc.)
        """
