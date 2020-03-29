from .base import BaseCollectionAction
from dictdiffer import diff
from mongoengine_migrate.exceptions import ActionError


# Empty collection schema contents skeleton
collection_schema_skel = {}


class CreateCollection(BaseCollectionAction):
    """Create new collection

    Accepts collection name as parameter such as:
    `CreateCollection("collection1")`
    """
    @classmethod
    def build_object_if_applicable(cls, collection_name: str, old_schema: str, new_schema: str):
        if collection_name not in old_schema and collection_name in new_schema:
            return cls(collection_name=collection_name)  # FIXME: parameters (indexes, acl, etc.)

    def to_schema_patch(self, current_schema: dict):
        """
        Return dictdiff patch which this Action is applied to a schema
        during forward run

        The main goal of this Action is to create collection. So this
        method raises ActionError if this goal could not be reached --
        if collection with that name is already exists in schema
        :param current_schema:
        :return: dictdiffer diff
        """
        if self.collection_name in current_schema:
            raise ActionError(f'Could not create collection {self.collection_name!r} since it '
                              f'already created')
        new_schema = current_schema.copy()
        new_schema[self.collection_name] = collection_schema_skel

        return diff(current_schema, new_schema)

    def run_forward(self):
        """
        Mongodb automatically creates collection on the first insert
        So, do nothing
        FIXME: parameters (indexes, acl, etc.)
        """

    def run_backward(self):
        """Drop collection in backward direction"""
        self.collection.drop()


class DropCollection(BaseCollectionAction):
    """Drop collection

    Accepts collection name as parameter such as:
    `DropCollection("collection1")`
    """
    @classmethod
    def build_object_if_applicable(cls, collection_name: str, old_schema: dict, new_schema: dict):
        if collection_name in old_schema and collection_name not in new_schema:
            return cls(collection_name=collection_name)  # FIXME: parameters (indexes, acl, etc.)

    def to_schema_patch(self, current_schema: dict):
        """
        Return dictdiff patch which this Action is applied to a schema
        during forward run

        The main goal of this Action is to drop collection. So this
        method does not concern if that collection is already dropped --
        it just means that the goal is already reached
        :param current_schema:
        :return: dictdiffer diff
        """
        if self.collection_name not in current_schema:
            return []
        new_schema = current_schema.copy()
        del new_schema[self.collection_name]

        return diff(current_schema, new_schema)

    def run_forward(self):
        """
        Mongodb automatically creates collection on the first insert
        So, do nothing
        """
        self.collection.drop()

    def run_backward(self):
        """
        Mongodb automatically creates collection on the first insert
        So, do nothing
        FIXME: parameters (indexes, acl, etc.)
        """
