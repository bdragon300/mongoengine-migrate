from .base import BaseCollectionAction

# Empty collection schema contents skeleton
collection_schema_skel = {}


class CreateCollection(BaseCollectionAction):
    """Create new collection

    Accepts collection name as parameter such as:
    `CreateCollection("collection1")`
    """
    @classmethod
    def build_object_if_applicable(cls, collection_name: str, old_schema: dict, new_schema: dict):
        if collection_name not in old_schema and collection_name in new_schema:
            return cls(collection_name=collection_name)  # FIXME: parameters (indexes, acl, etc.)

    def to_schema_patch(self, current_schema: dict):
        return [('add', '', [(self.collection_name, collection_schema_skel)])]

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
        return [('remove', '', [(self.collection_name, collection_schema_skel)])]

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
