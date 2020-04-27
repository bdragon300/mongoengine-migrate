from .base import BaseCollectionAction
from mongoengine_migrate.exceptions import ActionError


class CreateCollection(BaseCollectionAction):
    """Create new collection

    Ex.: `CreateCollection("collection1")`
    """
    @classmethod
    def build_object(cls, collection_name: str, old_schema: dict, new_schema: dict):
        if collection_name not in old_schema and collection_name in new_schema:
            return cls(collection_name=collection_name)  # FIXME: parameters (indexes, acl, etc.)

    def to_schema_patch(self, current_schema: dict):
        return [('add', '', [(self.collection_name, self.COLLECTION_SCHEMA_SKEL)])]

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

    Ex.: `DropCollection("collection1")`
    """
    @classmethod
    def build_object(cls, collection_name: str, old_schema: dict, new_schema: dict):
        if collection_name in old_schema and collection_name not in new_schema:
            return cls(collection_name=collection_name)  # FIXME: parameters (indexes, acl, etc.)

    def to_schema_patch(self, current_schema: dict):
        return [('remove', '', [(self.collection_name, self.COLLECTION_SCHEMA_SKEL)])]

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


class RenameCollection(BaseCollectionAction):
    """Rename collection

    Ex.: `RenameCollection("collection1", new_name="collection2")`
    """
    higher_priority = True

    #: How much percent of items in schema diff of two collections
    #: should be equal to consider such change as collection rename
    #: instead of drop/create
    similarity_threshold = 70

    def __init__(self, collection_name: str, new_name, **kwargs):
        super().__init__(collection_name, new_name=new_name, **kwargs)
        self.new_name = new_name

    @classmethod
    def build_object(cls, collection_name: str, old_schema: dict, new_schema: dict):
        # Check if field exists under different name in schema.
        # Field also can have small schema changes in the same time
        # So we try to get similarity percentage and if it more than
        # threshold then we're consider such change as rename/alter.
        # Otherwise it is drop/create
        match = collection_name in old_schema and collection_name not in new_schema
        if not match:
            return

        old_col_schema = old_schema[collection_name]
        candidates = []
        matches = 0
        compares = 0
        for name, schema in new_schema.items():
            # Skip collections which was not renamed
            if name in old_schema:
                continue

            # Exact match, collection was just renamed
            if old_col_schema == schema:
                candidates = [(name, schema)]
                break

            # Try to find collection by its schema similarity
            # Compares are counted as every field schema comparing
            fields = old_col_schema.keys() | schema.keys()
            for field_name in fields:
                old_field_schema = old_col_schema.get(field_name, {})
                field_schema = schema.get(field_name, {})
                common_keys = old_field_schema.keys() & field_schema.keys()
                compares += len(common_keys)
                matches += sum(
                    old_field_schema[k] == field_schema[k]
                    for k in common_keys
                )

            if (matches / compares * 100) >= cls.similarity_threshold:
                candidates.append((name, schema))

        if len(candidates) == 1:
            return cls(collection_name=collection_name, new_name=candidates[0][0])

    def to_schema_patch(self, current_schema: dict):
        item = current_schema.get(
            self.collection_name,
            current_schema.get(self.new_name)
        )
        return [
            ('remove', '', [(self.collection_name, item)]),
            ('add', '', [(self.new_name, item)])
        ]

    def run_forward(self):
        if self.collection.name in self.collection.database.list_collection_names():
            self.collection.rename(self.new_name)

    def run_backward(self):
        if self.collection.name in self.collection.database.list_collection_names():
            self.collection.rename(self.collection_name)
