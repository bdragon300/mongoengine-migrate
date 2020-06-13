from .base import BaseCreateDocument, BaseDropDocument, BaseRenameDocument
from mongoengine_migrate.flags import EMBEDDED_DOCUMENT_NAME_PREFIX


class CreateCollection(BaseCreateDocument):
    """Create new collection

    Ex.: `CreateCollection("collection1")`
    # FIXME: parameters (indexes, acl, etc.)
    """
    priority = 8

    @classmethod
    def build_object(cls, collection_name: str, left_schema: dict, right_schema: dict):
        if collection_name.startswith(EMBEDDED_DOCUMENT_NAME_PREFIX):
            # This is an embedded document
            return None

        return super().build_object(collection_name, left_schema, right_schema)

    def run_forward(self):
        """
        Mongodb automatically creates collection on the first insert
        So, do nothing
        FIXME: parameters (indexes, acl, etc.)
        """

    def run_backward(self):
        """Drop collection in backward direction"""
        self._run_ctx['collection'].drop()


class DropCollection(BaseDropDocument):
    """Drop collection

    Ex.: `DropCollection("collection1")`
    """
    priority = 16

    @classmethod
    def build_object(cls, collection_name: str, left_schema: dict, right_schema: dict):
        if collection_name.startswith(EMBEDDED_DOCUMENT_NAME_PREFIX):
            # This is an embedded document
            return None

        return super().build_object(collection_name, left_schema, right_schema)

    def run_forward(self):
        """
        Mongodb automatically creates collection on the first insert
        So, do nothing
        """
        self._run_ctx['collection'].drop()

    def run_backward(self):
        """
        Mongodb automatically creates collection on the first insert
        So, do nothing
        FIXME: parameters (indexes, acl, etc.)
        """


class RenameCollection(BaseRenameDocument):
    """Rename collection

    Ex.: `RenameCollection("collection1", new_name="collection2")`
    """
    priority = 6

    @classmethod
    def build_object(cls, collection_name: str, left_schema: dict, right_schema: dict):
        if collection_name.startswith(EMBEDDED_DOCUMENT_NAME_PREFIX):
            # This is an embedded document
            return None

        return super().build_object(collection_name, left_schema, right_schema)

    def run_forward(self):
        collection_names = self._run_ctx['collection'].database.list_collection_names()
        if self._run_ctx['collection'].name in collection_names:
            self._run_ctx['collection'].rename(self.new_name)

    def run_backward(self):
        collection_names = self._run_ctx['collection'].database.list_collection_names()
        if self._run_ctx['collection'].name in collection_names:
            self._run_ctx['collection'].rename(self.collection_name)
