from .base import BaseCreateDocument, BaseDropDocument, BaseRenameDocument
from mongoengine_migrate.flags import EMBEDDED_DOCUMENT_NAME_PREFIX


class CreateEmbedded(BaseCreateDocument):
    """Create new embedded document"""
    @classmethod
    def build_object(cls, collection_name: str, left_schema: dict, right_schema: dict):
        if not collection_name.startswith(EMBEDDED_DOCUMENT_NAME_PREFIX):
            # This is not an embedded document
            return None

        return super().build_object(collection_name, left_schema, right_schema)

    def run_forward(self):
        """Embedded documents are not required to do anything"""

    def run_backward(self):
        """Embedded documents are not required to do anything"""


class DropEmbedded(BaseDropDocument):
    """Drop embedded document"""
    @classmethod
    def build_object(cls, collection_name: str, left_schema: dict, right_schema: dict):
        if not collection_name.startswith(EMBEDDED_DOCUMENT_NAME_PREFIX):
            # This is not an embedded document
            return None

        return super().build_object(collection_name, left_schema, right_schema)

    def run_forward(self):
        """Embedded documents are not required to do anything"""

    def run_backward(self):
        """Embedded documents are not required to do anything"""


class RenameEmbedded(BaseRenameDocument):
    """Rename embedded document"""
    @classmethod
    def build_object(cls, collection_name: str, left_schema: dict, right_schema: dict):
        if not collection_name.startswith(EMBEDDED_DOCUMENT_NAME_PREFIX):
            # This is not an embedded document
            return None

        return super().build_object(collection_name, left_schema, right_schema)

    def run_forward(self):
        """Embedded documents are not required to do anything"""

    def run_backward(self):
        """Embedded documents are not required to do anything"""
