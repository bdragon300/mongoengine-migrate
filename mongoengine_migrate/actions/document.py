__all__ = [
    'CreateDocument',
    'DropDocument',
    'RenameDocument',
    'AlterDocument'
]

from mongoengine_migrate.flags import EMBEDDED_DOCUMENT_NAME_PREFIX
from mongoengine_migrate.schema import Schema
from .base import BaseCreateDocument, BaseDropDocument, BaseRenameDocument, BaseAlterDocument


class CreateDocument(BaseCreateDocument):
    """Create new document in db
    # FIXME: parameters (indexes, acl, etc.)
    """
    priority = 8

    @classmethod
    def build_object(cls, document_type: str, left_schema: Schema, right_schema: Schema):
        if document_type.startswith(EMBEDDED_DOCUMENT_NAME_PREFIX):
            # This is an embedded document
            return None

        return super().build_object(document_type, left_schema, right_schema)

    def run_forward(self):
        """
        Mongodb automatically creates collection on the first insert
        So, do nothing
        FIXME: parameters (indexes, acl, etc.)
        """

    def run_backward(self):
        """Drop collection in backward direction"""
        # If the document has 'allow_inheritance' then drop only if
        # no documents left which are point to collection
        skip = self.parameters.get('inherit') and self._is_my_collection_used_by_other_documents()
        if not skip:
            self._run_ctx['collection'].drop()
        # FIXME: add removing by _cls


class DropDocument(BaseDropDocument):
    """Drop a document"""
    priority = 16

    @classmethod
    def build_object(cls, document_type: str, left_schema: Schema, right_schema: Schema):
        if document_type.startswith(EMBEDDED_DOCUMENT_NAME_PREFIX):
            # This is an embedded document
            return None

        return super(DropDocument, cls).build_object(document_type, left_schema, right_schema)

    def run_forward(self):
        """
        Mongodb automatically creates collection on the first insert
        So, do nothing
        """
        # If the document has 'allow_inheritance' then drop only if
        # no documents left which are point to collection
        skip = self.parameters.get('inherit') and self._is_my_collection_used_by_other_documents()
        if not skip:
            self._run_ctx['collection'].drop()
        # FIXME: add removing by _cls

    def run_backward(self):
        """
        Mongodb automatically creates collection on the first insert
        So, do nothing
        FIXME: parameters (indexes, acl, etc.)
        """


class RenameDocument(BaseRenameDocument):
    """Rename document"""
    priority = 6

    @classmethod
    def build_object(cls, document_type: str, left_schema: Schema, right_schema: Schema):
        if document_type.startswith(EMBEDDED_DOCUMENT_NAME_PREFIX):
            # This is an embedded document
            return None

        return super(RenameDocument, cls).build_object(document_type, left_schema, right_schema)

    def run_forward(self):
        """Rename document only in schema, so do nothing"""

    def run_backward(self):
        """Rename document only in schema, so do nothing"""


class AlterDocument(BaseAlterDocument):
    # FIXME: set prioriry
    @classmethod
    def build_object(cls, document_type: str, left_schema: Schema, right_schema: Schema):
        if document_type.startswith(EMBEDDED_DOCUMENT_NAME_PREFIX):
            # This is an embedded document
            return None

        return super(AlterDocument, cls).build_object(document_type, left_schema, right_schema)

    def run_forward(self):
        # Rename collection
        collection_names = self._run_ctx['collection'].database.list_collection_names()
        # If the document has 'allow_inheritance' then rename only if
        # no documents left which are point to collection
        skip = self.parameters.get('inherit') and self._is_my_collection_used_by_other_documents()
        if not skip and self._run_ctx['collection'].name in collection_names:
            self._run_ctx['collection'].rename(self.parameters['collection'])

        # TODO: remove '_cls' after inherit becoming False

    def run_backward(self):
        # Rename collection
        collection_names = self._run_ctx['collection'].database.list_collection_names()
        # If the document has 'allow_inheritance' then rename only if
        # no documents left which are point to collection
        skip = self.parameters.get('inherit') and self._is_my_collection_used_by_other_documents()
        if not skip and self._run_ctx['collection'].name in collection_names:
            new_name = self._run_ctx['left_schema'][self.document_type].parameters['collection']
            self._run_ctx['collection'].rename(new_name)

        # TODO: remove '_cls' after inherit becoming False
