from copy import deepcopy

from mongoengine_migrate.flags import EMBEDDED_DOCUMENT_NAME_PREFIX
from mongoengine_migrate.schema import Schema
from .base import BaseCreateDocument, BaseDropDocument, BaseRenameDocument, BaseDocumentAction


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

        if document_type not in left_schema and document_type in right_schema:
            return cls(document_type=document_type,
                       collection=right_schema[document_type].parameters.get('collection'))

    def to_schema_patch(self, left_schema: Schema):
        item = Schema.Document()
        item.parameters['collection'] = self.parameters.get('collection')
        return [('add', '', [(self.document_type, item)])]

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


class AlterDocument(BaseDocumentAction):
    # FIXME: set prioriry
    @classmethod
    def build_object(cls, document_type: str, left_schema: Schema, right_schema: Schema):
        match = document_type in left_schema \
                and document_type in right_schema \
                and left_schema[document_type].parameters != right_schema[document_type].parameters
        if match:
            return cls(document_type=document_type, **right_schema[document_type].parameters)

    def to_schema_patch(self, left_schema: Schema):
        left_item = left_schema[self.document_type]
        right_item = deepcopy(left_item)
        right_item.properties.clear()
        right_item.properties.update(self.parameters)

        return [
            ('remove', '', [(self.document_type, left_item)]),
            ('add', '', [(self.document_type, right_item)])
        ]

    def run_forward(self):
        # Rename collection
        collection_names = self._run_ctx['collection'].database.list_collection_names()
        # If the document has 'allow_inheritance' then rename only if
        # no documents left which are point to collection
        skip = self.parameters.get('inherit') and self._is_my_collection_used_by_other_documents()
        if not skip and self._run_ctx['collection'].name in collection_names:
            self._run_ctx['collection'].rename(self.parameters['collection'])

    def run_backward(self):
        # Rename collection
        collection_names = self._run_ctx['collection'].database.list_collection_names()
        # If the document has 'allow_inheritance' then rename only if
        # no documents left which are point to collection
        skip = self.parameters.get('inherit') and self._is_my_collection_used_by_other_documents()
        if not skip and self._run_ctx['collection'].name in collection_names:
            new_name = self._run_ctx['left_schema'][self.document_type].parameters['collection']
            self._run_ctx['collection'].rename(new_name)
