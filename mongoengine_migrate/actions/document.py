__all__ = [
    'CreateDocument',
    'DropDocument',
    'RenameDocument',
    'AlterDocument'
]

import logging

from mongoengine_migrate.flags import EMBEDDED_DOCUMENT_NAME_PREFIX
from mongoengine_migrate.schema import Schema
from mongoengine_migrate.updater import DocumentUpdater, ByDocContext, ByPathContext
from mongoengine_migrate.utils import Diff
from .base import BaseCreateDocument, BaseDropDocument, BaseRenameDocument, BaseAlterDocument

log = logging.getLogger('mongoengine-migrate')


class CreateDocument(BaseCreateDocument):
    """Create new document in db"""
    priority = 60

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
        """

    def run_backward(self):
        """Drop collection in backward direction"""
        # If the document has 'allow_inheritance' then drop only if
        # no derived or parent documents left which are point to
        # the same collection
        skip = self.parameters.get('inherit') and self._is_my_collection_used_by_other_documents()
        if not skip:
            self._run_ctx['collection'].drop()
        # FIXME: add removing by _cls


class DropDocument(BaseDropDocument):
    """Drop a document"""
    priority = 140

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
        # no derived or parent documents left which are point to
        # the same collection
        skip = self.parameters.get('inherit') and self._is_my_collection_used_by_other_documents()
        if not skip:
            self._run_ctx['collection'].drop()
        # FIXME: add removing by _cls

    def run_backward(self):
        """
        Mongodb automatically creates collection on the first insert
        So, do nothing
        """


class RenameDocument(BaseRenameDocument):
    """Rename document"""
    priority = 50

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
    priority = 70

    @classmethod
    def build_object(cls, document_type: str, left_schema: Schema, right_schema: Schema):
        if document_type.startswith(EMBEDDED_DOCUMENT_NAME_PREFIX):
            # This is an embedded document
            return None

        return super(AlterDocument, cls).build_object(document_type, left_schema, right_schema)

    def change_collection(self, updater: DocumentUpdater, diff: Diff):
        self._check_diff(diff, False, str)

        old_collection = self._run_ctx['db'][diff.old]
        collection_names = self._run_ctx['collection'].database.list_collection_names()

        # If the document has 'allow_inheritance' then rename only if
        # no documents left which are point to collection
        skip = self.parameters.get('inherit') and self._is_my_collection_used_by_other_documents()
        if not skip and diff.old in collection_names:
            old_collection.rename(diff.new)
            # Update collection object in run context after renaming
            self._run_ctx['collection'] = self._run_ctx['db'][diff.new]

    def change_inherit(self, updater: DocumentUpdater, diff: Diff):
        """Remove '_cls' key if Document becomes non-inherit, otherwise
        do nothing
        """
        def by_path(ctx: ByPathContext):
            ctx.collection.update_many(
                {ctx.filter_dotpath + '._cls': {'$exists': True}, **ctx.extra_filter},
                {'$unset': {ctx.update_dotpath + '._cls': ''}},
                array_filters=ctx.build_array_filters()
            )

        self._check_diff(diff, False, bool)
        if diff.new:
            return

        updater.update_by_path(by_path)

    def change_dynamic(self, updater: DocumentUpdater, diff: Diff):
        """If document becomes non-dynamic then remove fields which
        are not defined in mongoengine Document
        """
        def by_doc(ctx: ByDocContext):
            extra_keys = ctx.document.keys() - self_schema.keys()
            if extra_keys:
                newdoc = {k: v for k, v in ctx.document.items() if k in self_schema.keys()}
                ctx.document.clear()
                ctx.document.update(newdoc)

        self._check_diff(diff, False, bool)
        if diff.new:
            return  # Nothing to do

        # Remove fields which are not in schema
        self_schema = self._run_ctx['left_schema'][self.document_type]  # type: Schema.Document
        updater.update_by_document(by_doc)
