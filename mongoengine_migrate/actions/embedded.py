__all__ = [
    'CreateEmbedded',
    'DropEmbedded',
    'RenameEmbedded',
    'AlterEmbedded'
]

import logging

from mongoengine_migrate.flags import EMBEDDED_DOCUMENT_NAME_PREFIX
from mongoengine_migrate.schema import Schema
from mongoengine_migrate.utils import Diff
from .base import BaseCreateDocument, BaseDropDocument, BaseRenameDocument, BaseAlterDocument
from mongoengine_migrate.updater import DocumentUpdater, ByDocContext, ByPathContext

log = logging.getLogger('mongoengine-migrate')


class CreateEmbedded(BaseCreateDocument):
    """
    Create new embedded document
    Should have the highest priority and be at top of every migration
    since fields actions could refer to this document and its schema
    representation.
    """
    priority = 30

    @classmethod
    def build_object(cls, document_type: str, left_schema: Schema, right_schema: Schema):
        if not document_type.startswith(EMBEDDED_DOCUMENT_NAME_PREFIX):
            # This is not an embedded document
            return None

        return super(CreateEmbedded, cls).build_object(document_type, left_schema, right_schema)

    def run_forward(self):
        """Embedded documents are not required to do anything"""

    def run_backward(self):
        """Embedded documents are not required to do anything"""


class DropEmbedded(BaseDropDocument):
    """
    Drop embedded document
    Should have the lowest priority and be at bottom of every migration
    since fields actions could refer to this document and its schema
    representation.
    """
    priority = 150

    @classmethod
    def build_object(cls, document_type: str, left_schema: Schema, right_schema: Schema):
        if not document_type.startswith(EMBEDDED_DOCUMENT_NAME_PREFIX):
            # This is not an embedded document
            return None

        return super(DropEmbedded, cls).build_object(document_type, left_schema, right_schema)

    def run_forward(self):
        """Embedded documents are not required to do anything"""

    def run_backward(self):
        """Embedded documents are not required to do anything"""


class RenameEmbedded(BaseRenameDocument):
    """
    Rename embedded document
    Should be checked before CreateEmbedded in order to detect renaming
    """
    priority = 20

    @classmethod
    def build_object(cls, document_type: str, left_schema: Schema, right_schema: Schema):
        if not document_type.startswith(EMBEDDED_DOCUMENT_NAME_PREFIX):
            # This is not an embedded document
            return None

        return super(RenameEmbedded, cls).build_object(document_type, left_schema, right_schema)

    def run_forward(self):
        """Embedded documents are not required to do anything"""

    def run_backward(self):
        """Embedded documents are not required to do anything"""


class AlterEmbedded(BaseAlterDocument):
    """Alter whole embedded document changes"""
    priority = 40

    @classmethod
    def build_object(cls, document_type: str, left_schema: Schema, right_schema: Schema):
        if not document_type.startswith(EMBEDDED_DOCUMENT_NAME_PREFIX):
            # This is not an embedded document
            return None

        return super(AlterEmbedded, cls).build_object(document_type, left_schema, right_schema)

    def change_inherit(self, updater: DocumentUpdater, diff: Diff):
        """Remove '_cls' key if EmbeddedDocument becomes non-inherit,
        otherwise do nothing
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
        are not defined in mongoengine EmbeddedDocument
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
