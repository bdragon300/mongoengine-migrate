__all__ = [
    'CreateEmbedded',
    'DropEmbedded',
    'RenameEmbedded',
    'AlterEmbedded'
]

import logging

from mongoengine_migrate.flags import EMBEDDED_DOCUMENT_NAME_PREFIX
from mongoengine_migrate.schema import Schema
from mongoengine_migrate.utils import Diff, UNSET, document_type_to_class_name
from mongoengine_migrate.mongo import mongo_version, DocumentUpdater, ByPathContext
from .base import BaseCreateDocument, BaseDropDocument, BaseRenameDocument, BaseAlterDocument

log = logging.getLogger('mongoengine-migrate')


class CreateEmbedded(BaseCreateDocument):
    """
    Create new embedded document
    Should have the highest priority and be at top of every migration
    since fields actions could refer to this document and its schema
    representation.
    """
    priority = 4

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
    priority = 18

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
    priority = 2

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
    priority = 5

    @classmethod
    def build_object(cls, document_type: str, left_schema: Schema, right_schema: Schema):
        if not document_type.startswith(EMBEDDED_DOCUMENT_NAME_PREFIX):
            # This is not an embedded document
            return None

        return super(AlterEmbedded, cls).build_object(document_type, left_schema, right_schema)

    def change_inherit(self, diff: Diff):
        self._check_diff(diff, False, bool)
        # TODO: remove '_cls' after inherit becoming False
        # TODO: raise error if other documents use the same collection
        #       when inherit becoming False

    @mongo_version(min_version='2.6')
    def change_dynamic(self, diff: Diff):
        return  # FIXME: fix all below
        def by_path(ctx: ByPathContext):
            dotpaths = {f'{ctx.filter_dotpath}.{k}': 1 for k in self_schema.keys()}
            ctx.collection.aggregate([
                {'$match': ctx.extra_filter},
                {'$project': dotpaths},
                {'$out': ctx.collection.name}  # >= 2.6
            ])  # FIXME: consider _cls for inherited documents

        self._check_diff(diff, False, bool)
        if diff.new is True:
            return  # Nothing to do

        # Remove fields which are not in schema
        self_schema = self._run_ctx['left_schema'][self.document_type]
        inherit = self_schema.parameters.get('inherit')
        document_cls = document_type_to_class_name(self.document_type) if inherit else None
        updater = DocumentUpdater(self._run_ctx['db'],
                                  self.document_type,
                                  self._run_ctx['left_schema'],
                                  None,
                                  document_cls)
        updater.update_by_path(by_path)
