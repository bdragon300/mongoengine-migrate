__all__ = [
    'CreateDocument',
    'DropDocument',
    'RenameDocument',
    'AlterDocument'
]

import logging
from typing import Any

from mongoengine_migrate.exceptions import SchemaError
from mongoengine_migrate.flags import EMBEDDED_DOCUMENT_NAME_PREFIX
from mongoengine_migrate.schema import Schema
from mongoengine_migrate.mongo import mongo_version
from mongoengine_migrate.utils import Diff, UNSET
from .base import BaseCreateDocument, BaseDropDocument, BaseRenameDocument, BaseAlterDocument

log = logging.getLogger('mongoengine-migrate')


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
        # no derived or parent documents left which are point to
        # the same collection
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
    priority = 9

    @classmethod
    def build_object(cls, document_type: str, left_schema: Schema, right_schema: Schema):
        if document_type.startswith(EMBEDDED_DOCUMENT_NAME_PREFIX):
            # This is an embedded document
            return None

        return super(AlterDocument, cls).build_object(document_type, left_schema, right_schema)

    def change_collection(self, diff: Diff):
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

    def change_inherit(self, diff: Diff):
        self._check_diff(diff, False, bool)
        # TODO: remove '_cls' after inherit becoming False
        # TODO: raise error if other documents use the same collection
        #       when inherit becoming False

    @mongo_version(min_version='2.6')
    def change_dynamic(self, diff: Diff):
        self._check_diff(diff, False, bool)

        if diff.new is True:
            return  # Nothing to do

        # Remove fields which are not in schema
        self_schema = self._run_ctx['left_schema'][self.document_type]

        project = {k: 1 for k in self_schema.keys()}
        self._run_ctx['collection'].aggregate([
            {'$project': project},
            {'$out': self_schema.parameters['collection']}  # >= 2.6
        ])  # FIXME: consider _cls for inherited documents
