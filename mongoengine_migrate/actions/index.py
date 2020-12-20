import logging
from copy import deepcopy
from typing import Optional

from mongoengine_migrate.schema import Schema
from .base import BaseIndexAction

log = logging.getLogger('mongoengine-migrate')


class CreateIndex(BaseIndexAction):
    """Create index in given document"""
    priority = 16  # FIXME: set correct priority

    @classmethod
    def build_object(cls,
                     document_type: str,
                     name: str,
                     left_schema: Schema,
                     right_schema: Schema) -> Optional['CreateIndex']:
        match = document_type in left_schema \
                and document_type in right_schema \
                and name not in left_schema.Document.indexes \
                and name in right_schema.Document.indexes
        if match:
            params = right_schema[document_type].indexes[name]
            return cls(document_type=document_type, name=name, **params)

    def to_schema_patch(self, left_schema: Schema):
        left_item = left_schema[self.document_type]
        right_item = deepcopy(left_item)
        right_item.indexes[self.name] = self.parameters

        # Document must be already created, therefore do 'change'
        return [('change', self.document_type, (left_item, right_item))]

    def run_forward(self):
        self._create_index()

    def run_backward(self):
        self._drop_index()


class DropIndex(BaseIndexAction):
    """Drop index in given document"""
    priority = 16  # FIXME: set correct priority

    @classmethod
    def build_object(cls,
                     document_type: str,
                     name: str,
                     left_schema: Schema,
                     right_schema: Schema) -> Optional['DropIndex']:
        match = document_type in left_schema \
                and document_type in right_schema \
                and name in left_schema.Document.indexes \
                and name not in right_schema.Document.indexes
        if match:
            params = right_schema[document_type].indexes[name]
            return cls(document_type=document_type, name=name, **params)

    def to_schema_patch(self, left_schema: Schema):
        left_item = left_schema[self.document_type]
        right_item = deepcopy(left_item)
        right_item.pop(self.name, None)

        # Document must be already created, therefore do 'change'
        return [('change', self.document_type, (left_item, right_item))]

    def run_forward(self):
        self._drop_index()

    def run_backward(self):
        self._create_index()
