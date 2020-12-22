from copy import deepcopy
from typing import Optional

from mongoengine_migrate.flags import EMBEDDED_DOCUMENT_NAME_PREFIX
from mongoengine_migrate.schema import Schema
from .base import BaseIndexAction


class CreateIndex(BaseIndexAction):
    """
    Create index in given document

    CreateIndex should go after DropIndex in order to avoid situation
    when user added explicit name for the index, but didn't change
    fields spec. MongoDB will raise duplicate index error in this case.
    Also the such index can be created by hand.
    """
    priority = 130

    @classmethod
    def build_object(cls,
                     document_type: str,
                     name: str,
                     left_schema: Schema,
                     right_schema: Schema) -> Optional['CreateIndex']:
        match = not document_type.startswith(EMBEDDED_DOCUMENT_NAME_PREFIX) \
                and document_type in left_schema \
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
        self._create_index(self.parameters)

    def run_backward(self):
        self._drop_index(self._run_ctx['left_index_schema'])


class DropIndex(BaseIndexAction):
    """
    Drop index in given document

    CreateIndex should go after DropIndex in order to avoid situation
    when user added explicit name for the index, but didn't change
    fields spec. MongoDB will raise duplicate index error in this case.
    Also the such index can be created by hand.
    """
    priority = 120

    @classmethod
    def build_object(cls,
                     document_type: str,
                     name: str,
                     left_schema: Schema,
                     right_schema: Schema) -> Optional['DropIndex']:
        match = not document_type.startswith(EMBEDDED_DOCUMENT_NAME_PREFIX) \
                and document_type in left_schema \
                and document_type in right_schema \
                and name in left_schema[document_type].indexes \
                and name not in right_schema[document_type].indexes
        if match:
            params = right_schema[document_type].indexes[name]
            return cls(document_type=document_type, name=name, **params)

    def to_schema_patch(self, left_schema: Schema):
        left_item = left_schema[self.document_type]
        right_item = deepcopy(left_item)
        right_item.indexes.pop(self.name, None)

        # Document must be already created, therefore do 'change'
        return [('change', self.document_type, (left_item, right_item))]

    def run_forward(self):
        self._drop_index(self.parameters)

    def run_backward(self):
        self._create_index(self._run_ctx['left_index_schema'])


class AlterIndex(BaseIndexAction):
    """Alter index parameters. Actually drop the existing index
    and create a new one with new parameters
    """
    priority = 110

    @classmethod
    def build_object(cls,
                     document_type: str,
                     name: str,
                     left_schema: Schema,
                     right_schema: Schema) -> Optional['AlterIndex']:
        right, left = right_schema, left_schema
        match = not document_type.startswith(EMBEDDED_DOCUMENT_NAME_PREFIX) \
                and document_type in left \
                and document_type in right \
                and name in left[document_type].indexes \
                and name in right[document_type].indexes \
                and right[document_type].indexes[name] != left[document_type].indexes[name]
        if match:
            params = right[document_type].indexes[name]
            return cls(document_type=document_type, name=name, **params)

    def to_schema_patch(self, left_schema: Schema):
        left_item = left_schema[self.document_type]
        right_item = deepcopy(left_item)
        right_item.indexes[self.name].clear()
        right_item.indexes[self.name].update(self.parameters)

        # Document must be already created, therefore do 'change'
        return [('change', self.document_type, (left_item, right_item))]

    def run_forward(self):
        self._drop_index(self._run_ctx['left_index_schema'])
        self._create_index(self.parameters)

    def run_backward(self):
        self._drop_index(self._run_ctx['left_index_schema'])
        self._create_index(self.parameters)
