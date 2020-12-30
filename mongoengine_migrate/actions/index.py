__all__ = [
    'CreateIndex',
    'DropIndex',
    'AlterIndex'
]

from copy import deepcopy
from typing import Optional, Sequence

from pymongo.database import Database

from mongoengine_migrate.flags import EMBEDDED_DOCUMENT_NAME_PREFIX
from mongoengine_migrate.graph import MigrationPolicy
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

    def __init__(self, document_type: str, index_name: str, *, fields: Sequence, **kwargs):
        super().__init__(document_type, index_name, fields=fields, **kwargs)

    @classmethod
    def build_object(cls,
                     document_type: str,
                     index_name: str,
                     left_schema: Schema,
                     right_schema: Schema) -> Optional['CreateIndex']:
        match = not document_type.startswith(EMBEDDED_DOCUMENT_NAME_PREFIX) \
                and document_type in left_schema \
                and document_type in right_schema \
                and index_name not in left_schema[document_type].indexes \
                and index_name in right_schema[document_type].indexes
        if match:
            params = right_schema[document_type].indexes[index_name]
            return cls(document_type=document_type, index_name=index_name, **params)

    def prepare(self, db: Database, left_schema: Schema, migration_policy: MigrationPolicy):
        self._prepare(db, left_schema, migration_policy, False)

        self._run_ctx['left_index_schema'] = \
            left_schema[self.document_type].indexes.get(self.index_name, {})

    def to_schema_patch(self, left_schema: Schema):
        left_item = left_schema[self.document_type]
        right_item = deepcopy(left_item)
        right_item.indexes[self.index_name] = self.parameters

        # Document must be already created, therefore do 'change'
        return [('change', self.document_type, (left_item, right_item))]

    def run_forward(self):
        self._create_index(self.parameters)

    def run_backward(self):
        self._drop_index(self.parameters)


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
                     index_name: str,
                     left_schema: Schema,
                     right_schema: Schema) -> Optional['DropIndex']:
        match = not document_type.startswith(EMBEDDED_DOCUMENT_NAME_PREFIX) \
                and document_type in left_schema \
                and document_type in right_schema \
                and index_name in left_schema[document_type].indexes \
                and index_name not in right_schema[document_type].indexes
        if match:
            params = left_schema[document_type].indexes[index_name]
            return cls(document_type=document_type, index_name=index_name, **params)

    def to_schema_patch(self, left_schema: Schema):
        left_item = left_schema[self.document_type]
        right_item = deepcopy(left_item)
        right_item.indexes.pop(self.index_name, None)

        # Document must be already created, therefore do 'change'
        return [('change', self.document_type, (left_item, right_item))]

    def run_forward(self):
        self._drop_index(self._run_ctx['left_index_schema'])

    def run_backward(self):
        self._create_index(self._run_ctx['left_index_schema'])


class AlterIndex(BaseIndexAction):
    """Alter index parameters. Actually drop the existing index
    and create a new one with new parameters
    """
    priority = 110

    def __init__(self, document_type: str, index_name: str, *, fields: Sequence, **kwargs):
        super().__init__(document_type, index_name, fields=fields, **kwargs)

    @classmethod
    def build_object(cls,
                     document_type: str,
                     index_name: str,
                     left_schema: Schema,
                     right_schema: Schema) -> Optional['AlterIndex']:
        right, left = right_schema, left_schema
        match = not document_type.startswith(EMBEDDED_DOCUMENT_NAME_PREFIX) \
                and document_type in left \
                and document_type in right \
                and index_name in left[document_type].indexes \
                and index_name in right[document_type].indexes \
                and right[document_type].indexes[index_name] != left[document_type].indexes[index_name]
        if match:
            params = right[document_type].indexes[index_name]
            return cls(document_type=document_type, index_name=index_name, **params)

    def to_schema_patch(self, left_schema: Schema):
        left_item = left_schema[self.document_type]
        right_item = deepcopy(left_item)
        right_item.indexes[self.index_name].clear()
        right_item.indexes[self.index_name].update(self.parameters)

        # Document must be already created, therefore do 'change'
        return [('change', self.document_type, (left_item, right_item))]

    def run_forward(self):
        self._drop_index(self._run_ctx['left_index_schema'])
        self._create_index(self.parameters)

    def run_backward(self):
        self._drop_index(self.parameters)
        self._create_index(self._run_ctx['left_index_schema'])
