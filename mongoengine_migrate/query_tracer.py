__all__ = [
    'HistoryCallKind',
    'CollectionQueryTracer',
    'DatabaseQueryTracer'
]

import logging
from enum import Enum
from typing import NamedTuple, Dict, Tuple, Any

import wrapt
from bson import ObjectId
from pymongo.collection import Collection

_sentinel = object()


log = logging.getLogger('mongoengine-migrate')


class HistoryCallKind(Enum):
    READ = 'READ'
    MODIFY = "MODIFY"
    AGGREGATE = "AGGREGATE"


class InsertOneResultMock(NamedTuple):
    inserted_id: ObjectId = ObjectId('000000000000000000000000')


class InsertManyResultMock(NamedTuple):
    inserted_ids: Tuple[ObjectId] = (ObjectId('000000000000000000000000'), )


class UpdateResultMock(NamedTuple):
    raw_result: Dict[str, Any] = {}
    matched_count: int = 0
    modified_count: int = 0
    upserted_id: ObjectId = ObjectId('000000000000000000000000')


class DeleteResultMock(NamedTuple):
    raw_result: Dict[str, Any] = {}
    deleted_count: int = 0


class BulkWriteResultMock(NamedTuple):
    bulk_api_result: Dict[str, Any] = {}
    inserted_count: int = 0
    matched_count: int = 0
    modified_count: int = 0
    deleted_count: int = 0
    upserted_count: int = 0
    upserted_ids: Tuple[ObjectId] = (ObjectId('000000000000000000000000'), )


def make_history_method(func_name, method_kind, return_value=_sentinel):
    def w(instance, *args, **kwargs):
        args_str = ', '.join(f'\n  {arg}' for arg in args)
        kwargs_str = ', '.join(f"\n  {name}={val}" for name, val in sorted(kwargs.items()))
        arguments = f'{args_str}{"," if kwargs_str else ""}{kwargs_str}'
        if arguments:
            arguments += '\n'
        collection_name = instance.__wrapped__.full_name
        log.info('* %s.%s(%s)', collection_name, func_name, arguments)

        if return_value == _sentinel:
            f = getattr(instance.__wrapped__, func_name)
            return f(*args, **kwargs)

        return return_value
    return w


class CollectionQueryTracer(wrapt.ObjectProxy):
    """
    pymongo.Collection wrapper object which mocks modification methods
    calls and writes their call to history
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    # Collection modification methods
    bulk_write = make_history_method('bulk_write', 'MODIFY', return_value=BulkWriteResultMock())
    insert_one = make_history_method('insert_one', 'MODIFY', return_value=InsertOneResultMock())
    insert_many = make_history_method('insert_many', 'MODIFY', return_value=InsertManyResultMock())
    replace_one = make_history_method('replace_one', 'MODIFY', return_value=UpdateResultMock())
    update_one = make_history_method('update_one', 'MODIFY', return_value=UpdateResultMock())
    update_many = make_history_method('update_many', 'MODIFY', return_value=UpdateResultMock())
    drop = make_history_method('drop', 'MODIFY', return_value=None)
    delete_one = make_history_method('delete_one', 'MODIFY', return_value=DeleteResultMock())
    delete_many = make_history_method('delete_many', 'MODIFY', return_value=DeleteResultMock())
    create_indexes = make_history_method('create_indexes', 'MODIFY', return_value=[])
    create_index = make_history_method('create_index', 'MODIFY', return_value=None)
    ensure_index = make_history_method('ensure_index', 'MODIFY', return_value=None)
    drop_indexes = make_history_method('drop_indexes', 'MODIFY', return_value=None)
    drop_index = make_history_method('drop_index', 'MODIFY', return_value=None)
    reindex = make_history_method('reindex', 'MODIFY', return_value=None)
    rename = make_history_method('rename', 'MODIFY', return_value=None)
    find_one_and_delete = make_history_method('find_one_and_delete', 'MODIFY', return_value=None)
    find_one_and_replace = make_history_method('find_one_and_replace', 'MODIFY', return_value=None)
    find_one_and_update = make_history_method('find_one_and_update', 'MODIFY', return_value=None)
    save = make_history_method('save', 'MODIFY', return_value=ObjectId('000000000000000000000000'))
    insert = make_history_method('insert', 'MODIFY', return_value=[])
    update = make_history_method('update', 'MODIFY', return_value=[])
    remove = make_history_method('remove', 'MODIFY', return_value=None)
    find_and_modify = make_history_method('find_and_modify', 'MODIFY', return_value=None)

    # Aggregation methods
    aggregate = make_history_method('aggregate', 'AGGREGATE', return_value=[])
    aggregate_raw_batches = make_history_method('aggregate_raw_batches',
                                                'AGGREGATE',
                                                return_value=[])

    # Collection reading methods
    find_one = make_history_method('find_one', 'READ')
    find = make_history_method('find', 'READ')
    find_raw_batches = make_history_method('find_raw_batches', 'READ')
    estimated_document_count = make_history_method('estimated_document_count', 'READ')
    count_documents = make_history_method('count_documents', 'READ')
    list_indexes = make_history_method('list_indexes', 'READ')
    index_information = make_history_method('index_information', 'READ')
    options = make_history_method('options', 'READ')
    watch = make_history_method('watch', 'READ')
    group = make_history_method('group', 'READ')
    distinct = make_history_method('distinct', 'READ')
    map_reduce = make_history_method('map_reduce', 'READ')
    inline_map_reduce = make_history_method('inline_map_reduce', 'READ')


class DatabaseQueryTracer(wrapt.ObjectProxy):
    """pymongo.Database wrapper which is acting as original object,
    but returns CollectionQueryTracer object instead of Collection
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __getitem__(self, item):
        col = super().__getitem__(item)
        return CollectionQueryTracer(col)

    def __getattr__(self, item):
        val = super().__getattr__(item)
        if isinstance(val, Collection):
            return CollectionQueryTracer(val)

        return val

    def get_collection(self, *args, **kwargs):
        col = super().get_collection(*args, **kwargs)
        return CollectionQueryTracer(col)

    def create_collection(self, *args, **kwargs):
        col = super().create_collection(*args, **kwargs)
        return CollectionQueryTracer(col)
