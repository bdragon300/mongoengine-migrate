from datetime import datetime
from typing import NamedTuple, Dict, Tuple, Any

import wrapt
from bson import ObjectId

_sentinel = object()


class HistoryCall(NamedTuple):
    collection_name: str
    method_name: str
    call_datetime: datetime
    args: Tuple[Any]
    kwargs: Dict[str, Any]

    def __str__(self):
        args_str = ', '.join(f'\n  {arg}' for arg in self.args)
        kwargs_str = ', '.join(f"\n  {name}={val}"
                               for name, val in sorted(self.kwargs.items()))
        arguments = f'{args_str}{"," if kwargs_str else ""}{kwargs_str}'
        if arguments:
            arguments += '\n'
        return f'[{self.call_datetime}] {self.collection_name}.{self.method_name}({arguments})'


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


def make_history_method(func_name, return_value=_sentinel):
    def w(instance, *args, **kwargs):
        instance.add_history_call(func_name, args, kwargs)

        if return_value == _sentinel:
            f = getattr(instance.__wrapped__, func_name)
            return f(*args, **kwargs)

        return return_value
    return w


class QueryTracer(wrapt.ObjectProxy):
    """
    pymongo.Collection wrapper object which mocks modification methods
    calls and writes their call to history
    """

    def __init__(self, *args, **kwrags):
        super().__init__(*args, **kwrags)
        self.call_history = []

    # Collection modification methods
    bulk_write = make_history_method('bulk_write', return_value=BulkWriteResultMock())
    insert_one = make_history_method('insert_one', return_value=InsertOneResultMock())
    insert_many = make_history_method('insert_many', return_value=InsertManyResultMock())
    replace_one = make_history_method('replace_one', return_value=UpdateResultMock())
    update_one = make_history_method('update_one', return_value=UpdateResultMock())
    update_many = make_history_method('update_many', return_value=UpdateResultMock())
    drop = make_history_method('drop', return_value=None)
    delete_one = make_history_method('delete_one', return_value=DeleteResultMock())
    delete_many = make_history_method('delete_many', return_value=DeleteResultMock())
    create_indexes = make_history_method('create_indexes', return_value=[])
    create_index = make_history_method('create_index', return_value=None)
    ensure_index = make_history_method('ensure_index', return_value=None)
    drop_indexes = make_history_method('drop_indexes', return_value=None)
    drop_index = make_history_method('drop_index', return_value=None)
    reindex = make_history_method('reindex', return_value=None)
    rename = make_history_method('rename', return_value=None)
    find_one_and_delete = make_history_method('find_one_and_delete', return_value=None)
    find_one_and_replace = make_history_method('find_one_and_replace', return_value=None)
    find_one_and_update = make_history_method('find_one_and_update', return_value=None)
    save = make_history_method('save', return_value=ObjectId('000000000000000000000000'))
    insert = make_history_method('insert', return_value=[])
    update = make_history_method('update', return_value=[])
    remove = make_history_method('remove', return_value=None)
    find_and_modify = make_history_method('find_and_modify', return_value=None)

    # Aggregation methods
    aggregate = make_history_method('aggregate', return_value=[])
    aggregate_raw_batches = make_history_method('aggregate_raw_batches', return_value=[])

    # Collection reading methods
    find_one = make_history_method('find_one')
    find = make_history_method('find')
    find_raw_batches = make_history_method('find_raw_batches')
    estimated_document_count = make_history_method('estimated_document_count')
    count_documents = make_history_method('count_documents')
    list_indexes = make_history_method('list_indexes')
    index_information = make_history_method('index_information')
    options = make_history_method('options')
    watch = make_history_method('watch')
    group = make_history_method('group')
    distinct = make_history_method('distinct')
    map_reduce = make_history_method('map_reduce')
    inline_map_reduce = make_history_method('inline_map_reduce')

    def add_history_call(self, func_name, args, kwargs):
        self.call_history.append(
            HistoryCall(
                collection_name=self.__wrapped__.full_name,
                method_name=func_name,
                call_datetime=datetime.now(),
                args=args,
                kwargs=kwargs
            )
        )
