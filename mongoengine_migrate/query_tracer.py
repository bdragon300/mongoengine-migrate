from datetime import datetime
from typing import NamedTuple, Dict, Tuple, Any

import wrapt
import functools
from bson import ObjectId


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


def history_method(f):
    @functools.wraps(f)
    def w(instance, *args, **kwargs):
        instance.call_history.append(
            HistoryCall(
                collection_name=instance.__wrapped__.full_name,
                method_name=f.__name__,
                call_datetime=datetime.now(),
                args=args,
                kwargs=kwargs
            )
        )
        return f(*args, **kwargs)

    return w


class QueryTracer(wrapt.ObjectProxy):
    """
    pymongo.Collection wrapper object which mocks modification methods
    calls and writes their call to history
    """

    def __init__(self, *args, **kwrags):
        super().__init__(*args, **kwrags)
        self.call_history = []

    @history_method
    def bulk_write(self, *args, **kwargs):
        return BulkWriteResultMock()

    @history_method
    def insert_one(self, *args, **kwargs):
        return InsertOneResultMock()

    @history_method
    def insert_many(self, *args, **kwargs):
        return InsertManyResultMock()

    @history_method
    def replace_once(self, *args, **kwargs):
        return UpdateResultMock()

    @history_method
    def update_one(self, *args, **kwargs):
        return UpdateResultMock()

    @history_method
    def update_many(self, *args, **kwargs):
        return UpdateResultMock()

    @history_method
    def drop(self, *args, **kwargs):
        pass

    @history_method
    def delete_one(self, *args, **kwargs):
        return DeleteResultMock()

    @history_method
    def delete_many(self, *args, **kwargs):
        return DeleteResultMock()

    @history_method
    def create_indexes(self, *args, **kwargs):
        return []

    @history_method
    def create_index(self, *args, **kwargs):
        pass

    @history_method
    def ensure_index(self, *args, **kwargs):
        pass

    @history_method
    def drop_indexes(self, *args, **kwargs):
        pass

    @history_method
    def drop_index(self, *args, **kwargs):
        pass

    @history_method
    def reindex(self, *args, **kwargs):
        pass

    @history_method
    def aggregate(self, *args, **kwargs):
        return []

    @history_method
    def rename(self, *args, **kwargs):
        pass

    @history_method
    def find_one_and_delete(self, *args, **kwargs):
        pass

    @history_method
    def find_one_and_replace(self, *args, **kwargs):
        pass

    @history_method
    def find_one_and_update(self, *args, **kwargs):
        pass

    @history_method
    def save(self, *args, **kwargs):
        return ObjectId('000000000000000000000000')

    @history_method
    def insert(self, *args, **kwargs):
        return []

    @history_method
    def update(self, *args, **kwargs):
        return []

    @history_method
    def remove(self, *args, **kwargs):
        pass

    @history_method
    def find_and_modify(self, *args, **kwargs):
        pass
