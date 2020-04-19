import re
from typing import Type

from mongoengine.fields import BaseField
from pymongo.collection import Collection

from mongoengine_migrate.exceptions import ActionError, MigrationError


def nothing(*args, **kwargs):
    pass


def deny(collection: Collection, db_field: str, from_cls: Type[BaseField], to_cls: Type[BaseField]):
    raise MigrationError(f"Unable to convert field {collection.name}.{db_field} "
                         f"from {from_cls.__name__!r} to {to_cls.__name__!r}. You can use "
                         f"error_policy for 'type_key' diff to override this")


def item_to_list(collection: Collection,
                 db_field: str,
                 from_cls: Type[BaseField],
                 to_cls: Type[BaseField]):
    collection.aggregate([
        {'$match': {db_field: {"$exists": True}}},
        {'$addFields': {db_field: [f"${db_field}"]}},
        {'$out': collection.name}
    ])


def extract_from_list(collection: Collection,
                      db_field: str,
                      from_cls: Type[BaseField],
                      to_cls: Type[BaseField]):
    collection.aggregate([
        {'$match': {db_field: {"$ne": None}}},
        {'$addFields': {db_field: {"$arrayElemAt": [f"${db_field}", 0]}}},
        {'$out': collection.name}
    ])


def to_string(collection: Collection,
              db_field: str,
              from_cls: Type[BaseField],
              to_cls: Type[BaseField]):
    __mongo_convert(collection, db_field, 'string')


def to_int(collection: Collection,
           db_field: str,
           from_cls: Type[BaseField],
           to_cls: Type[BaseField]):
    __mongo_convert(collection, db_field, 'int')


def to_long(collection: Collection,
            db_field: str,
            from_cls: Type[BaseField],
            to_cls: Type[BaseField]):
    __mongo_convert(collection, db_field, 'long')


def to_double(collection: Collection,
              db_field: str,
              from_cls: Type[BaseField],
              to_cls: Type[BaseField]):
    __mongo_convert(collection, db_field, 'double')


def to_decimal(collection: Collection,
               db_field: str,
               from_cls: Type[BaseField],
               to_cls: Type[BaseField]):
    __mongo_convert(collection, db_field, 'decimal')


def to_date(collection: Collection,
            db_field: str,
            from_cls: Type[BaseField],
            to_cls: Type[BaseField]):
    __mongo_convert(collection, db_field, 'date')


def to_bool(collection: Collection,
            db_field: str,
            from_cls: Type[BaseField],
            to_cls: Type[BaseField]):
    __mongo_convert(collection, db_field, 'bool')


def to_object_id(collection: Collection,
                 db_field: str,
                 from_cls: Type[BaseField],
                 to_cls: Type[BaseField]):
    __mongo_convert(collection, db_field, 'objectId')


def __mongo_convert(collection: Collection, db_field: str, target_type: str):
    """
    Launch field convertion pipeline with a given mongo convertion
    command. For example: '$toInt', '$toString', etc.
    :param collection:
    :param db_field:
    :param target_type:
    :return:
    """
    # TODO: implement also for mongo 3.x
    # TODO: use $convert with onError and onNull
    collection.aggregate([
        {'$match': {db_field: {"$ne": None}}},  # Field is not null
        {'$addFields': {
            '$convert': {
                'input': f'${db_field}',
                'to': target_type
            }
        }},
        {'$out': collection.name}
    ])
