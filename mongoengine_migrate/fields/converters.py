from ..actions.diff import AlterDiff
import re
from mongoengine.fields import BaseField, URLField
from pymongo.collection import Collection
from mongoengine_migrate.exceptions import ActionError, MigrationError
from typing import Type


def nothing(*args, **kwargs):
    pass


def deny(collection: Collection, db_field: str, from_cls: Type[BaseField], to_cls: Type[BaseField]):
    raise ActionError(f"Unable to convert field {collection.name}.{db_field} "
                      f"from {from_cls.__name__!r} to {to_cls.__name__!r}. You can use another "
                      f"altering policy for 'type_key' to override this")


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


def to_url_string(collection: Collection,
                  db_field: str,
                  from_cls: Type[BaseField],
                  to_cls: Type[BaseField]):
    __mongo_convert(collection, db_field, 'string')

    # Check if some records contains non-url values in db_field
    url_schemes = ["http", "https", "ftp", "ftps"]
    scheme_regex = rf'(?:({"|".join(url_schemes)}))://'
    url_regex = re.compile(
        r"^" + scheme_regex +
        # domain...
        r"(?:(?:[A-Z0-9](?:[A-Z0-9-_]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}(?<!-)\.?)|"
        r"localhost|"  # localhost...
        r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|"  # ...or ipv4
        r"\[?[A-F0-9]*:[A-F0-9:]+\]?)"  # ...or ipv6
        r"(?::\d+)?"  # optional port
        r"(?:/?|[/?]\S+)$",
        re.IGNORECASE
    )
    bad_records = collection.find(
        {"$and": [{db_field: url_regex}, {db_field: {"$ne": None}}]},
        limit=3
    )
    if bad_records.retrieved:
        examples = (
            f'{{_id: {x.get("_id", "unknown")},...{db_field}: {x.get(db_field, "unknown")}}}'
            for x in bad_records
        )
        raise MigrationError(f"Some of records in {collection.name}.{db_field} contain non-url"
                             f"values. First several examples {','.join(examples)}")


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
