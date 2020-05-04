from pymongo.collection import Collection

from mongoengine_migrate.exceptions import MigrationError


def check_empty_result(collection: Collection, db_field: str, find_filter: dict):
    """
    Find records in collection satisfied to a given filter expression
    and raise error if anything found
    :param collection: pymongo collection object to find in
    :param db_field: collection field name
    :param find_filter: collection.find() method filter argument
    :raises MigrationError: if any records found
    """
    bad_records = collection.find(find_filter, limit=3)
    if bad_records.retrieved:
        examples = (
            f'{{_id: {x.get("_id", "unknown")},...{db_field}: {x.get(db_field, "unknown")}}}'
            for x in bad_records
        )
        raise MigrationError(f"Field {collection.name}.{db_field} in some records "
                             f"has wrong values. First several examples: "
                             f"{','.join(examples)}")