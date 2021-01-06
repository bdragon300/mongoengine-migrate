# Migrations

Migration is a file contained instructions which are required to be executed to apply this 
migration. These files are actually normal Python files with an agreed-upon object layout, 
written in a declarative style.

Every migration (except for the first one) always depends on at least one another migration
which must be applied before this one. Thus all migrations are organized in one directed graph.
Dependencies are listed in `dependencies` variable. When you creating a new migration file using
`makemigrations` command, it will automatically set as dependend on the last migration.

Also a migration can set policy of changes handling. Available policies are following:

* `strict` -- existing collections and their data will be checked against schema and error will
be raised if check failed. It could be a record existence in collection with missed required 
fields, for instance. This is default.
* `relaxed` -- no schema checks will be performed.

# Actions 

Instructions written inside a migrations called "actions". They are executed in order as they
contained in list. Whey you rollback a migration, the execution order is reversed.

Each action relates to a mongoengine document which this action must make changes for. There are
field, index documents which are also tied up with a particular field or index respectively.

An action represents one change, but can execute several commands. For example, one `AlterField`
can handle change of several field parameters at once which leads to the execution of several
MongoDB updating commands.

### Dummy action

Dummy action does not make changes in database. Dummy action is suitable when you want to skip
action to be actually run, but don't want to remove it from migration to reflect this change
in schema (if you just remove it, then it will appearing again in subsequent `makemigrations` calls,
because such change still not in schema).

To make an action dummy, just add `dummy_action=True` to its parameters. Such as:

```
actions = [
# ...
    AlterField('Document1', 'field1', required=True, default=0, dummy_action=True),
# ...
]
```

### Custom action

`RunPython` action is suitable when you want to call your own code during applying a migration.

Example:

```python
from mongoengine_migrate.actions import *
from pymongo.database import Database
from pymongo.collection import Collection
from mongoengine_migrate.schema import Schema

def forward(db: Database, collection: Collection, left_schema: Schema):
    collection.update_one({'my_field': 1}, {'$set': {'your_field': 11}})

def backward(db: Database, collection: Collection, left_schema: Schema):
    collection.update_one({'my_field': 1}, {'$unset': {'your_field': ''}})

actions = [
# ...
# "forward_func" and "backward_func" are optional, but at least one of them must be set
    RunPython('Document1', forward_func=forward, backward_func=backward)
# ...
]
```

Callback functions parameters:

1. `pymongo.Database` object of current database
1. `pymongo.Collection` object of collection of given mongoengine document. If document is 
embedded (its name starts with "~" symbol) then this parameter will be None.
1. Every action can make changes in schema, `CreateDocument` inserts a document, for example. 
This parameter contains the Schema object with modifications which was already made by 
actions before the current one in chain.
