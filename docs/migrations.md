# Migrations

Migration is a file which contains instructions which have to be executed in order to apply this 
migration. These files are actually normal Python files with an agreed-upon object layout, 
written in a declarative style.

### Dependency graph

Every migration (except for the first one) always depends on at least one another migration.
Dependencies are listed in `dependencies` variable.

For example:

```python
# File: migration_3.py

dependencies = [
    'migration_1',
    'migration_2'
]
```

This means that `migration_3` will be applied only after both `migration_1` and `migration_2`
has been applied before. And vice versa, in order to unapply `migration_1`, the `migration_3`
must be unapplied first.

Hereby all migrations are organized in one directed graph.  When you creating a new migration 
file using `makemigrations` command, it will automatically fill `dependencies` with the last
migrations in the graph.

### Policy

Also a migration can set policy of changes handling. Available policies are following:

* `strict` -- *Default value*. Existing database data will be checked if they comply the schema.
Error will be thrown if not. For example, error will be raised, if we have a record with random
string in field which has "email" type in schema.
* `relaxed` -- just try to make changes in database without data check.

# Actions 

Every migration consists of instructions called "actions". They are contained in `actions` 
variable.

For example:

```python
from mongoengine_migrate.actions import *
import pymongo

actions = [
    AlterField('Book', 'year', type_key='IntField', min_value=None, max_value=None),
    CreateIndex('Book', 'caption_text', fields=[('caption', pymongo.TEXT)])
]
```

When you apply a migration, its actions are executed in order as they written in list: 
first the `Book.year` will be converted to integer, and then text index for `Book.caption`
field will be created.

Order is reversed if you unapply a migration: first index will be dropped, and then `Book.year`
will be converted back to string (as it was before migration).

An action represents one change, but can execute several commands. For example, more than 
one MongoDB command is required to execute to handle several parameters in `AlterField` action.
For example, rename the field and convert its values to another type. Or updating embedded
document schema.

### Dummy action

Dummy action does not make changes in database. Such action is suitable when you want to deny
it to make changes to database. *If you will just remove action from the chain, then it will be
appearing further in new migrations, because `mongoengine-migrate` won't "see" the change of
schema which this action should introduce to*.

To make an action dummy, just add `dummy_action=True` to its parameters:

```python
from mongoengine_migrate.actions import *

actions = [
# ...
    AlterField('Document1', 'field1', dummy_action=True, required=True, default=0),
# ...
]
```

### Custom action

`RunPython` action is suitable when you want to call your own code in migration. You can have
`RunPython` actions as many as you want.

For example:

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

Callback functions parameters are:

1. `pymongo.Database` object of current database
1. `pymongo.Collection` object of collection of given mongoengine document. If document is 
embedded (its name starts with "~" symbol) then this parameter will be None.
1. This parameter contains the Schema object modified by previous actions in chain.

{% include_relative navigation.md %}