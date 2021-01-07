# Mongoengine-migrate

![version](https://img.shields.io/pypi/v/mongoengine-migrate)
![pyversions](https://img.shields.io/pypi/pyversions/mongoengine-migrate)
![travis](https://img.shields.io/travis/com/bdragon300/mongoengine-migrate/master)
![license](https://img.shields.io/github/license/bdragon300/mongoengine-migrate)

Framework-agnostic schema migrations for [Mongoengine](http://mongoengine.org/) ODM. 
Inspired by Django migrations system.

[Read documentation](https://bdragon300.github.io/mongoengine-migrate/)

**WARNING:** *this is an unstable version of software. Please backup your data before migrating*

## Installation

```shell script
pip3 install mongoengine-migrate
```

## Features

* Documents
  * Creating, dropping, renaming
  * Renaming a collection
  * Creating, dropping, renaming of fields
  * Converting to/from a `DynamicDocument`
  * Inheritance
* Embedded documents
  * Recursive creating, dropping
  * Renaming
  * Recursive creating, dropping, renaming of fields
  * Converting to/from a `DynamicEmbeddedDocument`
  * Inheritance
* Altering fields in document and embedded documents
  * Changing of init parameters such as `db_field`, `required`, etc.
  * Convertion between field types (if possible)
* Indexes
  * Creating, dropping, renaming
  * Handling fields constraints such as `unique` and `unique_with`
* Automatic select a query or a python loop to perform an update depending on MongoDB version
* Two policies of how to work with existing data which does not meet to mongoengine schema

All mongoengine field types are supported, including simple types, lists, dicts, references, 
GridFS, geo types, generic types.

## Typical migration file

```python
from mongoengine_migrate.actions import *

# Existing data processing policy
# Possible values are: strict, relaxed
policy = "strict"

# Names of migrations which the current one is dependent by
dependencies = [
    'previous_migration'
]

# Action chain
actions = [
    CreateDocument('Author', collection='author'),
    CreateField('Author', 'name', choices=None, db_field='name', default=None, max_length=None,
        min_length=None, null=False, primary_key=False, regex=None, required=False,
        sparse=False, type_key='StringField', unique=False, unique_with=None),
    RenameField('Book', 'name', new_name='caption'),
    AlterField('Book', 'caption', required=True, db_field='caption'),
    AlterField('Book', 'year', type_key='IntField', min_value=None, max_value=None),
    DropField('Book', 'isbn'),
    CreateField('Book', 'author', choices=None, db_field='author', dbref=False, default=None,
        target_doctype='Author', null=False, primary_key=False, required=False, sparse=False,
        type_key='ReferenceField', unique=False, unique_with=None),
]
```
