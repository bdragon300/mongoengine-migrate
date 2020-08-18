# Mongoengine-migrate

![version](https://img.shields.io/pypi/v/mongoengine-migrate)
![pyversions](https://img.shields.io/pypi/pyversions/mongoengine-migrate)
![travis](https://img.shields.io/travis/com/bdragon300/mongoengine-migrate/master)
![license](https://img.shields.io/github/license/bdragon300/mongoengine-migrate)

Framework-agnostic schema migrations for [Mongoengine](http://mongoengine.org/) ODM. 
Inspired by Django migrations system.

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
* Automatic select a query or a python loop to perform an update depending on MongoDB version
* Two policies of how to work with existing data which does not meet to mongoengine schema

All mongoengine field types are supported, including simple types, lists, dicts, references, 
GridFS, geo types, generic types.

## Example

Let's assume that we already have the following Document declaration:

```python
from mongoengine import Document, fields
    
class Book(Document):
    name = fields.StringField(default='?')
    year = fields.StringField(max_length=4)
    isbn = fields.StringField()
```

Then we make some changes:

```python
from mongoengine import Document, fields

# Add Author Document
class Author(Document):
    name = fields.StringField(required=True)

class Book(Document):
    caption = fields.StringField(required=True, default='?')  # Make required and rename
    year = fields.IntField()  # Change type to IntField
    # Removed field isbn
    author = fields.ReferenceField(Author)  # Add field
```

Such changes should be reflected in database. The following command creates migration file
(`myproject.db` is a python module with mongoengine document declarations):

```shell script
mongoengine-migrate makemigrations -m myproject.db 
```

New migration file will be created:

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

Next, upgrade the database to the latest version:

```shell script
mongoengine-migrate migrate
```

Or to the certain migration:

```shell script
mongoengine-migrate migrate previous_migration
```

### Actual db changes 

During the running forward the migration created above the following changes will be made:
* "author" collection
  1. Nothing to do
* "book" collection
  1. Existing fields "name" will be renamed to "caption"
  1. All unset "caption" fields will be set to default value `'?'`
     (because this field was defined as "required")
  1. Existing fields "year" with string values will be casted to integer value
  1. "isbn" field will be dropped

On backward direction the following changes will be made:
* "book" collection
  1. "author" field will be dropped
  1. All integer values in "year" field will be casted back to string
  1. Existing "caption" fields will be renamed back to "name"
* "author" collection
  1. "name" field will be dropped
  1. whole "author" collection will be dropped
