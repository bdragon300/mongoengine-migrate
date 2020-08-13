# Mongoengine-migrate

![version](https://img.shields.io/pypi/v/mongoengine-migrate)
![pyversions](https://img.shields.io/pypi/pyversions/mongoengine-migrate)
![travis](https://img.shields.io/travis/com/bdragon300/mongoengine-migrate)
![license](https://img.shields.io/github/license/bdragon300/mongoengine-migrate)

**Work in progress**

Schema migrations for [Mongoengine](http://mongoengine.org/) ODM. Inspired by Django migrations system.

## Installation

```shell script
pip3 install mongoengine-migrate
```

## How it works

Let's assume that we already have the following Document declaration:

```python
from mongoengine import Document, fields
    
class Books(Document):
    name = fields.StringField()
    year = fields.StringField(max_length=4)
    isbn = fields.StringField()
```

Then we changed couple of things:

```python
from mongoengine import Document, fields

# Add Author Document
class Author(Document):
    name = fields.StringField(required=True)

class Books(Document):
    caption = fields.StringField(required=True)  # Make required and rename
    year = fields.IntField()  # Change type to IntField
    # Removed field isbn
    author = fields.ReferenceField(Author)  # Add field
```

Such changes should be reflected in database during upgrading. To 
detect changes run the command:

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
    'my_migration'
]

# Action chain
forward = [
    CreateDocument('Author', collection='author'),
    CreateField('Author', 'name', choices=None, db_field='name', default=None, max_length=None,
        min_length=None, null=False, primary_key=False, regex=None, required=True,
        sparse=False, type_key='StringField', unique=False, unique_with=None),
    RenameField('Books', 'name', new_name='caption'),
    AlterField('Books', 'caption', required=True, db_field='caption'),
    AlterField('Books', 'year', type_key='IntField', min_value=None, max_value=None),
    DropField('Books', 'isbn'),
    CreateField('Books', 'author', choices=None, db_field='author', dbref=False, default=None,
        target_doctype='Author', null=False, primary_key=False, required=False, sparse=False,
        type_key='ReferenceField', unique=False, unique_with=None),
]
```

Now in order to migrate database to the last migration, just run the command:

```shell script
mongoengine-migrate migrate
```

Or to migrate to the certain migration:

```shell script
mongoengine-migrate migrate my_migration
```
...to be continued 

## Roadmap

- [x] Migrations graph utilities and core code
- [x] Basic collection actions
- [x] Basic field actions
- [x] User-defined code action
- [x] Basic data type fields support (string, integer, float, etc.)
- [x] Dictionary, list fields support
- [x] Reference fields support
- [x] Embedded documents support + actions
- [x] Geo fields support
- [x] GridFS fields support
- [x] Altering fields in embedded documents
- [x] Document inheritance support
- [x] Dynamic documents support
- [x] Generic* fields support
- [ ] Index support
- [ ] Interactive mode
- [ ] Schema repair tools
- [ ] Alpha release

# Author

Igor Derkach, gosha753951@gmail.com
