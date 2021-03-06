# Overview

When you make changes in mongoengine documents 
declarations (remove a field, for instance), it should be reflected in the database
(this field should be actually removed from records). This tool detects such changes,
creates migration file and makes needed changes in db. If you worked with migration systems 
for SQL databases, you should get the idea.

### How it works

Unlike SQL databases the MongoDB is schemaless database, therefore in order to track mongoengine
schema changes we're needed to keep it somewhere. *Mongoengine-migrate* keeps current schema 
and migrations tree in a separate collection. By default it is "mongoengine_migrate".

#### MongoDB commands

*Mongoengine_migrate* tries to make changes by using the fastest way as possible. Usually it
is MongoDB update commands or pipelines. But sometimes this not possible because of too
old version of MongoDB server.

For this case each modification command has its "manual" counterpart. It updates records 
by iterating on them in python code and makes manual update. This variant could be
slower especially for big collections. It will be used automatically if MongoDB version is
lower than required to execute a certain command.

#### Common workflow

1. After you made changes in documents schema, run `mongoengine_migrate makemigrations`.
Your documents will be scanned and compared to the versions currently contained in your 
migration files, and a new migration file will be created if changes was detected.
1. In order to apply the last migration you run `mongoengine_migrate migrate`.
1. Once the migration is applied, commit the migration and the models change to your version 
control system as a single commit - that way, when other developers (or your production servers)
check out the code, they’ll get both the changes to your documents schema and the accompanying 
migration at the same time.
1. If you'll need to rollback schema to the certain migration, run 
`mongoengine_migrate migrate <migration_name>`.

### Example

Let's assume that we already have the following Document declaration:

```python
from mongoengine import Document, fields
    
class Book(Document):
    name = fields.StringField(default='?')
    year = fields.StringField(max_length=4)
    isbn = fields.StringField()
```

Then we made some changes:

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

```console
$ mongoengine_migrate makemigrations -m myproject.db 
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

```console
$ mongoengine_migrate migrate
```

You can rollback changes by downgrading to the previous migration:

```console
$ mongoengine_migrate migrate previous_migration
```

#### Actual db changes 

During the running forward the migration created above the following changes will be made:
* "author" collection
  * Nothing to do
* "book" collection
  * Existing fields "name" will be renamed to "caption"
  * All unset "caption" fields will be set to default value `'?'`
     (because this field was defined as "required")
  * Existing fields "year" with string values will be casted to integer value
  * "isbn" field will be dropped

On backward direction the following changes will be made:
* "book" collection
  * "author" field will be dropped
  * All integer values in "year" field will be casted back to string
  * Existing "caption" fields will be renamed back to "name"
* "author" collection
  * "name" field will be dropped
  * whole "author" collection will be dropped
