# Mongoengine-migrate

Framework-agnostic schema migrations for Mongoengine ODM. Inspired by Django migrations system.

**WARNING**: *this is an unstable version of software. Please backup your data before migrating*

## Features

* Documents
    * Creating, dropping, renaming
    * Renaming a collection
    * Creating, dropping, renaming of fields
    * Converting to/from a DynamicDocument
    * Inheritance
* Embedded documents
    * Recursive creating, dropping
    * Renaming
    * Recursive creating, dropping, renaming of fields
    * Converting to/from a DynamicEmbeddedDocument
    * Inheritance
* Altering fields in document and embedded documents
    * Changing of init parameters such as db_field, required, etc.
    * Convertion between field types (if possible)
* Automatic select a query or a python loop to perform an update depending on MongoDB version
* Two policies of how to work with existing data which does not meet to mongoengine schema

All mongoengine field types are supported, including simple types, lists, dicts, references, GridFS, geo types, generic types.
