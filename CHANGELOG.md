# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to
[Python Versioning](https://www.python.org/dev/peps/pep-0440/#public-version-identifiers).

## [0.0.1a1]
### Added
- Implement FallbackDocumentUpdater for perform an action in python loop when MongoDB version
  below minimal for query
- Add python loop update callback where version of MongoDB is restricted to minimal
- Implement migration policy
- Implement Generic* fields
- Add and setup Travis CI/CD
- Add tox test runner

### Changed
- Change class_filter build which is used for Document inheritance
- Flush schema to a db after every action completed instead of after whole action chain
- Fix inserting 're' module import statement in migration files
- Print warning if different documents use the same collection
- Print warning if collection in base and derived documents are different
- Improve convertion matrix
- Fix converters idempotency
- NamedTuple method declaration workaround for Python 3.6
- Fix change_dynamic method to use by_doc callback
- Rename --schema-only to --dummy-actions cli parameter
- Fix EmailFieldHandler.change_domain_whitelist method signature
- Implement change_inherit methods in actions
- Fix retrieving update paths for non-embedded documents in DocumentUpdater
- Using separate mongo connection for bulk writes
- Write only actually changed documents during update_by_document execution

### Removed
- Remove isinstance dict checks from by_doc callbacks


## [0.0.1a1.dev2]
### Added
- Implement geo fields
- Implement convertion of legacy coordinates pair to/from GeoJSON fields
- GridFS fields support (FileField, ImageField)
- Implement embedded fields updater (actions, field handlers, type_key converters)
- Implement Database query tracer
- Make Schema class as interface to schema dict
- Implement AlterDocument action
- Document inheritance support
- Improve logging and add log levels. Print queries using logging in "dry run" mode
- Dynamic documents support

### Changed
- Make ListField.max_length schema key as optional
- Change action chain build algorithm to iterate on actions instead of collections
- Setup Actions priority
- Substitute embedded documents before collections in actions chain
- Rename *Collection actions to *Document
- Define order of document types processing
- Add __all__ to python files
- Reraise SchemaError exception for any exception arisen in Schema class
- Fix fields updating in field handler methods, converters

### Removed
-- Remove AlterDiff which was used in Alter* actions to keep parameters diff


## [0.0.1a1.dev1]
### Added
- Migration graph manipulation code
- BaseAction implementation
- Actions registry
- Migration files manipulation code
- Mongoengine schema collecting code
- Field and collections related actions
- CLI interface
- RunPython action
- Field type convertion matrix
- Fill convertion matrix with basic mongoengine field types
- Dictionary, List fields support
- Reference fields support
- 'type_key' registry
- Wrapper class (query tracer) which mocks pymongo Collection methods calls in order to show these
  calls in "dry run" mode
- MongoDB version detection and make skipping of mongo queries which can't execute on current
  MongoDB version
- Add setuptools script
- Make db queries idempotential since MongoDB < 4.0 does not support multi-document transactions,
  MongoDB < 4.4 does not support collections creation in transactions
- Embedded documents support
- Add 'dummy_action' parameter to all Actions
- Introduce Action priority
