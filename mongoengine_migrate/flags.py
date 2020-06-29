"""This module contains flags setting on starting, via command line
for example
"""

#: Dry run mode. Don\'t modify the database and print modification
#: commands which would get executed
dry_run: bool = False


#: Migrate only schema, do not perform any modifications on database
schema_only: bool = False


#: MongoDB server version
mongo_version = None

#: If this prefix contains in collection name then this document
#: is considered as embedded
EMBEDDED_DOCUMENT_NAME_PREFIX = '~'


#: Separator between parent and clild classes in document name string
DOCUMENT_NAME_SEPARATOR = '->'


#: Maximum memory items buffer size on bulk write operations
#: Pay attention: max BSON size is 16Mb
#: https://docs.mongodb.com/manual/reference/limits/#bson-documents
BULK_BUFFER_LENGTH=10000
