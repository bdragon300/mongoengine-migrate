"""This module contains flags setting on starting, via command line
for example
"""
from typing import Optional

import pymongo
from pymongo.database import Database

#: Dry run mode. Don\'t modify the database and print modification
#: commands which would get executed
dry_run: bool = False


#: Migrate only schema, do not perform any modifications on database
schema_only: bool = False


#: MongoDB server version
mongo_version: Optional[str] = None


#: Another Database object that used for operations which must be
#: performed in a separate connection such as parallel bulk writes
database2: Optional[Database] = None


#: If this prefix contains in collection name then this document
#: is considered as embedded
EMBEDDED_DOCUMENT_NAME_PREFIX = '~'


#: Separator between parent and clild classes in document name string
DOCUMENT_NAME_SEPARATOR = '->'


#: Maximum memory items buffer size on bulk write operations
#: Pay attention: max BSON size is 16Mb
#: https://docs.mongodb.com/manual/reference/limits/#bson-documents
BULK_BUFFER_LENGTH = 10000


#: Separator which separates parts in index name
INDEX_NAME_SEPARATOR = '_'

#: Default field index type if no type explicitly set
#: See mongoengine code
DEFAULT_INDEX_TYPE = pymongo.ASCENDING
