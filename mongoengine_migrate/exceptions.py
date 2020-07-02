__all__ = [
    'MongoengineMigrateError',
    'MigrationGraphError',
    'ActionError',
    'SchemaError',
    'MigrationError',
    'InconsistencyError'
]


class MongoengineMigrateError(Exception):
    """Generic error"""


class MigrationGraphError(MongoengineMigrateError):
    """Error related to migration modules names, dependencies, etc."""


class ActionError(MongoengineMigrateError):
    """Error related to Action itself"""


class SchemaError(MongoengineMigrateError):
    """Error related to schema errors"""


class MigrationError(MongoengineMigrateError):
    """Error which could occur during migration"""


class InconsistencyError(MigrationError):
    """Error which could occur during migration if data inconsistency
    was detected
    """
