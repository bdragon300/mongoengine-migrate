__all__ = [
    'MongoengineMigrateError',
    'MigrationGraphError',
    'ActionError',
    'SchemaError',
    'MigrationError',
    'InconsistencyError'
]


class MongoengineMigrateError(Exception):
    """Generic migration error"""


class MigrationGraphError(MongoengineMigrateError):
    """Error related to migration modules names, dependencies, etc."""


class ActionError(MongoengineMigrateError):
    """Generic error occured during migration actions executing"""


class SchemaError(MongoengineMigrateError):
    """Generic error in db schema"""


class MigrationError(MongoengineMigrateError):
    """Error which could occur during migration"""


class InconsistencyError(MigrationError):
    """Error which could occur during migration if data inconsistency
    was detected"""
