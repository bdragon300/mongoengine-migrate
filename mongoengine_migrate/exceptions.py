__all__ = [
    'MigrationError',
    'ActionError',
    'SchemaError'
]


class MigrationError(Exception):
    """Generic migration error"""


class ActionError(MigrationError):
    """Generic error occured during migration actions executing"""


class SchemaError(MigrationError):
    """Generic error in db schema"""
