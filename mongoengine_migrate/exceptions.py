class MigrationError(Exception):
    """Generic migration error"""


class ActionError(MigrationError):
    """Generic action error occured during migration"""
