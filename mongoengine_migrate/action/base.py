from abc import ABCMeta, abstractmethod
from mongoengine_migrate.exceptions import ActionError


class BaseAction(metaclass=ABCMeta):
    """Base class of migrate actions

    Action represents one change within migration such as field
    altering, collection renaming, collection dropping, etc.

    Migration file consists of actions following by each other.
    Every action accepts collection name, field name and its new
    parameters if any.

    Action also can be serialized into dict diff in order to make
    diff to a db schema after migration run
    """
    def __init__(self, collection_name, *args, **kwargs):
        self.collection_name = collection_name
        self._init_args = args
        self._init_kwargs = kwargs

    def prepare(self, current_schema):
        """
        Prepare action before data migrate
        :param current_schema: db schema which is before migration
        :return:
        """
        self.current_schema = current_schema

    @abstractmethod
    def run_forward(self, db, collection):
        """
        Run command in forward direction
        :param db: pymongo.Database object
        :param collection: pymongo.Collection object
        """

    @abstractmethod
    def run_backward(self, db, collection):
        """
        Run command in backward direction
        :param db: pymongo.Database object
        :param collection: pymongo.Collection object=
        """

    @abstractmethod
    def as_schema_patch(self):
        """
        Return dict patch in forward direction to be applied to
        a schema dictionary
        :return:
        """

    def cleanup(self):
        """Cleanup callback executed after command chain run"""
