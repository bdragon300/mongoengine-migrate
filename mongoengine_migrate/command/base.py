from abc import ABCMeta, abstractmethod


class BaseCommand(metaclass=ABCMeta):
    """Base class for migrate command

    Command is a smallest piece of migration. Usually it contains of
    pymongo calls which changes some field parameter, e.g. makes field
    required.

    It's supposed that Command object is stateless
    """
    @abstractmethod
    def forward(self, db, col, old_schema, new_schema):
        """
        Make forward changes.

        Must return True on success, command will be treated as
        failed otherwise
        :param db: pymongo.Database object
        :param col: pymongo.Collection object
        :param old_schema: older migration schema
        :param new_schema: new schema
        :return: True on success
        """

    @abstractmethod
    def backward(self, db, col, old_schema, new_schema):
        """
        Revert changes

        Must return True on success, command will be treated as
        failed otherwise
        :param db: pymongo.Database object
        :param col: pymongo.Collection object
        :param old_schema: older migration schema
        :param new_schema: new schema, which we are reverting
        :return: True on success
        """
