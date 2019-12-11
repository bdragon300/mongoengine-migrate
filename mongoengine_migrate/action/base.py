from abc import ABCMeta, abstractmethod
from mongoengine_migrate.exceptions import ActionError


class BaseAction:
    """Base class of migrate actions

    Action represents one change within migration such as field
    altering, collection renaming, collection dropping, etc.

    Migration file consists of actions following by each other.
    Every action accepts collection name, field name and its new
    parameters if any.

    Action also is serialized into schema dict which is written to db
    along with migration metadata and used to downgrade migration.
    """
    def __init__(self, collection_name, *args, **kwargs):
        self.collection_name = collection_name
        self._init_args = args
        self._init_kwargs = kwargs

    def prepare(self, old_schema):
        """
        Prepare action before data migrate
        :param old_schema: previous (old) migration schema
        :return:
        """
        self.old_schema = old_schema

    def run_forward(self, db, collection, start_from=0, ignore_failed=False) -> list:
        """
        Run command chain in forward direction
        :param db: pymongo.Database object
        :param collection: pymongo.Collection object
        :param start_from: index in command chain to start execute,
         positive integer only
        :param ignore_failed: ignore failed commands in chain
        :return: tuple with pairs (command_number, exception object) or
         (command_number, command_result) if some of commands was failed
          and `ignore_failed` parameter is True. On success the function
         returns empty list
        """
        new_schema = self.as_schema()
        chain = self._build_commands_chain(self.old_schema, new_schema)  # type: list
        if start_from >= len(chain) or start_from < 0:
            raise ValueError(f'Command index {start_from} is out of range')

        return self._run_chain(chain[start_from:], db, collection, 'forward', ignore_failed)

    def run_backward(self, db, collection, start_from=None, ignore_failed=False) -> list:
        """
        Run command chain in backward direction from the end
        :param db: pymongo.Database object
        :param collection: pymongo.Collection object
        :param start_from: index in command chain to start execute,
        positive integer only. Default None value means end of chain
        :param ignore_failed: ignore failed commands in chain
        :return: tuple with pairs (command_number, exception object) or
         (command_number, command_result) if some of commands was failed
          and `ignore_failed` parameter is True. On success the function
         returns empty list
        """
        new_schema = self.as_schema()
        chain = self._build_commands_chain(self.old_schema, new_schema)  # type: list
        if start_from is not None:
            if start_from >= len(chain) or start_from < 0:
                raise ValueError(f'Command index {start_from} is out of range')
            chain = chain[:start_from + 1]

        return self._run_chain(chain[::-1], db, collection, 'backward', ignore_failed)

    def cleanup(self):
        """Cleanup callback executed after command chain run"""

    @abstractmethod
    def as_schema(self):
        """Return action as schema"""

    @abstractmethod
    def _build_commands_chain(self, old_schema, new_schema):
        """
        Build commands chain to be executed during migration based on
        difference of current schema and previous schema
        :param old_schema: older migration schema
        :return: list with Command objects
        """

    def _run_chain(self, chain, db, collection, method_name, ignore_failed=False) -> list:
        new_schema = self.as_schema()
        errors = []
        counter = 0
        for cmd in chain:
            try:
                # Command may return False/None or raise the exception
                # Both cases means that command was failed
                method = getattr(cmd, method_name)
                command_result = method(db, collection, self.old_schema, new_schema)
                if not command_result:
                    if ignore_failed:
                        errors.append((counter, command_result))
                    else:
                        raise ActionError(
                            f'Action #{counter} {type(cmd)} has unexpectly failed',
                            counter
                        )
            except Exception as e:
                if not ignore_failed:
                    raise ActionError(
                        f'Action #{counter} {type(cmd)} has failed',
                        counter
                    ) from e
                errors.append((counter, e))

            counter += 1

        return errors
