import weakref
from abc import ABCMeta, abstractmethod
from mongoengine_migrate.fields.base import mongoengine_fields_mapping, CommonFieldType

# Concrete Actions registry
# {class_name: action_class}
actions_registry = {}


class BaseActionMeta(ABCMeta):
    def __new__(mcs, name, bases, attrs):
        attrs['_meta'] = weakref.proxy(mcs)

        c = super(BaseActionMeta, mcs).__new__(mcs, name, bases, attrs)
        if not name.startswith('Base'):
            actions_registry[name] = c

        return c


class BaseAction(metaclass=BaseActionMeta):
    """Base class for migration actions

    Action represents one change within migration such as field
    altering, collection renaming, collection dropping, etc.

    Migration file consists of actions following by each other.
    Every action accepts collection name, field name and its new
    parameters if any.

    Action also can be serialized into dict diff in order to make
    diff to a db schema after migration run
    """

    # Actions with set `factory_exclusive` are tested by a actions
    # factory firstly. If such Actions are applicable then they
    # patch tested schema and the rest actions are tested
    # with patched schema.
    # This flag is suitable for rename actions when the other actions
    # should detect changes of field/collection with new name.
    factory_exclusive = False

    def __init__(self, collection_name, *args, **kwargs):
        """
        :param collection_name: Name of collection where the migration
         will be performed on
        :param args: Action positional parameters
        :param kwargs: Action keyword parameters
        """
        self.collection_name = collection_name
        self._init_args = args
        self._init_kwargs = kwargs  # TODO: rename to field_params or smth

        self.current_schema = None
        self.db = None
        self.collection = None

    def prepare(self, db, current_schema: dict):
        """
        Prepare action before Action run (both forward and backward)
        :param db: pymongo.Database object
        :param current_schema: db schema which is before migration
        :return:
        """
        self.current_schema = current_schema
        self.db = db
        self.collection = db['collection']

    def cleanup(self):
        """Cleanup after Action run (both forward and backward)"""

    @abstractmethod
    def run_forward(self):
        """Run command in forward direction"""

    @abstractmethod
    def run_backward(self):
        """Run command in backward direction"""

    @abstractmethod
    def to_schema_patch(self, current_schema: dict):
        """
        Return dictdiff patch which this Action is applied to a schema
        during forward run.

        This function returns diff to applied in forward direction
        to get needed changes in schema. Note that this function also
        is used on downgrade process, where this diff is swapped and
        applied to schema in reverse order
        :param current_schema:
        :return: dictdiffer diff
        """

    @abstractmethod
    def to_python_expr(self) -> str:
        """
        Return string of python code which creates current object with
        the same state
        """


# TODO: add to prepare() checking if db_field param has not dots
class BaseFieldAction(BaseAction):
    """Base class for action which changes one field"""

    def __init__(self,
                 collection_name: str,
                 field_name: str,
                 *args,
                 **kwargs):
        """
        :param collection_name: collection name where we performing a
         change
        :param field_name: field which is changed
        """
        super().__init__(collection_name, *args, **kwargs)
        self.field_name = field_name

    @property
    def field_type_cls(self):
        return mongoengine_fields_mapping.get(self._init_kwargs.get('type_key'), CommonFieldType)

    @classmethod
    @abstractmethod
    def build_object_if_applicable(cls,
                                   collection_name: str,
                                   field_name: str,
                                   old_schema: dict,
                                   new_schema: dict):
        """
        Factory method which may produce filled in object of concrete
        action if this action can be used to reflect such field change
        in schema. Return None if the action is not applicable for such
        change.

        This method in actions is used to guess which action is
        suitable to reflect schema change. It's called for several
        times for each field which was modified in mongoengine models.

        For example, on field deleting this method defined in
        CreateField action should return None, but those one in
        DeleteField action should return DeleteField object with
        set up parameters of change (type of field, is required, etc.)

        :param collection_name: collection we are considering in
         schemas diff
        :param field_name: field we are considering in schemas diff
        :param old_schema: current database schema (before migration
         apply)
        :param new_schema: schema which will be current after the
         migration will get applied
        :return: object of self type or None
        """
        pass

    def to_python_expr(self) -> str:
        args_str = ''.join(
            ', ' + getattr(arg, 'to_python_expr', lambda: repr(arg))()
            for arg in self._init_args
        )
        kwargs = {
            name: getattr(val, 'to_python_expr', lambda: repr(val))()
            for name, val in self._init_kwargs.items()
        }
        kwargs_str = ''.join(f", {name}={val}" for name, val in kwargs.items())  # TODO: sort kwargs
        return f'{self.__class__.__name__}({self.collection_name!r}, {self.field_name!r}' \
               f'{args_str}{kwargs_str})'


class BaseCollectionAction(BaseAction):
    """
    Base class for actions which change a collection at whole such as
    renaming, creating, dropping, etc.
    """
    @classmethod
    @abstractmethod
    def build_object_if_applicable(cls, collection_name: str, old_schema: dict, new_schema: dict):
        pass

    def to_python_expr(self) -> str:
        args_str = ''.join(
            ', ' + getattr(arg, 'to_python_expr', lambda: repr(arg))()
            for arg in self._init_args
        )
        kwargs = {
            name: getattr(val, 'to_python_expr', lambda: repr(val))()
            for name, val in self._init_kwargs.items()
        }
        kwargs_str = ''.join(f", {name}={val}" for name, val in kwargs.items())  # TODO: sort kwargs
        return f'{self.__class__.__name__}({self.collection_name!r}{args_str}{kwargs_str})'
