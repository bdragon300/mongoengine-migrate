from abc import ABCMeta, abstractmethod
import weakref

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
        self.current_schema = None
        self._init_args = args
        self._init_kwargs = kwargs

    def prepare(self, db, current_schema):
        """
        Prepare action before data migrate
        :param db: pymongo.Database object
        :param current_schema: db schema which is before migration
        :return:
        """
        self.current_schema = current_schema  # type: dict
        self.db = db
        self.collection = db['collection']

    def cleanup(self):
        """Cleanup callback executed after command chain run"""

    @abstractmethod
    def run_forward(self):
        """
        Run command in forward direction
        """

    @abstractmethod
    def run_backward(self):
        """
        Run command in backward direction
        """

    @abstractmethod
    def as_schema_patch(self, current_schema):
        """
        Return dict patch in forward direction to be applied to
        a schema dictionary
        :param current_schema:
        :return:
        """

    @abstractmethod
    def as_python_expr(self) -> str:
        """
        Return string of python code which creates current object with
        the same state
        """
        pass


class BaseFieldAction(BaseAction):
    """Base class for action which changes one field"""
    def __init__(self, collection_name, field_name, field_type_cls, *args, **kwargs):
        """
        :param collection_name: collection name where we performing a
         change
        :param field_name: field that is changed
        :param field_type_cls: FieldType class with target type of field
        """
        super().__init__(collection_name, *args, **kwargs)
        self.field_name = field_name
        self.field_type_cls = field_type_cls

    @classmethod
    @abstractmethod
    def build_object_if_applicable(cls, collection_name, field_name, old_schema, new_schema):
        """
        Factory method which may produce filled in object of concrete
        action if this action can be used to reflect such field change
        in schema. Return None if the action is not applicable for such
        change.

        This method is used for guess which action is suitable to
        reflect schema change. It's called for several times for each
        field that was modified in mongoengine models.

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
        :return:
        """
        pass

    def as_python_expr(self) -> str:
        args_str = ''.join(
            ', ' + getattr(arg, 'as_python_expr', lambda: repr(arg))()
            for arg in self._init_args
        )
        kwargs = {
            name: getattr(val, 'as_python_expr', lambda: repr(val))()
            for name, val in self._init_kwargs.items()
        }
        kwargs_str = ''.join(f", {name}={val}" for name, val in kwargs.items())
        return f'{self.__class__.__name__}(' \
               f'{self.collection_name!r}, {self.field_name!r}, {self.field_type_cls.__name__}' \
               f'{args_str}{kwargs_str})'


class BaseCollectionAction(BaseAction):
    """
    Base class for action which changes collection at whole such as
    renaming, creating, dropping, etc.
    """
    @classmethod
    @abstractmethod
    def build_object_if_applicable(cls, collection_name, old_schema, new_schema):
        pass

    def as_python_expr(self) -> str:
        args_str = ''.join(
            ', ' + getattr(arg, 'as_python_expr', lambda o: f"'{o}'")()
            for arg in self._init_args
        )
        kwargs = {
            name: getattr(val, 'as_python_expr', lambda o: f"'{o}'")()
            for name, val in self._init_kwargs.items()
        }
        kwargs_str = ''.join(f", {name}='{val}'" for name, val in kwargs.items())
        return f'{self.__class__.__name__}(' \
               f'"{self.collection_name}"{args_str}{kwargs_str})'
