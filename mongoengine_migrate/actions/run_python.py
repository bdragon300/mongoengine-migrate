from mongoengine_migrate.exceptions import ActionError
from .base import BaseAction


class RunPython(BaseAction):
    """
    Action which runs user defined functions. It's supposed
    that user must also handle possible schema changes in his functions.
    """
    # TODO: implement handling of user's schema changes and to_schema_patch
    def __init__(self, collection_name, forward_func=None, backward_func=None, *args, **kwargs):
        super().__init__(collection_name, *args, **kwargs)

        if forward_func is None and backward_func is None:
            raise ActionError("forward_func and backward_func are not set")

        self.forward_func = forward_func
        self.backward_func = backward_func

    def run_forward(self):
        if self.forward_func is not None:
            self.forward_func(self.db, self.collection, self.current_schema)

    def run_backward(self):
        if self.backward_func is not None:
            self.backward_func(self.db, self.collection, self.current_schema)

    def to_schema_patch(self, current_schema: dict):
        """
        We can't predict what code will be placed to user functions
        so don't suppose any schema changes
        """
        return []

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
        ff_expr = f'forward_func={self.forward_func.__name__ if self.forward_func else None}'
        bf_expr = f'backward_func={self.backward_func.__name__ if self.backward_func else None}'
        return f'{self.__class__.__name__}({self.collection_name!r}, ' \
               f'{ff_expr + ", " if self.forward_func else ""}' \
               f'{bf_expr + ", " if self.backward_func else ""}' \
               f'{args_str}{kwargs_str})'
