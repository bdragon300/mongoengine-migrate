__all__ = [
    'RunPython'
]

import logging

from mongoengine_migrate.exceptions import ActionError
from mongoengine_migrate.schema import Schema
from .base import BaseAction

log = logging.getLogger('mongoengine-migrate')


class RunPython(BaseAction):
    """
    Action which runs user defined functions. It's supposed
    that user must also handle possible schema changes in his functions.
    """
    # TODO: implement handling of user's schema changes and to_schema_patch
    def __init__(self, document_type, *, forward_func=None, backward_func=None, **kwargs):
        super().__init__(document_type, **kwargs)

        if forward_func is None and backward_func is None:
            raise ActionError("forward_func and backward_func are not set")

        self.forward_func = forward_func
        self.backward_func = backward_func

    def run_forward(self):
        if self.forward_func is not None:
            self.forward_func(self._run_ctx['db'],
                              self._run_ctx['collection'],
                              self._run_ctx['left_schema'])

    def run_backward(self):
        if self.backward_func is not None:
            self.backward_func(self._run_ctx['db'],
                               self._run_ctx['collection'],
                               self._run_ctx['left_schema'])

    def to_schema_patch(self, left_schema: Schema):
        """
        We can't predict what code will be placed to user functions
        so don't assume any schema changes
        """
        return []

    def to_python_expr(self) -> str:
        parameters = {
            name: getattr(val, 'to_python_expr', lambda: repr(val))()
            for name, val in self.parameters.items()
        }
        if self.dummy_action:
            parameters['dummy_action'] = True

        kwargs = []
        if self.forward_func:
            kwargs.append(f', forward_func={self.forward_func.__name__}')
        if self.backward_func:
            kwargs.append(f', backward_func={self.backward_func.__name__}')
        kwargs += [f", {name!s}={val!s}" for name, val in sorted(parameters.items())]
        kwargs_str = ''.join(kwargs)

        return f'{self.__class__.__name__}({self.document_type!r}{kwargs_str})'
