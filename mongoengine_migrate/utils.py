__all__ = [
    'UNSET',
    'Diff',
    'Slotinit',
    'get_closest_parent',
    'get_document_type',
    'document_type_to_class_name'
]

import inspect
from typing import Type, Iterable, Optional, NamedTuple, Any

from mongoengine import EmbeddedDocument
from mongoengine.base import BaseDocument

from .flags import EMBEDDED_DOCUMENT_NAME_PREFIX, DOCUMENT_NAME_SEPARATOR


class _Unset:
    def __str__(self):
        return 'UNSET'

    def __repr__(self):
        return '<UNSET>'

    def __bool__(self):
        return False


#: Value indicates that such schema key is unset
UNSET = _Unset()


class Diff(NamedTuple):
    """Diff of schema key values for alter methods"""
    old: Any
    new: Any
    key: str

    def __str__(self):
        return f"Diff({self.key}: {self.old}, {self.new})"

    def __repr__(self):
        return f"<Diff({self.key}: {self.old}, {self.new})>"


class Slotinit(object):
    """
    Set class __slots__ in constructor kwargs
    If slot was omitted in kwargs then its value will be taken from
    `defaults` dict (if any). Other omitted slots will remain
    uninitialized.

    Example:
        class Car(Slotinit):
            __slots__ = ('color', 'engine_power', 'bodywork_type')
            defaults = {'color': 'black', 'bodywork_type': 'sedan'}

        # Slots will be:
        # color='blue', engine_power='100hp', bodywork_type='sedan'
        my_car = Car(color='blue', engine_power='100hp')
    """

    defaults = {}
    __slots__ = ()

    def __init__(self, **kwargs):
        for slot in self.__slots__:
            if slot in kwargs:
                setattr(self, slot, kwargs[slot])
            elif slot in self.defaults:
                setattr(self, slot, self.defaults[slot])

    def __eq__(self, other):
        if self is other:
            return True
        if not isinstance(other, Slotinit):
            return False

        try:
            return set(self.__slots__) == set(other.__slots__) \
                   and all(getattr(self, slot) == getattr(other, slot) for slot in self.__slots__)
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)


def get_closest_parent(target: Type, classes: Iterable[Type]) -> Type:
    """
    Find which class in given list is the closest parent to
    a target class.
    :param target: class which we are comparing of
    :param classes:
    :return: the closest parent or None if not found
    """
    target_mro = inspect.getmro(target)
    res = None
    min_distance = float('inf')
    for parent in classes:
        found = [x for x in enumerate(target_mro) if x[1] == parent]
        if found:
            distance, klass = found[0]
            if distance == 0:  # Skip if klass is target
                continue
            if distance < min_distance:
                res = klass
                min_distance = distance

    return res


def get_document_type(document_cls: Type[BaseDocument]) -> Optional[str]:
    """
    Return document type for `document_type` parameter of Action
    :param document_cls: document class
    :return: document type or None if unable to get it (if document_cls
     is abstract)
    """
    # Class name consisted of its name and all parent names separated
    # by dots, see mongoengine.base.DocumentMetaclass
    document_type = getattr(document_cls, '_class_name')
    # Replace dots on our separator since "dictdiffer" package
    # treats dot in key as dict keys path
    document_type = document_type.replace('.', DOCUMENT_NAME_SEPARATOR)
    if issubclass(document_cls, EmbeddedDocument):
        document_type = f'{EMBEDDED_DOCUMENT_NAME_PREFIX}{document_type}'

    return document_type


def document_type_to_class_name(document_type: str) -> str:
    """
    Convert document type string to class name used by mongoengine
     (`Document._class_name` property).
     E.g. '~Doc1->Doc2' to 'Doc1.Doc2'
    :param document_type:
    :return:
    """
    cls_name = document_type.replace(DOCUMENT_NAME_SEPARATOR, '.')
    emb_prefix = EMBEDDED_DOCUMENT_NAME_PREFIX
    if cls_name.startswith(emb_prefix):
        cls_name = cls_name[len(emb_prefix):]

    return cls_name
