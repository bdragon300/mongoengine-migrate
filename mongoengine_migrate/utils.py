import inspect
from typing import Type, Iterable, Optional

from mongoengine import EmbeddedDocument, Document
from mongoengine.base import BaseDocument

from .flags import EMBEDDED_DOCUMENT_NAME_PREFIX, DOCUMENT_NAME_SEPARATOR


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

        try:
            return all((
                    type(self) == type(other),
                    set(self.__slots__) == set(other.__slots__),
                    all(getattr(self, slot) == getattr(other, slot) for slot in self.__slots__)
            ))
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
    # Mongoengine places class name only to
    # `EmbeddedDocument._class_name` instead of all classes in
    # hierarchy.
    # As a workaround we construct class name here by hand both for
    # embedded documents and not embedded
    base_classes = (EmbeddedDocument, Document)
    bases = [base.__name__ for base in inspect.getmro(document_cls)
             if (issubclass(base, base_classes)
                 and base not in base_classes
                 and not getattr(base, "_meta", {}).get("abstract", False))]
    document_type = DOCUMENT_NAME_SEPARATOR.join(reversed(bases))

    if issubclass(document_cls, EmbeddedDocument):
        document_type = f'{EMBEDDED_DOCUMENT_NAME_PREFIX}{document_type}'

    return document_type
