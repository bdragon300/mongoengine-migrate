from abc import ABCMeta, abstractmethod
from typing import Iterable, Type

from dictdiffer import patch

from mongoengine_migrate.exceptions import ActionError
from .base import (
    actions_registry,
    BaseFieldAction,
    BaseCollectionAction,
    BaseAction
)


class BaseActionFactory(metaclass=ABCMeta):
    """
    Base abstract class for Actions abstract factory. The main aim
    is to produce actions chain suitable to handle change of schema
    written in db and current schema collected from mongoengine models.

    Concrete factory produces actions with same kind of change such as
    collection change, fields change in collection, etc.
    """
    @staticmethod
    @abstractmethod
    def get_actions_chain(collection_name: str,
                          old_schema: dict,
                          new_schema: dict) -> Iterable[BaseAction]:
        """
        Produce Action objects iterable which consists of actions which
        can handle such change of a given collection
        :param collection_name:
        :param old_schema: schema before change or "schema in db"
        :param new_schema: schema which would be after change or
         "schema in models"
        :return: iterable of Action objects
        """
        pass


class FieldActionFactory(BaseActionFactory):
    """
    Factory for Actions for fields such as renaming, changing
    the signature, dropping, etc.
    """
    @staticmethod
    def get_actions_chain(collection_name: str,
                          old_schema: dict,
                          new_schema: dict) -> Iterable[BaseFieldAction]:
        old_collection_schema = old_schema.get(collection_name, {})
        new_collection_schema = new_schema.get(collection_name, {})
        # Take all fields which was created, changed and dropped
        fields = old_collection_schema.keys() | new_collection_schema.keys()
        for field in fields:
            for action_cls in actions_registry.values():
                if not issubclass(action_cls, BaseFieldAction):
                    continue
                action_obj = action_cls.build_object_if_applicable(collection_name,
                                                                   field,
                                                                   old_schema,
                                                                   new_schema)
                if action_obj is not None:
                    yield action_obj


class CollectionActionFactory(BaseActionFactory):
    """
    Factory for Actions for collections such as renaming, dropping, etc.
    """
    @staticmethod
    def get_actions_chain(collection_name: str,
                          old_schema: dict,
                          new_schema: dict) -> Iterable[BaseCollectionAction]:
        action_chain = (
            action_cls.build_object_if_applicable(collection_name, old_schema, new_schema)
            for action_cls in actions_registry.values()
            if issubclass(action_cls, BaseCollectionAction)
        )
        yield from (a for a in action_chain if a is not None)


def build_actions_chain(old_schema: dict, new_schema: dict) -> Iterable[BaseAction]:
    """
    Build Action objects chain to handle such schema change. Uses all
    Action factories
    :param old_schema: schema before change or "schema in db"
    :param new_schema: schema which would be after change or
     "schema in models"
    :return: iterable of Action objects
    """
    action_chain = []

    all_collections = old_schema.keys() | new_schema.keys()
    current_schema = old_schema.copy()
    for collection_name in all_collections:
        for factory in (CollectionActionFactory, FieldActionFactory):
            new_actions = list(factory.get_actions_chain(
                collection_name,
                current_schema,
                new_schema
            ))

            # Apply actions changes to a temporary schema
            for action_obj in new_actions:
                current_schema = patch(action_obj.to_schema_patch(current_schema), current_schema)
            action_chain.extend(new_actions)
    if new_schema != current_schema:
        # TODO: ability to force process without error
        raise ActionError('Could not reach current schema after applying whole Action chain. '
                          'This could be a problem in some Action which does not react to schema'
                          ' change it should react or produces wrong schema diff')

    return action_chain
