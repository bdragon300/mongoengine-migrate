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
    is to produce actions chain suitable to handle change between
    current schema and schema collected from mongoengine models.

    Concrete factory produces actions with concrete kind of change
    such as collection change or field change.
    """
    @staticmethod
    @abstractmethod
    def get_actions_chain(collection_name: str,
                          old_schema: dict,
                          new_schema: dict) -> Iterable[BaseAction]:
        """
        Produce Action objects iterable with actions suitable to
        process changes between given schemas
        :param collection_name: collection name to consider
        :param old_schema: current schema
        :param new_schema: schema collected from mongoengine models
        :return: iterable of Action objects
        """
        pass


class FieldActionFactory(BaseActionFactory):
    """Factory of field Actions
    """
    @staticmethod
    def get_actions_chain(collection_name: str,
                          old_schema: dict,
                          new_schema: dict) -> Iterable[BaseFieldAction]:
        old_collection_schema = old_schema.get(collection_name, {})
        new_collection_schema = new_schema.get(collection_name, {})
        # Take all fields to detect if they created, changed or dropped
        fields = old_collection_schema.keys() | new_collection_schema.keys()
        chain = []
        # Exclusive actions first
        registry = [a for a in actions_registry.values() if a.factory_exclusive] + \
                   [a for a in actions_registry.values() if not a.factory_exclusive]

        for action_cls in registry:
            for field in fields:
                if not issubclass(action_cls, BaseFieldAction):
                    continue
                action_obj = action_cls.build_object_if_applicable(collection_name,
                                                                   field,
                                                                   old_schema,
                                                                   new_schema)
                if action_obj is not None:
                    if action_obj.factory_exclusive:
                        old_schema = patch(action_obj.to_schema_patch(old_schema), old_schema)
                    chain.append(action_obj)

        return chain


class CollectionActionFactory(BaseActionFactory):
    """Factory of collection Actions
    """
    @staticmethod
    def get_actions_chain(collection_name: str,
                          old_schema: dict,
                          new_schema: dict) -> Iterable[BaseCollectionAction]:
        # Exclusive actions first
        registry = [a for a in actions_registry.values() if a.factory_exclusive] + \
                   [a for a in actions_registry.values() if not a.factory_exclusive]
        chain = []

        for action_cls in registry:
            if not issubclass(action_cls, BaseCollectionAction):
                continue
            action_obj = action_cls.build_object_if_applicable(collection_name,
                                                               old_schema,
                                                               new_schema)
            if action_obj is not None:
                if action_obj.factory_exclusive:
                    old_schema = patch(action_obj.to_schema_patch(old_schema), old_schema)
                chain.append(action_obj)

        return chain


def build_actions_chain(old_schema: dict, new_schema: dict) -> Iterable[BaseAction]:
    """
    Build full Action objects chain which suitable for such schema
    change.
    :param old_schema: current schema
    :param new_schema: schema collected from mongoengine models
    :return: iterable of Action objects
    """
    action_chain = []

    # Existed or dropped collections should be iterated before created
    # ones in order to avoid triggering CreateCollection action before
    # RenameCollection action
    all_collections = list(old_schema.keys()) + list(new_schema.keys() - old_schema.keys())
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
                # TODO: handle patch errors (if schema is corrupted)
                current_schema = patch(action_obj.to_schema_patch(current_schema), current_schema)
            action_chain.extend(new_actions)

    if new_schema != current_schema:
        from dictdiffer import diff
        print(list(diff(current_schema, new_schema)))
        # TODO: ability to force process without error
        raise ActionError('Could not reach current schema after applying whole Action chain. '
                          'This could be a problem in some Action which does not react to schema'
                          ' change it should react or produces wrong schema diff')

    return action_chain
