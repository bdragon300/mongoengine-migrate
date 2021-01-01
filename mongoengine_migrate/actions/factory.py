__all__ = [
    'build_actions_chain',
    'build_document_action_chain',
    'build_field_action_chain',
    'get_all_document_types'
]

import logging
from copy import copy
from typing import Iterable, Type

from dictdiffer import patch, diff

import mongoengine_migrate.flags as flags
from mongoengine_migrate.exceptions import ActionError
from mongoengine_migrate.schema import Schema
from .base import (
    actions_registry,
    BaseFieldAction,
    BaseDocumentAction,
    BaseIndexAction,
    BaseAction
)

log = logging.getLogger('mongoengine-migrate')


def build_actions_chain(left_schema: Schema, right_schema: Schema) -> Iterable[BaseAction]:
    """
    Build full Action objects chain which suitable for such schema
    change.
    :param left_schema: current schema
    :param right_schema: schema collected from mongoengine models
    :return: iterable of Action objects
    """
    action_chain = []

    # Actions registry sorted by priority
    registry = list(sorted(actions_registry.values(), key=lambda x: x.priority))

    left_schema = copy(left_schema)
    document_types = get_all_document_types(left_schema, right_schema)
    for action_cls in registry:
        if issubclass(action_cls, BaseDocumentAction):
            new_actions = list(
                build_document_action_chain(action_cls, left_schema, right_schema, document_types)
            )
        elif issubclass(action_cls, BaseFieldAction):
            new_actions = list(
                build_field_action_chain(action_cls, left_schema, right_schema, document_types)
            )
        elif issubclass(action_cls, BaseIndexAction):
            new_actions = list(
                build_index_action_chain(action_cls, left_schema, right_schema, document_types)
            )
        else:
            continue

        for action in new_actions:
            log.debug('> %s', action)
            try:
                left_schema = patch(action.to_schema_patch(left_schema), left_schema)
            except (TypeError, ValueError, KeyError) as e:
                raise ActionError(
                    f"Unable to apply schema patch of {action!r}. More likely that the "
                    f"schema is corrupted. You can use schema repair tools to fix this issue"
                ) from e
        action_chain.extend(new_actions)
        document_types = get_all_document_types(left_schema, right_schema)

    if right_schema != left_schema:
        log.error(
            'Schema is still not reached the target state after applying all actions. '
            'Changes left to make (diff): %s',
            list(diff(left_schema, right_schema))
        )
        raise ActionError('Could not reach target schema state after applying whole Action chain. '
                          'This could be a problem in some Action which does not process schema '
                          'properly or produces wrong schema diff. This is a programming error')

    return action_chain


def build_document_action_chain(action_cls: Type[BaseDocumentAction],
                                left_schema: Schema,
                                right_schema: Schema,
                                document_types: Iterable[str]) -> Iterable[BaseAction]:
    """
    Walk through schema changes, and produce chain of Action objects
    of given type which could handle schema changes from left to right
    :param action_cls: Action type to consider
    :param left_schema:
    :param right_schema:
    :param document_types: list of document types to inspect
    :return: iterable of suitable Action objects
    """
    for document_type in document_types:
        action_obj = action_cls.build_object(document_type, left_schema, right_schema)
        if action_obj is not None:
            try:
                left_schema = patch(action_obj.to_schema_patch(left_schema), left_schema)
            except (TypeError, ValueError, KeyError) as e:
                raise ActionError(
                    f"Unable to apply schema patch of {action_obj!r}. More likely that the "
                    f"schema is corrupted. You can use schema repair tools to fix this issue"
                ) from e

            yield action_obj


def build_field_action_chain(action_cls: Type[BaseFieldAction],
                             left_schema: Schema,
                             right_schema: Schema,
                             document_types: Iterable[str]) -> Iterable[BaseAction]:
    """
    Walk through schema changes, and produce chain of Action objects
    of given type which could handle schema changes from left to right
    :param action_cls: Action type to consider
    :param left_schema:
    :param right_schema:
    :param document_types: list of document types to inspect
    :return: iterable of suitable Action objects
    """
    for document_type in document_types:
        # Take all fields to detect if they created, changed or dropped
        all_fields = left_schema.get(document_type, {}).keys() | \
                     right_schema.get(document_type, {}).keys()
        for field in all_fields:
            action_obj = action_cls.build_object(document_type,
                                                 field,
                                                 left_schema,
                                                 right_schema)
            if action_obj is not None:
                try:
                    left_schema = patch(action_obj.to_schema_patch(left_schema), left_schema)
                except (TypeError, ValueError, KeyError) as e:
                    raise ActionError(
                        f"Unable to apply schema patch of {action_obj!r}. More likely that the "
                        f"schema is corrupted. You can use schema repair tools to fix this issue"
                    ) from e

                yield action_obj


def build_index_action_chain(action_cls: Type[BaseIndexAction],
                             left_schema: Schema,
                             right_schema: Schema,
                             document_types: Iterable[str]) -> Iterable[BaseAction]:
    """
    Walk through schema changes, and produce chain of Action objects
    of given type which could handle schema changes from left to right
    :param action_cls: Action type to consider
    :param left_schema:
    :param right_schema:
    :param document_types: list of document types to inspect
    :return: iterable of suitable Action objects
    """
    for document_type in document_types:
        # Take all indexes to detect if they created, altered, dropped
        all_indexes = left_schema.get(document_type, Schema.Document()).indexes.keys() | \
                      right_schema.get(document_type, Schema.Document()).indexes.keys()
        for index_name in all_indexes:
            action_obj = action_cls.build_object(document_type,
                                                 index_name,
                                                 left_schema,
                                                 right_schema)
            if action_obj is not None:
                try:
                    left_schema = patch(action_obj.to_schema_patch(left_schema), left_schema)
                except (TypeError, ValueError, KeyError) as e:
                    raise ActionError(
                        f"Unable to apply schema patch of {action_obj!r}. More likely that the "
                        f"schema is corrupted. You can use schema repair tools to fix this issue"
                    ) from e

                yield action_obj


def get_all_document_types(left_schema: Schema, right_schema: Schema) -> Iterable[str]:
    """
    Return list of all document types collected from both schemas

    Document types should be processed in certain order depending
    if they are embedded or not, inherited or not. Embedded documents
    should be processed before common documents because their fields
    could refer to embedded documents which must be present in schema
    at the moment when migration of document is running. Also base
    document (common or embedded) should be processed before derived one

    So, common order is:

    1. Embedded documents
    2. Derived embedded documents ordered by their hierarchy depth
    3. Documents
    4. Derived documents ordered by their hierarchy depth

    :param left_schema:
    :param right_schema:
    :return:
    """
    def mark(document_type: str) -> int:
        # 0 -- embedded documents
        # 0 + derived depth -- embedded documents
        # 1000 -- documents
        # 1000 + derived_depth -- documents
        m = 0
        if not document_type.startswith(flags.EMBEDDED_DOCUMENT_NAME_PREFIX):
            m = 1000

        m += document_type.count(flags.DOCUMENT_NAME_SEPARATOR)

        return m

    all_document_types = ((k, mark(k)) for k in left_schema.keys() | right_schema.keys())
    all_document_types = sorted(all_document_types, key=lambda x: x[1])

    return [k[0] for k in all_document_types]
