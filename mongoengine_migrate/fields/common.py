__all__ = [
    'NumberFieldHandler',
    'StringFieldHandler',
    'URLFieldHandler',
    'EmailFieldHandler',
    'DecimalFieldHandler',
    'ComplexDateTimeFieldHandler',
    'ListFieldHandler',
    'DictFieldHandler',
    'BinaryFieldHandler',
    'SequenceFieldHandler',
    'UUIDFieldHandler',
    'ReferenceFieldHandler',
    'CachedReferenceFieldHandler',
    'FileFieldHandler',
    'ImageFieldHandler',
    'EmbeddedDocumentFieldHandler',
    'EmbeddedDocumentListFieldHandler'
]

import re
from typing import Type, Collection, Union

import bson
import mongoengine.fields

from mongoengine_migrate.exceptions import SchemaError, InconsistencyError
from mongoengine_migrate.mongo import (
    check_empty_result,
    mongo_version
)
from ..updater import ByPathContext, ByDocContext, DocumentUpdater
from mongoengine_migrate.utils import get_document_type, Diff, UNSET
from .base import CommonFieldHandler
from .converters import to_string, to_decimal


class NumberFieldHandler(CommonFieldHandler):
    field_classes = [
        mongoengine.fields.IntField,
        mongoengine.fields.LongField,
        mongoengine.fields.FloatField,
    ]
    schema_skel_keys = {'min_value', 'max_value'}

    def change_min_value(self, updater: DocumentUpdater, diff: Diff):
        """
        Change min_value of field. Force set to minimum if value is
        less than limitation (if any)
        """
        def by_path(ctx: ByPathContext):
            ctx.collection.update_many(
                {ctx.filter_dotpath: {'$lt': diff.new}, **ctx.extra_filter},
                {'$set': {ctx.update_dotpath: diff.new}},
                array_filters=ctx.build_array_filters()
            )

        self._check_diff(updater, diff, True, (int, float))
        if diff.new in (UNSET, None):
            return

        updater.update_by_path(by_path)

    def change_max_value(self, updater: DocumentUpdater, diff: Diff):
        """
        Change max_value of field. Force set to maximum if value is
        more than limitation (if any)
        """
        def by_path(ctx: ByPathContext):
            ctx.collection.update_many(
                {ctx.filter_dotpath: {'$gt': diff.new}, **ctx.extra_filter},
                {'$set': {ctx.update_dotpath: diff.new}},
                array_filters=ctx.build_array_filters()
            )

        self._check_diff(updater, diff, True, (int, float))
        if diff.new in (UNSET, None):
            return

        updater.update_by_path(by_path)


class StringFieldHandler(CommonFieldHandler):
    field_classes = [
        mongoengine.fields.StringField,
    ]

    schema_skel_keys = {'max_length', 'min_length', 'regex'}

    def change_max_length(self, updater: DocumentUpdater, diff: Diff):
        """Cut off a string if it longer than limitation (if any)"""
        def by_doc(ctx: ByDocContext):
            doc = ctx.document
            match = updater.field_name in doc and len(doc[updater.field_name]) > diff.new
            if match:
                doc[updater.field_name] = doc[updater.field_name][:diff.new]

        self._check_diff(updater, diff, True, int)
        if diff.new in (UNSET, None):
            return
        if diff.new < 0:
            diff.new = 0

        # Cut too long strings
        updater.update_by_document(by_doc)

    @mongo_version(min_version='3.6')
    def change_min_length(self, updater: DocumentUpdater, diff: Diff):
        """Raise error if string is shorter than limitation (if any)"""
        def by_path(ctx: ByPathContext):
            fltr = {
                ctx.filter_dotpath: {'$ne': None},
                **ctx.extra_filter,
                "$expr": {"$lt": [{"$strLenCP": f"${ctx.filter_dotpath}"}, diff.new]},  # >= 3.6
            }
            check_empty_result(ctx.collection, ctx.filter_dotpath, fltr)

        def by_doc(ctx: ByDocContext):
            doc = ctx.document
            val = doc.get(updater.field_name)
            if isinstance(val, str) and len(val) < diff.new:
                raise InconsistencyError(
                    f"String field {ctx.collection.name}.{ctx.filter_dotpath} on one of "
                    f"records has length less than minimum: {doc}"
                )

        self._check_diff(updater, diff, True, int)
        if diff.new in (UNSET, None):
            return
        if diff.new < 0:
            diff.new = 0

        # We can't to increase string length, so raise error if
        # there was found strings which are shorter than should be
        if self.migration_policy.name == 'strict':
            updater.update_combined(by_path, by_doc, False)

    def change_regex(self, updater: DocumentUpdater, diff: Diff):
        """Raise error if string does not match regex (if any)"""
        def by_path(ctx: ByPathContext):
            fltr = {
                ctx.filter_dotpath: {'$not': re.compile(diff.new), '$ne': None, '$exists': True},
                **ctx.extra_filter
            }
            check_empty_result(ctx.collection, ctx.filter_dotpath, fltr)

        self._check_diff(updater, diff, True, (str, type(re.compile('.'))))
        if diff.new in (UNSET, None):
            return

        if self.migration_policy.name == 'strict':
            updater.update_by_path(by_path)


class URLFieldHandler(StringFieldHandler):
    field_classes = [
        mongoengine.fields.URLField,
    ]

    schema_skel_keys = {'schemes'}  # TODO: implement url_regex

    def change_schemes(self, updater: DocumentUpdater, diff: Diff):
        """Raise error if url has scheme not from list"""
        def by_path(ctx: ByPathContext):
            fltr = {ctx.filter_dotpath: {'$not': scheme_regex, '$ne': None}, **ctx.extra_filter}
            check_empty_result(ctx.collection, ctx.filter_dotpath, fltr)

        self._check_diff(updater, diff, False, Collection)
        if not diff.new or diff.new == UNSET:
            return

        # Check if some records contains non-url values in db_field
        scheme_regex = re.compile(rf'\A(?:({"|".join(re.escape(x) for x in diff.new)}))://')
        if self.migration_policy.name == 'strict':
            updater.update_by_path(by_path)


class EmailFieldHandler(StringFieldHandler):
    field_classes = [
        mongoengine.fields.EmailField
    ]

    schema_skel_keys = {'domain_whitelist', 'allow_utf8_user', 'allow_ip_domain'}

    USER_REGEX = re.compile(
        # `dot-atom` defined in RFC 5322 Section 3.2.3.
        r"(\A[-!#$%&'*+/=?^_`{}|~0-9A-Z]+(\.[-!#$%&'*+/=?^_`{}|~0-9A-Z]+)*@.+\Z"
        # `quoted-string` defined in RFC 5322 Section 3.2.4.
        r'|\A"([\001-\010\013\014\016-\037!#-\[\]-\177]|\\[\001-\011\013\014\016-\177])*"@.+\Z)',
        re.IGNORECASE,
    )

    UTF8_USER_REGEX = re.compile(
        # RFC 6531 Section 3.3 extends `atext` (used by dot-atom) to
        # include `UTF8-non-ascii`.
        r"(\A[-!#$%&'*+/=?^_`{}|~0-9A-Z\u0080-\U0010FFFF]+(\.[-!#$%&'*+/=?^_`{}|~0-9A-Z\u0080-\U0010FFFF]+)*@.+\Z"
        # `quoted-string`
        r'|\A"([\001-\010\013\014\016-\037!#-\[\]-\177]|\\[\001-\011\013\014\016-\177])*"@.+\Z)',
        re.IGNORECASE | re.UNICODE,
    )

    IP_DOMAIN_REGEX = re.compile(
        # ipv6
        r'\A[^@]+@\[(::)?([A-F0-9]{1,4}::?){0,7}([A-F0-9]{1,4})?\]\Z'
        # ipv4
        r'|\A[^@]+@\[\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\]\Z'
    )

    DOMAIN_REGEX = re.compile(
        r"\A[^@]+@((?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+)(?:[A-Z0-9-]{2,63}(?<!-))\Z",
        re.IGNORECASE,
    )

    def change_domain_whitelist(self, updater: DocumentUpdater, diff: Diff):
        """
        `domain_whitelist` parameter is affected to domain validation:
        if domain in email address is in that list then validation will
        be skipped. So, do nothing here
        """

    def change_allow_utf8_user(self, updater: DocumentUpdater, diff: Diff):
        """Raise error if email address has wrong user name"""
        def by_path(ctx: ByPathContext):
            # Find records which doesn't match to the regex
            fltr = {ctx.filter_dotpath: {'$not': regex, '$ne': None}, **ctx.extra_filter}
            check_empty_result(ctx.collection, ctx.filter_dotpath, fltr)

        self._check_diff(updater, diff, False, bool)
        if diff.new == UNSET:
            return

        regex = self.UTF8_USER_REGEX if diff.new is True else self.USER_REGEX
        if self.migration_policy.name == 'strict':
            updater.update_by_path(by_path)

    def change_allow_ip_domain(self, updater: DocumentUpdater, diff: Diff):
        """
        Raise error if email has domain which not in `domain_whitelist`
        when `allow_ip_domain` is True. Otherwise do nothing
        """
        def by_path(ctx: ByPathContext):
            # Find records with ip domains and raise error if found
            fltr = {"$and": [
                {ctx.filter_dotpath: {'$ne': None}},
                {ctx.filter_dotpath: self.IP_DOMAIN_REGEX},
                {ctx.filter_dotpath: {'$not': re.compile(rf'\A[^@]+@({whitelist_regex})\Z')}},
                *[{k: v} for k, v in ctx.extra_filter.items()]
            ]}
            check_empty_result(ctx.collection, ctx.filter_dotpath, fltr)

        self._check_diff(updater, diff, False, bool)
        if diff.new is True or diff.new == UNSET:
            return

        whitelist_regex = '|'.join(
            re.escape(x) for x in self.left_field_schema.get('domain_whitelist', [])
        ) or '.*'
        if self.migration_policy.name == 'strict':
            updater.update_by_path(by_path)

    def convert_type(self,
                     updater: DocumentUpdater,
                     from_field_cls: Type[mongoengine.fields.BaseField],
                     to_field_cls: Type[mongoengine.fields.BaseField]):
        def by_path(ctx: ByPathContext):
            fltr = {'$and': [
                {ctx.filter_dotpath: {'$ne': None}},
                {ctx.filter_dotpath: {'$not': self.DOMAIN_REGEX}},
                {ctx.filter_dotpath: {'$not': self.IP_DOMAIN_REGEX}},
                {ctx.filter_dotpath: {'$not': re.compile(rf'\A[^@]+@({whitelist_regex})\Z')}},
                *[{k: v} for k, v in ctx.extra_filter.items()],
            ]}
            check_empty_result(ctx.collection, ctx.filter_dotpath, fltr)

        to_string(updater)

        # Find records with ip domains and raise error if found
        whitelist_regex = '|'.join(
            re.escape(x) for x in self.left_field_schema.get('domain_whitelist', [])
        ) or '.*'
        if self.migration_policy.name == 'strict':
            updater.update_by_path(by_path)


class DecimalFieldHandler(NumberFieldHandler):
    field_classes = [mongoengine.fields.DecimalField]

    schema_skel_keys = {'force_string', 'precision', 'rounding'}

    def change_force_string(self, updater: DocumentUpdater, diff: Diff):
        """
        Convert to string or decimal depending on `force_string` flag
        """
        self._check_diff(updater, diff, False, bool)
        if diff.new == UNSET:
            return

        if diff.new is True:
            to_string(updater)
        else:
            to_decimal(updater)

    def change_precision(self, updater: DocumentUpdater, diff: Diff):
        """This one is related only for python. Nothing to do"""
        pass

    def change_rounding(self, updater: DocumentUpdater, diff: Diff):
        """This one is related only for python. Nothing to do"""
        pass

    def convert_type(self,
                     updater: DocumentUpdater,
                     from_field_cls: Type[mongoengine.fields.BaseField],
                     to_field_cls: Type[mongoengine.fields.BaseField]):
        if self.left_field_schema.get('force_string', True):
            to_string(updater)
        else:
            to_decimal(updater)


class ComplexDateTimeFieldHandler(StringFieldHandler):
    field_classes = [mongoengine.fields.ComplexDateTimeField]

    schema_skel_keys = {'separator'}

    def change_separator(self, updater: DocumentUpdater, diff: Diff):
        """Change separator in datetime strings"""
        def by_doc(ctx: ByDocContext):
            doc = ctx.document
            if updater.field_name in doc:
                doc[updater.field_name] = doc[updater.field_name].replace(diff.old, diff.new)

        self._check_diff(updater, diff, False, str)
        if not diff.new or not diff.old:
            raise SchemaError(f'{updater.document_type}{updater.field_name}.separator '
                              f'must not be empty')
        if diff.new == UNSET:
            return

        updater.update_by_document(by_doc)


class ListFieldHandler(CommonFieldHandler):
    field_classes = [
        mongoengine.fields.ListField
    ]

    schema_skel_keys = set()  # TODO: implement "field"

    @classmethod
    def schema_skel(cls) -> dict:
        """
        Return db schema skeleton dict, which contains keys taken from
        `schema_skel_keys` and Nones as values
        """
        skel = super(ListFieldHandler, cls).schema_skel()

        # Add optional keys to skel if they are set in field object
        # * `max_length` was added in mongoengine 0.19.0
        optional_keys = {'max_length'}
        assert all(issubclass(c, mongoengine.fields.ListField) for c in cls.field_classes), \
            'If you wanna add field class then this code should be rewritten'
        field_obj = mongoengine.fields.ListField()
        skel.update({k: None for k in optional_keys if hasattr(field_obj, k)})

        return skel

    def change_max_length(self, updater: DocumentUpdater, diff: Diff):
        """Cut off a list if it longer than limitation (if any)"""
        def by_doc(ctx: ByDocContext):
            doc = ctx.document
            match = updater.field_name in doc and len(doc[updater.field_name]) > diff.new
            if match:
                doc[updater.field_name] = doc[updater.field_name][:diff.new]

        self._check_diff(updater, diff, True, int)
        if diff.new in (UNSET, None):
            return

        updater.update_by_document(by_doc)


class DictFieldHandler(CommonFieldHandler):
    pass  # TODO: implement "field" param


class BinaryFieldHandler(CommonFieldHandler):
    field_classes = [
        mongoengine.fields.BinaryField
    ]

    schema_skel_keys = {'max_bytes'}

    def change_max_bytes(self, updater: DocumentUpdater, diff: Diff):
        """
        $binarySize expression is not available in MongoDB yet,
        so do nothing
        """
        # TODO: add python update
        self._check_diff(updater, diff, True, int)
        if diff.new in (UNSET, None):
            return

        pass


class SequenceFieldHandler(CommonFieldHandler):
    field_classes = [
        mongoengine.fields.SequenceField
    ]

    # TODO: warning on using non-default value_decorator
    # TODO: db_alias
    schema_skel_keys = {'collection_name', 'sequence_name'}

    def change_collection_name(self, updater: DocumentUpdater, diff: Diff):
        """Typically changing the collection name should not require
        to do any changes
        """
        self._check_diff(updater, diff, False, str)
        pass

    def change_sequence_name(self, updater: DocumentUpdater, diff: Diff):
        """Typically changing the sequence name should not require
        to do any changes
        """
        self._check_diff(updater, diff, False, str)
        pass


class UUIDFieldHandler(CommonFieldHandler):
    field_classes = [
        mongoengine.fields.UUIDField
    ]

    schema_skel_keys = {'binary'}

    def change_binary(self, updater: DocumentUpdater, diff: Diff):
        self._check_diff(updater, diff, False, bool)

        if diff.new is True:
            pass
            # TODO: convert to bson Binary
        else:
            pass
            # TODO: convert Binary to string


class ReferenceFieldHandler(CommonFieldHandler):
    field_classes = [
        mongoengine.fields.ReferenceField,
        mongoengine.fields.LazyReferenceField
    ]

    schema_skel_keys = {'target_doctype', 'dbref'}

    def change_target_doctype(self, updater: DocumentUpdater, diff: Diff):
        """Collection could not exist in db, so do nothing"""
        self._check_diff(updater, diff, False, str)

    def change_dbref(self, updater: DocumentUpdater, diff: Diff):
        """Change reference storing format: ObjectId or DBRef"""
        self._check_diff(updater, diff, False, bool)

        if diff.new is True:
            self._objectid_to_dbref(updater)
        else:
            self._dbref_to_objectid(updater)

    def _objectid_to_dbref(self, updater: DocumentUpdater):
        def by_doc(ctx: ByDocContext):
            doc = ctx.document
            if isinstance(doc.get(updater.field_name), bson.ObjectId):
                doc[updater.field_name] = bson.DBRef(ctx.collection.name, doc[updater.field_name])

        updater.update_by_document(by_doc)

    def _dbref_to_objectid(self, updater: DocumentUpdater):
        def by_doc(ctx: ByDocContext):
            doc = ctx.document
            if isinstance(doc.get(updater.field_name), bson.DBRef):
                doc[updater.field_name] = doc[updater.field_name].id

        updater.update_by_document(by_doc)

    @classmethod
    def build_schema(
            cls,
            field_obj: Union[
                mongoengine.fields.ReferenceField,
                mongoengine.fields.LazyReferenceField
            ]) -> dict:
        schema = super(ReferenceFieldHandler, cls).build_schema(field_obj)

        # 'document_type' is restricted to use Document class
        # as value by mongoengine itself
        document_type_cls = field_obj.document_type
        schema['target_doctype'] = get_document_type(document_type_cls)

        return schema


class CachedReferenceFieldHandler(CommonFieldHandler):
    field_classes = [
        mongoengine.fields.CachedReferenceField
    ]

    schema_skel_keys = {'target_doctype', 'fields'}

    def change_target_doctype(self, updater: DocumentUpdater, diff: Diff):
        """Collection could not exist in db, so do nothing"""
        self._check_diff(updater, diff, False, str)

    def change_fields(self, updater: DocumentUpdater, diff: Diff):
        def by_doc(ctx: ByDocContext):
            if updater.field_name in ctx.document:
                if isinstance(ctx.document[updater.field_name], dict):
                    ctx.document[updater.field_name] = {
                        k: v for k, v in ctx.document[updater.field_name].items()
                        if k in keep_fields
                    }

        self._check_diff(updater, diff, False, (list, tuple))

        if diff.new:
            keep_fields = set(diff.new)
            if not updater.is_embedded:
                keep_fields.add('_id')
            updater.update_by_document(by_doc)

    @classmethod
    def build_schema(
            cls,
            field_obj: Union[
                mongoengine.fields.ReferenceField,
                mongoengine.fields.LazyReferenceField
            ]) -> dict:
        schema = super(CachedReferenceFieldHandler, cls).build_schema(field_obj)

        # 'document_type' is restricted to use Document class
        # as value by mongoengine itself
        document_type_cls = field_obj.document_type
        schema['target_doctype'] = get_document_type(document_type_cls)

        return schema


class FileFieldHandler(CommonFieldHandler):
    field_classes = [
        mongoengine.fields.FileField
    ]

    # TODO: db_alias
    schema_skel_keys = {'collection_name'}

    def change_collection_name(self, updater: DocumentUpdater, diff: Diff):
        """Typically changing the collection name should not require
        to do any changes
        """
        self._check_diff(updater, diff, False, str)
        pass


class ImageFieldHandler(FileFieldHandler):
    field_classes = [
        mongoengine.fields.ImageField
    ]

    schema_skel_keys = {'size', 'thumbnail_size'}

    def change_thumbnail_size(self, updater: DocumentUpdater, diff: Diff):
        """Typically changing the attribute should not require
        to do any changes
        """
        self._check_diff(updater, diff, False, (list, tuple))
        pass

    def change_size(self, updater: DocumentUpdater, diff: Diff):
        """Typically changing the attribute should not require
        to do any changes
        """
        self._check_diff(updater, diff, False, (list, tuple))
        pass


class EmbeddedDocumentFieldHandler(CommonFieldHandler):
    field_classes = [
        mongoengine.fields.EmbeddedDocumentField
    ]

    schema_skel_keys = {'target_doctype'}

    def change_target_doctype(self, updater: DocumentUpdater, diff: Diff):
        self._check_diff(updater, diff, False, str)
        # FIXME: decide what to do with existed embedded docs?

    @classmethod
    def build_schema(cls, field_obj: mongoengine.fields.EmbeddedDocumentField) -> dict:
        schema = super(EmbeddedDocumentFieldHandler, cls).build_schema(field_obj)

        document_type_cls = field_obj.document_type
        schema['target_doctype'] = get_document_type(document_type_cls)

        return schema


class EmbeddedDocumentListFieldHandler(ListFieldHandler):
    field_classes = [
        mongoengine.fields.EmbeddedDocumentListField
    ]

    # FIXME: move 'target_doctype' changing from here to ListField 'db_field'
    #        this require to fix embedded document recursive walk method
    #        to also consider field.document_type of ListField
    #        This will help to manage with situation when ListField
    #        with EmbeddedDocumentField is defined by user
    schema_skel_keys = {'target_doctype'}

    def change_target_doctype(self, updater: DocumentUpdater, diff: Diff):
        self._check_diff(updater, diff, False, str)
        # FIXME: decide what to do with existed embedded docs?

    @classmethod
    def build_schema(cls, field_obj: mongoengine.fields.EmbeddedDocumentListField) -> dict:
        schema = super(EmbeddedDocumentListFieldHandler, cls).build_schema(field_obj)

        document_type_cls = field_obj.field.document_type
        schema['target_doctype'] = get_document_type(document_type_cls)

        return schema
