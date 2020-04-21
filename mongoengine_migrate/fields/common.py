import re
from typing import List, Dict, Any, Type, Collection

import mongoengine.fields

from mongoengine_migrate.exceptions import MigrationError
from .base import CommonFieldType
from ..actions.diff import AlterDiff, UNSET
from .converters import to_string, to_decimal


class NumberFieldType(CommonFieldType):
    mongoengine_field_classes = [
        mongoengine.fields.IntField,
        mongoengine.fields.LongField,
        mongoengine.fields.FloatField,
    ]
    schema_skel_keys = {'min_value', 'max_value'}

    def change_min_value(self, diff: AlterDiff):
        self._check_diff(diff, True, True, (int, float))
        if diff.new in (UNSET, None):
            return
        # FIXME: treat None in diff old/new as value absence. Here and further
        self.collection.update_many(
            {self.db_field: {'$lt': diff.new}},
            {'$set': {self.db_field: diff.new}}
        )
        # # FIXME: make replacing on error only here and further
        # if diff.error_policy == 'replace':
        #     if diff.default < diff.new:
        #         raise MigrationError(f'Cannot set min_value for '
        #                              f'{self.collection.name}.{self.db_field} because default value'
        #                              f'{diff.default} is less than min_value')
        #     self.collection.update_many(
        #         {self.db_field: {'$lt': diff.new}},
        #         {'$set': {self.db_field: diff.default}}
        #     )

    def change_max_value(self, diff: AlterDiff):
        self._check_diff(diff, True, True, (int, float))
        if diff.new in (UNSET, None):
            return

        self.collection.update_many(
            {self.db_field: {'$gt': diff.new}},
            {'$set': {self.db_field: diff.new}}
        )

        # if diff.error_policy == 'replace':
        #     if diff.default < diff.new:
        #         raise MigrationError(f'Cannot set max_value for '
        #                              f'{self.collection.name}.{self.db_field} because default value'
        #                              f'{diff.default} is greater than max_value')
        #     self.collection.update_many(
        #         {self.db_field: {'$gt': diff.new}},
        #         {'$set': {self.db_field: diff.default}}
        #     )


class StringFieldType(CommonFieldType):
    mongoengine_field_classes = [
        mongoengine.fields.StringField,
    ]

    schema_skel_keys = {'max_length', 'min_length', 'regex'}

    def change_max_length(self, diff: AlterDiff):
        self._check_diff(diff, True, True, int)
        if diff.new in (UNSET, None):
            return
        if diff.new < 0:
            diff.new = 0

        # Cut too long strings
        self.collection.aggregate([
            {'$match': {
                self.db_field: {"$ne": None},
                "$expr": {"$gt": [{"$strLenCP": f"${self.db_field}"}, diff.new]},
            }},
            {'$addFields': {
                self.db_field: {"$substr": [f'${self.db_field}', 0, diff.new]}
            }},
            {'$out': self.collection.name}
        ])

        # if diff.error_policy == 'replace':
        #     if diff.default is not None and len(diff.default) > diff.new:
        #         raise MigrationError(f'Cannot set default value for '
        #                              f'{self.collection.name}.{self.db_field} because default value'
        #                              f'{diff.default} is longer than max_length')
        #     replace_pipeline = [
        #         {'$addFields': {self.db_field: diff.default}},
        #         {'$out': self.collection.name}
        #     ]
        #     self.collection.aggregate(pipeline + replace_pipeline)

    def change_min_length(self, diff: AlterDiff):
        self._check_diff(diff, True, True, int)
        if diff.new in (UNSET, None):
            return
        if diff.new < 0:
            diff.new = 0

        res = self.collection.find({
            self.db_field: {'$ne': None},
            "$where": f"this.{self.db_field}.length < {diff.new}"
        })
        # We can't to increase string length, so raise error if
        # there was found strings which are shorter than should be
        if res.retrieved > 0:
            raise MigrationError(f'Cannot migrate min_length for '
                                 f'{self.collection.name}.{self.db_field} because '
                                 f'{res.retrieved} documents are less than min_length')
        # if diff.error_policy == 'replace':
        #     if diff.default is not None and len(diff.default) < diff.new:
        #         raise MigrationError(f"Cannot set min_length for "
        #                              f"{self.collection.name}.{self.db_field} because default value"
        #                              f"'{diff.default}' is shorter than min_length")
        #     replace_pipeline = [
        #         {'$addFields': {self.db_field: diff.default}},
        #         {'$out': self.collection.name}
        #     ]
        #     self.collection.aggregate(pipeline + replace_pipeline)

    def change_regex(self, diff: AlterDiff):
        self._check_diff(diff, True, True, (str, type(re.compile('.'))))
        if diff.new in (UNSET, None):
            return

        wrong_count = self.collection.find(
            {self.db_field: {
                '$not': re.compile(diff.new),
                '$ne': None
            }}
        ).retrieved
        if wrong_count > 0:
            raise MigrationError(f'Cannot migrate regex for '
                                 f'{self.collection.name}.{self.db_field} because '
                                 f'{wrong_count} documents do not match this regex')

        # if diff.error_policy == 'replace':
        #     matched = re.match(diff.new, diff.default)
        #     if not matched:
        #         raise MigrationError(f"Cannot set regex for "
        #                              f"{self.collection.name}.{self.db_field} because default value"
        #                              f"'{diff.default}' does not match that regex")
        #     self.collection.update_many({self.db_field: {'$not': {'$regex': diff.new}}},
        #                                 {'$set': {self.db_field: diff.new}})


class URLFieldType(StringFieldType):
    mongoengine_field_classes = [
        mongoengine.fields.URLField,
    ]

    schema_skel_keys = {'schemes'}  # TODO: implement url_regex

    def change_schemes(self, diff: AlterDiff):
        self._check_diff(diff, True, False, Collection)
        if not diff.new or diff.new == UNSET:
            return

        # Check if some records contains non-url values in db_field
        scheme_regex = re.compile(rf'\A(?:({"|".join(re.escape(x) for x in diff.new)}))://')
        bad_records = self.collection.find(
            {self.db_field: {
                '$not': scheme_regex,
                '$ne': None
            }},
            limit=3
        )
        if bad_records.retrieved:  # FIXME: Remove such copypaste from everywhere around
            examples = (
                f'{{_id: {x.get("_id", "unknown")},...{self.db_field}: ' \
                f'{x.get(self.db_field, "unknown")}}}'
                for x in bad_records
            )
            raise MigrationError(f"Some of records in {self.collection.name}.{self.db_field} "
                                 f"contain schemes not from list. This cannot be converted. "
                                 f"First several examples {','.join(examples)}")

    def convert_type(self,
                     from_field_cls: Type[mongoengine.fields.BaseField],
                     to_field_cls: Type[mongoengine.fields.BaseField]):
        to_string(self.collection, self.db_field)

        url_regex = re.compile(
            r"\A[A-Z]{3,}://[A-Z0-9\-._~:/?#\[\]@!$&'()*+,;%=]\Z",
            re.IGNORECASE
        )
        bad_records = self.collection.find(
            {self.db_field: {
                '$not': url_regex,
                '$ne': None
            }},
            limit=3
        )
        if bad_records.retrieved:
            examples = (
                f'{{_id: {x.get("_id", "unknown")},...{self.db_field}: ' \
                f'{x.get(self.db_field, "unknown")}}}'
                for x in bad_records
            )
            raise MigrationError(f"Some of records in {self.collection.name}.{self.db_field} "
                                 f"contain non-url values. First several examples "
                                 f"{','.join(examples)}")


class EmailFieldType(StringFieldType):
    mongoengine_field_classes = [
        mongoengine.fields.EmailField  # TODO: implement checks
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

    def change_domain_whitelist(self, diff: AlterDiff):
        """
        `domain_whitelist` parameter is affected to domain validation:
        if domain in email address is in that list then validation will
        be skipped. So, do nothing here
        """

    def change_allow_utf8_user(self, diff: AlterDiff):
        self._check_diff(diff, True, False, bool)
        if diff.new == UNSET:
            return

        regex = self.UTF8_USER_REGEX if diff.new is True else self.USER_REGEX

        # Find records which doesn't match to the regex
        wrong_count = self.collection.find(
            {self.db_field: {
                '$not': regex,
                '$ne': None
            }}
        ).retrieved
        if wrong_count > 0:
            raise MigrationError(f'Cannot change allow_utf8_user for '
                                 f'{self.collection.name}.{self.db_field} because '
                                 f'{wrong_count} documents contain bad email addresses')

    def change_allow_ip_domain(self, diff: AlterDiff):
        self._check_diff(diff, True, False, bool)
        if diff.new is True or diff.new == UNSET:
            return

        # Find records with ip domains and raise error if found
        whitelist_regex = '|'.join(
            re.escape(x) for x in self.field_schema.get('domain_whitelist', [])
        ) or '.*'
        wrong_count = self.collection.find(
            {"$and": [
                {self.db_field: {'$ne': None}},
                {self.db_field: self.IP_DOMAIN_REGEX},
                {self.db_field: {'$not': re.compile(rf'\A[^@]+@({whitelist_regex})\Z')}}
            ]}
        ).retrieved
        if wrong_count > 0:
            raise MigrationError(f'Cannot change allow_ip_domain for '
                                 f'{self.collection.name}.{self.db_field} because '
                                 f'{wrong_count} documents contain bad email addresses')

    def convert_type(self,
                     from_field_cls: Type[mongoengine.fields.BaseField],
                     to_field_cls: Type[mongoengine.fields.BaseField]):
        to_string(self.collection, self.db_field)

        # Find records with ip domains and raise error if found
        whitelist_regex = '|'.join(
            re.escape(x) for x in self.field_schema.get('domain_whitelist', [])
        ) or '.*'
        wrong_count = self.collection.find(
            {'$and': [
                {self.db_field: {'$ne': None}},
                {self.db_field: {'$not': self.DOMAIN_REGEX}},
                {self.db_field: {'$not': self.IP_DOMAIN_REGEX}},
                {self.db_field: {'$not': re.compile(rf'\A[^@]+@({whitelist_regex})\Z')}}
            ]}
        ).retrieved
        if wrong_count > 0:
            raise MigrationError(f'Cannot migrate field {self.collection.name}.{self.db_field}'
                                 f'from {from_field_cls} to {to_field_cls} because '
                                 f'{wrong_count} documents contain bad email addresses')


class DecimalFieldType(NumberFieldType):
    mongoengine_field_classes = [mongoengine.fields.DecimalField]

    schema_skel_keys = {'force_string', 'precision', 'rounding'}

    def change_force_string(self, diff: AlterDiff):
        self._check_diff(diff, True, False, bool)
        if diff.new == UNSET:
            return

        if diff.new is True:
            to_string(self.collection, self.db_field)
        else:
            to_decimal(self.collection, self.db_field)

        # TODO: implement 'replace'

    def change_precision(self, diff: AlterDiff):
        """This one is related only for python. Nothing to do"""
        pass

    def change_rounding(self, diff: AlterDiff):
        """This one is related only for python. Nothing to do"""
        pass

    def convert_type(self,
                     from_field_cls: Type[mongoengine.fields.BaseField],
                     to_field_cls: Type[mongoengine.fields.BaseField]):
        if self.field_schema.get('force_string', True):
            to_string(self.collection, self.db_field)
        else:
            to_decimal(self.collection, self.db_field)


class ComplexDateTimeFieldType(StringFieldType):
    mongoengine_field_classes = [mongoengine.fields.ComplexDateTimeField]

    schema_skel_keys = {'separator'}

    def change_separator(self, diff: AlterDiff):
        self._check_diff(diff, True, False, str)
        if not diff.new or not diff.old:
            raise MigrationError('Empty separator specified')
        if diff.new == UNSET:
            return

        old_sep = re.escape(diff.old)
        old_regex = r'\A' + str(old_sep.join([r"\d{4}"] + [r"\d{2}"] * 5 + [r"\d{6}"])) + r'\Z'

        self.collection.aggregate([
            {'$match': {
                '$and': [
                    {self.db_field: re.compile(old_regex)},
                    {self.db_field: {"$ne": None}}
                ]
            }},
            {'$addFields': {
                self.db_field: {
                    '$reduce': {
                        'input': {'$split': [f'${self.db_field}', diff.old]},
                        'initialValue': '',
                        'in': {'$concat': ['$$value', diff.new, '$$this']}
                    }
                }
            }},
            {'$addFields': {
                self.db_field: {"$substr": [f'${self.db_field}', 1, -1]}
            }},
            {'$out': self.collection.name}
        ])

        # if diff.error_policy == 'replace':
        #     self.collection.update_many({self.db_field: {'$not': {'$regex': old_format}}},
        #                                 {'$set': {self.db_field: diff.default}})

    def convert_type(self,
                     from_field_cls: Type[mongoengine.fields.BaseField],
                     to_field_cls: Type[mongoengine.fields.BaseField]):
        # We should not know which separator is used, so use '.+'
        # Separator change is handled by appropriate method
        regex = r'\A' + str('.+'.join([r"\d{4}"] + [r"\d{2}"] * 5 + [r"\d{6}"])) + r'\Z'
        bad_records = self.collection.find(
            {self.db_field: {
                '$not': regex,
                '$ne': None
            }},
            limit=3
        )
        if bad_records.retrieved:
            examples = (
                f'{{_id: {x.get("_id", "unknown")},...{self.db_field}: ' \
                f'{x.get(self.db_field, "unknown")}}}'
                for x in bad_records
            )
            raise MigrationError(f"Some of records in {self.collection.name}.{self.db_field} "
                                 f"contain bad values. This cannot be converted. "
                                 f"First several examples {','.join(examples)}")


class ListFieldType(CommonFieldType):
    mongoengine_field_classes = [
        mongoengine.fields.ListField
    ]

    schema_skel_keys = {'max_length'}  # TODO: implement "field"

    def change_max_length(self, diff: AlterDiff):
        self._check_diff(diff, True, True, int)
        if diff.new in (UNSET, None):
            return

        self.collection.aggregate([
            {'$match': {
                self.db_field: {"$ne": None},
                "$expr": {"$gt": [{"$size": f"${self.db_field}"}, diff.new]},
            }},
            {'$addFields': {
                self.db_field: {"$slice": [f'${self.db_field}', diff.new]}
            }},
            {'$out': self.collection.name}
        ])


class DictFieldType(CommonFieldType):
    pass  # TODO: implement "field" param


class BinaryFieldType(CommonFieldType):
    mongoengine_field_classes = [
        mongoengine.fields.BinaryField
    ]

    schema_skel_keys = {'max_bytes'}

    def change_max_bytes(self, diff: AlterDiff):
        """
        $binarySize expression is not available yet, so do nothing
        """
        self._check_diff(diff, True, True, int)
        if diff.new in (UNSET, None):
            return

        pass


class SequenceFieldType(CommonFieldType):
    mongoengine_field_classes = [
        mongoengine.fields.SequenceField
    ]

    # TODO: warning on using non-default value_decorator
    schema_skel_keys = {'collection_name', 'sequence_name'}

    def change_collection_name(self, diff: AlterDiff):
        """Typically changing the collection name should not require
        to do any changes
        """
        self._check_diff(diff, True, False, str)
        pass

    def change_sequence_name(self, diff: AlterDiff):
        """Typically changing the sequence name should not require
        to do any changes
        """
        self._check_diff(diff, True, False, str)
        pass


class UUIDFieldType(CommonFieldType):
    mongoengine_field_classes = [
        mongoengine.fields.UUIDField
    ]

    schema_skel_keys = {'binary'}

    def change_binary(self, diff: AlterDiff):
        self._check_diff(diff, True, False, bool)

        if diff.new is True:
            pass
            # TODO: convert to bson Binary
        else:
            pass
            # TODO: convert Binary to string



# ObjectIdField
# EmbeddedDocumentField
# GenericEmbeddedDocumentField -- ???
# ListField
# EmbeddedDocumentListField
# SortedListField
# DictField
# MapField
# ReferenceField
# CachedReferenceField
# GenericReferenceField -- ???
# BinaryField
# SequenceField
# UUIDField
# LazyReferenceField
# GenericLazyReferenceField -- ???
#
#
# GeoPointField
# PointField
# LineStringField
# PolygonField
# MultiPointField
# MultiLineStringField
# MultiPolygonField
#
#
# FileField
# ImageField
#