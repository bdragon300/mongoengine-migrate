import re
from typing import Type, Collection, Union

import mongoengine.fields

from mongoengine_migrate.exceptions import MigrationError
from .base import CommonFieldHandler
from .converters import to_string, to_decimal
from ..actions.diff import AlterDiff, UNSET
from ..mongo import check_empty_result
from ..mongo import mongo_version


class NumberFieldHandler(CommonFieldHandler):
    field_classes = [
        mongoengine.fields.IntField,
        mongoengine.fields.LongField,
        mongoengine.fields.FloatField,
    ]
    schema_skel_keys = {'min_value', 'max_value'}

    def change_min_value(self, diff: AlterDiff):
        """
        Change min_value of field. Force set to minimum if value is
        less than limitation (if any)
        """
        self._check_diff(diff, True, (int, float))
        if diff.new in (UNSET, None):
            return

        self.collection.update_many(
            {self.db_field: {'$lt': diff.new}},
            {'$set': {self.db_field: diff.new}}
        )

    def change_max_value(self, diff: AlterDiff):
        """
        Change max_value of field. Force set to maximum if value is
        more than limitation (if any)
        """
        self._check_diff(diff, True, (int, float))
        if diff.new in (UNSET, None):
            return

        self.collection.update_many(
            {self.db_field: {'$gt': diff.new}},
            {'$set': {self.db_field: diff.new}}
        )


class StringFieldHandler(CommonFieldHandler):
    field_classes = [
        mongoengine.fields.StringField,
    ]

    schema_skel_keys = {'max_length', 'min_length', 'regex'}

    @mongo_version(min_version='3.6')
    def change_max_length(self, diff: AlterDiff):
        """Cut off a string if it longer than limitation (if any)"""
        self._check_diff(diff, True, int)
        if diff.new in (UNSET, None):
            return
        if diff.new < 0:
            diff.new = 0

        # Cut too long strings
        self.collection.aggregate([
            {'$match': {
                self.db_field: {"$ne": None},
                "$expr": {"$gt": [{"$strLenCP": f"${self.db_field}"}, diff.new]},  # >= 3.6
            }},
            {'$addFields': {  # >= 3.4
                self.db_field: {"$substr": [f'${self.db_field}', 0, diff.new]}
            }},
            {'$out': self.collection.name}  # >= 2.6
        ])

    @mongo_version(min_version='3.6')
    def change_min_length(self, diff: AlterDiff):
        """Raise error if string is shorter than limitation (if any)"""
        self._check_diff(diff, True, int)
        if diff.new in (UNSET, None):
            return
        if diff.new < 0:
            diff.new = 0

        # We can't to increase string length, so raise error if
        # there was found strings which are shorter than should be
        fltr = {
            self.db_field: {'$ne': None},
            "$where": f"this.{self.db_field}.length < {diff.new}"  # >=3.6
        }
        check_empty_result(self.collection, self.db_field, fltr)

    def change_regex(self, diff: AlterDiff):
        """Raise error if string does not match regex (if any)"""
        self._check_diff(diff, True, (str, type(re.compile('.'))))
        if diff.new in (UNSET, None):
            return

        fltr = {self.db_field: {'$not': re.compile(diff.new), '$ne': None}}
        check_empty_result(self.collection, self.db_field, fltr)


class URLFieldHandler(StringFieldHandler):
    field_classes = [
        mongoengine.fields.URLField,
    ]

    schema_skel_keys = {'schemes'}  # TODO: implement url_regex

    def change_schemes(self, diff: AlterDiff):
        """Raise error if url has scheme not from list"""
        self._check_diff(diff, False, Collection)
        if not diff.new or diff.new == UNSET:
            return

        # Check if some records contains non-url values in db_field
        scheme_regex = re.compile(rf'\A(?:({"|".join(re.escape(x) for x in diff.new)}))://')
        fltr = {self.db_field: {'$not': scheme_regex, '$ne': None}}
        check_empty_result(self.collection, self.db_field, fltr)


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

    def change_domain_whitelist(self, diff: AlterDiff):
        """
        `domain_whitelist` parameter is affected to domain validation:
        if domain in email address is in that list then validation will
        be skipped. So, do nothing here
        """

    def change_allow_utf8_user(self, diff: AlterDiff):
        """Raise error if email address has wrong user name"""
        self._check_diff(diff, False, bool)
        if diff.new == UNSET:
            return

        regex = self.UTF8_USER_REGEX if diff.new is True else self.USER_REGEX

        # Find records which doesn't match to the regex
        fltr = {self.db_field: {'$not': regex, '$ne': None}}
        check_empty_result(self.collection, self.db_field, fltr)

    def change_allow_ip_domain(self, diff: AlterDiff):
        """
        Raise error if email has domain which not in `domain_whitelist`
        when `allow_ip_domain` is True. Otherwise do nothing
        """
        self._check_diff(diff, False, bool)
        if diff.new is True or diff.new == UNSET:
            return

        # Find records with ip domains and raise error if found
        whitelist_regex = '|'.join(
            re.escape(x) for x in self.left_field_schema.get('domain_whitelist', [])
        ) or '.*'
        fltr = {"$and": [
            {self.db_field: {'$ne': None}},
            {self.db_field: self.IP_DOMAIN_REGEX},
            {self.db_field: {'$not': re.compile(rf'\A[^@]+@({whitelist_regex})\Z')}}
        ]}
        check_empty_result(self.collection, self.db_field, fltr)

    def convert_type(self,
                     from_field_cls: Type[mongoengine.fields.BaseField],
                     to_field_cls: Type[mongoengine.fields.BaseField]):
        to_string(self.collection, self.db_field)

        # Find records with ip domains and raise error if found
        whitelist_regex = '|'.join(
            re.escape(x) for x in self.left_field_schema.get('domain_whitelist', [])
        ) or '.*'
        fltr = {'$and': [
            {self.db_field: {'$ne': None}},
            {self.db_field: {'$not': self.DOMAIN_REGEX}},
            {self.db_field: {'$not': self.IP_DOMAIN_REGEX}},
            {self.db_field: {'$not': re.compile(rf'\A[^@]+@({whitelist_regex})\Z')}}
        ]}
        check_empty_result(self.collection, self.db_field, fltr)


class DecimalFieldHandler(NumberFieldHandler):
    field_classes = [mongoengine.fields.DecimalField]

    schema_skel_keys = {'force_string', 'precision', 'rounding'}

    def change_force_string(self, diff: AlterDiff):
        """
        Convert to string or decimal depending on `force_string` flag
        """
        self._check_diff(diff, False, bool)
        if diff.new == UNSET:
            return

        if diff.new is True:
            to_string(self.collection, self.db_field)
        else:
            to_decimal(self.collection, self.db_field)

    def change_precision(self, diff: AlterDiff):
        """This one is related only for python. Nothing to do"""
        pass

    def change_rounding(self, diff: AlterDiff):
        """This one is related only for python. Nothing to do"""
        pass

    def convert_type(self,
                     from_field_cls: Type[mongoengine.fields.BaseField],
                     to_field_cls: Type[mongoengine.fields.BaseField]):
        if self.left_field_schema.get('force_string', True):
            to_string(self.collection, self.db_field)
        else:
            to_decimal(self.collection, self.db_field)


class ComplexDateTimeFieldHandler(StringFieldHandler):
    field_classes = [mongoengine.fields.ComplexDateTimeField]

    schema_skel_keys = {'separator'}

    @mongo_version(min_version='3.4')
    def change_separator(self, diff: AlterDiff):
        """Change separator in datetime strings"""
        self._check_diff(diff, False, str)
        if not diff.new or not diff.old:
            raise MigrationError('Empty separator specified')
        if diff.new == UNSET:
            return

        old_sep = re.escape(diff.old)
        old_regex = r'\A' + str(old_sep.join([r"\d{4}"] + [r"\d{2}"] * 5 + [r"\d{6}"])) + r'\Z'

        self.collection.aggregate([
            {'$match': {
                '$and': [
                    {self.db_field: {"$ne": None}},
                    {self.db_field: re.compile(old_regex)},
                ]
            }},
            {'$addFields': {  # >=3.4
                self.db_field: {
                    '$reduce': {  # >=3.4
                        'input': {'$split': [f'${self.db_field}', diff.old]},  # $split >=3.4
                        'initialValue': '',
                        'in': {'$concat': ['$$value', diff.new, '$$this']}
                    }
                }
            }},
            {'$addFields': {  # >=3.4
                self.db_field: {"$substr": [f'${self.db_field}', 1, -1]}
            }},
            {'$out': self.collection.name}  # >= 2.6
        ])


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
        skel = CommonFieldHandler.schema_skel()

        # Add optional keys to skel if they are set in field object
        # * `max_length` was added in mongoengine 0.19.0
        optional_keys = {'max_length'}
        assert cls.field_classes == [mongoengine.fields.ListField], \
            'If you wanna add field class then this code should be rewritten'
        field_obj = mongoengine.fields.ListField()
        skel.update({k: None for k in optional_keys if hasattr(field_obj, k)})

        return skel

    @mongo_version(min_version='3.6')
    def change_max_length(self, diff: AlterDiff):
        """Cut off a list if it longer than limitation (if any)"""
        self._check_diff(diff, True, int)
        if diff.new in (UNSET, None):
            return

        self.collection.aggregate([
            {'$match': {
                self.db_field: {"$ne": None},
                "$expr": {"$gt": [{"$size": f"${self.db_field}"}, diff.new]},  # $expr >= 3.6
            }},
            {'$addFields': {  # >=3.4
                self.db_field: {"$slice": [f'${self.db_field}', diff.new]}  # $slice >=3.2
            }},
            {'$out': self.collection.name}  # >= 2.6
        ])


class DictFieldHandler(CommonFieldHandler):
    pass  # TODO: implement "field" param


class BinaryFieldHandler(CommonFieldHandler):
    field_classes = [
        mongoengine.fields.BinaryField
    ]

    schema_skel_keys = {'max_bytes'}

    def change_max_bytes(self, diff: AlterDiff):
        """
        $binarySize expression is not available in MongoDB yet,
        so do nothing
        """
        self._check_diff(diff, True, int)
        if diff.new in (UNSET, None):
            return

        pass


class SequenceFieldHandler(CommonFieldHandler):
    field_classes = [
        mongoengine.fields.SequenceField
    ]

    # TODO: warning on using non-default value_decorator
    # TODO: db_alias
    schema_skel_keys = {'link_collection', 'sequence_name'}

    @classmethod
    def build_schema(cls, field_obj: mongoengine.fields.SequenceField) -> dict:
        schema = super().build_schema(field_obj)
        schema['link_collection'] = field_obj.collection_name

        return schema

    def change_link_collection(self, diff: AlterDiff):
        """Typically changing the collection name should not require
        to do any changes
        """
        self._check_diff(diff, False, str)
        pass

    def change_sequence_name(self, diff: AlterDiff):
        """Typically changing the sequence name should not require
        to do any changes
        """
        self._check_diff(diff, False, str)
        pass


class UUIDFieldHandler(CommonFieldHandler):
    field_classes = [
        mongoengine.fields.UUIDField
    ]

    schema_skel_keys = {'binary'}

    def change_binary(self, diff: AlterDiff):
        self._check_diff(diff, False, bool)

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

    schema_skel_keys = {'link_collection', 'dbref'}

    def change_link_collection(self, diff: AlterDiff):
        """Collection could be not existed in db, so do nothing"""
        self._check_diff(diff, False, str)

    def change_dbref(self, diff: AlterDiff):
        """Change reference storing format: ObjectId or DBRef"""
        self._check_diff(diff, False, bool)

        if diff.new is True:
            self._objectid_to_dbref()
        else:
            self._dbref_to_objectid()

    @mongo_version(min_version='3.6')
    def _objectid_to_dbref(self):
        self.collection.aggregate([
            {'$match': {
                self.db_field: {"$ne": None},
                # $expr >= 3.6, $type >= 3.4
                "$expr": {"$eq": [{"$type": f'${self.db_field}'}, 'objectId']}
            }},
            {'$addFields': {  # >= 3.4
                self.db_field: {
                    '$ref': self.collection.name,
                    '$id': f"${self.db_field}"
                }
            }},
            {'$out': self.collection.name}  # >= 2.6
        ])

    @mongo_version(min_version='3.6')
    def _dbref_to_objectid(self):
        self.collection.aggregate([
            {'$match': {
                f'{self.db_field}.$id': {"$ne": None},
                f'{self.db_field}.$ref': {"$ne": None},
                # $expr >= 3.6, $type >= 3.4
                "$expr": {"$eq": [{"$type": f'${self.db_field}.$id'}, 'objectId']}
            }},
            {'$addFields': {self.db_field: f"${self.db_field}.$id"}},  # >= 3.4
            {'$out': self.collection.name}  # >= 2.6
        ])

    @classmethod
    def build_schema(
            cls,
            field_obj: Union[
                mongoengine.fields.ReferenceField,
                mongoengine.fields.LazyReferenceField
            ]) -> dict:
        schema = super().build_schema(field_obj)

        # 'document_type' is restricted to use only Document class
        # by mongoengine itself
        document_type = field_obj.document_type
        schema['link_collection'] = document_type._get_collection_name()

        return schema


class CachedReferenceFieldHandler(CommonFieldHandler):
    field_classes = [
        mongoengine.fields.CachedReferenceField
    ]

    schema_skel_keys = {'fields'}

    def change_fields(self, diff: AlterDiff):
        self._check_diff(diff, False, (list, tuple))

        to_remove = set(diff.old) - set(diff.new)
        if to_remove:
            paths = {f'{self.db_field}.{f}': '' for f in to_remove}
            self.collection.update_many(
                {self.db_field: {'$ne': None}},
                {'$unset': paths}
            )


class FileFieldHandler(CommonFieldHandler):
    field_classes = [
        mongoengine.fields.FileField
    ]

    # TODO: db_alias
    schema_skel_keys = {'link_collection'}

    @classmethod
    def build_schema(cls, field_obj: mongoengine.fields.FileField) -> dict:
        schema = super().build_schema(field_obj)
        schema['link_collection'] = field_obj.collection_name

        return schema

    def change_link_collection(self, diff: AlterDiff):
        """Typically changing the collection name should not require
        to do any changes
        """
        self._check_diff(diff, False, str)
        pass


class ImageFieldHandler(FileFieldHandler):
    field_classes = [
        mongoengine.fields.ImageField
    ]

    schema_skel_keys = {'size', 'thumbnail_size'}

    def change_thumbnail_size(self, diff: AlterDiff):
        """Typically changing the attribute should not require
        to do any changes
        """
        self._check_diff(diff, False, (list, tuple))
        pass

    def change_size(self, diff: AlterDiff):
        """Typically changing the attribute should not require
        to do any changes
        """
        self._check_diff(diff, False, (list, tuple))
        pass


# class EmbeddedDocumentFieldHandler(CommonFieldHandler):
#     field_classes = [
#         mongoengine.fields.EmbeddedDocumentField
#     ]
#
#     schema_skel_keys = {'document_type'}
#
#     def change_document_type(self, diff: AlterDiff):
#         self._check_diff(diff, False, str)
#
#         try:
#             document = get_document(diff.new)
#         except mongoengine.errors.NotRegistered as e:
#             raise MigrationError(f'Could not find document {diff.new}, field: {self.db_field}, '
#                                  f'diff: {diff!s}') from e
#
#     def build_schema(cls, field_obj: mongoengine.fields.BaseField) -> dict:
#         schema = super().build_schema(field_obj)
#
#         document_type = field_obj.document_type