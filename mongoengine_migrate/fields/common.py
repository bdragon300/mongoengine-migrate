import mongoengine.fields

from .base import CommonFieldType
from ..actions.diff import AlterDiff
from mongoengine_migrate.exceptions import MigrationError
from typing import List, Dict, Any, Type
import mongoengine.fields
import re


class NumberFieldTypeBase(CommonFieldType):
    """Base class for number field types"""
    @classmethod
    def schema_skel(cls) -> dict:
        params = {'min_value', 'max_value'}
        skel = CommonFieldType.schema_skel()
        skel.update({f: None for f in params})
        return skel

    def change_min_value(self, diff: AlterDiff):
        # FIXME: treat None in diff old/new as value absence. Here and further
        if diff.policy == 'modify':
            self.collection.update_many(
                {self.db_field: {'$lt': diff.new}},
                {'$set': {self.db_field: diff.new}}
            )
        # FIXME: make replacing on error only here and further
        if diff.policy == 'replace':
            if diff.default < diff.new:
                raise MigrationError(f'Cannot set min_value for '
                                     f'{self.collection.name}.{self.db_field} because default value'
                                     f'{diff.default} is less than min_value')
            self.collection.update_many(
                {self.db_field: {'$lt': diff.new}},
                {'$set': {self.db_field: diff.default}}
            )

    def change_max_value(self, diff: AlterDiff):
        if diff.policy == 'modify':
            self.collection.update_many(
                {self.db_field: {'$gt': diff.new}},
                {'$set': {self.db_field: diff.new}}
            )

        if diff.policy == 'replace':
            if diff.default < diff.new:
                raise MigrationError(f'Cannot set max_value for '
                                     f'{self.collection.name}.{self.db_field} because default value'
                                     f'{diff.default} is greater than max_value')
            self.collection.update_many(
                {self.db_field: {'$gt': diff.new}},
                {'$set': {self.db_field: diff.default}}
            )


class StringFieldType(CommonFieldType):
    mongoengine_field_classes = [
        mongoengine.fields.StringField,
        mongoengine.fields.URLField,   # TODO: implement checks
        mongoengine.fields.EmailField  # TODO: implement checks
    ]

    @classmethod
    def schema_skel(cls):
        params = {'max_length', 'min_length', 'regex'}
        skel = CommonFieldType.schema_skel()
        skel.update({f: None for f in params})
        return skel

    def change_max_length(self, diff: AlterDiff):
        pipeline: List[Dict[str, Any]] = [
            {"$project": {"strlen": {"$strLenCP": f"${self.db_field}"}}},
            {"$match": {"strlen": {"$gt": diff.new}}},
        ]
        if diff.policy == 'modify':
            # Cut too long strings
            replace_pipeline = [
                {'$addFields': {self.db_field: {'$substr': {f'${self.db_field}', 0, diff.new}}}},
                {'$out': self.collection.name}
            ]
            self.collection.aggregate(pipeline + replace_pipeline)

        if diff.policy == 'replace':
            if diff.default is not None and len(diff.default) > diff.new:
                raise MigrationError(f'Cannot set default value for '
                                     f'{self.collection.name}.{self.db_field} because default value'
                                     f'{diff.default} is longer than max_length')
            replace_pipeline = [
                {'$addFields': {self.db_field: diff.default}},
                {'$out': self.collection.name}
            ]
            self.collection.aggregate(pipeline + replace_pipeline)

    def change_min_length(self, diff: AlterDiff):
        pipeline: List[Dict[str, Any]] = [
            {"$project": {"strlen": {"$strLenCP": f"${self.db_field}"}}},
            {"$match": {"strlen": {"$lt": diff.new}}},
        ]
        if diff.policy == 'modify':
            # We can't to increase string length, so raise error if
            # there was found strings which are shorter than should be
            agg = self.collection.aggregate(pipeline + [{"$count": "exceeded_count"}])
            exceeded_count = agg[0] if agg else 0
            if exceeded_count > 0:
                raise MigrationError(f'Cannot migrate min_length for '
                                     f'{self.collection.name}.{self.db_field} because '
                                     f'{exceeded_count} documents are less than min_length')
        if diff.policy == 'replace':
            if diff.default is not None and len(diff.default) < diff.new:
                raise MigrationError(f"Cannot set min_length for "
                                     f"{self.collection.name}.{self.db_field} because default value"
                                     f"'{diff.default}' is shorter than min_length")
            replace_pipeline = [
                {'$addFields': {self.db_field: diff.default}},
                {'$out': self.collection.name}
            ]
            self.collection.aggregate(pipeline + replace_pipeline)

    def change_regex(self, diff: AlterDiff):
        if diff.policy == 'modify':
            wrong_count = self.collection.find(
                {self.db_field: {'$not': {'$regex': diff.new}}}
            ).retrieved
            if wrong_count > 0:
                raise MigrationError(f'Cannot migrate regex for '
                                     f'{self.collection.name}.{self.db_field} because '
                                     f'{wrong_count} documents do not match this regex')

        if diff.policy == 'replace':
            matched = re.match(diff.new, diff.default)
            if not matched:
                raise MigrationError(f"Cannot set regex for "
                                     f"{self.collection.name}.{self.db_field} because default value"
                                     f"'{diff.default}' does not match that regex")
            self.collection.update_many({self.db_field: {'$not': {'$regex': diff.new}}},
                                        {'$set': {self.db_field: diff.new}})

    def convert_from(self,
                     from_field_cls: Type[mongoengine.fields.BaseField],
                     to_field_cls: Type[mongoengine.fields.BaseField]):
        self._convertion_command('$toString')


class IntFieldType(NumberFieldTypeBase):
    mongoengine_field_classes = [mongoengine.fields.IntField]

    def convert_from(self,
                     from_field_cls: Type[mongoengine.fields.BaseField],
                     to_field_cls: Type[mongoengine.fields.BaseField]):
        self._convertion_command('$toInt')


class LongFieldType(NumberFieldTypeBase):
    mongoengine_field_classes = [mongoengine.fields.LongField]

    def convert_from(self,
                     from_field_cls: Type[mongoengine.fields.BaseField],
                     to_field_cls: Type[mongoengine.fields.BaseField]):
        self._convertion_command('$toLong')


class FloatFieldType(NumberFieldTypeBase):
    mongoengine_field_classes = [mongoengine.fields.FloatField]

    def convert_from(self,
                     from_field_cls: Type[mongoengine.fields.BaseField],
                     to_field_cls: Type[mongoengine.fields.BaseField]):
        self._convertion_command('$toDouble')


class DecimalFieldType(NumberFieldTypeBase):
    mongoengine_field_classes = [mongoengine.fields.DecimalField]

    def change_force_string(self, diff: AlterDiff):
        if diff.policy == 'modify':
            if diff.new is True:
                self._convertion_command('$toString')
            else:
                self._convertion_command('$toDecimal')

        # TODO: implement 'replace'

    def change_precision(self, diff: AlterDiff):
        """This one is related only for python. Nothing to do"""
        pass

    def change_rounding(self, diff: AlterDiff):
        """This one is related only for python. Nothing to do"""
        pass

    def convert_from(self,
                     from_field_cls: Type[mongoengine.fields.BaseField],
                     to_field_cls: Type[mongoengine.fields.BaseField]):
        if self.field_schema.get('force_string', False):
            self._convertion_command('$toString')
        else:
            self._convertion_command('$toDecimal')


class DateTimeFieldType(CommonFieldType):
    mongoengine_field_classes = [
        mongoengine.fields.DateTimeField,
        mongoengine.fields.DateField
    ]

    def convert_from(self,
                     from_field_cls: Type[mongoengine.fields.BaseField],
                     to_field_cls: Type[mongoengine.fields.BaseField]):
        self._convertion_command('$toDate')


class ComplexDateTimeFieldType(StringFieldType):
    mongoengine_field_classes = [mongoengine.fields.ComplexDateTimeField]

    def change_separator(self, diff: AlterDiff):
        old_sep = re.escape(diff.old)
        old_format = ''.join(('^', old_sep.join(["\\d{4}"] + ["\\d{2}"] * 5 + ["\\d{6}"]), '$'))

        if diff.policy == 'modify':
            replace_expr = [(f'field[{c}]', diff.new) for c in range(7)]
            replace_expr = [x for pair in replace_expr for x in pair][:-1]
            self.collection.aggregate([
                {'$match': {self.db_field: {'$regex': old_format}}},
                {'$addFields': {self.db_field: {'$split': ['$field', diff.old]}}},
                {'$addFields': {self.db_field: {'$concat': replace_expr}}},
                {'$out': self.collection.name}
            ])

        if diff.policy == 'replace':
            self.collection.update_many({self.db_field: {'$not': {'$regex': old_format}}},
                                        {'$set': {self.db_field: diff.default}})

    def convert_from(self,
                     from_field_cls: Type[mongoengine.fields.BaseField],
                     to_field_cls: Type[mongoengine.fields.BaseField]):
        pass  #TODO


class EmbeddedDocumentFieldType(CommonFieldType):
    # TODO: implement embedded documents
    def convert_from(self,
                     from_field_cls: Type[mongoengine.fields.BaseField],
                     to_field_cls: Type[mongoengine.fields.BaseField]):
        pass
