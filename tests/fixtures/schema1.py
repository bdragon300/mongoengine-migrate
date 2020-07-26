import jsonpath_rw
from bson import ObjectId
from mongoengine import Document, fields, EmbeddedDocument

from mongoengine_migrate.schema import Schema


def get_classes():
    class Schema1EmbDoc1(EmbeddedDocument):
        embdoc1_int = fields.IntField()
        embdoc1_str = fields.StringField()
        embdoc1_str_empty = fields.StringField()
        embdoc1_str_ten = fields.StringField(choices=[str(x) for x in range(11)])
        embdoc1_float = fields.FloatField()
        embdoc1_int_empty = fields.IntField()
        embdoc1_long = fields.LongField()
        embdoc1_decimal = fields.DecimalField()
        embdoc1_complex_datetime = fields.ComplexDateTimeField()
        embdoc1_list = fields.ListField()
        embdoc1_ref_doc1 = fields.ReferenceField('Schema1Doc1')
        embdoc1_emb_embdoc1 = fields.EmbeddedDocumentField('self')
        embdoc1_emblist_embdoc1 = fields.EmbeddedDocumentListField('self')
        embdoc1_emb_embdoc2 = fields.EmbeddedDocumentField('Schema1EmbDoc2')
        embdoc1_emblist_embdoc2 = fields.EmbeddedDocumentListField('Schema1EmbDoc2')

    class Schema1EmbDoc2(EmbeddedDocument):
        embdoc2_int = fields.IntField()
        embdoc2_str = fields.StringField()
        embdoc2_str_empty = fields.StringField()
        embdoc2_str_ten = fields.StringField(choices=[str(x) for x in range(11)])
        embdoc2_float = fields.FloatField()
        embdoc2_int_empty = fields.IntField()
        embdoc2_long = fields.LongField()
        embdoc2_decimal = fields.DecimalField()
        embdoc2_complex_datetime = fields.ComplexDateTimeField()
        embdoc2_list = fields.ListField()
        embdoc2_ref_doc1 = fields.ReferenceField('Schema1Doc1')
        embdoc2_emb_embdoc2 = fields.EmbeddedDocumentField('self')
        embdoc2_emblist_embdoc2 = fields.EmbeddedDocumentListField('self')

    class Schema1Doc1(Document):
        doc1_int = fields.IntField()
        doc1_str = fields.StringField()
        doc1_str_empty = fields.StringField()
        doc1_str_ten = fields.StringField(choices=[str(x) for x in range(11)])
        doc1_float = fields.FloatField()
        doc1_int_empty = fields.IntField()
        doc1_long = fields.LongField()
        doc1_decimal = fields.DecimalField()
        doc1_complex_datetime = fields.ComplexDateTimeField()
        doc1_list = fields.ListField()
        doc1_ref_self = fields.ReferenceField('self')
        doc1_cachedref_self = fields.CachedReferenceField('self')
        doc1_emb_embdoc1 = fields.EmbeddedDocumentField('Schema1EmbDoc1')
        doc1_emb_embdoc2 = fields.EmbeddedDocumentField('Schema1EmbDoc2')
        doc1_emblist_embdoc1 = fields.EmbeddedDocumentListField('Schema1EmbDoc1')
        doc1_emblist_embdoc2 = fields.EmbeddedDocumentListField('Schema1EmbDoc2')

    return Schema1EmbDoc1, Schema1EmbDoc2, Schema1Doc1


def get_schema():
    schema = Schema({
        '~Schema1EmbDoc1': Schema.Document(
            {
                'embdoc1_int': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': None,
                    'sparse': False, 'unique': False, 'required': False, 'db_field': 'embdoc1_int',
                    'primary_key': False, 'type_key': 'IntField', 'max_value': None,
                    'min_value': None},
                'embdoc1_str': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': None,
                    'sparse': False, 'unique': False, 'required': False, 'db_field': 'embdoc1_str',
                    'primary_key': False, 'type_key': 'StringField', 'max_length': None,
                    'regex': None, 'min_length': None},
                'embdoc1_str_empty': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': None,
                    'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'embdoc1_str_empty',
                    'primary_key': False, 'type_key': 'StringField', 'max_length': None,
                    'regex': None, 'min_length': None},
                'embdoc1_str_ten': {
                    'unique_with': None, 'null': False,
                    'choices': ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10'],
                    'default': None, 'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'embdoc1_str_ten', 'primary_key': False, 'type_key': 'StringField',
                    'max_length': None, 'regex': None, 'min_length': None},
                'embdoc1_float': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': None,
                    'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'embdoc1_float', 'primary_key': False, 'type_key': 'FloatField',
                    'max_value': None, 'min_value': None},
                'embdoc1_long': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': None,
                    'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'embdoc1_long', 'primary_key': False, 'type_key': 'LongField',
                    'max_value': None, 'min_value': None},
                'embdoc1_int_empty': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': None,
                    'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'embdoc1_int_empty', 'primary_key': False, 'type_key': 'IntField',
                    'max_value': None, 'min_value': None},
                'embdoc1_decimal': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': None,
                    'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'embdoc1_decimal', 'primary_key': False,
                    'type_key': 'DecimalField', 'max_value': None, 'min_value': None,
                    'force_string': False, 'precision': 2, 'rounding': 'ROUND_HALF_UP'},
                'embdoc1_complex_datetime': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': None,
                    'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'embdoc1_complex_datetime', 'primary_key': False,
                    'type_key': 'ComplexDateTimeField', 'max_length': None,
                    'regex': None, 'min_length': None, 'separator': '.'},
                'embdoc1_list': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': [],
                    'sparse': False, 'unique': False, 'required': False, 'db_field': 'embdoc1_list',
                    'primary_key': False, 'type_key': 'ListField', 'max_length': None},
                'embdoc1_ref_doc1': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': [],
                    'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'embdoc1_ref_doc1', 'primary_key': False,
                    'type_key': 'ReferenceField', 'target_doctype': 'Schema1Doc1', 'dbref': False},
                'embdoc1_emb_embdoc1': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': None,
                    'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'embdoc1_emb_embdoc1', 'primary_key': False,
                    'type_key': 'EmbeddedDocumentField', 'target_doctype': '~Schema1EmbDoc1'},
                'embdoc1_emblist_embdoc1': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': [],
                    'sparse': False,'unique': False, 'required': False,
                    'db_field': 'embdoc1_emblist_embdoc1', 'primary_key': False,
                    'type_key': 'EmbeddedDocumentListField', 'target_doctype': '~Schema1EmbDoc1',
                    'max_length': None},
                'embdoc1_emb_embdoc2': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': None,
                    'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'embdoc1_emb_embdoc2', 'primary_key': False,
                    'type_key': 'EmbeddedDocumentField', 'target_doctype': '~Schema1EmbDoc2'},
                'embdoc1_emblist_embdoc2': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': [],
                    'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'embdoc1_emblist_embdoc2', 'primary_key': False,
                    'type_key': 'EmbeddedDocumentListField', 'target_doctype': '~Schema1EmbDoc2',
                    'max_length': None}
            },
            parameters={}
        ),
        '~Schema1EmbDoc2': Schema.Document(
            {
                'embdoc2_int': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': None,
                    'sparse': False, 'unique': False, 'required': False, 'db_field': 'embdoc2_int',
                    'primary_key': False, 'type_key': 'IntField', 'max_value': None,
                    'min_value': None},
                'embdoc2_str': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': None,
                    'sparse': False, 'unique': False, 'required': False, 'db_field': 'embdoc2_str',
                    'primary_key': False, 'type_key': 'StringField', 'max_length': None,
                    'regex': None, 'min_length': None},
                'embdoc2_str_empty': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': None,
                    'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'embdoc2_str_empty',
                    'primary_key': False, 'type_key': 'StringField', 'max_length': None,
                    'regex': None, 'min_length': None},
                'embdoc2_str_ten': {
                    'unique_with': None, 'null': False,
                    'choices': ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10'],
                    'default': None, 'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'embdoc2_str_ten', 'primary_key': False, 'type_key': 'StringField',
                    'max_length': None, 'regex': None, 'min_length': None},
                'embdoc2_float': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': None,
                    'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'embdoc2_float', 'primary_key': False, 'type_key': 'FloatField',
                    'max_value': None, 'min_value': None},
                'embdoc2_long': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': None,
                    'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'embdoc2_long', 'primary_key': False, 'type_key': 'LongField',
                    'max_value': None, 'min_value': None},
                'embdoc2_int_empty': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': None,
                    'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'embdoc2_int_empty', 'primary_key': False, 'type_key': 'IntField',
                    'max_value': None, 'min_value': None},
                'embdoc2_decimal': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': None,
                    'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'embdoc2_decimal', 'primary_key': False,
                    'type_key': 'DecimalField', 'max_value': None, 'min_value': None,
                    'force_string': False, 'precision': 2, 'rounding': 'ROUND_HALF_UP'},
                'embdoc2_complex_datetime': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': None,
                    'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'embdoc2_complex_datetime', 'primary_key': False,
                    'type_key': 'ComplexDateTimeField', 'max_length': None,
                    'regex': None, 'min_length': None, 'separator': '.'},
                'embdoc2_list': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': [],
                    'sparse': False, 'unique': False, 'required': False, 'db_field': 'embdoc2_list',
                    'primary_key': False, 'type_key': 'ListField', 'max_length': None},
                'embdoc2_ref_doc1': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': [],
                    'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'embdoc2_ref_doc1', 'primary_key': False,
                    'type_key': 'ReferenceField', 'target_doctype': 'Schema1Doc1', 'dbref': False},
                'embdoc2_emb_embdoc2': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': None,
                    'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'embdoc2_emb_embdoc2', 'primary_key': False,
                    'type_key': 'EmbeddedDocumentField', 'target_doctype': '~Schema1EmbDoc2'},
                'embdoc2_emblist_embdoc2': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': [],
                    'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'embdoc2_emblist_embdoc2', 'primary_key': False,
                    'type_key': 'EmbeddedDocumentListField', 'target_doctype': '~Schema1EmbDoc2',
                    'max_length': None}
            },
            parameters={}
        ),
        'Schema1Doc1': Schema.Document(
            {
                'doc1_int': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': None,
                    'sparse': False, 'unique': False, 'required': False, 'db_field': 'doc1_int',
                    'primary_key': False, 'type_key': 'IntField', 'max_value': None,
                    'min_value': None},
                'doc1_str': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': None,
                    'sparse': False, 'unique': False, 'required': False, 'db_field': 'doc1_str',
                    'primary_key': False, 'type_key': 'StringField', 'max_length': None,
                    'regex': None, 'min_length': None},
                'doc1_str_empty': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': None,
                    'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'doc1_str_empty',
                    'primary_key': False, 'type_key': 'StringField', 'max_length': None,
                    'regex': None, 'min_length': None},
                'doc1_str_ten': {
                    'unique_with': None, 'null': False,
                    'choices': ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10'],
                    'default': None, 'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'doc1_str_ten', 'primary_key': False, 'type_key': 'StringField',
                    'max_length': None, 'regex': None, 'min_length': None},
                'doc1_float': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': None,
                    'sparse': False, 'unique': False, 'required': False, 'db_field': 'doc1_float',
                    'primary_key': False, 'type_key': 'FloatField', 'max_value': None,
                    'min_value': None},
                'doc1_long': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': None,
                    'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'doc1_long', 'primary_key': False, 'type_key': 'LongField',
                    'max_value': None, 'min_value': None},
                'doc1_int_empty': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': None,
                    'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'doc1_int_empty', 'primary_key': False, 'type_key': 'IntField',
                    'max_value': None, 'min_value': None},
                'doc1_decimal': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': None,
                    'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'doc1_decimal', 'primary_key': False,
                    'type_key': 'DecimalField', 'max_value': None, 'min_value': None,
                    'force_string': False, 'precision': 2, 'rounding': 'ROUND_HALF_UP'},
                'doc1_complex_datetime': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': None,
                    'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'doc1_complex_datetime', 'primary_key': False,
                    'type_key': 'ComplexDateTimeField', 'max_length': None,
                    'regex': None, 'min_length': None, 'separator': '.'},
                'doc1_list': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': [],
                    'sparse': False, 'unique': False, 'required': False, 'db_field': 'doc1_list',
                    'primary_key': False, 'type_key': 'ListField', 'max_length': None},
                'doc1_ref_self': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': [],
                    'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'doc1_ref_self', 'primary_key': False,
                    'type_key': 'ReferenceField', 'target_doctype': 'self', 'dbref': False},
                'doc1_cachedref_self': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': [],
                    'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'doc1_cachedref_self', 'primary_key': False,
                    'type_key': 'CachedReferenceField', 'target_doctype': 'self', 'fields': []},
                'doc1_emb_embdoc1': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': None,
                    'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'doc1_emb_embdoc1', 'primary_key': False,
                    'type_key': 'EmbeddedDocumentField', 'target_doctype': '~Schema1EmbDoc1'},
                'doc1_emb_embdoc2': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': None,
                    'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'doc1_emb_embdoc2', 'primary_key': False,
                    'type_key': 'EmbeddedDocumentField', 'target_doctype': '~Schema1EmbDoc2'},
                'doc1_emblist_embdoc1': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': [],
                    'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'doc1_emblist_embdoc1', 'primary_key': False,
                    'type_key': 'EmbeddedDocumentListField', 'target_doctype': '~Schema1EmbDoc1',
                    'max_length': None},
                'doc1_emblist_embdoc2': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': [],
                    'sparse': False, 'unique': False, 'required': False,
                    'db_field': 'doc1_emblist_embdoc2', 'primary_key': False,
                    'type_key': 'EmbeddedDocumentListField',  'target_doctype': '~Schema1EmbDoc2',
                    'max_length': None}
            },
            parameters={'collection': 'schema1_doc1'}
        )
    })

    return schema


def setup_db():
    Schema1EmbDoc1, Schema1EmbDoc2, Schema1Doc1 = get_classes()

    # doc1
    n = 1
    doc1_1 = Schema1Doc1(id=ObjectId(f'{n:024}'),
                         doc1_int=n,
                         doc1_str=f'str{n}',
                         doc1_str_ten=str(n % 10),
                         doc1_list=[n * 10000 + 1, f'str{n * 10000 + 1}', None]).save()

    # doc1.embdoc1
    n = 2
    embdoc2_1 = Schema1EmbDoc1(embdoc1_int=n * 10 + 1,
                               embdoc1_str=f'str{n * 10 + 1}',
                               embdoc1_str_ten=str(n % 10),
                               embdoc1_list=[
                                   n * 10000 + 1001, f'str{n * 10000 + 1001}', None
                               ])
    doc1_1 = Schema1Doc1(id=ObjectId(f'{n:024}'),
                         doc1_int=n,
                         doc1_str=f'str{n}',
                         doc1_str_ten=str(n % 10),
                         doc1_emb_embdoc1=embdoc2_1).save()

    # doc1.[embdoc1]
    n = 3
    embdoc1_x = [
        Schema1EmbDoc1(embdoc1_int=n * 10 + 1,
                       embdoc1_str=f'str{n * 10 + 1}',
                       embdoc1_str_ten=str(n % 10),
                       embdoc1_list=[n * 10000 + 1001, f'str{n * 10000 + 1001}', None]),
        Schema1EmbDoc1(embdoc1_int=n * 10 + 2,
                       embdoc1_str=f'str{n * 10 + 2}',
                       embdoc1_str_ten=str(n % 10),
                       embdoc1_list=[n * 10000 + 2001, f'str{n * 10000 + 2001}', None]),
    ]
    doc1_1 = Schema1Doc1(id=ObjectId(f'{n:024}'),
                         doc1_int=n,
                         doc1_str=f'str{n}',
                         doc1_str_ten=str(n % 10),
                         doc1_emblist_embdoc1=embdoc1_x).save()

    # doc1.embdoc1.[embdoc2]
    n = 4
    embdoc2_x = [
        Schema1EmbDoc2(embdoc2_int=n * 100 + 11,
                       embdoc2_str=f'str{n * 100 + 11}',
                       embdoc2_str_ten=str(n % 10),
                       embdoc2_list=[n * 100 + 11, f'str{n * 100 + 11}', None]),
        Schema1EmbDoc2(embdoc2_int=n * 100 + 12,
                       embdoc2_str=f'str{n * 100 + 12}',
                       embdoc2_str_ten=str(n % 10),
                       embdoc2_list=[n * 100 + 11, f'str{n * 100 + 11}', None]),
    ]
    embdoc2_1 = Schema1EmbDoc1(embdoc1_int=n * 10 + 1,
                               embdoc1_str=f'str{n * 10 + 1}',
                               embdoc1_str_ten=str(n % 10),
                               embdoc1_emblist_embdoc2=embdoc2_x)
    doc1_1 = Schema1Doc1(id=ObjectId(f'{n:024}'),
                         doc1_int=n,
                         doc1_str=f'str{n}',
                         doc1_str_ten=str(n % 10),
                         doc1_emb_embdoc1=embdoc2_1).save()

    items = (
        ('embdoc1', Schema1EmbDoc1, 'embdoc2', Schema1EmbDoc2),
        ('embdoc1', Schema1EmbDoc1, 'embdoc1', Schema1EmbDoc1),
    )
    for field1, cls1, field2, cls2 in items:
        # doc1.[embdoc1].[embdoc1/embdoc2]
        n += 1  # 5, 7
        embdoc2_1 = [
            cls2(**{
                f'{field2}_int': n * 100 + 11,
                f'{field2}_str': f'str{n * 100 + 11}',
                f'{field2}_str_ten': str(n % 10),
                f'{field2}_list': [n * 10000 + 1101, f'str{n * 10000 + 1101}', None]
            }),
            cls2(**{
                f'{field2}_int': n * 100 + 12,
                f'{field2}_str': f'str{n * 100 + 12}',
                f'{field2}_str_ten': str(n % 10),
                f'{field2}_list': [n * 10000 + 1201, f'str{n * 10000 + 1201}', None]
            })
        ]
        embdoc2_2 = [
            cls2(**{
                f'{field2}_int': n * 100 + 21,
                f'{field2}_str': f'str{n * 100 + 21}',
                f'{field2}_str_ten': str(n % 10),
                f'{field2}_list': [n * 10000 + 2101, f'str{n * 10000 + 2101}', None]
            }),
            cls2(**{
                f'{field2}_int': n * 100 + 22,
                f'{field2}_str': f'str{n * 100 + 22}',
                f'{field2}_str_ten': str(n % 10),
                f'{field2}_list': [n * 10000 + 2201, f'str{n * 10000 + 2201}', None]
            })
        ]
        embdoc1_x = [
            cls1(**{
                f'{field1}_int': n * 10 + 1,
                f'{field1}_str': f'str{n * 10 + 1}',
                f'{field1}_str_ten': str(n % 10),
                f'{field1}_emblist_{field2}': embdoc2_1
            }),
            cls1(**{
                f'{field1}_int': n * 10 + 2,
                f'{field1}_str': f'str{n * 10 + 2}',
                f'{field1}_str_ten': str(n % 10),
                f'{field1}_emblist_{field2}': embdoc2_2
            })
        ]
        doc1_1 = Schema1Doc1(**{f'id': ObjectId(f'{n:024}'),
                                f'doc1_int': n,
                                f'doc1_str': f'str{n}',
                                f'doc1_str_ten': str(n % 10),
                                f'doc1_emblist_{field1}': embdoc1_x}).save()

        # doc1.[embdoc1].embdoc1.[embdoc1/embdoc2]
        n += 1  # 6, 8
        embdoc2_1 = [
            cls2(**{
                f'{field2}_int': n * 1000 + 111,
                f'{field2}_str': f'str{n * 1000 + 111}',
                f'{field2}_str_ten': str(n % 10),
                f'{field2}_list': [n * 10000 + 1111, f'str{n * 10000 + 1111}', None]
            }),
            cls2(**{
                f'{field2}_int': n * 1000 + 112,
                f'{field2}_str': f'str{n * 1000 + 112}',
                f'{field2}_str_ten': str(n % 10),
                f'{field2}_list': [n * 10000 + 1121, f'str{n * 10000 + 1121}', None]
            })
        ]
        embdoc2_2 = [
            cls2(**{
                f'{field2}_int': n * 1000 + 211,
                f'{field2}_str': f'str{n * 1000 + 211}',
                f'{field2}_str_ten': str(n % 10),
                f'{field2}_list': [n * 10000 + 2111, f'str{n * 10000 + 2111}', None]
            }),
            cls2(**{
                f'{field2}_int': n * 1000 + 212,
                f'{field2}_str': f'str{n * 1000 + 212}',
                f'{field2}_str_ten': str(n % 10),
                f'{field2}_list': [n * 10000 + 2121, f'str{n * 10000 + 2121}', None]
            })
        ]
        embdoc1_1 = cls1(**{
            f'{field1}_int': n * 100 + 11,
            f'{field1}_str': f'str{n * 100 + 11}',
            f'{field1}_str_ten': str(n % 10),
            f'{field1}_emblist_{field2}': embdoc2_1
        })
        embdoc1_2 = cls1(**{
            f'{field1}_int': n * 100 + 21,
            f'{field1}_str': f'str{n * 100 + 21}',
            f'{field1}_str_ten': str(n % 10),
            f'{field1}_emblist_{field2}': embdoc2_2
        })
        embdoc1_x = [
            cls1(**{
                f'{field1}_int': n * 10 + 1,
                f'{field1}_str': f'str{n * 10 + 1}',
                f'{field1}_str_ten': str(n % 10),
                f'{field1}_emb_{field1}': embdoc1_1
            }),
            cls1(**{
                f'{field1}_int': n * 10 + 2,
                f'{field1}_str': f'str{n * 10 + 2}',
                f'{field1}_str_ten': str(n % 10),
                f'{field1}_emb_{field1}': embdoc1_2
            })
        ]
        doc1_1 = Schema1Doc1(**{f'id': ObjectId(f'{n:024}'),
                                f'doc1_int': n,
                                f'doc1_str': f'str{n}',
                                f'doc1_str_ten': str(n % 10),
                                f'doc1_emblist_{field1}': embdoc1_x}).save()


def get_embedded_jsonpath_parsers(document_type):
    if document_type == '~Schema1EmbDoc1':
        return (
            # doc1.embdoc1
            # doc1.embdoc1.[embdoc2]
            jsonpath_rw.parse(
                'schema1_doc1[*].doc1_emb_embdoc1'
            ),
            # doc1.[embdoc1]
            jsonpath_rw.parse(
                'schema1_doc1[*].doc1_emblist_embdoc1[*]'
            ),
            # doc1.[embdoc1].[embdoc1/embdoc2]
            jsonpath_rw.parse(
                'schema1_doc1[*].doc1_emblist_embdoc1[*].embdoc1_emblist_embdoc1[*]'
            ),
            # doc1.[embdoc1].embdoc1.[embdoc1/embdoc2]
            jsonpath_rw.parse(
                'schema1_doc1[*].doc1_emblist_embdoc1[*].embdoc1_emb_embdoc1[*]'
            ),
            jsonpath_rw.parse(
                'schema1_doc1[*].doc1_emblist_embdoc1[*].embdoc1_emb_embdoc1[*].'
                'embdoc1_emblist_embdoc1[*]'
            ),
        )
    elif document_type == '~Schema1EmbDoc2':
        return (
            # doc1.embdoc1.[embdoc2]
            jsonpath_rw.parse(
                'schema1_doc1[*].doc1_emb_embdoc1.embdoc1_emblist_embdoc2[*]'
            ),
            # doc1.[embdoc1].[embdoc1/embdoc2]
            jsonpath_rw.parse(
                'schema1_doc1[*].doc1_emblist_embdoc1[*].embdoc1_emblist_embdoc2[*]'
            ),
            # doc1.[embdoc1].embdoc1.[embdoc1/embdoc2]
            jsonpath_rw.parse(
                'schema1_doc1[*].doc1_emblist_embdoc1[*].embdoc1_emb_embdoc1[*].'
                'embdoc1_emblist_embdoc2[*]'
            ),
        )
    elif document_type == 'Schema1Doc1':
        return jsonpath_rw.parse('schema1_doc1[*]'),
    else:
        raise ValueError('Unknown document_type')
