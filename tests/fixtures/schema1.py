from mongoengine import Document, fields, EmbeddedDocument

from mongoengine_migrate.schema import Schema


def get_classes():
    class Schema1EmbDoc1(EmbeddedDocument):
        embdoc1_int = fields.IntField()
        embdoc1_str = fields.StringField()
        embdoc1_list = fields.ListField()
        embdoc1_emb_embdoc1 = fields.EmbeddedDocumentField('self')
        embdoc1_emblist_embdoc1 = fields.EmbeddedDocumentListField('self')
        embdoc1_emb_embdoc2 = fields.EmbeddedDocumentField('Schema1EmbDoc2')
        embdoc1_emblist_embdoc2 = fields.EmbeddedDocumentListField('Schema1EmbDoc2')

    class Schema1EmbDoc2(EmbeddedDocument):
        embdoc2_int = fields.IntField()
        embdoc2_str = fields.StringField()
        embdoc2_list = fields.ListField()
        embdoc2_emb_embdoc2 = fields.EmbeddedDocumentField('self')
        embdoc2_emblist_embdoc2 = fields.EmbeddedDocumentListField('self')
        # embdoc2_emb_embdoc1 = fields.EmbeddedDocumentField('Schema1EmbDoc1')
        # embdoc2_emblist_embdoc1 = fields.EmbeddedDocumentListField('Schema1EmbDoc1')

    class Schema1Doc1(Document):
        doc1_int = fields.IntField()
        doc1_str = fields.StringField()
        doc1_list = fields.ListField()
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
                    'regex': None,
                    'min_length': None},
                'embdoc1_list': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': [],
                    'sparse': False, 'unique': False, 'required': False, 'db_field': 'embdoc1_list',
                    'primary_key': False, 'type_key': 'ListField', 'max_length': None},
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
                'embdoc2_list': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': [],
                    'sparse': False, 'unique': False, 'required': False, 'db_field': 'embdoc2_list',
                    'primary_key': False, 'type_key': 'ListField', 'max_length': None},
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
                'doc1_list': {
                    'unique_with': None, 'null': False, 'choices': None, 'default': [],
                    'sparse': False, 'unique': False, 'required': False, 'db_field': 'doc1_list',
                    'primary_key': False, 'type_key': 'ListField', 'max_length': None},
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

    # field
    n = 1
    doc1_1 = Schema1Doc1(doc1_int=n, doc1_str=f'str{n}').save()

    # field(array)
    n += 1
    doc1_1 = Schema1Doc1(doc1_int=n,
                         doc1_str=f'str{n}',
                         doc1_list=[n * 10000 + 1, f'str{n * 10000 + 1}', None]).save()

    # field.field
    n += 1
    embdoc1_1 = Schema1EmbDoc1(embdoc1_int=n * 10 + 1, embdoc1_str=f'str{n * 10 + 1}')
    doc1_1 = Schema1Doc1(doc1_int=n, doc1_str=f'str{n}', doc1_emb_embdoc1=embdoc1_1).save()

    # field.field(array)
    n += 1
    embdoc1_1 = Schema1EmbDoc1(embdoc1_int=n * 10 + 1,
                               embdoc1_str=f'str{n * 10 + 1}',
                               embdoc1_list=[
                                   n * 10000 + 1001, f'str{n * 10000 + 1001}', None
                               ])
    doc1_1 = Schema1Doc1(doc1_int=n, doc1_str=f'str{n}', doc1_emb_embdoc1=embdoc1_1).save()

    # field.field(embdoc1).$[embdoc2].field
    n += 1
    embdoc2_x = [
        Schema1EmbDoc2(embdoc2_int=n * 100 + 11, embdoc2_str=f'str{n * 100 + 11}'),
        Schema1EmbDoc2(embdoc2_int=n * 100 + 12, embdoc2_str=f'str{n * 100 + 12}'),
    ]
    embdoc1_1 = Schema1EmbDoc1(embdoc1_int=n * 10 + 1,
                               embdoc1_str=f'str{n * 10 + 1}',
                               embdoc1_emblist_embdoc2=embdoc2_x)
    doc1_1 = Schema1Doc1(doc1_int=n, doc1_str=f'str{n}', doc1_emb_embdoc1=embdoc1_1).save()

    # field.$[embdoc1].field
    n += 1
    embdoc1_x = [
        Schema1EmbDoc1(embdoc1_int=n * 10 + 1, embdoc1_str=f'str{n * 10 + 1}'),
        Schema1EmbDoc1(embdoc1_int=n * 10 + 2, embdoc1_str=f'str{n * 10 + 2}'),
    ]
    doc1_1 = Schema1Doc1(doc1_int=n, doc1_str=f'str{n}', doc1_emblist_embdoc1=embdoc1_x).save()

    # field.$[embdoc1].field(array)
    n += 1
    embdoc1_x = [
        Schema1EmbDoc1(embdoc1_int=n * 10 + 1,
                       embdoc1_str=f'str{n * 10 + 1}',
                       embdoc1_list=[n * 10000 + 1001, f'str{n * 10000 + 1001}', None]),
        Schema1EmbDoc1(embdoc1_int=n * 10 + 2,
                       embdoc1_str=f'str{n * 10 + 2}',
                       embdoc1_list=[n * 10000 + 2001, f'str{n * 10000 + 2001}', None]),
    ]
    doc1_1 = Schema1Doc1(doc1_int=n, doc1_str=f'str{n}', doc1_emblist_embdoc1=embdoc1_x).save()

    items = (
        ('embdoc1', Schema1EmbDoc1, 'embdoc2', Schema1EmbDoc2),
        # ('embdoc2', Schema1EmbDoc2, 'embdoc1', Schema1EmbDoc1),
        ('embdoc1', Schema1EmbDoc1, 'embdoc1', Schema1EmbDoc1),
    )
    for field1, cls1, field2, cls2 in items:
        # field.$[embdoc1].$[embdoc2].field
        n += 1
        embdoc2_1 = [
            cls2(**{f'{field2}_int': n * 100 + 11, f'{field2}_str': f'str{n * 100 + 11}'}),
            cls2(**{f'{field2}_int': n * 100 + 12, f'{field2}_str': f'str{n * 100 + 12}'})
        ]
        embdoc2_2 = [
            cls2(**{f'{field2}_int': n * 100 + 21, f'{field2}_str': f'str{n * 100 + 21}'}),
            cls2(**{f'{field2}_int': n * 100 + 22, f'{field2}_str': f'str{n * 100 + 22}'})
        ]
        embdoc1_x = [
            cls1(**{
                f'{field1}_int': n * 10 + 1,
                f'{field1}_str': f'str{n * 10 + 1}',
                f'{field1}_emblist_{field2}': embdoc2_1
            }),
            cls1(**{
                f'{field1}_int': n * 10 + 2,
                f'{field1}_str': f'str{n * 10 + 2}',
                f'{field1}_emblist_{field2}': embdoc2_2
            })
        ]
        doc1_1 = Schema1Doc1(**{
            f'doc1_int': n, f'doc1_str': f'str{n}', f'doc1_emblist_{field1}': embdoc1_x
        }).save()

        # field.$[embdoc1].$[embdoc2].field(array)
        n += 1
        embdoc2_1 = [
            cls2(**{
                f'{field2}_int': n * 100 + 11,
                f'{field2}_str': f'str{n * 100 + 11}',
                f'{field2}_list': [n * 10000 + 1101, f'str{n * 10000 + 1101}', None]
            }),
            cls2(**{
                f'{field2}_int': n * 100 + 12,
                f'{field2}_str': f'str{n * 100 + 12}',
                f'{field2}_list': [n * 10000 + 1201, f'str{n * 10000 + 1201}', None]
            })
        ]
        embdoc2_2 = [
            cls2(**{
                f'{field2}_int': n * 100 + 21,
                f'{field2}_str': f'str{n * 100 + 21}',
                f'{field2}_list': [n * 10000 + 2101, f'str{n * 10000 + 2101}', None]
            }),
            cls2(**{
                f'{field2}_int': n * 100 + 22,
                f'{field2}_str': f'str{n * 100 + 22}',
                f'{field2}_list': [n * 10000 + 2201, f'str{n * 10000 + 2201}', None]
            })
        ]
        embdoc1_x = [
            cls1(**{
                f'{field1}_int': n * 10 + 1,
                f'{field1}_str': f'str{n * 10 + 1}',
                f'{field1}_emblist_{field2}': embdoc2_1
            }),
            cls1(**{
                f'{field1}_int': n * 10 + 2,
                f'{field1}_str': f'str{n * 10 + 2}',
                f'{field1}_emblist_{field2}': embdoc2_2
            })
        ]
        doc1_1 = Schema1Doc1(**{
            f'doc1_int': n, f'doc1_str': f'str{n}', f'doc1_emblist_{field1}': embdoc1_x
        }).save()

        # field.$[embdoc1].field(embdoc1).$[embdoc1].field
        n += 1
        embdoc1_1 = [
            cls1(**{f'{field1}_int': n * 1000 + 111, f'{field1}_str': f'str{n * 1000 + 111}'}),
            cls1(**{f'{field1}_int': n * 1000 + 112, f'{field1}_str': f'str{n * 1000 + 112}'})
        ]
        embdoc1_2 = [
            cls1(**{f'{field1}_int': n * 1000 + 211, f'{field1}_str': f'str{n * 1000 + 211}'}),
            cls1(**{f'{field1}_int': n * 1000 + 212, f'{field1}_str': f'str{n * 1000 + 212}'})
        ]
        embdoc1_3 = [
            cls1(**{
                f'{field1}_int': n * 100 + 11,
                f'{field1}_str': f'str{n * 100 + 11}',
                f'{field1}_emblist_{field1}': embdoc1_1
            }),
            cls1(**{
                f'{field1}_int': n * 100 + 21,
                f'{field1}_str': f'str{n * 100 + 21}',
                f'{field1}_emblist_{field1}': embdoc1_2
            })
        ]
        embdoc1_x = [
            cls1(**{
                f'{field1}_int': n * 10 + 1,
                f'{field1}_str': f'str{n * 10 + 1}',
                f'{field1}_emb_{field1}': embdoc1_3[0]
            }),
            cls1(**{
                f'{field1}_int': n * 10 + 2,
                f'{field1}_str': f'str{n * 10 + 2}',
                f'{field1}_emb_{field1}': embdoc1_3[1]
            })
        ]
        doc1_1 = Schema1Doc1(**{
            f'doc1_int': n, f'doc1_str': f'str{n}', f'doc1_emblist_{field1}': embdoc1_x
        }).save()

        # field.$[embdoc1].field(embdoc1).$[embdoc1].field(array)
        n += 1
        embdoc1_1 = [
            cls1(**{
                f'{field1}_int': n * 1000 + 111,
                f'{field1}_str': f'str{n * 1000 + 111}',
                f'{field1}_list': [n * 10000 + 1111, f'str{n * 10000 + 1111}', None]
            }),
            cls1(**{
                f'{field1}_int': n * 1000 + 112,
                f'{field1}_str': f'str{n * 1000 + 112}',
                f'{field1}_list': [n * 10000 + 1121, f'str{n * 10000 + 1121}', None]
            })
        ]
        embdoc1_2 = [
            cls1(**{
                f'{field1}_int': n * 1000 + 211,
                f'{field1}_str': f'str{n * 1000 + 211}',
                f'{field1}_list': [n * 10000 + 2111, f'str{n * 10000 + 2111}', None]
            }),
            cls1(**{
                f'{field1}_int': n * 1000 + 212,
                f'{field1}_str': f'str{n * 1000 + 212}',
                f'{field1}_list': [n * 10000 + 2121, f'str{n * 10000 + 2121}', None]
            })
        ]
        embdoc1_3 = [
            cls1(**{
                f'{field1}_int': n * 100 + 11,
                f'{field1}_str': f'str{n * 100 + 11}',
                f'{field1}_emblist_{field1}': embdoc1_1
            }),
            cls1(**{
                f'{field1}_int': n * 100 + 21,
                f'{field1}_str': f'str{n * 100 + 21}',
                f'{field1}_emblist_{field1}': embdoc1_2
            })
        ]
        embdoc1_x = [
            cls1(**{
                f'{field1}_int': n * 10 + 1,
                f'{field1}_str': f'str{n * 10 + 1}',
                f'{field1}_emb_{field1}': embdoc1_3[0]
            }),
            cls1(**{
                f'{field1}_int': n * 10 + 2,
                f'{field1}_str': f'str{n * 10 + 2}',
                f'{field1}_emb_{field1}': embdoc1_3[1]
            })
        ]
        doc1_1 = Schema1Doc1(**{
            f'doc1_int': n, f'doc1_str': f'str{n}', f'doc1_emblist_{field1}': embdoc1_x
        }).save()
