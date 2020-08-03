import itertools

import pytest
from bson import ObjectId

from mongoengine_migrate.exceptions import MigrationError
from mongoengine_migrate.fields import converters
from mongoengine_migrate.updater import DocumentUpdater


def test_deny__should_raise_error(test_db, load_fixture):
    schema = load_fixture('schema1').get_schema()
    updater = DocumentUpdater(test_db, 'Schema1Doc1', schema, 'doc1_str')

    with pytest.raises(MigrationError):
        converters.deny(updater)


@pytest.mark.parametrize('document_type,field_name', (
        ('Schema1Doc1', 'doc1_str'),
        ('~Schema1EmbDoc1', 'embdoc1_str'),
        ('~Schema1EmbDoc2', 'embdoc2_str')
))
def test_drop_field__should_drop_field(test_db, load_fixture, document_type, field_name, dump_db):
    schema = load_fixture('schema1').get_schema()
    updater = DocumentUpdater(test_db, document_type, schema, field_name)

    expect = dump_db()
    parsers = load_fixture('schema1').get_embedded_jsonpath_parsers(document_type)
    for doc in itertools.chain.from_iterable(p.find(expect) for p in parsers):
        del doc.value[field_name]

    converters.drop_field(updater)

    assert dump_db() == expect


@pytest.mark.parametrize('document_type,field_name', (
        ('Schema1Doc1', 'doc1_str'),
        ('~Schema1EmbDoc1', 'embdoc1_str'),
        ('~Schema1EmbDoc2', 'embdoc2_str'),
        ('Schema1Doc1', 'doc1_list'),
        ('~Schema1EmbDoc1', 'embdoc1_list'),
        ('~Schema1EmbDoc2', 'embdoc2_list'),
))
def test_item_to_list__should_wrap_value_in_a_list_with_single_element(
        test_db, load_fixture, document_type, field_name, dump_db
):
    schema = load_fixture('schema1').get_schema()
    updater = DocumentUpdater(test_db, document_type, schema, field_name)

    expect = dump_db()
    parsers = load_fixture('schema1').get_embedded_jsonpath_parsers(document_type)
    for doc in itertools.chain.from_iterable(p.find(expect) for p in parsers):
        doc.value[field_name] = [doc.value[field_name]]

    converters.item_to_list(updater)

    assert dump_db() == expect


@pytest.mark.parametrize('document_type,field_name', (
        ('Schema1Doc1', 'doc1_list'),
        ('~Schema1EmbDoc1', 'embdoc1_list'),
        ('~Schema1EmbDoc2', 'embdoc2_list')
))
def test_extract_from_list__should_extract_the_first_value_from_list(
        test_db, load_fixture, document_type, field_name, dump_db
):
    schema = load_fixture('schema1').get_schema()
    updater = DocumentUpdater(test_db, document_type, schema, field_name)

    expect = dump_db()
    parsers = load_fixture('schema1').get_embedded_jsonpath_parsers(document_type)
    for doc in itertools.chain.from_iterable(p.find(expect) for p in parsers):
        if len(doc.value[field_name]):
            doc.value[field_name] = doc.value[field_name][0]
        else:
            doc.value[field_name] = None

    converters.extract_from_list(updater)

    assert dump_db() == expect


@pytest.mark.parametrize('document_type,field_name', (
        ('Schema1Doc1', 'doc1_int'),
        ('~Schema1EmbDoc1', 'embdoc1_int'),
        ('~Schema1EmbDoc2', 'embdoc2_int')
))
def test_extract_from_list__if_value_is_not_list__should_raise_error(
        test_db, load_fixture, document_type, field_name, dump_db
):
    schema = load_fixture('schema1').get_schema()
    updater = DocumentUpdater(test_db, document_type, schema, field_name)

    with pytest.raises(MigrationError):
        converters.extract_from_list(updater)


@pytest.mark.parametrize('document_type,field_name', (
        ('Schema1Doc1', 'doc1_int'),
        ('~Schema1EmbDoc1', 'embdoc1_int'),
        ('~Schema1EmbDoc2', 'embdoc2_int'),
        ('Schema1Doc1', 'doc1_list'),
        ('~Schema1EmbDoc1', 'embdoc1_list'),
        ('~Schema1EmbDoc2', 'embdoc2_list')
))
def test_to_string__should_convert_to_string(
        test_db, load_fixture, document_type, field_name, dump_db
):
    schema = load_fixture('schema1').get_schema()
    updater = DocumentUpdater(test_db, document_type, schema, field_name)

    expect = dump_db()
    parsers = load_fixture('schema1').get_embedded_jsonpath_parsers(document_type)
    for doc in itertools.chain.from_iterable(p.find(expect) for p in parsers):
        doc.value[field_name] = str(doc.value[field_name])

    converters.to_string(updater)

    assert dump_db() == expect


@pytest.mark.parametrize('document_type,field_name', (
        ('Schema1Doc1', 'doc1_str_ten'),
        ('~Schema1EmbDoc1', 'embdoc1_str_ten'),
        ('~Schema1EmbDoc2', 'embdoc2_str_ten')
))
def test_to_int__should_convert_to_int(
        test_db, load_fixture, document_type, field_name, dump_db
):
    schema = load_fixture('schema1').get_schema()
    updater = DocumentUpdater(test_db, document_type, schema, field_name)

    expect = dump_db()
    parsers = load_fixture('schema1').get_embedded_jsonpath_parsers(document_type)
    for doc in itertools.chain.from_iterable(p.find(expect) for p in parsers):
        doc.value[field_name] = int(doc.value[field_name])

    converters.to_int(updater)

    assert dump_db() == expect


@pytest.mark.parametrize('document_type,field_name', (
        ('Schema1Doc1', 'doc1_str'),
        ('~Schema1EmbDoc1', 'embdoc1_str'),
        ('~Schema1EmbDoc2', 'embdoc2_str'),
        ('Schema1Doc1', 'doc1_list'),
        ('~Schema1EmbDoc1', 'embdoc1_list'),
        ('~Schema1EmbDoc2', 'embdoc2_list')
))
def test_to_int__if_value_does_not_contain_number__should_raise_error(
        test_db, load_fixture, document_type, field_name
):
    schema = load_fixture('schema1').get_schema()
    updater = DocumentUpdater(test_db, document_type, schema, field_name)

    with pytest.raises(MigrationError):
        converters.to_int(updater)


@pytest.mark.parametrize('document_type,field_name', (
        ('Schema1Doc1', 'doc1_str_ten'),
        ('~Schema1EmbDoc1', 'embdoc1_str_ten'),
        ('~Schema1EmbDoc2', 'embdoc2_str_ten')
))
def test_to_long__should_convert_to_int(
        test_db, load_fixture, document_type, field_name, dump_db
):
    schema = load_fixture('schema1').get_schema()
    updater = DocumentUpdater(test_db, document_type, schema, field_name)

    expect = dump_db()
    parsers = load_fixture('schema1').get_embedded_jsonpath_parsers(document_type)
    for doc in itertools.chain.from_iterable(p.find(expect) for p in parsers):
        doc.value[field_name] = int(doc.value[field_name])

    converters.to_long(updater)

    assert dump_db() == expect


@pytest.mark.parametrize('document_type,field_name', (
        ('Schema1Doc1', 'doc1_str'),
        ('~Schema1EmbDoc1', 'embdoc1_str'),
        ('~Schema1EmbDoc2', 'embdoc2_str'),
        ('Schema1Doc1', 'doc1_list'),
        ('~Schema1EmbDoc1', 'embdoc1_list'),
        ('~Schema1EmbDoc2', 'embdoc2_list')
))
def test_to_long__if_value_does_not_contain_number__should_raise_error(
        test_db, load_fixture, document_type, field_name
):
    schema = load_fixture('schema1').get_schema()
    updater = DocumentUpdater(test_db, document_type, schema, field_name)

    with pytest.raises(MigrationError):
        converters.to_long(updater)


@pytest.mark.parametrize('document_type,field_name', (
        ('Schema1Doc1', 'doc1_str_ten'),
        ('~Schema1EmbDoc1', 'embdoc1_str_ten'),
        ('~Schema1EmbDoc2', 'embdoc2_str_ten'),
        ('Schema1Doc1', 'doc1_float'),
        ('~Schema1EmbDoc1', 'embdoc1_float'),
        ('~Schema1EmbDoc2', 'embdoc2_float'),
))
def test_to_double__should_convert_to_float(
        test_db, load_fixture, document_type, field_name, dump_db
):
    schema = load_fixture('schema1').get_schema()
    updater = DocumentUpdater(test_db, document_type, schema, field_name)

    expect = dump_db()
    parsers = load_fixture('schema1').get_embedded_jsonpath_parsers(document_type)
    for doc in itertools.chain.from_iterable(p.find(expect) for p in parsers):
        if field_name in doc.value:
            doc.value[field_name] = float(doc.value[field_name])

    converters.to_double(updater)

    assert dump_db() == expect


@pytest.mark.parametrize('document_type,field_name', (
        ('Schema1Doc1', 'doc1_str'),
        ('~Schema1EmbDoc1', 'embdoc1_str'),
        ('~Schema1EmbDoc2', 'embdoc2_str'),
        ('Schema1Doc1', 'doc1_list'),
        ('~Schema1EmbDoc1', 'embdoc1_list'),
        ('~Schema1EmbDoc2', 'embdoc2_list')
))
def test_to_double__if_value_does_not_contain_number__should_raise_error(
        test_db, load_fixture, document_type, field_name
):
    schema = load_fixture('schema1').get_schema()
    updater = DocumentUpdater(test_db, document_type, schema, field_name)

    with pytest.raises(MigrationError):
        converters.to_double(updater)


@pytest.mark.parametrize('document_type,field_name', (
        ('Schema1Doc1', 'doc1_str_ten'),
        ('~Schema1EmbDoc1', 'embdoc1_str_ten'),
        ('~Schema1EmbDoc2', 'embdoc2_str_ten')
))
def test_to_decimal__should_convert_to_float(
        test_db, load_fixture, document_type, field_name, dump_db
):
    schema = load_fixture('schema1').get_schema()
    updater = DocumentUpdater(test_db, document_type, schema, field_name)

    expect = dump_db()
    parsers = load_fixture('schema1').get_embedded_jsonpath_parsers(document_type)
    for doc in itertools.chain.from_iterable(p.find(expect) for p in parsers):
        if field_name in doc.value:
            doc.value[field_name] = float(doc.value[field_name])

    converters.to_decimal(updater)

    assert dump_db() == expect


@pytest.mark.parametrize('document_type,field_name', (
        ('Schema1Doc1', 'doc1_str'),
        ('~Schema1EmbDoc1', 'embdoc1_str'),
        ('~Schema1EmbDoc2', 'embdoc2_str'),
        ('Schema1Doc1', 'doc1_list'),
        ('~Schema1EmbDoc1', 'embdoc1_list'),
        ('~Schema1EmbDoc2', 'embdoc2_list')
))
def test_to_decimal__if_value_does_not_contain_number__should_raise_error(
        test_db, load_fixture, document_type, field_name
):
    schema = load_fixture('schema1').get_schema()
    updater = DocumentUpdater(test_db, document_type, schema, field_name)

    with pytest.raises(MigrationError):
        converters.to_decimal(updater)
