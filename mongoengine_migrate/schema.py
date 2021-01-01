__all__ = ['Schema']

from typing import Sequence

from mongoengine_migrate.exceptions import SchemaError
from mongoengine_migrate.utils import normalize_index_fields_spec


class SchemaAccessMixin:
    """Replace possible KeyError exceptions to SchemaError in dict key
    access methods
    """
    def __access(self, method, item, *args, **kwargs):
        try:
            return method(item, *args, **kwargs)
        except KeyError as e:
            raise SchemaError(f'Unknown key {item!r}') from e

    def __getitem__(self, item):
        return self.__access(super().__getitem__, item)

    def __delitem__(self, key):
        return self.__access(super().__delitem__, key)

    def pop(self, key, *args):
        return self.__access(super().pop, key, *args)

    def popitem(self):
        try:
            return super().popitem()
        except KeyError as e:
            raise SchemaError(f'Schema is empty') from e


class Schema(SchemaAccessMixin, dict):
    """Database schema wrapper"""
    class Document(SchemaAccessMixin, dict):
        class Parameters(SchemaAccessMixin, dict):
            pass

        class Indexes(SchemaAccessMixin, dict):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                for name, spec in self.items():
                    if isinstance(spec.get('fields'), Sequence):
                        self[name]['fields'] = list(normalize_index_fields_spec(spec['fields']))

        def __init__(self, *args, **kwargs):
            self.__parameters = kwargs.pop('parameters', Schema.Document.Parameters())
            self.__indexes = kwargs.pop('indexes', Schema.Document.Indexes())
            super().__init__(*args, **kwargs)

        @property
        def parameters(self) -> Parameters:
            return self.__parameters

        @property
        def indexes(self) -> Indexes:
            return self.__indexes

        def load(self, document_schema: dict):
            self.__parameters = Schema.Document.Parameters(document_schema.get('parameters', {}))
            self.__indexes = Schema.Document.Indexes(document_schema.get('indexes', {}))
            self.update(document_schema.get('fields', {}))
            return self

        def dump(self) -> dict:
            return {
                'fields': dict(self.items()),
                'parameters': self.__parameters,
                'indexes': self.__indexes
            }

        def __eq__(self, other):
            if self is other:
                return True
            if not isinstance(other, Schema.Document):
                return False

            return self.items() == other.items() \
                   and self.parameters == other.parameters \
                   and self.indexes == other.indexes

        def __ne__(self, other):
            return not self.__eq__(other)

        def __str__(self):
            return f'Document({super().__repr__()}, ' \
                   f'parameters={self.parameters!s}, indexes={self.indexes!s})'

        def __repr__(self):
            return f'Document({super().__repr__()}, ' \
                   f'parameters={self.parameters!r}, indexes={self.indexes!r})'

    def load(self, db_schema: dict):
        """Load schema from db dict schema representation"""
        self.update({name: Schema.Document().load(schema) for name, schema in db_schema.items()})
        return self

    def dump(self) -> dict:
        """Return schema representation for write to db"""
        return {name: doc.dump() for name, doc in self.items()}

    def __str__(self):
        return f'Schema({super().__repr__()})'

    def __repr__(self):
        return f'Schema({super().__repr__()})'
