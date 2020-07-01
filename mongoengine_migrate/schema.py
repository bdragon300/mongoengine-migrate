class Schema(dict):
    """Database schema wrapper"""
    class Document(dict):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.__parameters = {}

        @property
        def parameters(self) -> dict:
            return self.__parameters

        def load(self, document_schema: dict):
            self.__parameters = document_schema.get('parameters', {})
            self.update(document_schema.get('fields', {}))
            return self

        def dump(self) -> dict:
            return {'fields': dict(self.items()), 'parameters': self.__parameters}

        def __eq__(self, other):
            if self is other:
                return True
            if not isinstance(other, Schema.Document):
                return False

            return self.items() == other.items() and self.parameters == other.parameters

        def __ne__(self, other):
            return not self.__eq__(other)

        def __str__(self):
            return f'Document({super().__str__()}, parameters={self.parameters!s}'

        def __repr__(self):
            return f'Document({super().__repr__()}, parameters={self.parameters!r}'

    def load(self, db_schema: dict):
        """Load schema from db dict schema representation"""
        self.update({name: Schema.Document().load(schema) for name, schema in db_schema.items()})
        return self

    def dump(self) -> dict:
        """Return schema representation for write to db"""
        return {name: doc.dump() for name, doc in self.items()}
