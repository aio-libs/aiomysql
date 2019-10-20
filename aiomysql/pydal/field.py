class Field:
    def __init__(self, pydal, pydal_cursor, pydal_field) -> None:
        super().__init__()
        self._pydal_field = pydal_field
        self._pydal = pydal
        self._pydal_cursor = pydal_cursor

    def __eq__(self, other):
        return self._pydal_field.__eq__(other)

    def __gt__(self, other):
        return self._pydal_field.__gt__(other)

    def __ge__(self, other):
        return self._pydal_field.__ge__(other)

    def __lt__(self, other):
        return self._pydal_field.__lt__(other)

    def __le__(self, other):
        return self._pydal_field.__le__(other)

    def __invert__(self):
        return self._pydal_field.__invert__()

    def __str__(self):
        return self._pydal_field.__str__()

    def like(self, expression, case_sensitive=True, escape=None):
        return self._pydal_field.like(expression, case_sensitive, escape)
