class Row:
    def __init__(self, field_names, raw_data) -> None:
        super().__init__()
        self._raw_data = raw_data
        self._row_data = {}
        self._field_names = field_names
        for k, v in enumerate(field_names):
            self._row_data[v] = raw_data[k]

    @property
    def field_names(self):
        return self._field_names

    @property
    def raw_data(self):
        return self._raw_data

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return self._row_data.__str__()


class Rows:
    def __init__(self, description, raw_data) -> None:
        super().__init__()
        self._rows = []
        self._field_names = []
        self._raw_data = raw_data
        for f in description:
            self._field_names.append(f[0])
        for r in raw_data:
            self._rows.append(Row(self._field_names, r))

    @property
    def rows(self):
        return self._rows

    @property
    def field_names(self):
        return self._field_names

    @property
    def raw_data(self):
        return self._raw_data

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return self._rows.__str__()

    def __getitem__(self, key):
        return self.rows[key]
