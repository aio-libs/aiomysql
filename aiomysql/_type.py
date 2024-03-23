from typing import NamedTuple, Optional


class Description(NamedTuple):
    #: the name of the column returned.
    name: str
    #: the type of the column.
    type_code: int
    #: the actual length of the column in bytes.
    display_size: Optional[int]
    #: the size in bytes of the column associated to this column on the server.
    internal_size: int
    #: total number of significant digits in columns of type NUMERIC. None for other types.
    precision: Optional[int]
    #: count of decimal digits in the fractional part in columns of type NUMERIC. None for other types.
    scale: Optional[int]
    #: always None as not easy to retrieve from the libpq.
    null_ok: Optional[bool]
