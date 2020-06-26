import datetime
import json
import struct

from pymysql.connections import (FieldDescriptorPacket, MysqlPacket)
from pymysql.constants import (COMMAND, FIELD_TYPE)
from pymysql.converters import through
from pymysql.err import Error

from .utils import _decide_encoding


class PreparedStatement(object):
    def __init__(self, connection, stmt_id, params, columns):
        self.connection = connection
        self.stmt_id = stmt_id
        self.params = params
        self.columns = columns
        self._rows = None
        self._rownumber = 0
        self._rowcount = 0

    async def execute(self, *args):
        if len(args) != len(self.params):
            raise Error("argument count doesn't match")
        self.connection._next_seq_id = 0
        data = struct.pack("!B", COMMAND.COM_STMT_EXECUTE)
        data += struct.pack("<I", self.stmt_id)
        # CURSOR_TYPE_NO_CURSOR
        data += struct.pack("!B", 0)
        # iteration count, always 1
        data += struct.pack("<I", 1)
        # TODO: params
        self.connection.write_packet(data)

        self._rows = await self._read_result()
        self._rowcount = len(self._rows)
        return self._rowcount

    async def fetchone(self):
        if self._rows is None:
            return None

        result = self._rows[self._rownumber]
        self._rownumber += 1
        return result

    async def fetchmany(self, size=1):
        if self._rows is None:
            return []

        end = self._rownumber + size
        result = self._rows[self._rownumber:end]
        self._rownumber = min(end, len(self._rows))

        return result

    async def fetchall(self):
        if self._rows is None:
            return []

        if self._rownumber:
            result = self._rows[self._rownumber:]
        else:
            result = self._rows
        self._rownumber = len(self._rows)
        return result

    async def _read_result(self):
        # noinspection PyProtectedMember
        packet = await self.connection._read_packet()
        columns = []
        columns_num = packet.read_uint8()
        # read column definitions
        for _ in range(columns_num):
            # noinspection PyProtectedMember
            columns.append(await self.connection._read_packet(FieldDescriptorPacket))
        # noinspection PyProtectedMember
        packet = await self.connection._read_packet()
        if not packet.is_eof_packet():
            raise Error("expecting EOF packet")
        converters = []
        for c in columns:
            encoding = _decide_encoding(
                self.connection.use_unicode, self.connection.encoding, c)
            converter = self.connection.decoders.get(c.type_code)
            if converter is through:
                converter = None
            converters.append((encoding, converter))
        result = []
        while True:
            # noinspection PyProtectedMember
            packet = await self.connection._read_packet(BinaryResultSetPacket)
            if packet.is_eof_packet():
                return result
            packet.set_columns(columns)
            packet.set_converters(converters)
            result.append(packet.get_result())


_string_types = {
    FIELD_TYPE.STRING, FIELD_TYPE.VARCHAR, FIELD_TYPE.VAR_STRING, FIELD_TYPE.ENUM,
    FIELD_TYPE.SET, FIELD_TYPE.LONG_BLOB, FIELD_TYPE.MEDIUM_BLOB, FIELD_TYPE.BLOB,
    FIELD_TYPE.TINY_BLOB, FIELD_TYPE.GEOMETRY, FIELD_TYPE.BIT, FIELD_TYPE.DECIMAL,
    FIELD_TYPE.NEWDECIMAL, FIELD_TYPE.JSON,
}
_date_types = {
    FIELD_TYPE.DATE, FIELD_TYPE.DATETIME, FIELD_TYPE.TIMESTAMP,
}


class BinaryResultSetPacket(MysqlPacket):
    def __init__(self, data, encoding):
        super().__init__(data, encoding)
        self._columns = []
        self._converters = {}

    def get_result(self):
        if self.is_eof_packet():
            return []
        # header
        self.advance(1)
        # parse binary result row
        # https://dev.mysql.com/doc/internals/en/binary-protocol-resultset-row.html
        null_bitmap_len = (len(self._columns) + 7 + 2) >> 3
        null_bitmap = self.read(null_bitmap_len)
        result = []
        for i, c in enumerate(self._columns):
            if null_bitmap[(i + 2) >> 3] & (1 << ((i + 2) % 8)):
                result.append(None)
                continue
            # https://dev.mysql.com/doc/internals/en/binary-protocol-value.html
            if c.type_code in _string_types:
                n, is_none = self._read_length_encoded_integer()
                if is_none:
                    result.append(None)
                    continue
                data = self.read(n)
                encoding, converter = self._converters[i]
                if encoding is not None:
                    data = data.decode(encoding)
                if converter is not None:
                    data = converter(data)
                result.append(data)
                continue
            if c.type_code == FIELD_TYPE.LONGLONG:
                result.append(self.read_struct("<q")[0])
                continue
            if c.type_code in {FIELD_TYPE.LONG, FIELD_TYPE.INT24}:
                result.append(self.read_struct("<i")[0])
                continue
            if c.type_code in {FIELD_TYPE.SHORT, FIELD_TYPE.YEAR}:
                result.append(self.read_struct("<h")[0])
                continue
            if c.type_code == FIELD_TYPE.TINY:
                result.append(self.read_struct("<b")[0])
                continue
            if c.type_code == FIELD_TYPE.DOUBLE:
                result.append(self.read_struct("<d")[0])
                continue
            if c.type_code == FIELD_TYPE.FLOAT:
                result.append(self.read_struct("<f")[0])
                continue
            if c.type_code in _date_types:
                n = self.read_uint8()
                if n == 0:
                    result.append(None)
                    continue
                if n not in {4, 7, 11}:
                    raise Error("unexpected data")
                year = self.read_uint16()
                month = self.read_uint8()
                day = self.read_uint8()
                if n == 4:
                    result.append(datetime.date(year, month, day))
                    continue
                hour = self.read_uint8()
                minute = self.read_uint8()
                second = self.read_uint8()
                if n == 7:
                    result.append(
                        datetime.datetime(year, month, day, hour, minute, second))
                    continue
                microsecond = self.read_uint32()
                if n == 11:
                    result.append(
                        datetime.datetime(
                            year, month, day, hour, minute, second, microsecond))
                    continue
            if c.type_code == FIELD_TYPE.TIME:
                n = self.read_uint8()
                if n == 0:
                    result.append(None)
                    continue
                if n not in {8, 12}:
                    raise Error("unexpected data")
                negate = -1 if self.read_uint8() == 1 else 1
                days = self.read_uint32()
                hour = self.read_uint8()
                minute = self.read_uint8()
                second = self.read_uint8()
                time_delta = datetime.timedelta(
                    days=days, seconds=second, minutes=minute, hours=hour) * negate
                if n == 8:
                    result.append(time_delta)
                    continue
                time_delta += datetime.timedelta(microseconds=self.read_uint32())
                result.append(time_delta)
                continue

        return tuple(result)

    def set_columns(self, columns):
        self._columns = columns

    def set_converters(self, converters):
        self._converters = converters

    def _read_length_encoded_integer(self):
        n = self.read_uint8()
        if n < 0xfb:
            return n, False
        if n == 0xfb:
            return 0, True
        if n == 0xfc:
            return self.read_uint16(), False
        if n == 0xfd:
            return self.read_uint24(), False
        if n == 0xfe:
            return self.read_uint64(), False
        raise Error("unexpected value")
