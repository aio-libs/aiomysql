"""
aiomysql: A pure-Python MySQL client library for asyncio.

Copyright (c) 2010, 2013-2014 PyMySQL contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

"""
from pymysql.constants import FIELD_TYPE
from pymysql.converters import escape_dict, escape_sequence, escape_string
from pymysql.err import Warning, Error, InterfaceError, DataError, \
    DatabaseError, OperationalError, IntegrityError, InternalError, \
    NotSupportedError, ProgrammingError, MySQLError
from pymysql.times import Date, Time, Timestamp, \
    DateFromTicks, TimeFromTicks, TimestampFromTicks

from .connection import connect
from .pool import create_pool, Pool

__all__ = [
    'BINARY',
    'Connection',
    'DATE',
    'Date',
    'Time',
    'Timestamp',
    'DateFromTicks',
    'TimeFromTicks',
    'TimestampFromTicks',
    'DataError',
    'DatabaseError',
    'Error',
    'FIELD_TYPE',
    'IntegrityError',
    'InterfaceError',
    'InternalError',
    'MySQLError',
    'NULL',
    'NUMBER',
    'NotSupportedError',
    'DBAPISet',
    'OperationalError',
    'ProgrammingError',
    'ROWID',
    'STRING',
    'TIME',
    'TIMESTAMP',
    'Warning',
    'apilevel',
    'connections',
    'constants',
    'converters',
    'cursors',
    'escape_dict',
    'escape_sequence',
    'escape_string',
    'paramstyle',
    'threadsafety',
    'version_info',
    "NULL", "__version__",

    'connect',
    'create_pool',
    'Pool'
]


threadsafety = 1
apilevel = "2.0"
paramstyle = "format"

NULL = "NULL"

__version__ = '0.0.1'


class DBAPISet(frozenset):
    def __ne__(self, other):
        if isinstance(other, set):
            return super(DBAPISet, self).__ne__(self, other)
        else:
            return other not in self

    def __eq__(self, other):
        if isinstance(other, frozenset):
            return frozenset.__eq__(self, other)
        else:
            return other in self

    def __hash__(self):
        return frozenset.__hash__(self)
