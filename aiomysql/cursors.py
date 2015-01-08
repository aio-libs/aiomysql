import asyncio
import re

from pymysql.err import (
    Warning, Error, InterfaceError, DataError,
    DatabaseError, OperationalError, IntegrityError, InternalError,
    NotSupportedError, ProgrammingError)


# : Regular expression for :meth:`Cursor.executemany`.
#: executemany only suports simple bulk insert.
#: You can use it to load large dataset.
RE_INSERT_VALUES = re.compile(
    r"""INSERT\s.+\sVALUES\s+(\(\s*%s\s*(,\s*%s\s*)*\))\s*\Z""",
    re.IGNORECASE | re.DOTALL)


class Cursor(object):
    """Cursor is used to interact with the database."""

    #: Max stetement size which :meth:`executemany` generates.
    #:
    #: Max size of allowed statement is max_allowed_packet -
    # packet_header_size.
    #: Default value of max_allowed_packet is 1048576.
    max_stmt_length = 1024000

    def __init__(self, connection):
        '''
        Do not create an instance of a Cursor yourself. Call
        connections.Connection.cursor().
        '''
        self.connection = connection
        self.description = None
        self.rownumber = 0
        self.rowcount = -1
        self.arraysize = 1
        self._executed = None
        self._result = None
        self._rows = None

    def __del__(self):
        '''
        When this gets GC'd close it.
        '''
        self.close()

    @asyncio.coroutine
    def close(self):
        """Closing a cursor just exhausts all remaining data."""
        conn = self.connection
        if conn is None:
            return
        try:
            while (yield from self.nextset()):
                pass
        finally:
            self.connection = None

    def _get_db(self):
        if not self.connection:
            raise ProgrammingError("Cursor closed")
        return self.connection

    def _check_executed(self):
        if not self._executed:
            raise ProgrammingError("execute() first")

    def _conv_row(self, row):
        return row

    def setinputsizes(self, *args):
        """Does nothing, required by DB API."""

    def setoutputsizes(self, *args):
        """Does nothing, required by DB API."""

    @asyncio.coroutine
    def nextset(self):
        """Get the next query set"""
        conn = self._get_db()
        current_result = self._result
        if current_result is None or current_result is not conn._result:
            return
        if not current_result.has_next:
            return
        yield from conn.next_result()
        self._do_get_result()
        return True

    def _escape_args(self, args, conn):
        if isinstance(args, (tuple, list)):
            return tuple(conn.escape(arg) for arg in args)
        elif isinstance(args, dict):
            return dict((key, conn.escape(val)) for (key, val) in args.items())
        else:
            # If it's not a dictionary let's try escaping it anyways.
            # Worst case it will throw a Value error
            return conn.escape(args)

    @asyncio.coroutine
    def execute(self, query, args=None):
        """Execute a query"""
        conn = self._get_db()

        while (yield from self.nextset()):
            pass

        if args is not None:
            query = query % self._escape_args(args, conn)

        yield from self._query(query)
        self._executed = query
        return self.rowcount

    @asyncio.coroutine
    def executemany(self, query, args):
        """Run several data against one query

        PyMySQL can execute bulkinsert for query like 'INSERT ... VALUES (%s)'.
        In other form of queries, just run :meth:`execute` many times.
        """
        if not args:
            return

        m = RE_INSERT_VALUES.match(query)
        if m:
            q_values = m.group(1).rstrip()
            assert q_values[0] == '(' and q_values[-1] == ')'
            q_prefix = query[:m.start(1)]
            yield from self._do_execute_many(q_prefix, q_values, args,
                                             self.max_stmt_length,
                                             self._get_db().encoding)
        else:
            rows = 0
            for arg in args:
                yield from self.execute(query, arg)
                rows += self.rowcount
            self.rowcount = rows
        return self.rowcount

    @asyncio.coroutine
    def _do_execute_many(self, prefix, values, args, max_stmt_length,
                         encoding):
        conn = self._get_db()
        escape = self._escape_args
        if isinstance(prefix, str):
            prefix = prefix.encode(encoding)
        sql = bytearray(prefix)
        args = iter(args)
        v = values % escape(next(args), conn)
        if isinstance(v, str):
            v = v.encode(encoding)
        sql += v
        rows = 0
        for arg in args:
            v = values % escape(arg, conn)
            if isinstance(v, str):
                v = v.encode(encoding)
            if len(sql) + len(v) + 1 > max_stmt_length:
                print(sql)
                yield from self.execute(bytes(sql))
                rows += self.rowcount
                sql = bytearray(prefix)
            else:
                sql += b','
            sql += v
        yield from self.execute(bytes(sql))
        rows += self.rowcount
        self.rowcount = rows

    @asyncio.coroutine
    def callproc(self, procname, args=()):
        """Execute stored procedure procname with args

        procname -- string, name of procedure to execute on server

        args -- Sequence of parameters to use with procedure

        Returns the original args.

        Compatibility warning: PEP-249 specifies that any modified
        parameters must be returned. This is currently impossible
        as they are only available by storing them in a server
        variable and then retrieved by a query. Since stored
        procedures return zero or more result sets, there is no
        reliable way to get at OUT or INOUT parameters via callproc.
        The server variables are named @_procname_n, where procname
        is the parameter above and n is the position of the parameter
        (from zero). Once all result sets generated by the procedure
        have been fetched, you can issue a SELECT @_procname_0, ...
        query using .execute() to get any OUT or INOUT values.

        Compatibility warning: The act of calling a stored procedure
        itself creates an empty result set. This appears after any
        result sets generated by the procedure. This is non-standard
        behavior with respect to the DB-API. Be sure to use nextset()
        to advance through all result sets; otherwise you may get
        disconnected.
        """
        conn = self._get_db()
        for index, arg in enumerate(args):
            q = "SET @_%s_%d=%s" % (procname, index, conn.escape(arg))
            yield from self._query(q)
            yield from self.nextset()

        q = "CALL %s(%s)" % (procname,
                             ','.join(['@_%s_%d' % (procname, i)
                                       for i in range(len(args))]))
        yield from self._query(q)
        self._executed = q
        return args

    def fetchone(self):
        ''' Fetch the next row '''
        self._check_executed()
        if self._rows is None or self.rownumber >= len(self._rows):
            return None
        result = self._rows[self.rownumber]
        self.rownumber += 1
        return result

    def fetchmany(self, size=None):
        ''' Fetch several rows '''
        self._check_executed()
        if self._rows is None:
            return None
        end = self.rownumber + (size or self.arraysize)
        result = self._rows[self.rownumber:end]
        self.rownumber = min(end, len(self._rows))
        return result

    def fetchall(self):
        ''' Fetch all the rows '''
        self._check_executed()
        if self._rows is None:
            return None
        if self.rownumber:
            result = self._rows[self.rownumber:]
        else:
            result = self._rows
        self.rownumber = len(self._rows)
        return result

    def scroll(self, value, mode='relative'):
        self._check_executed()
        if mode == 'relative':
            r = self.rownumber + value
        elif mode == 'absolute':
            r = value
        else:
            raise ProgrammingError("unknown scroll mode %s" % mode)

        if not (0 <= r < len(self._rows)):
            raise IndexError("out of range")
        self.rownumber = r

    @asyncio.coroutine
    def _query(self, q):
        conn = self._get_db()
        self._last_executed = q
        yield from conn.query(q)
        self._do_get_result()

    def _do_get_result(self):
        conn = self._get_db()

        self.rownumber = 0
        self._result = result = conn._result

        self.rowcount = result.affected_rows
        self.description = result.description
        self.lastrowid = result.insert_id
        self._rows = result.rows

    def __iter__(self):
        return iter(self.fetchone, None)

    Warning = Warning
    Error = Error
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError


class DictCursorMixin(object):
    # You can override this to use OrderedDict or other dict-like types.
    dict_type = dict

    def _do_get_result(self):
        super(DictCursorMixin, self)._do_get_result()
        fields = []
        if self.description:
            for f in self._result.fields:
                name = f.name
                if name in fields:
                    name = f.table_name + '.' + name
                fields.append(name)
            self._fields = fields

        if fields and self._rows:
            self._rows = [self._conv_row(r) for r in self._rows]

    def _conv_row(self, row):
        if row is None:
            return None
        return self.dict_type(zip(self._fields, row))


class DictCursor(DictCursorMixin, Cursor):
    """A cursor which returns results as a dictionary"""
