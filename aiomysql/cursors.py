import asyncio

from pymysql.cursors import RE_INSERT_VALUES
from pymysql.err import (
    Warning, Error, InterfaceError, DataError,
    DatabaseError, OperationalError, IntegrityError, InternalError,
    NotSupportedError, ProgrammingError)


class Cursor:
    """Cursor is used to interact with the database."""

    #: Max stetement size which :meth:`executemany` generates.
    #:
    #: Max size of allowed statement is max_allowed_packet -
    # packet_header_size.
    #: Default value of max_allowed_packet is 1048576.
    max_stmt_length = 1024000

    def __init__(self, connection):
        """Do not create an instance of a Cursor yourself. Call
        connections.Connection.cursor().
        """
        self._connection = connection
        self._description = None
        self._rownumber = 0
        self._rowcount = -1
        self._arraysize = 1
        self._executed = None
        self._result = None
        self._rows = None
        self._lastrowid = None

    @property
    def connection(self):
        """This read-only attribute return a reference to the Connection
        object on which the cursor was created."""
        return self._connection

    @property
    def description(self):
        """This read-only attribute is a sequence of 7-item sequences.

        Each of these sequences is a collections.namedtuple containing
        information describing one result column:

        0.  name: the name of the column returned.
        1.  type_code: the type of the column.
        2.  display_size: the actual length of the column in bytes.
        3.  internal_size: the size in bytes of the column associated to
            this column on the server.
        4.  precision: total number of significant digits in columns of
            type NUMERIC. None for other types.
        5.  scale: count of decimal digits in the fractional part in
            columns of type NUMERIC. None for other types.
        6.  null_ok: always None as not easy to retrieve from the libpq.

        This attribute will be None for operations that do not
        return rows or if the cursor has not had an operation invoked
        via the execute() method yet.

        """
        return self._description

    @property
    def rowcount(self):
        """Returns the number of rows that has been produced of affected.

        This read-only attribute specifies the number of rows that the
        last :meth:`execute` produced (for Data Query Language
        statements like SELECT) or affected (for Data Manipulation
        Language statements like UPDATE or INSERT).

        The attribute is -1 in case no .execute() has been performed
        on the cursor or the row count of the last operation if it
        can't be determined by the interface.

        """
        return self._rowcount

    @property
    def rownumber(self):
        """Row index.

        This read-only attribute provides the current 0-based index of the
        cursor in the result set or ``None`` if the index cannot be
        determined."""

        return self._rownumber

    @property
    def arraysize(self):
        """How many rows will be returned by fetchmany() call.

        This read/write attribute specifies the number of rows to
        fetch at a time with fetchmany(). It defaults to
        1 meaning to fetch a single row at a time.

        """
        return self._arraysize

    @arraysize.setter
    def arraysize(self, val):
        """How many rows will be returned by fetchmany() call.

        This read/write attribute specifies the number of rows to
        fetch at a time with fetchmany(). It defaults to
        1 meaning to fetch a single row at a time.

        """
        self._arraysize = val

    @property
    def lastrowid(self):
        """This read-only property returns the value generated for an
        AUTO_INCREMENT column by the previous INSERT or UPDATE statement
        or None when there is no such value available. For example,
        if you perform an INSERT into a table that contains an AUTO_INCREMENT
        column, lastrowid returns the AUTO_INCREMENT value for the new row.
        """
        return self._lastrowid

    @asyncio.coroutine
    def close(self):
        """Closing a cursor just exhausts all remaining data."""
        conn = self._connection
        if conn is None:
            return
        try:
            while (yield from self.nextset()):
                pass
        finally:
            self._connection = None

    @property
    def closed(self):
        return True if not self._connection else False

    def _get_db(self):
        if not self._connection:
            raise ProgrammingError("Cursor closed")
        return self._connection

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
        return self._rowcount

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
                rows += self._rowcount
            self._rowcount = rows
        return self._rowcount

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
                rows += self._rowcount
                sql = bytearray(prefix)
            else:
                sql += b','
            sql += v
        yield from self.execute(bytes(sql))
        rows += self._rowcount
        self._rowcount = rows

    @asyncio.coroutine
    def callproc(self, procname, args=()):
        """Execute stored procedure procname with args

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

        :param procname: ``str``, name of procedure to execute on server
        :param args: `sequence of parameters to use with procedure
        :returns: the original args.
        """
        conn = self._get_db()
        for index, arg in enumerate(args):
            q = "SET @_%s_%d=%s" % (procname, index, conn.escape(arg))
            yield from self._query(q)
            yield from self.nextset()

        _args = ','.join('@_%s_%d' % (procname, i) for i in range(len(args)))
        q = "CALL %s(%s)" % (procname, _args)
        yield from self._query(q)
        self._executed = q
        return args

    def fetchone(self):
        """Fetch the next row """
        self._check_executed()
        if self._rows is None or self._rownumber >= len(self._rows):
            return None
        result = self._rows[self._rownumber]
        self._rownumber += 1
        return result

    def fetchmany(self, size=None):
        """ Fetch several rows """
        self._check_executed()
        if self._rows is None:
            return None
        end = self._rownumber + (size or self._arraysize)
        result = self._rows[self._rownumber:end]
        self._rownumber = min(end, len(self._rows))
        return result

    def fetchall(self):
        """Fetch all the rows """
        self._check_executed()
        if self._rows is None:
            return None
        if self._rownumber:
            result = self._rows[self._rownumber:]
        else:
            result = self._rows
        self._rownumber = len(self._rows)
        return result

    def scroll(self, value, mode='relative'):
        """Scroll the cursor in the result set to a new position according
         to mode.

        If mode is relative (default), value is taken as offset to the
        current position in the result set, if set to absolute, value
        states an absolute target position. An IndexError should be raised in
        case a scroll operation would leave the result set. In this case,
        the cursor position is left undefined (ideal would be to
        not move the cursor at all).
        """
        self._check_executed()
        if mode == 'relative':
            r = self._rownumber + value
        elif mode == 'absolute':
            r = value
        else:
            raise ProgrammingError("unknown scroll mode %s" % mode)

        if not (0 <= r < len(self._rows)):
            raise IndexError("out of range")
        self._rownumber = r

    @asyncio.coroutine
    def _query(self, q):
        conn = self._get_db()
        self._last_executed = q
        yield from conn.query(q)
        self._do_get_result()

    def _do_get_result(self):
        conn = self._get_db()
        self._rownumber = 0
        self._result = result = conn._result
        self._rowcount = result.affected_rows
        self._description = result.description
        self._lastrowid = result.insert_id
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


class DictCursor(Cursor):
    # You can override this to use OrderedDict or other dict-like types.
    dict_type = dict

    def _do_get_result(self):
        super()._do_get_result()
        fields = []
        if self._description:
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
