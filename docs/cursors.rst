Cursor
======

.. class:: Cursor

    A cursor for connection.

    Allows Python code to execute :term:`MySQL` command in a database
    session. Cursors are created by the :meth:`Connection.cursor` coroutine:
    they are bound to the connection for the entire lifetime and all
    the commands are executed in the context of the database session
    wrapped by the connection.

    Cursors that are created from the same connection are not isolated,
    i.e., any changes done to the database by a cursor are immediately
    visible by the other cursors. Cursors created from different
    connections can or can not be isolated, depending on the
    connections’ isolation level.

    Use :meth:`Connection.cursor()` for getting cursor for connection.


    .. attribute:: echo

        Return echo mode status.

   .. attribute:: description

        This read-only attribute is a sequence of 7-item sequences.

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

   .. attribute:: rowcount

        Returns the number of rows that has been produced of affected.

        This read-only attribute specifies the number of rows that the
        last :meth:`execute` produced (for Data Query Language
        statements like SELECT) or affected (for Data Manipulation
        Language statements like UPDATE or INSERT).

        The attribute is -1 in case no .execute() has been performed
        on the cursor or the row count of the last operation if it
        can't be determined by the interface.

   .. attribute:: rownumber

        Row index.

        This read-only attribute provides the current 0-based index of the
        cursor in the result set or ``None`` if the index cannot be
        determined.

   .. attribute:: arraysize

        How many rows will be returned by fetchmany() call.

        This read/write attribute specifies the number of rows to
        fetch at a time with fetchmany(). It defaults to
        1 meaning to fetch a single row at a time.

   .. attribute:: lastrowid

        This read-only property returns the value generated for an
        AUTO_INCREMENT column by the previous INSERT or UPDATE statement
        or None when there is no such value available. For example,
        if you perform an INSERT into a table that contains an AUTO_INCREMENT
        column, lastrowid returns the AUTO_INCREMENT value for the new row.

   .. attribute:: closed

        The readonly property that returns ``True`` if connections was detached
        from current cursor
