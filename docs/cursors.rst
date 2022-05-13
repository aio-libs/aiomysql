.. _aiomysql-cursors:

Cursor
======

.. class:: Cursor

    A cursor for connection.

    Allows Python code to execute :term:`MySQL` command in a database
    session. Cursors are created by the :meth:`Connection.cursor`
    :ref:`coroutine <coroutine>`: they are bound to the connection for
    the entire lifetime and all the commands are executed in the context
    of the database session wrapped by the connection.

    Cursors that are created from the same connection are not isolated,
    i.e., any changes done to the database by a cursor are immediately
    visible by the other cursors. Cursors created from different
    connections can or can not be isolated, depending on the
    connections’ isolation level.

    .. code:: python

        import asyncio
        import aiomysql

        loop = asyncio.get_event_loop()

        async def test_example():
            conn = await aiomysql.connect(host='127.0.0.1', port=3306,
                                          user='root', password='',
                                          db='mysql', loop=loop)

            # create default cursor
            cursor = await conn.cursor()

            # execute sql query
            await cursor.execute("SELECT Host, User FROM user")

            # fetch all results
            r = await cursor.fetchall()

            # detach cursor from connection
            await cursor.close()

            # close connection
            conn.close()

        loop.run_until_complete(test_example())


    Use :meth:`Connection.cursor()` for getting cursor for connection.

    .. attribute:: connection

        This read-only attribute return a reference to the :class:`Connection`
        object on which the cursor was created

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
            type ``NUMERIC``. None for other types.
        5.  scale: count of decimal digits in the fractional part in
            columns of type ``NUMERIC``. None for other types.
        6.  null_ok: always None.

        This attribute will be None for operations that do not
        return rows or if the cursor has not had an operation invoked
        via the :meth:`Cursor.execute()` method yet.

   .. attribute:: rowcount

        Returns the number of rows that has been produced of affected.

        This read-only attribute specifies the number of rows that the
        last :meth:`Cursor.execute()` produced (for Data Query Language
        statements like SELECT) or affected (for Data Manipulation
        Language statements like ``UPDATE`` or ``INSERT``).

        The attribute is -1 in case no :meth:`Cursor.execute()` has been
        performed on the cursor or the row count of the last operation if it
        can't be determined by the interface.

   .. attribute:: rownumber

        Row index. This read-only attribute provides the current 0-based index
        of the cursor in the result set or ``None`` if the index cannot be
        determined.

   .. attribute:: arraysize

        How many rows will be returned by :meth:`Cursor.fetchmany()` call.

        This read/write attribute specifies the number of rows to
        fetch at a time with :meth:`Cursor.fetchmany()`. It defaults to
        1 meaning to fetch a single row at a time.

   .. attribute:: lastrowid

        This read-only property returns the value generated for an
        `AUTO_INCREMENT` column by the previous `INSERT` or `UPDATE` statement
        or None when there is no such value available. For example,
        if you perform an `INSERT` into a table that contains an
        `AUTO_INCREMENT` column, :attr:`Cursor.lastrowid` returns the
        `AUTO_INCREMENT` value for the new row.

   .. attribute:: closed

        The readonly property that returns ``True`` if connections was detached
        from current cursor

   .. method:: close()

        :ref:`Coroutine <coroutine>` to close the cursor now (rather than
        whenever ``del`` is executed). The cursor will be unusable from this
        point forward; closing a cursor just exhausts all remaining data.

   .. method:: execute(query, args=None)

        :ref:`Coroutine <coroutine>`, executes the given operation substituting
        any markers with the given parameters.

        For example, getting all rows where id is 5::

            await cursor.execute("SELECT * FROM t1 WHERE id=%s", (5,))

        :param str query: sql statement
        :param list args: tuple or list of arguments for sql query
        :returns int: number of rows that has been produced of affected

   .. method:: executemany(query, args)

        The `executemany()` :ref:`coroutine <coroutine>` will execute the
        operation iterating over the list of parameters in seq_params.

        Example: Inserting 3 new employees and their phone number::

            data = [
                ('Jane','555-001'),
                ('Joe', '555-001'),
                ('John', '555-003')
               ]
            stmt = "INSERT INTO employees (name, phone)
                VALUES ('%s','%s')"
            await cursor.executemany(stmt, data)

        `INSERT` statements are optimized by batching the data, that is
        using the MySQL multiple rows syntax.

        :param str  query: sql statement
        :param list args: tuple or list of arguments for sql query

   .. method:: callproc(procname, args)

        Execute  stored procedure procname with args, this method is
        :ref:`coroutine <coroutine>`.

        Compatibility warning: PEP-249 specifies that any modified
        parameters must be returned. This is currently impossible
        as they are only available by storing them in a server
        variable and then retrieved by a query. Since stored
        procedures return zero or more result sets, there is no
        reliable way to get at OUT or INOUT parameters via `callproc`.
        The server variables are named `@_procname_n`, where `procname`
        is the parameter above and n is the position of the parameter
        (from zero). Once all result sets generated by the procedure
        have been fetched, you can issue a `SELECT @_procname_0`, ...
        query using :meth:`Cursor.execute()` to get any OUT or INOUT values.
        Basic usage example::

            conn = await aiomysql.connect(host='127.0.0.1', port=3306,
                                          user='root', password='',
                                          db='mysql', loop=self.loop)

            cur = await conn.cursor()
            await cur.execute("""CREATE PROCEDURE myinc(p1 INT)
                              BEGIN
                                  SELECT p1 + 1;
                              END
                              """)

            await cur.callproc('myinc', [1])
            (ret, ) = await cur.fetchone()
            assert 2, ret

            await cur.close()
            conn.close()

        Compatibility warning: The act of calling a stored procedure
        itself creates an empty result set. This appears after any
        result sets generated by the procedure. This is non-standard
        behavior with respect to the DB-API. Be sure to use
        :meth:`Cursor.nextset()` to advance through all result sets; otherwise
        you may get disconnected.

        :param str procname: name of procedure to execute on server
        :param args: sequence of parameters to use with procedure
        :returns: the original args.

   .. method:: fetchone()

        Fetch the next row :ref:`coroutine <coroutine>`.

   .. method:: fetchmany(size=None)

        :ref:`Coroutine <coroutine>` the next set of rows of a query result,
        returning a list of tuples. When no more rows are available, it
        returns an empty list.

        The number of rows to fetch per call is specified by the parameter.
        If it is not given, the cursor's :attr:`Cursor.arraysize` determines
        the number of rows to be fetched. The method should try to fetch as
        many rows as indicated by the size parameter. If this is not possible
        due to the specified number of rows not being available, fewer rows
        may be returned ::

            cursor = await connection.cursor()
            await cursor.execute("SELECT * FROM test;")
            r = cursor.fetchmany(2)
            print(r)
            # [(1, 100, "abc'def"), (2, None, 'dada')]
            r = await cursor.fetchmany(2)
            print(r)
            # [(3, 42, 'bar')]
            r = await cursor.fetchmany(2)
            print(r)
            # []

        :param int size: number of rows to return
        :returns list: of fetched rows

   .. method:: fetchall()

        :ref:`Coroutine <coroutine>` returns all rows of a query result set::

         await cursor.execute("SELECT * FROM test;")
         r = await cursor.fetchall()
         print(r)
         # [(1, 100, "abc'def"), (2, None, 'dada'), (3, 42, 'bar')]

        :returns list: list of fetched rows

   .. method:: scroll(value, mode='relative')

        Scroll the cursor in the result set to a new position according
        to mode. This method is :ref:`coroutine <coroutine>`.

        If mode is ``relative`` (default), value is taken as offset to the
        current position in the result set, if set to ``absolute``, value
        states an absolute target position. An IndexError should be raised in
        case a scroll operation would leave the result set. In this case,
        the cursor position is left undefined (ideal would be to
        not move the cursor at all).

        .. note::

            According to the :term:`DBAPI`, the exception raised for a cursor out
            of bound should have been :exc:`IndexError`.  The best option is
            probably to catch both exceptions in your code::

                try:
                    await cur.scroll(1000 * 1000)
                except (ProgrammingError, IndexError), exc:
                    deal_with_it(exc)

        :param int value: move cursor to next position according to mode.
        :param str mode: scroll mode, possible modes: `relative` and `absolute`


.. class:: DictCursor

    A cursor which returns results as a dictionary. All methods and arguments
    same as :class:`Cursor`, see example::

        import asyncio
        import aiomysql

        loop = asyncio.get_event_loop()

        async def test_example():
            conn = await aiomysql.connect(host='127.0.0.1', port=3306,
                                          user='root', password='',
                                          db='mysql', loop=loop)

            # create dict cursor
            cursor = await conn.cursor(aiomysql.DictCursor)

            # execute sql query
            await cursor.execute(
                "SELECT * from people where name='bob'")

            # fetch all results
            r = await cursor.fetchone()
            print(r)
            # {'age': 20, 'DOB': datetime.datetime(1990, 2, 6, 23, 4, 56),
            # 'name': 'bob'}

        loop.run_until_complete(test_example())

    You can customize your dictionary, see example::

        import asyncio
        import aiomysql

        class AttrDict(dict):
            """Dict that can get attribute by dot, and doesn't raise KeyError"""

            def __getattr__(self, name):
                try:
                    return self[name]
                except KeyError:
                    return None

        class AttrDictCursor(aiomysql.DictCursor):
            dict_type = AttrDict

        loop = asyncio.get_event_loop()

        async def test_example():
            conn = await aiomysql.connect(host='127.0.0.1', port=3306,
                                          user='root', password='',
                                          db='mysql', loop=loop)

            # create your dict cursor
            cursor = await conn.cursor(AttrDictCursor)

            # execute sql query
            await cursor.execute(
                "SELECT * from people where name='bob'")

            # fetch all results
            r = await cursor.fetchone()
            print(r)
            # {'age': 20, 'DOB': datetime.datetime(1990, 2, 6, 23, 4, 56),
            # 'name': 'bob'}
            print(r.age)
            # 20
            print(r.foo)
            # None

        loop.run_until_complete(test_example())


.. class:: SSCursor

    Unbuffered Cursor, mainly useful for queries that return a lot of
    data, or for connections to remote servers over a slow network.

    Instead of copying every row of data into a buffer, this will fetch
    rows as needed. The upside of this, is the client uses much less memory,
    and rows are returned much faster when traveling over a slow network,
    or if the result set is very big.

    There are limitations, though. The MySQL protocol doesn't support
    returning the total number of rows, so the only way to tell how many rows
    there are is to iterate over every row returned. Also, it currently isn't
    possible to scroll backwards, as only the current row is held in memory.
    All methods are the same as in :class:`Cursor` but with different
    behaviour.

   .. method:: fetchall()
        Same as :meth:`Cursor.fetchall` :ref:`coroutine <coroutine>`,
        useless for large queries, as all rows fetched one by one.

   .. method:: fetchmany(size=None, mode='relative')
        Same as :meth:`Cursor.fetchall`, but each row fetched one by one.

   .. method:: scroll(size=None)
        Same as :meth:`Cursor.scroll`, but move cursor on server side one by
        one. If you want to move 20 rows forward scroll will make 20 queries
        to move cursor. Currently only forward scrolling is supported.


.. class:: SSDictCursor

    An unbuffered cursor, which returns results as a dictionary.
