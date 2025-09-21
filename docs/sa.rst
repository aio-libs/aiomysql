.. _aiomysql-sa:

:mod:`aiomysql.sa` --- support for SQLAlchemy functional SQL layer
==================================================================

.. module:: aiomysql.sa
    :synopsis: support for SQLAlchemy functional SQL layer
.. currentmodule:: aiomysql.sa


Intro
-----
.. note::  :term:`sqlalchemy` support ported from aiopg_, so api should be
           very familiar for aiopg_ user.

While :ref:`core API <aiomysql-index>` provides a core support for
access to :term:`MySQL` database, manipulations with raw SQL strings
too annoying.

Fortunately we can use excellent :ref:`aiomysql <aiomysql-index>` as **SQL query builder**.


Example::

    import asyncio
    import sqlalchemy as sa

    from aiomysql.sa import create_engine


    metadata = sa.MetaData()

    tbl = sa.Table(
        "tbl",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("val", sa.String(255)),
    )


    async def go():
        engine = await create_engine(
            user="root",
            db="test_pymysql",
            host="127.0.0.1",
            password="",
        )

        async with engine.acquire() as conn:
            async with conn.begin() as transaction:
                await conn.execute(tbl.insert().values(val="abc"))
                await transaction.commit()

                res = await conn.execute(tbl.select())
                async for row in res:
                    print(row.id, row.val)

        engine.close()
        await engine.wait_closed()


    asyncio.run(go())


So you can execute SQL query built by
``tbl.insert().values(val='abc')`` or ``tbl.select()`` expressions.

:term:`sqlalchemy` has rich and very powerful set of SQL construction
functions, please read :ref:`tutorial <aiomysql-index>` for full list
of available operations.

Also we provide SQL transactions support. Please take a look on
:meth:`SAConnection.begin` method and family.


.. _aiopg: https://github.com/aio-libs/aiopg

Engine
------

.. function:: create_engine(*, minsize=1, maxsize=10, loop=None, \
                            dialect=dialect, **kwargs)

    A :ref:`coroutine <coroutine>` for :class:`Engine` creation.

    Returns :class:`Engine` instance with embedded connection pool.

    The pool has *minsize* opened connections to :term:`MySQL` server.

    At *kwargs* function accepts all parameters that
    :func:`aiomysql.connect` does.


.. data:: dialect

    An instance of :term:`SQLAlchemy` dialect set up for :term:`pymysql` usage.

    An :class:`sqlalchemy.engine.interfaces.Dialect` instance.

    .. seealso:: :mod:`sqlalchemy.dialects.mysql.pymysql`
                 PyMySQL dialect.


.. class:: Engine

    Connects a :class:`aiomysql.Pool` and
    :class:`sqlalchemy.engine.interfaces.Dialect` together to provide a
    source of database connectivity and behavior.

    An :class:`Engine` object is instantiated publicly using the
    :func:`create_engine` coroutine.


    .. attribute:: dialect

        A :class:`sqlalchemy.engine.interfaces.Dialect` for the engine,
        readonly property.

    .. attribute:: name

        A name of the dialect, readonly property.

    .. attribute:: driver

        A driver of the dialect, readonly property.

    .. attribute:: minsize

        A minimal size of the pool (*read-only*), ``1`` by default.

    .. attribute:: maxsize

        A maximal size of the pool (*read-only*), ``10`` by default.

    .. attribute:: size

        A current size of the pool (*readonly*). Includes used and free
        connections.

    .. attribute:: freesize

        A count of free connections in the pool (*readonly*).

    .. method:: close()

        Close engine.

        Mark all engine connections to be closed on getting back to engine.
        Closed engine doesn't allow to acquire new connections.

        If you want to wait for actual closing of acquired connection please
        call :meth:`wait_closed` after :meth:`close`.

      .. warning:: The method is not a :ref:`coroutine <coroutine>`.

    .. method:: terminate()

        Terminate engine.

        Close engine's pool with instantly closing all acquired connections
        also.

        :meth:`wait_closed` should be called after :meth:`terminate` for
        waiting for actual finishing.

      .. warning:: The method is not a :ref:`coroutine <coroutine>`.

    .. method:: wait_closed()

        A :ref:`coroutine <coroutine>` that waits for releasing and
        closing all acquired connections.

        Should be called after :meth:`close` for waiting for actual engine
        closing.

    .. method:: acquire()

        Get a connection from pool.

        This method is a :ref:`coroutine <coroutine>`.

        Returns a :class:`SAConnection` instance.

    .. method:: release()

        Revert back connection *conn* to pool.

      .. warning:: The method is not a :ref:`coroutine <coroutine>`.

Connection
----------

.. class:: SAConnection

   A wrapper for :class:`aiomysql.Connection` instance.

   The class provides methods for executing *SQL queries* and working with
   *SQL transactions*.

   .. method:: execute(query, *multiparams, **params)

        Executes a *SQL* *query* with optional parameters.

        This method is a :ref:`coroutine <coroutine>`.

        :param query: a SQL query string or any :term:`sqlalchemy`
                        expression (see :ref:`aiomysql core <aiomysql-index>`)

        :param \*multiparams/\**params: represent bound parameter values
         to be used in the execution.  Typically, the format is either a
         dictionary passed to \*multiparams::

             await conn.execute(
                 table.insert(),
                 {"id":1, "value":"v1"}
             )

         ...or individual key/values interpreted by \**params::

             await conn.execute(
                 table.insert(), id=1, value="v1"
             )

         In the case that a plain SQL string is passed, a tuple or
         individual values in \*multiparams may be passed::

             await conn.execute(
                 "INSERT INTO table (id, value) VALUES (%d, %s)",
                 (1, "v1")
             )

             await conn.execute(
                 "INSERT INTO table (id, value) VALUES (%s, %s)",
                 1, "v1"
             )

        :returns: :class:`ResultProxy` instance with results of SQL
                  query execution.

   .. method:: scalar(query, *multiparams, **params)

        Executes a *SQL* *query* and returns a scalar value.

        This method is a :ref:`coroutine <coroutine>`.

        .. seealso:: :meth:`SAConnection.execute` and :meth:`ResultProxy.scalar`.

   .. attribute:: closed

        The readonly property that returns ``True`` if connections is closed.

   .. method:: begin()

        Begin a transaction and return a transaction handle.

        This method is a :ref:`coroutine <coroutine>`.

        The returned object is an instance of :class:`.Transaction`.
        This object represents the "scope" of the transaction,
        which completes when either the :meth:`.Transaction.rollback`
        or :meth:`.Transaction.commit` method is called.

        Nested calls to :meth:`.begin` on the same :class:`.SAConnection`
        will return new :class:`.Transaction` objects that represent
        an emulated transaction within the scope of the enclosing
        transaction, that is::

            trans = await conn.begin()   # outermost transaction
            trans2 = await conn.begin()  # "inner"
            await trans2.commit()          # does nothing
            await trans.commit()           # actually commits

        Calls to :meth:`.Transaction.commit` only have an effect
        when invoked via the outermost :class:`.Transaction` object, though the
        :meth:`.Transaction.rollback` method of any of the
        :class:`.Transaction` objects will roll back the
        transaction.

        .. seealso::

           :meth:`.SAConnection.begin_nested` - use a SAVEPOINT

           :meth:`.SAConnection.begin_twophase` - use a two phase (XA)
                     transaction

   .. method:: begin_nested()

        Begin a nested transaction and return a transaction handle.

        This method is a :ref:`coroutine <coroutine>`.

        The returned object is an instance of :class:`.NestedTransaction`.

        Any transaction in the hierarchy may ``commit`` and
        ``rollback``, however the outermost transaction still controls
        the overall ``commit`` or ``rollback`` of the transaction of a
        whole. It utilizes SAVEPOINT facility of :term:`MySQL` server.

        .. seealso::

           :meth:`.SAConnection.begin`, :meth:`.SAConnection.begin_twophase`.

   .. method:: begin_twophase(xid=None)

        Begin a two-phase or XA transaction and return a transaction
        handle.

        This method is a :ref:`coroutine <coroutine>`.

        The returned object is an instance of
        :class:`.TwoPhaseTransaction`, which in addition to the methods
        provided by :class:`.Transaction`, also provides a
        :meth:`~.TwoPhaseTransaction.prepare` method.

        :param xid: the two phase transaction id.  If not supplied, a
            random id will be generated.

        .. seealso::
           :meth:`.SAConnection.begin`, :meth:`.SAConnection.begin_twophase`.

   .. method:: recover_twophase()

        Return a list of prepared twophase transaction ids.

        This method is a :ref:`coroutine <coroutine>`.

   .. method:: rollback_prepared(xid)

        Rollback prepared twophase transaction *xid*.

        This method is a :ref:`coroutine <coroutine>`.

   .. method:: commit_prepared(xid)

        Commit prepared twophase transaction *xid*.

        This method is a :ref:`coroutine <coroutine>`.

   .. attribute:: in_transaction

        The readonly property that returns ``True`` if a transaction is
        in progress.

   .. method:: close()

        Close this :class:`SAConnection`.

        This method is a :ref:`coroutine <coroutine>`.

        This results in a release of the underlying database
        resources, that is, the :class:`aiomysql.Connection` referenced
        internally. The :class:`aiomysql.Connection` is typically restored
        back to the connection-holding :class:`aiomysql.Pool` referenced
        by the :class:`.Engine` that produced this
        :class:`SAConnection`. Any transactional state present on
        the :class:`aiomysql.Connection` is also unconditionally released via
        calling :meth:`Transaction.rollback` method.

        After :meth:`~.SAConnection.close` is called, the
        :class:`.SAConnection` is permanently in a closed state,
        and will allow no further operations.

ResultProxy
-----------

.. class:: ResultProxy

    Wraps a *DB-API like* :class:`Cursor` object to provide easier
    access to row columns.

    Individual columns may be accessed by their integer position,
    case-sensitive column name, or by :class:`sqlalchemy.schema.Column``
    object. e.g.::

        async for row in conn.execute(...):
            col1 = row[0]    # access via integer position
            col2 = row['col2']   # access via name
            col3 = row[mytable.c.mycol] # access via Column object.

    :class:`ResultProxy` also handles post-processing of result column
    data using :class:`sqlalchemy.types.TypeEngine` objects, which are
    referenced from the originating SQL statement that produced this
    result set.

    .. attribute:: dialect

        The readonly property that returns
        :class:`sqlalchemy.engine.interfaces.Dialect` dialect
        for the :class:`ResultProxy` instance.

        .. seealso:: :data:`dialect` global data.

    .. method:: keys()

        Return the current set of string keys for rows.

    .. attribute:: rowcount

        The readonly property that returns the 'rowcount' for this result.

        The 'rowcount' reports the number of rows *matched*
        by the WHERE criterion of an UPDATE or DELETE statement.

        .. note::

            Notes regarding :attr:`ResultProxy.rowcount`:

            * This attribute returns the number of rows *matched*,
              which is not necessarily the same as the number of rows
              that were actually *modified* - an UPDATE statement, for example,
              may have no net change on a given row if the SET values
              given are the same as those present in the row already.
              Such a row would be matched but not modified.
            * :attr:`ResultProxy.rowcount` is *only* useful in conjunction
              with an UPDATE or DELETE statement.  Contrary to what the Python
              DBAPI says, it does *not* return the
              number of rows available from the results of a SELECT statement
              as DBAPIs cannot support this functionality when rows are
              unbuffered.
            * Statements that use RETURNING does not return a correct
              rowcount.

    .. attribute:: lastrowid

        Returns the 'lastrowid' accessor on the DBAPI cursor.

        value generated for an *AUTO_INCREMENT* column by the previous INSERT
        or UPDATE statement or None when there is no such value available. For
        example, if you perform an *INSERT* into a table that contains an
        AUTO_INCREMENT column, `lastrowid` returns the *AUTO_INCREMENT* value
        for the new row.

    .. attribute:: returns_rows

        A readonly property that returns ``True`` if this
        :class:`ResultProxy` returns rows.

        I.e. if it is legal to call the methods
        :meth:`ResultProxy.fetchone`,
        :meth:`ResultProxy.fetchmany`,
        :meth:`ResultProxy.fetchall`.

    .. attribute:: closed

        Return ``True`` if this :class:`ResultProxy` is closed (no
        pending rows in underlying cursor).

    .. method:: close()

        Close this :class:`ResultProxy`.

        Closes the underlying :class:`aiomysql.Cursor` corresponding to the
        execution.

        Note that any data cached within this :class:`ResultProxy` is
        still available.  For some types of results, this may include
        buffered rows.

        This method is called automatically when:

        * all result rows are exhausted using the fetchXXX() methods.
        * cursor.description is None.

    .. method:: fetchall()

        Fetch all rows, just like :meth:`aiomysql.Cursor.fetchall`.

        This method is a :ref:`coroutine <coroutine>`.

        The connection is closed after the call.

        Returns a list of :class:`RowProxy`.

    .. method:: fetchone()

        Fetch one row, just like :meth:`aiomysql.Cursor.fetchone`.

        This method is a :ref:`coroutine <coroutine>`.

        If a row is present, the cursor remains open after this is called.

        Else the cursor is automatically closed and ``None`` is returned.

        Returns an :class:`RowProxy` instance or ``None``.

    .. method:: fetchmany(size=None)

        Fetch many rows, just like :meth:`aiomysql.Cursor.fetchmany`.

        This method is a :ref:`coroutine <coroutine>`.

        If rows are present, the cursor remains open after this is called.

        Else the cursor is automatically closed and an empty list is returned.

        Returns a list of :class:`RowProxy`.

    .. method:: first()

        Fetch the first row and then close the result set unconditionally.

        This method is a :ref:`coroutine <coroutine>`.

        Returns ``None`` if no row is present or an :class:`RowProxy` instance.

    .. method:: scalar()

        Fetch the first column of the first row, and close the result set.

        Returns ``None`` if no row is present or an :class:`RowProxy` instance.


.. class:: RowProxy

    A :class:`collections.abc.Mapping` for representing a row in query result.

    Keys are column names, values are result values.

    Individual columns may be accessed by their integer position,
    case-sensitive column name, or by :class:`sqlalchemy.schema.Column``
    object.

    Has overloaded operators ``__eq__`` and ``__ne__`` for comparing two rows.

    The :class:`RowProxy` is *not hashable*.

    ..method:: as_tuple()

     Return a tuple with values from :meth:`RowProxy.values`.

Transaction objects
-------------------

.. class:: Transaction

    Represent a database transaction in progress.

    The :class:`Transaction` object is procured by
    calling the :meth:`SAConnection.begin` method of
    :class:`SAConnection`::

       async with engine.acquire() as conn:
           trans = await conn.begin()
           try:
               await conn.execute("insert into x (a, b) values (1, 2)")
           except Exception:
               await trans.rollback()
           else:
               await trans.commit()

    The object provides :meth:`.rollback` and :meth:`.commit`
    methods in order to control transaction boundaries.

    .. seealso::

        :meth:`SAConnection.begin`, :meth:`SAConnection.begin_twophase`,
        :meth:`SAConnection.begin_nested`.

    .. attribute:: is_active

        A readonly property that returns ``True`` if a transaction is active.

    .. attribute:: connection

        A readonly property that returns :class:`SAConnection` for transaction.

    .. method:: close()

        Close this :class:`Transaction`.

        This method is a :ref:`coroutine <coroutine>`.

        If this transaction is the base transaction in a begin/commit
        nesting, the transaction will :meth:`Transaction.rollback`.
        Otherwise, the method returns.

        This is used to cancel a :class:`Transaction` without affecting
        the scope of an enclosing transaction.

    .. method:: rollback()

        Roll back this :class:`Transaction`.

        This method is a :ref:`coroutine <coroutine>`.

    .. method:: commit()

        Commit this :class:`Transaction`.

        This method is a :ref:`coroutine <coroutine>`.


.. class:: NestedTransaction

    Represent a 'nested', or SAVEPOINT transaction.

    A new :class:`NestedTransaction` object may be procured
    using the :meth:`SAConnection.begin_nested` method.

    The interface is the same as that of :class:`Transaction`.

     .. seealso::  `SAVEPOINT, ROLLBACK TO SAVEPOINT, and RELEASE SAVEPOINT`__
          on :term:`MySQL`:

    .. __: https://dev.mysql.com/doc/refman/8.4/en/savepoint.html


.. class:: TwoPhaseTransaction

    Represent a two-phase transaction.

    A new :class:`TwoPhaseTransaction` object may be procured
    using the :meth:`SAConnection.begin_twophase` method.

    The interface is the same as that of :class:`Transaction`
    with the addition of the :meth:`TwoPhaseTransaction.prepare` method.

    .. attribute:: xid

        A readonly property that returns twophase transaction id.

    .. method:: prepare()

        Prepare this :class:`TwoPhaseTransaction`.

        This method is a :ref:`coroutine <coroutine>`.

        After a `PREPARE`, the transaction can be committed.

    .. seealso:: :term:`MySQL` commands for two phase transactions:

        https://dev.mysql.com/doc/refman/8.4/en/xa-statements.html
