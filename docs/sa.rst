.. _aiomysql-rpc:

:mod:`aiomysql.sa` --- support for SQLAlchemy functional SQL layer
===============================================================

.. module:: aiomysql.sa
    :synopsis: support for SQLAlchemy functional SQL layer
.. currentmodule:: aiomysql.sa


Intro
-----
.. note::  :term:`sqlalchemy` support ported from aiopg_, so api should be
           very familiar for aiopg_ user.

While :ref:`core API <aiomysql-core>` provides a core support for access
to :term:`MySQL` database, manipulations with raw SQL
strings too annoying.

Fortunately we can use excellent :ref:`core_toplevel` as **SQL query builder**.


Example::

    import asyncio
    import sqlalchemy as sa

    from aiomysql.sa import create_engine


    metadata = sa.MetaData()

    tbl = sa.Table('tbl', metadata,
                   sa.Column('id', sa.Integer, primary_key=True),
                   sa.Column('val', sa.String(255)))


    @asyncio.coroutine
    def go():
        engine = yield from create_engine(user='root',
                                          db='test_pymysql',
                                          host='127.0.0.1',
                                          password='')

        with (yield from engine) as conn:
            yield from conn.execute(tbl.insert().values(val='abc'))

            res = yield from conn.execute(tbl.select())
            for row in res:
                print(row.id, row.val)

    asyncio.get_event_loop().run_until_complete(go())


So you can execute SQL query built by
``tbl.insert().values(val='abc')`` or ``tbl.select()`` expressions.

:term:`sqlalchemy` has rich and very powerful set of SQL construction
functions, please read :ref:`tutorial <core_toplevel>` for full list
of available operations.

Also we provide SQL transactions support. Please take a look on
:meth:`SAConnection.begin` method and family.


.. _aiopg: https://github.com/aio-libs/aiopg

Engine
------

.. function:: create_engine(*, minsize=10, maxsize=10, loop=None, \
                            dialect=dialect, timeout=60, **kwargs)

    A :ref:`coroutine <coroutine>` for :class:`Engine` creation.

    Returns :class:`Engine` instance with embedded connection pool.

    The pool has *minsize* opened connections to :term:`MySQL` server.


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

        A minimal size of the pool (*read-only*), ``10`` by default.

    .. attribute:: maxsize

        A maximal size of the pool (*read-only*), ``10`` by default.

    .. attribute:: size

        A current size of the pool (*readonly*). Includes used and free
        connections.

    .. attribute:: freesize

        A count of free connections in the pool (*readonly*).

    .. attribute:: timeout

        A read-only float representing default timeout for operations
        for connections from pool.

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
