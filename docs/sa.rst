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
