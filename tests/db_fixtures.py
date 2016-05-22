import asyncio
import os

import pytest
import aiomysql
import ipdb
ipdb.set_trace()


@pytest.fixture
def mysql_params():
    params = {"host": os.environ.get('MYSQL_HOST', 'localhost'),
              "port": os.environ.get('MYSQL_PORT', 3306),
              "user": os.environ.get('MYSQL_USER', 'aiomysql'),
              "db": os.environ.get('MYSQL_DB', 'test_pymysql'),
              "password": os.environ.get('MYSQL_PASSWORD', 'mypass')}
    return params


@pytest.yield_fixture
def connection(mysql_params, loop):
    coro = aiomysql.connect(loop=loop, **mysql_params)
    conn = loop.run_until_complete(coro)
    yield conn
    loop.run_until_complete(conn.ensure_closed())


@pytest.yield_fixture
def cursor(connection, loop):
    cur = loop.run_until_complete(connection.cursor())
    yield cur
    loop.run_until_complete(cur.close())


@pytest.yield_fixture
def pool_creator(mysql_params, loop):
    pool = None

    @asyncio.coroutine
    def f(*kw):
        nonlocal pool
        conn_kw = mysql_params.copy()
        conn_kw.update(kw)
        pool = yield from aiomysql.create_pool(**conn_kw)
        return pool

    yield f

    if pool is not None:
        pool.close()
        loop.run_until_complete(pool.wait_closed())
