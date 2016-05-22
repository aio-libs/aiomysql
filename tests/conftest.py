import asyncio
import gc
import os
import sys

import aiomysql
import pytest


@pytest.yield_fixture
def loop(request):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(None)

    yield loop

    if not loop._closed:
        loop.call_soon(loop.stop)
        loop.run_forever()
        loop.close()
    gc.collect()
    asyncio.set_event_loop(None)


@pytest.mark.tryfirst
def pytest_pycollect_makeitem(collector, name, obj):
    if collector.funcnamefilter(name):
        if not callable(obj):
            return
        item = pytest.Function(name, parent=collector)
        if 'run_loop' in item.keywords:
            return list(collector._genfunctions(name, obj))


@pytest.mark.tryfirst
def pytest_pyfunc_call(pyfuncitem):
    """
    Run asyncio marked test functions in an event loop instead of a normal
    function call.
    """
    if 'run_loop' in pyfuncitem.keywords:
        funcargs = pyfuncitem.funcargs
        loop = funcargs['loop']
        testargs = {arg: funcargs[arg]
                    for arg in pyfuncitem._fixtureinfo.argnames}
        loop.run_until_complete(pyfuncitem.obj(**testargs))
        return True


def pytest_runtest_setup(item):
    if 'run_loop' in item.keywords and 'loop' not in item.fixturenames:
        # inject an event loop fixture for all async tests
        item.fixturenames.append('loop')


def pytest_ignore_collect(path, config):
    if 'pep492' in str(path):
        if sys.version_info < (3, 5, 0):
            return True


@pytest.fixture
def mysql_params():
    params = {"host": os.environ.get('MYSQL_HOST', 'localhost'),
              "port": os.environ.get('MYSQL_PORT', 3306),
              "user": os.environ.get('MYSQL_USER', 'aiomysql'),
              "db": os.environ.get('MYSQL_DB', 'test_pymysql'),
              "password": os.environ.get('MYSQL_PASSWORD', 'mypass'),
              "local_infile": True,
              "use_unicode": True,
              }
    return params


@pytest.yield_fixture
def cursor(connection, loop):
    # TODO: fix this workaround
    @asyncio.coroutine
    def f():
        cur = yield from connection.cursor()
        return cur

    cur = loop.run_until_complete(f())
    yield cur
    loop.run_until_complete(cur.close())


@pytest.yield_fixture
def connection(mysql_params, loop):
    coro = aiomysql.connect(loop=loop, **mysql_params)
    conn = loop.run_until_complete(coro)
    yield conn
    loop.run_until_complete(conn.ensure_closed())


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
