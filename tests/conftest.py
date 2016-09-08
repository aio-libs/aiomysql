import asyncio
import gc
import os
import sys

import aiomysql
import pytest


PY_35 = sys.version_info >= (3, 5)
if PY_35:
    import uvloop
else:
    uvloop = None


def pytest_generate_tests(metafunc):
    if 'loop_type' in metafunc.fixturenames:
        loop_type = ['asyncio', 'uvloop'] if uvloop else ['asyncio']
        metafunc.parametrize("loop_type", loop_type)


@pytest.yield_fixture
def loop(request, loop_type):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(None)

    if uvloop and loop_type == 'uvloop':
        loop = uvloop.new_event_loop()
    else:
        loop = asyncio.new_event_loop()

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
              "user": os.environ.get('MYSQL_USER', 'root'),
              "db": os.environ.get('MYSQL_DB', 'test_pymysql'),
              "password": os.environ.get('MYSQL_PASSWORD', ''),
              "local_infile": True,
              "use_unicode": True,
              }
    return params


# TODO: fix this workaround
@asyncio.coroutine
def _cursor_wrapper(conn):
    cur = yield from conn.cursor()
    return cur


@pytest.yield_fixture
def cursor(connection, loop):
    cur = loop.run_until_complete(_cursor_wrapper(connection))
    yield cur
    loop.run_until_complete(cur.close())


@pytest.yield_fixture
def connection(mysql_params, loop):
    coro = aiomysql.connect(loop=loop, **mysql_params)
    conn = loop.run_until_complete(coro)
    yield conn
    loop.run_until_complete(conn.ensure_closed())


@pytest.yield_fixture
def connection_creator(mysql_params, loop):
    connections = []

    @asyncio.coroutine
    def f(**kw):
        conn_kw = mysql_params.copy()
        conn_kw.update(kw)
        _loop = conn_kw.pop('loop', loop)
        conn = yield from aiomysql.connect(loop=_loop, **conn_kw)
        connections.append(conn)
        return conn

    yield f

    for conn in connections:
        loop.run_until_complete(conn.ensure_closed())


@pytest.yield_fixture
def pool_creator(mysql_params, loop):
    pools = []

    @asyncio.coroutine
    def f(**kw):
        conn_kw = mysql_params.copy()
        conn_kw.update(kw)
        _loop = conn_kw.pop('loop', loop)
        pool = yield from aiomysql.create_pool(loop=_loop, **conn_kw)
        pools.append(pool)
        return pool

    yield f

    for pool in pools:
        pool.close()
        loop.run_until_complete(pool.wait_closed())


@pytest.yield_fixture
def table_cleanup(loop, connection):
    table_list = []
    cursor = loop.run_until_complete(_cursor_wrapper(connection))

    def _register_table(table_name):
        table_list.append(table_name)

    yield _register_table
    for t in table_list:
        # TODO: probably this is not safe code
        sql = "DROP TABLE IF EXISTS {};".format(t)
        loop.run_until_complete(cursor.execute(sql))
