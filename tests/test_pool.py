import asyncio

import pytest
from aiomysql.connection import Connection
from aiomysql.pool import Pool


@pytest.mark.run_loop
def test_create_pool(pool_creator):
    pool = yield from pool_creator()
    assert isinstance(pool, Pool)
    assert 1 == pool.minsize
    assert 10 == pool.maxsize
    assert 1 == pool.size
    assert 1 == pool.freesize
    assert not pool.echo


@pytest.mark.run_loop
def test_create_pool2(pool_creator):
    pool = yield from pool_creator(minsize=10, maxsize=20)
    assert isinstance(pool, Pool)
    assert 10 == pool.minsize
    assert 20 == pool.maxsize
    assert 10 == pool.size
    assert 10 == pool.freesize


@pytest.mark.run_loop
def test_acquire(pool_creator):
    pool = yield from pool_creator()
    conn = yield from pool.acquire()
    assert isinstance(conn, Connection)
    assert not conn.closed
    cursor = yield from conn.cursor()
    yield from cursor.execute('SELECT 1')
    val = yield from cursor.fetchone()
    yield from cursor.close()
    assert (1,) == val
    pool.release(conn)


@pytest.mark.run_loop
def test_release(pool_creator):
    pool = yield from pool_creator()
    conn = yield from pool.acquire()
    assert 0 == pool.freesize
    assert {conn} == pool._used
    pool.release(conn)
    assert 1 == pool.freesize
    assert not pool._used


@pytest.mark.run_loop
def test_release_closed(pool_creator):
    pool = yield from pool_creator(minsize=10, maxsize=10)
    conn = yield from pool.acquire()
    assert 9 == pool.freesize
    yield from conn.ensure_closed()
    pool.release(conn)
    assert 9 == pool.freesize
    assert not pool._used
    assert 9 == pool.size

    conn2 = yield from pool.acquire()
    assert 9 == pool.freesize
    assert 10 == pool.size
    pool.release(conn2)


@pytest.mark.run_loop
def test_bad_context_manager_usage(pool_creator):
    pool = yield from pool_creator()
    with pytest.raises(RuntimeError):
        with pool:
            pass


@pytest.mark.run_loop
def test_context_manager(pool_creator):
    pool = yield from pool_creator(minsize=10, maxsize=10)
    with (yield from pool) as conn:
        assert isinstance(conn, Connection)
        assert 9 == pool.freesize
        assert {conn} == pool._used
    assert 10 == pool.freesize


@pytest.mark.run_loop
def test_clear(pool_creator):
    pool = yield from pool_creator()
    yield from pool.clear()
    assert 0 == pool.freesize


@pytest.mark.run_loop
def test_initial_empty(pool_creator):
    pool = yield from pool_creator(minsize=0)
    assert 10 == pool.maxsize
    assert 0 == pool.minsize
    assert 0 == pool.size
    assert 0 == pool.freesize

    with (yield from pool):
        assert 1 == pool.size
        assert 0 == pool.freesize
    assert 1 == pool.size
    assert 1 == pool.freesize

    conn1 = yield from pool.acquire()
    assert 1 == pool.size
    assert 0 == pool.freesize

    conn2 = yield from pool.acquire()
    assert 2 == pool.size
    assert 0 == pool.freesize

    pool.release(conn1)
    assert 2 == pool.size
    assert 1 == pool.freesize

    pool.release(conn2)
    assert 2 == pool.size
    assert 2 == pool.freesize


@pytest.mark.run_loop
def test_parallel_tasks(pool_creator, loop):
    pool = yield from pool_creator(minsize=0, maxsize=2)
    assert 2 == pool.maxsize
    assert 0 == pool.minsize
    assert 0 == pool.size
    assert 0 == pool.freesize

    fut1 = pool.acquire()
    fut2 = pool.acquire()

    conn1, conn2 = yield from asyncio.gather(fut1, fut2, loop=loop)
    assert 2 == pool.size
    assert 0 == pool.freesize
    assert {conn1, conn2} == pool._used

    pool.release(conn1)
    assert 2 == pool.size
    assert 1 == pool.freesize
    assert {conn2} == pool._used

    pool.release(conn2)
    assert 2 == pool.size
    assert 2 == pool.freesize
    assert not conn1.closed
    assert not conn2.closed

    conn3 = yield from pool.acquire()
    assert conn3 is conn1
    pool.release(conn3)


@pytest.mark.run_loop
def test_parallel_tasks_more(pool_creator, loop):
    pool = yield from pool_creator(minsize=0, maxsize=3)

    fut1 = pool.acquire()
    fut2 = pool.acquire()
    fut3 = pool.acquire()

    conn1, conn2, conn3 = yield from asyncio.gather(fut1, fut2, fut3,
                                                    loop=loop)
    assert 3 == pool.size
    assert 0 == pool.freesize
    assert {conn1, conn2, conn3} == pool._used

    pool.release(conn1)
    assert 3 == pool.size
    assert 1 == pool.freesize
    assert {conn2, conn3} == pool._used

    pool.release(conn2)
    assert 3 == pool.size
    assert 2 == pool.freesize
    assert {conn3} == pool._used
    assert not conn1.closed
    assert not conn2.closed

    pool.release(conn3)
    assert 3, pool.size
    assert 3, pool.freesize
    assert not pool._used
    assert not conn1.closed
    assert not conn2.closed
    assert not conn3.closed

    conn4 = yield from pool.acquire()
    assert conn4 is conn1
    pool.release(conn4)


@pytest.mark.run_loop
def test_default_event_loop(pool_creator, loop):
    asyncio.set_event_loop(loop)
    pool = yield from pool_creator(loop=None)
    assert pool._loop is loop


@pytest.mark.run_loop
def test_release_with_invalid_status(pool_creator):
    pool = yield from pool_creator(minsize=10, maxsize=10)
    conn = yield from pool.acquire()
    assert 9 == pool.freesize
    assert {conn} == pool._used
    cur = yield from conn.cursor()
    yield from cur.execute('BEGIN')
    yield from cur.close()

    pool.release(conn)
    assert 9 == pool.freesize
    assert not pool._used
    assert conn.closed


@pytest.mark.run_loop
def test_release_with_invalid_status_wait_release(pool_creator):
    pool = yield from pool_creator(minsize=10, maxsize=10)
    conn = yield from pool.acquire()
    assert 9 == pool.freesize
    assert {conn} == pool._used
    cur = yield from conn.cursor()
    yield from cur.execute('BEGIN')
    yield from cur.close()

    yield from pool.release(conn)
    assert 9 == pool.freesize
    assert not pool._used
    assert conn.closed


@pytest.mark.run_loop
def test__fill_free(pool_creator, loop):
    pool = yield from pool_creator(minsize=1)
    with (yield from pool):
        assert 0 == pool.freesize
        assert 1 == pool.size

        conn = yield from asyncio.wait_for(pool.acquire(),
                                           timeout=0.5,
                                           loop=loop)
        assert 0 == pool.freesize
        assert 2 == pool.size
        pool.release(conn)
        assert 1 == pool.freesize
        assert 2 == pool.size
    assert 2 == pool.freesize
    assert 2 == pool.size


@pytest.mark.run_loop
def test_connect_from_acquire(pool_creator):
    pool = yield from pool_creator(minsize=0)
    assert 0 == pool.freesize
    assert 0 == pool.size
    with (yield from pool):
        assert 1 == pool.size
        assert 0 == pool.freesize
    assert 1 == pool.size
    assert 1 == pool.freesize


@pytest.mark.run_loop
def test_concurrency(pool_creator):
    pool = yield from pool_creator(minsize=2, maxsize=4)
    c1 = yield from pool.acquire()
    c2 = yield from pool.acquire()
    assert 0 == pool.freesize
    assert 2 == pool.size
    pool.release(c1)
    pool.release(c2)


@pytest.mark.run_loop
def test_invalid_minsize_and_maxsize(pool_creator):
    with pytest.raises(ValueError):
        yield from pool_creator(minsize=-1)

    with pytest.raises(ValueError):
        yield from pool_creator(minsize=5, maxsize=2)


@pytest.mark.run_loop
def test_true_parallel_tasks(pool_creator, loop):
    pool = yield from pool_creator(minsize=0, maxsize=1)
    assert 1 == pool.maxsize
    assert 0 == pool.minsize
    assert 0 == pool.size
    assert 0 == pool.freesize

    maxsize = 0
    minfreesize = 100

    @asyncio.coroutine
    def inner():
        nonlocal maxsize, minfreesize
        maxsize = max(maxsize, pool.size)
        minfreesize = min(minfreesize, pool.freesize)
        conn = yield from pool.acquire()
        maxsize = max(maxsize, pool.size)
        minfreesize = min(minfreesize, pool.freesize)
        yield from asyncio.sleep(0.01, loop=loop)
        pool.release(conn)
        maxsize = max(maxsize, pool.size)
        minfreesize = min(minfreesize, pool.freesize)

    yield from asyncio.gather(inner(), inner(), loop=loop)

    assert 1 == maxsize
    assert 0 == minfreesize


@pytest.mark.run_loop
def test_cannot_acquire_after_closing(pool_creator):
    pool = yield from pool_creator()
    pool.close()

    with pytest.raises(RuntimeError):
        yield from pool.acquire()


@pytest.mark.run_loop
def test_wait_closed(pool_creator, loop):
    pool = yield from pool_creator(minsize=10, maxsize=10)

    c1 = yield from pool.acquire()
    c2 = yield from pool.acquire()
    assert 10 == pool.size
    assert 8 == pool.freesize

    ops = []

    @asyncio.coroutine
    def do_release(conn):
        yield from asyncio.sleep(0, loop=loop)
        pool.release(conn)
        ops.append('release')

    @asyncio.coroutine
    def wait_closed():
        yield from pool.wait_closed()
        ops.append('wait_closed')

    pool.close()
    yield from asyncio.gather(wait_closed(),
                              do_release(c1),
                              do_release(c2),
                              loop=loop)
    assert ['release', 'release', 'wait_closed'] == ops
    assert 0 == pool.freesize


@pytest.mark.run_loop
def test_echo(pool_creator):
    pool = yield from pool_creator(echo=True)
    assert pool.echo

    with (yield from pool) as conn:
        assert conn.echo


@pytest.mark.run_loop
def test_terminate_with_acquired_connections(pool_creator):
    pool = yield from pool_creator()
    conn = yield from pool.acquire()
    pool.terminate()
    yield from pool.wait_closed()
    assert conn.closed


@pytest.mark.run_loop
def test_release_closed_connection(pool_creator):
    pool = yield from pool_creator()
    conn = yield from pool.acquire()
    conn.close()

    pool.release(conn)
    pool.close()


@pytest.mark.run_loop
def test_wait_closing_on_not_closed(pool_creator):
    pool = yield from pool_creator()

    with pytest.raises(RuntimeError):
        yield from pool.wait_closed()
    pool.close()


@pytest.mark.run_loop
def test_release_terminated_pool(pool_creator):
    pool = yield from pool_creator()
    conn = yield from pool.acquire()
    pool.terminate()
    yield from pool.wait_closed()

    pool.release(conn)
    pool.close()


@pytest.mark.run_loop
def test_release_terminated_pool_wait_release(pool_creator):
    pool = yield from pool_creator()
    conn = yield from pool.acquire()
    pool.terminate()
    yield from pool.wait_closed()

    yield from pool.release(conn)
    pool.close()


@pytest.mark.run_loop
def test_close_with_acquired_connections(pool_creator, loop):
    pool = yield from pool_creator()
    conn = yield from pool.acquire()
    pool.close()

    with pytest.raises(asyncio.TimeoutError):
        yield from asyncio.wait_for(pool.wait_closed(), 0.1, loop=loop)
    pool.release(conn)


@asyncio.coroutine
def _set_global_conn_timeout(conn, t):
    # create separate connection to setup global connection timeouts
    # https://dev.mysql.com/doc/refman/5.1/en/server-system-variables
    # .html#sysvar_interactive_timeout
    cur = yield from conn.cursor()
    yield from cur.execute('SET GLOBAL wait_timeout=%s;', t)
    yield from cur.execute('SET GLOBAL interactive_timeout=%s;', t)
    yield from cur.close()


@pytest.mark.run_loop
def test_drop_connection_if_timedout(pool_creator, connection_creator, loop):
    conn = yield from connection_creator()
    yield from _set_global_conn_timeout(conn, 2)
    yield from conn.ensure_closed()
    try:
        pool = yield from pool_creator(minsize=3, maxsize=3)
        # sleep, more then connection timeout
        yield from asyncio.sleep(3, loop=loop)
        conn = yield from pool.acquire()
        cur = yield from conn.cursor()
        # query should not throw exception OperationalError
        yield from cur.execute('SELECT 1;')
        pool.release(conn)
        pool.close()
        yield from pool.wait_closed()
    finally:
        # setup default timeouts
        conn = yield from connection_creator()
        yield from _set_global_conn_timeout(conn, 28800)
        yield from conn.ensure_closed()


@pytest.mark.skip(reason='Not implemented')
@pytest.mark.run_loop
def test_create_pool_with_timeout(pool_creator):
    pool = yield from pool_creator(minsize=3, maxsize=3)
    timeout = 0.1
    assert timeout == pool.timeout
    conn = yield from pool.acquire()
    assert timeout == conn.timeout
    pool.release(conn)


@pytest.mark.run_loop
def test_cancelled_connection(pool_creator, loop):
    pool = yield from pool_creator(minsize=0, maxsize=1)

    try:
        with (yield from pool) as conn:
            curs = yield from conn.cursor()
            # Cancel a cursor in the middle of execution, before it
            # could read even the first packet (SLEEP assures the
            # timings)
            task = loop.create_task(curs.execute(
                "SELECT 1 as id, SLEEP(0.1) as xxx"))
            yield from asyncio.sleep(0.05, loop=loop)
            task.cancel()
            yield from task
    except asyncio.CancelledError:
        pass

    with (yield from pool) as conn:
        cur2 = yield from conn.cursor()
        res = yield from cur2.execute("SELECT 2 as value, 0 as xxx")
        names = [x[0] for x in cur2.description]
        # If we receive ["id", "xxx"] - we corrupted the connection
        assert names == ["value", "xxx"]
        res = yield from cur2.fetchall()
        # If we receive [(1, 0)] - we retrieved old cursor's values
        assert list(res) == [(2, 0)]


@asyncio.coroutine
def test_pool_with_connection_recycling(pool_creator, loop):
    pool = yield from pool_creator(minsize=1,
                                   maxsize=1,
                                   pool_recycle=3)
    with (yield from pool) as conn:
        cur = yield from conn.cursor()
        yield from cur.execute('SELECT 1;')
        val = yield from cur.fetchone()
        assert (1,) == val

    yield from asyncio.sleep(5, loop=loop)

    assert 1 == pool.freesize
    with (yield from pool) as conn:
        cur = yield from conn.cursor()
        yield from cur.execute('SELECT 1;')
        val = yield from cur.fetchone()
        assert (1,) == val
