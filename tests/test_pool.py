import asyncio

import pytest
from aiomysql.connection import Connection
from aiomysql.pool import Pool


@pytest.mark.run_loop
async def test_create_pool(pool_creator):
    pool = await pool_creator()
    assert isinstance(pool, Pool)
    assert 1 == pool.minsize
    assert 10 == pool.maxsize
    assert 1 == pool.size
    assert 1 == pool.freesize
    assert not pool.echo


@pytest.mark.run_loop
async def test_create_pool2(pool_creator):
    pool = await pool_creator(minsize=10, maxsize=20)
    assert isinstance(pool, Pool)
    assert 10 == pool.minsize
    assert 20 == pool.maxsize
    assert 10 == pool.size
    assert 10 == pool.freesize


@pytest.mark.run_loop
async def test_acquire(pool_creator):
    pool = await pool_creator()
    conn = await pool.acquire()
    assert isinstance(conn, Connection)
    assert not conn.closed
    cursor = await conn.cursor()
    await cursor.execute('SELECT 1')
    val = await cursor.fetchone()
    await cursor.close()
    assert (1,) == val
    pool.release(conn)


@pytest.mark.run_loop
async def test_release(pool_creator):
    pool = await pool_creator()
    conn = await pool.acquire()
    assert 0 == pool.freesize
    assert {conn} == pool._used
    pool.release(conn)
    assert 1 == pool.freesize
    assert not pool._used


@pytest.mark.run_loop
async def test_release_closed(pool_creator):
    pool = await pool_creator(minsize=10, maxsize=10)
    conn = await pool.acquire()
    assert 9 == pool.freesize
    await conn.ensure_closed()
    pool.release(conn)
    assert 9 == pool.freesize
    assert not pool._used
    assert 9 == pool.size

    conn2 = await pool.acquire()
    assert 9 == pool.freesize
    assert 10 == pool.size
    pool.release(conn2)


@pytest.mark.run_loop
async def test_bad_context_manager_usage(pool_creator):
    pool = await pool_creator()
    with pytest.raises(RuntimeError):
        with pool:
            pass


@pytest.mark.run_loop
async def test_context_manager(pool_creator):
    pool = await pool_creator(minsize=10, maxsize=10)
    async with pool.get() as conn:
        assert isinstance(conn, Connection)
        assert 9 == pool.freesize
        assert {conn} == pool._used
    assert 10 == pool.freesize


@pytest.mark.run_loop
async def test_clear(pool_creator):
    pool = await pool_creator()
    await pool.clear()
    assert 0 == pool.freesize


@pytest.mark.run_loop
async def test_initial_empty(pool_creator):
    pool = await pool_creator(minsize=0)
    assert 10 == pool.maxsize
    assert 0 == pool.minsize
    assert 0 == pool.size
    assert 0 == pool.freesize

    async with pool.get():
        assert 1 == pool.size
        assert 0 == pool.freesize
    assert 1 == pool.size
    assert 1 == pool.freesize

    conn1 = await pool.acquire()
    assert 1 == pool.size
    assert 0 == pool.freesize

    conn2 = await pool.acquire()
    assert 2 == pool.size
    assert 0 == pool.freesize

    pool.release(conn1)
    assert 2 == pool.size
    assert 1 == pool.freesize

    pool.release(conn2)
    assert 2 == pool.size
    assert 2 == pool.freesize


@pytest.mark.run_loop
async def test_parallel_tasks(pool_creator, loop):
    pool = await pool_creator(minsize=0, maxsize=2)
    assert 2 == pool.maxsize
    assert 0 == pool.minsize
    assert 0 == pool.size
    assert 0 == pool.freesize

    fut1 = pool.acquire()
    fut2 = pool.acquire()

    conn1, conn2 = await asyncio.gather(fut1, fut2, loop=loop)
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

    conn3 = await pool.acquire()
    assert conn3 is conn1
    pool.release(conn3)


@pytest.mark.run_loop
async def test_parallel_tasks_more(pool_creator, loop):
    pool = await pool_creator(minsize=0, maxsize=3)

    fut1 = pool.acquire()
    fut2 = pool.acquire()
    fut3 = pool.acquire()

    conn1, conn2, conn3 = await asyncio.gather(fut1, fut2, fut3,
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

    conn4 = await pool.acquire()
    assert conn4 is conn1
    pool.release(conn4)


@pytest.mark.run_loop
async def test_default_event_loop(pool_creator, loop):
    asyncio.set_event_loop(loop)
    pool = await pool_creator(loop=None)
    assert pool._loop is loop


@pytest.mark.run_loop
async def test_release_with_invalid_status(pool_creator):
    pool = await pool_creator(minsize=10, maxsize=10)
    conn = await pool.acquire()
    assert 9 == pool.freesize
    assert {conn} == pool._used
    cur = await conn.cursor()
    await cur.execute('BEGIN')
    await cur.close()

    pool.release(conn)
    assert 9 == pool.freesize
    assert not pool._used
    assert conn.closed


@pytest.mark.run_loop
async def test_release_with_invalid_status_wait_release(pool_creator):
    pool = await pool_creator(minsize=10, maxsize=10)
    conn = await pool.acquire()
    assert 9 == pool.freesize
    assert {conn} == pool._used
    cur = await conn.cursor()
    await cur.execute('BEGIN')
    await cur.close()

    await pool.release(conn)
    assert 9 == pool.freesize
    assert not pool._used
    assert conn.closed


@pytest.mark.run_loop
async def test__fill_free(pool_creator, loop):
    pool = await pool_creator(minsize=1)
    async with pool.get():
        assert 0 == pool.freesize
        assert 1 == pool.size

        conn = await asyncio.wait_for(pool.acquire(),
                                      timeout=0.5, loop=loop)
        assert 0 == pool.freesize
        assert 2 == pool.size
        pool.release(conn)
        assert 1 == pool.freesize
        assert 2 == pool.size
    assert 2 == pool.freesize
    assert 2 == pool.size


@pytest.mark.run_loop
async def test_connect_from_acquire(pool_creator):
    pool = await pool_creator(minsize=0)
    assert 0 == pool.freesize
    assert 0 == pool.size
    async with pool.get():
        assert 1 == pool.size
        assert 0 == pool.freesize
    assert 1 == pool.size
    assert 1 == pool.freesize


@pytest.mark.run_loop
async def test_concurrency(pool_creator):
    pool = await pool_creator(minsize=2, maxsize=4)
    c1 = await pool.acquire()
    c2 = await pool.acquire()
    assert 0 == pool.freesize
    assert 2 == pool.size
    pool.release(c1)
    pool.release(c2)


@pytest.mark.run_loop
async def test_invalid_minsize_and_maxsize(pool_creator):
    with pytest.raises(ValueError):
        await pool_creator(minsize=-1)

    with pytest.raises(ValueError):
        await pool_creator(minsize=5, maxsize=2)


@pytest.mark.run_loop
async def test_true_parallel_tasks(pool_creator, loop):
    pool = await pool_creator(minsize=0, maxsize=1)
    assert 1 == pool.maxsize
    assert 0 == pool.minsize
    assert 0 == pool.size
    assert 0 == pool.freesize

    maxsize = 0
    minfreesize = 100

    async def inner():
        nonlocal maxsize, minfreesize
        maxsize = max(maxsize, pool.size)
        minfreesize = min(minfreesize, pool.freesize)
        conn = await pool.acquire()
        maxsize = max(maxsize, pool.size)
        minfreesize = min(minfreesize, pool.freesize)
        await asyncio.sleep(0.01, loop=loop)
        pool.release(conn)
        maxsize = max(maxsize, pool.size)
        minfreesize = min(minfreesize, pool.freesize)

    await asyncio.gather(inner(), inner(), loop=loop)

    assert 1 == maxsize
    assert 0 == minfreesize


@pytest.mark.run_loop
async def test_cannot_acquire_after_closing(pool_creator):
    pool = await pool_creator()
    pool.close()

    with pytest.raises(RuntimeError):
        await pool.acquire()


@pytest.mark.run_loop
async def test_wait_closed(pool_creator, loop):
    pool = await pool_creator(minsize=10, maxsize=10)

    c1 = await pool.acquire()
    c2 = await pool.acquire()
    assert 10 == pool.size
    assert 8 == pool.freesize

    ops = []

    async def do_release(conn):
        await asyncio.sleep(0, loop=loop)
        pool.release(conn)
        ops.append('release')

    async def wait_closed():
        await pool.wait_closed()
        ops.append('wait_closed')

    pool.close()
    await asyncio.gather(wait_closed(), do_release(c1), do_release(c2),
                         loop=loop)
    assert ['release', 'release', 'wait_closed'] == ops
    assert 0 == pool.freesize


@pytest.mark.run_loop
async def test_echo(pool_creator):
    pool = await pool_creator(echo=True)
    assert pool.echo

    async with pool.get() as conn:
        assert conn.echo


@pytest.mark.run_loop
async def test_terminate_with_acquired_connections(pool_creator):
    pool = await pool_creator()
    conn = await pool.acquire()
    pool.terminate()
    await pool.wait_closed()
    assert conn.closed


@pytest.mark.run_loop
async def test_release_closed_connection(pool_creator):
    pool = await pool_creator()
    conn = await pool.acquire()
    conn.close()

    pool.release(conn)
    pool.close()


@pytest.mark.run_loop
async def test_wait_closing_on_not_closed(pool_creator):
    pool = await pool_creator()

    with pytest.raises(RuntimeError):
        await pool.wait_closed()
    pool.close()


@pytest.mark.run_loop
async def test_release_terminated_pool(pool_creator):
    pool = await pool_creator()
    conn = await pool.acquire()
    pool.terminate()
    await pool.wait_closed()

    pool.release(conn)
    pool.close()


@pytest.mark.run_loop
async def test_release_terminated_pool_wait_release(pool_creator):
    pool = await pool_creator()
    conn = await pool.acquire()
    pool.terminate()
    await pool.wait_closed()

    await pool.release(conn)
    pool.close()


@pytest.mark.run_loop
async def test_close_with_acquired_connections(pool_creator, loop):
    pool = await pool_creator()
    conn = await pool.acquire()
    pool.close()

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(pool.wait_closed(), 0.1, loop=loop)
    pool.release(conn)


async def _set_global_conn_timeout(conn, t):
    # create separate connection to setup global connection timeouts
    # https://dev.mysql.com/doc/refman/5.1/en/server-system-variables
    # .html#sysvar_interactive_timeout
    cur = await conn.cursor()
    await cur.execute('SET GLOBAL wait_timeout=%s;', t)
    await cur.execute('SET GLOBAL interactive_timeout=%s;', t)
    await cur.close()


@pytest.mark.run_loop
async def test_drop_connection_if_timedout(pool_creator,
                                           connection_creator, loop):
    conn = await connection_creator()
    await _set_global_conn_timeout(conn, 2)
    await conn.ensure_closed()
    try:
        pool = await pool_creator(minsize=3, maxsize=3)
        # sleep, more then connection timeout
        await asyncio.sleep(3, loop=loop)
        conn = await pool.acquire()
        cur = await conn.cursor()
        # query should not throw exception OperationalError
        await cur.execute('SELECT 1;')
        pool.release(conn)
        pool.close()
        await pool.wait_closed()
    finally:
        # setup default timeouts
        conn = await connection_creator()
        await _set_global_conn_timeout(conn, 28800)
        await conn.ensure_closed()


@pytest.mark.skip(reason='Not implemented')
@pytest.mark.run_loop
async def test_create_pool_with_timeout(pool_creator):
    pool = await pool_creator(minsize=3, maxsize=3)
    timeout = 0.1
    assert timeout == pool.timeout
    conn = await pool.acquire()
    assert timeout == conn.timeout
    pool.release(conn)


@pytest.mark.run_loop
async def test_cancelled_connection(pool_creator, loop):
    pool = await pool_creator(minsize=0, maxsize=1)

    try:
        async with pool.get() as conn:
            curs = await conn.cursor()
            # Cancel a cursor in the middle of execution, before it
            # could read even the first packet (SLEEP assures the
            # timings)
            task = loop.create_task(curs.execute(
                "SELECT 1 as id, SLEEP(0.1) as xxx"))
            await asyncio.sleep(0.05, loop=loop)
            task.cancel()
            await task
    except asyncio.CancelledError:
        pass

    async with pool.get() as conn:
        cur2 = await conn.cursor()
        res = await cur2.execute("SELECT 2 as value, 0 as xxx")
        names = [x[0] for x in cur2.description]
        # If we receive ["id", "xxx"] - we corrupted the connection
        assert names == ["value", "xxx"]
        res = await cur2.fetchall()
        # If we receive [(1, 0)] - we retrieved old cursor's values
        assert list(res) == [(2, 0)]


async def test_pool_with_connection_recycling(pool_creator, loop):
    pool = await pool_creator(minsize=1, maxsize=1, pool_recycle=3)
    async with pool.get() as conn:
        cur = await conn.cursor()
        await cur.execute('SELECT 1;')
        val = await cur.fetchone()
        assert (1,) == val

    await asyncio.sleep(5, loop=loop)

    assert 1 == pool.freesize
    async with pool.get() as conn:
        cur = await conn.cursor()
        await cur.execute('SELECT 1;')
        val = await cur.fetchone()
        assert (1,) == val
