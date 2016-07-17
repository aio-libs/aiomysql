import warnings

import aiomysql
import pytest

from aiomysql import sa, create_pool
from sqlalchemy import MetaData, Table, Column, Integer, String


meta = MetaData()
tbl = Table('tbl', meta,
            Column('id', Integer, nullable=False, primary_key=True),
            Column('name', String(255)))


@pytest.fixture
def table(loop, connection_creator, table_cleanup):
    async def f():
        connection = await connection_creator()
        cursor = await connection.cursor()
        await cursor.execute("DROP TABLE IF EXISTS tbl;")
        await cursor.execute("""CREATE TABLE tbl (
                 id MEDIUMINT NOT NULL AUTO_INCREMENT,
                 name VARCHAR(255) NOT NULL,
                 PRIMARY KEY (id));""")

        for i in [(1, 'a'), (2, 'b'), (3, 'c')]:
            await cursor.execute("INSERT INTO tbl VALUES(%s, %s)", i)

        await cursor.execute("commit;")
        await cursor.close()

    table_cleanup('tbl')
    loop.run_until_complete(f())


@pytest.mark.run_loop
async def test_cursor(table, cursor):
    ret = []
    await cursor.execute('SELECT * from tbl;')

    assert not cursor.closed
    async with cursor:
        async for i in cursor:
            ret.append(i)

    assert [(1, 'a'), (2, 'b'), (3, 'c')] == ret
    assert cursor.closed


@pytest.mark.run_loop
async def test_cursor_lightweight(table, cursor):
    await cursor.execute('SELECT * from tbl;')

    assert not cursor.closed
    async with cursor:
        pass

    assert cursor.closed


@pytest.mark.run_loop
async def test_cursor_method(connection):
    async with connection.cursor() as cursor:
        await cursor.execute('SELECT 42;')
        value = await cursor.fetchone()
        assert value == (42,)

    assert cursor.closed


@pytest.mark.run_loop
async def test_connection(connection):
    assert not connection.closed
    async with connection:
        assert not connection.closed

    assert connection.closed


@pytest.mark.run_loop
async def test_connection_exception(connection):
    assert not connection.closed
    with pytest.raises(RuntimeError) as ctx:
        async with connection:
            assert not connection.closed
            raise RuntimeError('boom')
    assert str(ctx.value) == 'boom'
    assert connection.closed


@pytest.mark.run_loop
async def test_connect_method(mysql_params, loop):
    async with aiomysql.connect(loop=loop, **mysql_params) as connection:
        async with connection.cursor() as cursor:
            await cursor.execute("SELECT 42")
            value = await cursor.fetchone()
            assert value, (42,)

    assert cursor.closed
    assert connection.closed


@pytest.mark.run_loop
async def test_connect_method_exception(mysql_params, loop):
    with pytest.raises(RuntimeError) as ctx:
        async with aiomysql.connect(loop=loop, **mysql_params) as connection:
            assert not connection.closed
            raise RuntimeError('boom')

    assert str(ctx.value) == 'boom'
    assert connection.closed


@pytest.mark.run_loop
async def test_pool(table, pool_creator, loop):
    pool = await pool_creator()
    async with pool.acquire() as conn:
        async with (await conn.cursor()) as cur:
            await cur.execute("SELECT * from tbl")
            ret = []
            async for i in cur:
                ret.append(i)
            assert [(1, 'a'), (2, 'b'), (3, 'c')] == ret


@pytest.mark.run_loop
async def test_create_pool_deprecations(mysql_params, loop):
    async with create_pool(loop=loop, **mysql_params) as pool:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            async with pool.get() as conn:
                pass
    assert issubclass(w[-1].category, DeprecationWarning)
    assert conn.closed

    async with create_pool(loop=loop, **mysql_params) as pool:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            with await pool as conn:
                pass
    assert issubclass(w[-1].category, DeprecationWarning)
    assert conn.closed


@pytest.mark.run_loop
async def test_sa_connection(table, mysql_params, loop):
    async with sa.create_engine(loop=loop, **mysql_params) as engine:
        connection = await engine.acquire()
        assert not connection.closed
        async with connection:
            ret = []
            async for i in connection.execute(tbl.select()):
                ret.append(i)
            assert [(1, 'a'), (2, 'b'), (3, 'c')] == ret
        assert connection.closed


@pytest.mark.run_loop
async def test_sa_transaction(table, mysql_params, loop):
    async with sa.create_engine(loop=loop, **mysql_params) as engine:
        async with engine.acquire() as connection:
            cnt = await connection.scalar(tbl.count())
            assert 3 == cnt

            async with (await connection.begin()) as tr:
                assert tr.is_active
                await connection.execute(tbl.delete())

            assert not tr.is_active
            cnt = await connection.scalar(tbl.count())
            assert 0 == cnt


@pytest.mark.run_loop
async def test_sa_transaction_rollback(loop, mysql_params, table):
    async with sa.create_engine(loop=loop, **mysql_params) as engine:
        async with engine.acquire() as conn:
            cnt = await conn.scalar(tbl.count())
            assert 3 == cnt

            with pytest.raises(RuntimeError) as ctx:
                async with (await conn.begin()) as tr:
                    assert tr.is_active
                    await conn.execute(tbl.delete())
                    raise RuntimeError("Exit")
            assert str(ctx.value) == "Exit"
            assert not tr.is_active
            cnt = await conn.scalar(tbl.count())
            assert 3 == cnt


@pytest.mark.run_loop
async def test_create_engine(loop, mysql_params, table):
    async with sa.create_engine(loop=loop, **mysql_params) as engine:
        async with engine.acquire() as conn:
            ret = []
            async for i in conn.execute(tbl.select()):
                ret.append(i)
            assert [(1, 'a'), (2, 'b'), (3, 'c')] == ret


@pytest.mark.run_loop
async def test_engine(loop, mysql_params, table):
    engine = await sa.create_engine(loop=loop, **mysql_params)
    async with engine:
        async with engine.acquire() as conn:
            ret = []
            async for i in conn.execute(tbl.select()):
                ret.append(i)
            assert [(1, 'a'), (2, 'b'), (3, 'c')] == ret


@pytest.mark.run_loop
async def test_transaction_context_manager(loop, mysql_params, table):
    async with sa.create_engine(loop=loop, **mysql_params) as engine:
        async with engine.acquire() as conn:
            async with conn.begin() as tr:
                async with conn.execute(tbl.select()) as cursor:
                    ret = []
                    async for i in conn.execute(tbl.select()):
                        ret.append(i)
                    assert [(1, 'a'), (2, 'b'), (3, 'c')] == ret
                assert cursor.closed
            assert not tr.is_active

            tr2 = await conn.begin()
            async with tr2:
                assert tr2.is_active
                async with conn.execute('SELECT 1;') as cursor:
                    rec = await cursor.scalar()
                    assert rec == 1
                    await cursor.close()
            assert not tr2.is_active


@pytest.mark.run_loop
async def test_transaction_context_manager_error(loop, mysql_params, table):
    async with sa.create_engine(loop=loop, **mysql_params) as engine:
        async with engine.acquire() as conn:
            with pytest.raises(RuntimeError) as ctx:
                async with conn.begin() as tr:
                    assert tr.is_active
                    raise RuntimeError('boom')
            assert str(ctx.value) == 'boom'
            assert not tr.is_active
    assert conn.closed


@pytest.mark.run_loop
async def test_transaction_context_manager_commit_once(loop, mysql_params,
                                                       table):
    async with sa.create_engine(loop=loop, **mysql_params) as engine:
        async with engine.acquire() as conn:
            async with conn.begin() as tr:
                # check that in context manager we do not execute
                # commit for second time. Two commits in row causes
                # InvalidRequestError exception
                await tr.commit()
            assert not tr.is_active

            tr2 = await conn.begin()
            async with tr2:
                assert tr2.is_active
                # check for double commit one more time
                await tr2.commit()
            assert not tr2.is_active
    assert conn.closed
