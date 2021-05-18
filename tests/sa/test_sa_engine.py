import asyncio

import pytest
from sqlalchemy import MetaData, Table, Column, Integer, String

from aiomysql import sa

meta = MetaData()
tbl = Table('sa_tbl3', meta,
            Column('id', Integer, nullable=False,
                   primary_key=True),
            Column('name', String(255)))


@pytest.fixture()
def make_engine(connection, mysql_params):
    async def _make_engine(**kwargs):
        return (await sa.create_engine(db=mysql_params['db'],
                                       user=mysql_params['user'],
                                       password=mysql_params['password'],
                                       host=mysql_params['host'],
                                       port=mysql_params['port'],
                                       minsize=10,
                                       **kwargs))
    return _make_engine


async def start(engine):
    async with engine.acquire() as conn:
        await conn.execute("DROP TABLE IF EXISTS sa_tbl3")
        await conn.execute("CREATE TABLE sa_tbl3 "
                           "(id serial, name varchar(255))")


@pytest.mark.run_loop
async def test_dialect(make_engine):
    engine = await make_engine()
    await start(engine)

    assert sa.engine._dialect == engine.dialect


@pytest.mark.run_loop
async def test_name(make_engine):
    engine = await make_engine()
    await start(engine)
    assert 'mysql' == engine.name


@pytest.mark.run_loop
async def test_driver(make_engine):
    engine = await make_engine()
    await start(engine)
    assert 'pymysql' == engine.driver

# @pytest.mark.run_loop
# async def test_dsn(self):
#     self.assertEqual(
#         'dbname=aiomysql user=aiomysql password=xxxxxx host=127.0.0.1',
#         engine.dsn)


@pytest.mark.run_loop
async def test_minsize(make_engine):
    engine = await make_engine()
    await start(engine)
    assert 10 == engine.minsize


@pytest.mark.run_loop
async def test_maxsize(make_engine):
    engine = await make_engine()
    await start(engine)
    assert 10 == engine.maxsize


@pytest.mark.run_loop
async def test_size(make_engine):
    engine = await make_engine()
    await start(engine)
    assert 10 == engine.size


@pytest.mark.run_loop
async def test_freesize(make_engine):
    engine = await make_engine()
    await start(engine)
    assert 10 == engine.freesize


@pytest.mark.run_loop
async def test_make_engine_with_default_loop(make_engine):
    engine = await make_engine()
    await start(engine)

    engine.close()
    await engine.wait_closed()


@pytest.mark.run_loop
async def test_not_context_manager(make_engine):
    engine = await make_engine()
    await start(engine)
    with pytest.raises(RuntimeError):
        with engine:
            pass


@pytest.mark.run_loop
async def test_release_transacted(make_engine):
    engine = await make_engine()
    await start(engine)
    conn = await engine.acquire()
    tr = await conn.begin()
    with pytest.raises(sa.InvalidRequestError):
        engine.release(conn)
    del tr


@pytest.mark.run_loop
async def test_cannot_acquire_after_closing(make_engine):
    engine = await make_engine()
    await start(engine)
    engine.close()

    with pytest.raises(RuntimeError):
        await engine.acquire()
    await engine.wait_closed()


@pytest.mark.run_loop
async def test_wait_closed(make_engine):
    engine = await make_engine()
    await start(engine)

    c1 = await engine.acquire()
    c2 = await engine.acquire()
    assert 10 == engine.size
    assert 8 == engine.freesize

    ops = []

    async def do_release(conn):
        await asyncio.sleep(0)
        engine.release(conn)
        ops.append('release')

    async def wait_closed():
        await engine.wait_closed()
        ops.append('wait_closed')

    engine.close()
    await asyncio.gather(wait_closed(), do_release(c1),
                         do_release(c2))
    assert ['release', 'release', 'wait_closed'] == ops
    assert 0 == engine.freesize
    engine.close()
    await engine.wait_closed()


@pytest.mark.run_loop
async def test_terminate_with_acquired_connections(make_engine):
    engine = await make_engine()
    await start(engine)

    conn = await engine.acquire()
    engine.terminate()
    await engine.wait_closed()

    assert conn.closed
