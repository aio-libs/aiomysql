import pytest
from sqlalchemy import bindparam
from sqlalchemy import MetaData, Table, Column, Integer, String

from aiomysql import sa


meta = MetaData()
tbl = Table('sa_tbl_cache_test', meta,
            Column('id', Integer, nullable=False,
                   primary_key=True),
            Column('val', String(255)))


@pytest.fixture()
def make_engine(connection, mysql_params, loop):
    engines = []

    async def _make_engine(**kwargs):
        if "unix_socket" in mysql_params:
            conn_args = {"unix_socket": mysql_params["unix_socket"]}
        else:
            conn_args = {
                "host": mysql_params['host'],
                "port": mysql_params['port'],
            }
            if "ssl" in mysql_params:
                conn_args["ssl"] = mysql_params["ssl"]

        engine = await sa.create_engine(
            db=mysql_params['db'],
            user=mysql_params['user'],
            password=mysql_params['password'],
            minsize=10,
            **conn_args,
            **kwargs,
        )

        engines.append(engine)

        return engine

    yield _make_engine

    for engine in engines:
        engine.terminate()
        loop.run_until_complete(engine.wait_closed())


async def start(engine):
    async with engine.acquire() as conn:
        tx = await conn.begin()
        await conn.execute("DROP TABLE IF EXISTS "
                           "sa_tbl_cache_test")
        await conn.execute("CREATE TABLE sa_tbl_cache_test"
                           "(id serial, val varchar(255))")
        await conn.execute(tbl.insert().values(val='some_val_1'))
        await conn.execute(tbl.insert().values(val='some_val_2'))
        await conn.execute(tbl.insert().values(val='some_val_3'))
        await tx.commit()


@pytest.mark.run_loop
async def test_dialect(make_engine):
    cache = dict()
    engine = await make_engine(compiled_cache=cache)
    await start(engine)

    async with engine.acquire() as conn:
        # check select with params not added to cache
        q = tbl.select().where(tbl.c.val == 'some_val_1')
        cursor = await conn.execute(q)
        row = await cursor.fetchone()
        assert 'some_val_1' == row.val
        assert 0 == len(cache)

        # check select with bound params added to cache
        select_by_val = tbl.select().where(
            tbl.c.val == bindparam('value')
        )
        cursor = await conn.execute(
            select_by_val, {'value': 'some_val_3'}
        )
        row = await cursor.fetchone()
        assert 'some_val_3' == row.val
        assert 1 == len(cache)

        cursor = await conn.execute(
            select_by_val, value='some_val_2'
        )
        row = await cursor.fetchone()
        assert 'some_val_2' == row.val
        assert 1 == len(cache)

        select_all = tbl.select()
        cursor = await conn.execute(select_all)
        rows = await cursor.fetchall()
        assert 3 == len(rows)
        assert 2 == len(cache)

        # check insert with bound params not added to cache
        await conn.execute(tbl.insert().values(val='some_val_4'))
        assert 2 == len(cache)

        # check insert with bound params added to cache
        q = tbl.insert().values(val=bindparam('value'))
        await conn.execute(q, value='some_val_5')
        assert 3 == len(cache)

        await conn.execute(q, value='some_val_6')
        assert 3 == len(cache)

        await conn.execute(q, {'value': 'some_val_7'})
        assert 3 == len(cache)

        cursor = await conn.execute(select_all)
        rows = await cursor.fetchall()
        assert 7 == len(rows)
        assert 3 == len(cache)

        # check update with params not added to cache
        q = tbl.update().where(
            tbl.c.val == 'some_val_1'
        ).values(val='updated_val_1')
        await conn.execute(q)
        assert 3 == len(cache)
        cursor = await conn.execute(
            select_by_val, value='updated_val_1'
        )
        row = await cursor.fetchone()
        assert 'updated_val_1' == row.val

        # check update with bound params added to cache
        q = tbl.update().where(
            tbl.c.val == bindparam('value')
        ).values(val=bindparam('update'))
        await conn.execute(
            q, value='some_val_2', update='updated_val_2'
        )
        assert 4 == len(cache)
        cursor = await conn.execute(
            select_by_val, value='updated_val_2'
        )
        row = await cursor.fetchone()
        assert 'updated_val_2' == row.val
