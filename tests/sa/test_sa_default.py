import datetime

import pytest
from sqlalchemy import MetaData, Table, Column, Integer, String
from sqlalchemy import func, DateTime, Boolean

from aiomysql import sa

meta = MetaData()
table = Table('sa_tbl_default_test', meta,
              Column('id', Integer, nullable=False, primary_key=True),
              Column('string_length', Integer,
                     default=func.length('qwerty')),
              Column('number', Integer, default=100, nullable=False),
              Column('description', String(255), nullable=False,
                     default='default test'),
              Column('created_at', DateTime,
                     default=datetime.datetime.now),
              Column('enabled', Boolean, default=True))


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
        await conn.execute("DROP TABLE IF EXISTS sa_tbl_default_test")
        await conn.execute("CREATE TABLE sa_tbl_default_test "
                           "(id integer,"
                           " string_length integer, "
                           "number integer,"
                           " description VARCHAR(255), "
                           "created_at DATETIME(6), "
                           "enabled TINYINT)")


@pytest.mark.run_loop
async def test_default_fields(make_engine):
    engine = await make_engine()
    await start(engine)
    async with engine.acquire() as conn:
        await conn.execute(table.insert().values())
        res = await conn.execute(table.select())
        row = await res.fetchone()
        assert row.string_length == 6
        assert row.number == 100
        assert row.description == 'default test'
        assert row.enabled is True
        assert type(row.created_at) == datetime.datetime


@pytest.mark.run_loop
async def test_default_fields_isnull(make_engine):
    engine = await make_engine()
    await start(engine)
    async with engine.acquire() as conn:
        created_at = None
        enabled = False
        await conn.execute(table.insert().values(
            enabled=enabled,
            created_at=created_at,
        ))

        res = await conn.execute(table.select())
        row = await res.fetchone()
        assert row.number == 100
        assert row.string_length == 6
        assert row.description == 'default test'
        assert row.enabled == enabled
        assert row.created_at == created_at


@pytest.mark.run_loop
async def test_default_fields_edit(make_engine):
    engine = await make_engine()
    await start(engine)
    async with engine.acquire() as conn:
        created_at = datetime.datetime.now()
        description = 'new descr'
        enabled = False
        number = 111
        await conn.execute(table.insert().values(
            description=description,
            enabled=enabled,
            created_at=created_at,
            number=number,
        ))

        res = await conn.execute(table.select())
        row = await res.fetchone()
        assert row.number == number
        assert row.string_length == 6
        assert row.description == description
        assert row.enabled == enabled
        assert row.created_at == created_at
