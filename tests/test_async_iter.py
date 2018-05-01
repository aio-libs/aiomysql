import pytest

from aiomysql import SSCursor
from aiomysql import sa

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
async def test_async_cursor(cursor, table):
    ret = []
    await cursor.execute('SELECT * from tbl;')
    async for i in cursor:
        ret.append(i)
    assert [(1, 'a'), (2, 'b'), (3, 'c')] == ret


@pytest.mark.run_loop
async def test_async_cursor_server_side(connection, table):
    ret = []
    cursor = await connection.cursor(SSCursor)
    await cursor.execute('SELECT * from tbl;')
    async for i in cursor:
        ret.append(i)
    assert [(1, 'a'), (2, 'b'), (3, 'c')] == ret


@pytest.mark.run_loop
async def test_async_iter_over_sa_result(mysql_params, table, loop):
    ret = []
    engine = await sa.create_engine(**mysql_params, loop=loop)
    conn = await engine.acquire()

    async for i in (await conn.execute(tbl.select())):
        ret.append(i)

    assert [(1, 'a'), (2, 'b'), (3, 'c')] == ret
    engine.terminate()
