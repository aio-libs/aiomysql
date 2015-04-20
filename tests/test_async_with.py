import asyncio
from tests.base import AIOPyMySQLTestCase
from aiomysql import SSCursor
from aiomysql import sa

from sqlalchemy import MetaData, Table, Column, Integer, String

meta = MetaData()
tbl = Table('tbl', meta,
            Column('id', Integer, nullable=False,
                   primary_key=True),
            Column('name', String(255)))


class TestAsyncWith(AIOPyMySQLTestCase):

    @asyncio.coroutine
    def _prepare(self, conn):
        cur = yield from conn.cursor()
        yield from cur.execute("DROP TABLE IF EXISTS tbl;")

        yield from cur.execute("""CREATE TABLE tbl (
                 id MEDIUMINT NOT NULL AUTO_INCREMENT,
                 name VARCHAR(255) NOT NULL,
                 PRIMARY KEY (id));""")

        for i in [(1, 'a'), (2, 'b'), (3, 'c')]:
            yield from cur.execute("INSERT INTO tbl VALUES(%s, %s)", i)
        yield from conn.commit()

    def test_cursor(self):

        async def go():
            ret = []
            conn = self.connections[0]
            cur = await conn.cursor()
            await cur.execute('SELECT * from tbl;')

            self.assertFalse(cur.closed)
            async with cur:
                async for i in cur:
                    ret.append(i)

            self.assertEqual([(1, 'a'), (2, 'b'), (3, 'c')], ret)
            self.assertTrue(cur.closed)

        self.loop.run_until_complete(go())

    def test_cursor_lightweight(self):

        async def go():
            conn = self.connections[0]
            cur = await conn.cursor()
            await cur.execute('SELECT * from tbl;')

            self.assertFalse(cur.closed)
            async with cur:
                pass

            self.assertTrue(cur.closed)

        self.loop.run_until_complete(go())

    def test_connection(self):

        async def go():
            conn = self.connections[0]

            self.assertFalse(conn.closed)
            async with conn:
                pass

            self.assertTrue(conn.closed)

        self.loop.run_until_complete(go())
