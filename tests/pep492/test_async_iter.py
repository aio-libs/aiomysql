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


class TestAsyncIter(AIOPyMySQLTestCase):

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

    def test_async_cursor(self):

        async def go():

            ret = []
            conn = self.connections[0]
            await self._prepare(conn)

            cur = await conn.cursor()
            await cur.execute('SELECT * from tbl;')
            async for i in cur:
                ret.append(i)

            self.assertEqual([(1, 'a'), (2, 'b'), (3, 'c')], ret)

        self.loop.run_until_complete(go())

    def test_async_cursor_server_side(self):

        async def go():
            ret = []
            conn = self.connections[0]
            await self._prepare(conn)

            cur = await conn.cursor(SSCursor)
            await cur.execute('SELECT * from tbl;')
            async for i in cur:
                ret.append(i)

            self.assertEqual([(1, 'a'), (2, 'b'), (3, 'c')], ret)

        self.loop.run_until_complete(go())

    def test_async_iter_over_sa_result(self):

        async def go():

            ret = []
            engine = await sa.create_engine(loop=self.loop,
                                            db=self.db,
                                            user=self.user,
                                            password=self.password,
                                            host=self.host)
            conn = await engine.acquire()
            await self._prepare(conn.connection)

            async for i in (await conn.execute(tbl.select())):
                ret.append(i)

            self.assertEqual([(1, 'a'), (2, 'b'), (3, 'c')], ret)
            engine.terminate()

        self.loop.run_until_complete(go())
