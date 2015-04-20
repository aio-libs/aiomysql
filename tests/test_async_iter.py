import asyncio
from tests.base import AIOPyMySQLTestCase
from aiomysql import SSCursor


class TestAsyncIterationOverCursor(AIOPyMySQLTestCase):

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
            cur = await conn.cursor(SSCursor)
            await cur.execute('SELECT * from tbl;')
            async for i in cur:
                ret.append(i)

            self.assertEqual([(1, 'a'), (2, 'b'), (3, 'c')], ret)

        self.loop.run_until_complete(go())
