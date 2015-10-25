import asyncio
import aiomysql
from aiomysql import sa, create_pool
from sqlalchemy import MetaData, Table, Column, Integer, String

from tests.base import AIOPyMySQLTestCase

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
            await self._prepare(conn)

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
            await self._prepare(conn)

            cur = await conn.cursor()
            await cur.execute('SELECT * from tbl;')

            self.assertFalse(cur.closed)
            async with cur:
                pass

            self.assertTrue(cur.closed)

        self.loop.run_until_complete(go())

    def test_cursor_method(self):

        async def go():
            conn = self.connections[0]
            async with conn.cursor() as cur:
                await cur.execute('SELECT 42;')
                value = await cur.fetchone()
                self.assertEqual(value, (42,))

            self.assertTrue(cur.closed)

        self.loop.run_until_complete(go())

    def test_connection(self):

        async def go():
            conn = self.connections[0]

            self.assertFalse(conn.closed)
            async with conn:
                self.assertFalse(conn.closed)

            self.assertTrue(conn.closed)

        self.loop.run_until_complete(go())

    def test_connect_method(self):
        async def go():
            async with aiomysql.connect(loop=self.loop, host=self.host,
                                        port=self.port, user=self.user,
                                        db=self.db, password=self.password,
                                        use_unicode=True, echo=True) as conn:
                async with (await conn.cursor()) as cur:
                    await cur.execute("SELECT 42")
                    value = await cur.fetchone()
                    self.assertEqual(value, (42,))

            self.assertTrue(cur.closed)
            self.assertTrue(conn.closed)

        self.loop.run_until_complete(go())

    def test_pool(self):

        async def go():
            pool = await create_pool(host=self.host, port=self.port,
                                     user=self.user, db=self.db,
                                     password=self.password, use_unicode=True,
                                     loop=self.loop)
            async with (await pool) as conn:
                await self._prepare(conn)

                async with (await conn.cursor()) as cur:
                    await cur.execute("SELECT * from tbl")
                    ret = []
                    async for i in cur:
                        ret.append(i)
                    self.assertEqual([(1, 'a'), (2, 'b'), (3, 'c')], ret)

        self.loop.run_until_complete(go())

    def test_create_pool(self):

        async def go():
            async with create_pool(host=self.host, port=self.port,
                                   user=self.user, db=self.db,
                                   password=self.password, use_unicode=True,
                                   loop=self.loop) as pool:
                async with pool.get() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("SELECT 42;")
                        value = await cur.fetchone()
                        self.assertEqual(value, (42,))

            self.assertTrue(cur.closed)
            self.assertTrue(conn.closed)

        self.loop.run_until_complete(go())

    def test_engine(self):

        async def go():
            async with (await sa.create_engine(host=self.host,
                                               port=self.port,
                                               user=self.user,
                                               db=self.db,
                                               password=self.password,
                                               use_unicode=True,
                                               loop=self.loop)) as engine:
                async with engine.connect() as conn:
                    await self._prepare(conn.connection)

                    ret = []
                    async for i in (await conn.execute(tbl.select())):
                        ret.append(i)
                    self.assertEqual([(1, 'a'), (2, 'b'), (3, 'c')], ret)

        self.loop.run_until_complete(go())

    def test_sa_connection(self):

        async def go():
            async with (await sa.create_engine(host=self.host,
                                               port=self.port,
                                               user=self.user,
                                               db=self.db,
                                               password=self.password,
                                               use_unicode=True,
                                               loop=self.loop)) as engine:
                conn = await engine.acquire()
                self.assertFalse(conn.closed)
                async with conn:
                    await self._prepare(conn.connection)
                    ret = []
                    async for i in (await conn.execute(tbl.select())):
                        ret.append(i)
                    self.assertEqual([(1, 'a'), (2, 'b'), (3, 'c')], ret)
                self.assertTrue(conn.closed)

        self.loop.run_until_complete(go())

    def test_sa_transaction(self):

        async def go():
            async with (await sa.create_engine(host=self.host,
                                               port=self.port,
                                               user=self.user,
                                               db=self.db,
                                               password=self.password,
                                               use_unicode=True,
                                               loop=self.loop)) as engine:
                async with engine.connect() as conn:
                    await self._prepare(conn.connection)

                    cnt = await conn.scalar(tbl.count())
                    self.assertEqual(3, cnt)

                    async with (await conn.begin()) as tr:
                        self.assertTrue(tr.is_active)
                        await conn.execute(tbl.delete())

                    self.assertFalse(tr.is_active)
                    cnt = await conn.scalar(tbl.count())
                    self.assertEqual(0, cnt)

        self.loop.run_until_complete(go())

    def test_sa_transaction_rollback(self):

        async def go():
            async with (await sa.create_engine(host=self.host,
                                               port=self.port,
                                               user=self.user,
                                               db=self.db,
                                               password=self.password,
                                               use_unicode=True,
                                               loop=self.loop)) as engine:
                async with engine.connect() as conn:
                    await self._prepare(conn.connection)

                    cnt = await conn.scalar(tbl.count())
                    self.assertEqual(3, cnt)

                    with self.assertRaisesRegex(RuntimeError, "Exit"):
                        async with (await conn.begin()) as tr:
                            self.assertTrue(tr.is_active)
                            await conn.execute(tbl.delete())
                            raise RuntimeError("Exit")

                    self.assertFalse(tr.is_active)
                    cnt = await conn.scalar(tbl.count())
                    self.assertEqual(3, cnt)

        self.loop.run_until_complete(go())
