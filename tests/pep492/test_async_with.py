import aiomysql
from aiomysql import sa, create_pool
from sqlalchemy import MetaData, Table, Column, Integer, String

import pytest
import warnings
from tests.base import AIOPyMySQLTestCase

meta = MetaData()
tbl = Table('tbl', meta,
            Column('id', Integer, nullable=False,
                   primary_key=True),
            Column('name', String(255)))


class TestAsyncWith(AIOPyMySQLTestCase):

    def _conn_kw(self):
        kw = dict(host=self.host, port=self.port, user=self.user,
                  db=self.db, password=self.password, use_unicode=True,
                  loop=self.loop)
        return kw

    async def _prepare(self, conn):
        cur = await conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS tbl;")

        await cur.execute("""CREATE TABLE tbl (
                 id MEDIUMINT NOT NULL AUTO_INCREMENT,
                 name VARCHAR(255) NOT NULL,
                 PRIMARY KEY (id));""")

        for i in [(1, 'a'), (2, 'b'), (3, 'c')]:
            await cur.execute("INSERT INTO tbl VALUES(%s, %s)", i)
        await conn.commit()

    def test_cursor(self):

        async def go():
            ret = []
            conn = self.connections[0]
            await self._prepare(conn)

            cur = await conn.cursor()
            await cur.execute('SELECT * from tbl;')

            assert not cur.closed
            async with cur:
                async for i in cur:
                    ret.append(i)

            assert [(1, 'a'), (2, 'b'), (3, 'c')] == ret
            assert cur.closed

        self.loop.run_until_complete(go())

    def test_cursor_lightweight(self):

        async def go():
            conn = self.connections[0]
            await self._prepare(conn)

            cur = await conn.cursor()
            await cur.execute('SELECT * from tbl;')

            assert not cur.closed
            async with cur:
                pass

            assert cur.closed

        self.loop.run_until_complete(go())

    def test_cursor_method(self):

        async def go():
            conn = self.connections[0]
            async with conn.cursor() as cur:
                await cur.execute('SELECT 42;')
                value = await cur.fetchone()
                assert value == (42,)

            assert cur.closed

        self.loop.run_until_complete(go())

    def test_connection(self):

        async def go():
            conn = self.connections[0]

            assert not conn.closed
            async with conn:
                assert not conn.closed

            assert conn.closed

        self.loop.run_until_complete(go())

    def test_connection_exception(self):

        async def go():
            conn = self.connections[0]

            assert not conn.closed
            with pytest.raises(RuntimeError) as ctx:
                async with conn:
                    assert not conn.closed
                    raise RuntimeError('boom')
            assert str(ctx.value) == 'boom'
            assert conn.closed

        self.loop.run_until_complete(go())

    def test_connect_method(self):
        async def go():
            async with aiomysql.connect(loop=self.loop, host=self.host,
                                        port=self.port, user=self.user,
                                        db=self.db, password=self.password,
                                        use_unicode=True, echo=True) as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT 42")
                    value = await cur.fetchone()
                    assert value, (42,)

            assert cur.closed
            assert conn.closed

        self.loop.run_until_complete(go())

    def test_connect_method_exception(self):
        kw = dict(loop=self.loop, host=self.host, port=self.port,
                  user=self.user, db=self.db, password=self.password,
                  use_unicode=True, echo=True)
        async def go():
            with pytest.raises(RuntimeError) as ctx:
                async with aiomysql.connect(**kw) as conn:
                    assert not conn.closed
                    raise RuntimeError('boom')

            assert str(ctx.value) == 'boom'
            assert conn.closed

        self.loop.run_until_complete(go())

    def test_pool(self):
        kw = self._conn_kw()
        async def go():
            pool = await create_pool(**kw)
            async with pool.acquire() as conn:
                await self._prepare(conn)

                async with (await conn.cursor()) as cur:
                    await cur.execute("SELECT * from tbl")
                    ret = []
                    async for i in cur:
                        ret.append(i)
                    assert [(1, 'a'), (2, 'b'), (3, 'c')] == ret

        self.loop.run_until_complete(go())

    def test_create_pool_deprecations(self):

        kw = self._conn_kw()
        async def go():
            async with create_pool(**kw) as pool:
                with warnings.catch_warnings(record=True) as w:
                    warnings.simplefilter("always")
                    async with pool.get() as conn:
                        pass
            assert issubclass(w[-1].category, DeprecationWarning)
            assert conn.closed

            async with create_pool(**kw) as pool:
                with warnings.catch_warnings(record=True) as w:
                    warnings.simplefilter("always")
                    with await pool as conn:
                        pass
            assert issubclass(w[-1].category, DeprecationWarning)
            assert conn.closed

        self.loop.run_until_complete(go())

    def test_sa_connection(self):

        kw = self._conn_kw()
        async def go():
            async with sa.create_engine(**kw) as engine:
                conn = await engine.acquire()
                assert not conn.closed
                async with conn:
                    await self._prepare(conn.connection)
                    ret = []
                    async for i in conn.execute(tbl.select()):
                        ret.append(i)
                    assert [(1, 'a'), (2, 'b'), (3, 'c')] == ret
                assert conn.closed

        self.loop.run_until_complete(go())

    def test_sa_transaction(self):

        kw = self._conn_kw()
        async def go():
            async with sa.create_engine(**kw) as engine:
                async with engine.acquire() as conn:
                    await self._prepare(conn.connection)

                    cnt = await conn.scalar(tbl.count())
                    assert 3 == cnt

                    async with (await conn.begin()) as tr:
                        assert tr.is_active
                        await conn.execute(tbl.delete())

                    assert not tr.is_active
                    cnt = await conn.scalar(tbl.count())
                    assert 0 == cnt

        self.loop.run_until_complete(go())

    def test_sa_transaction_rollback(self):
        kw = self._conn_kw()
        async def go():
            async with sa.create_engine(**kw) as engine:
                async with engine.acquire() as conn:
                    await self._prepare(conn.connection)

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

        self.loop.run_until_complete(go())

    def test_create_engine(self):
        kw = self._conn_kw()
        async def go():
            async with sa.create_engine(**kw) as engine:
                async with engine.acquire() as conn:
                    await self._prepare(conn.connection)

                    ret = []
                    async for i in conn.execute(tbl.select()):
                        ret.append(i)
                    assert [(1, 'a'), (2, 'b'), (3, 'c')] == ret

        self.loop.run_until_complete(go())

    def test_engine(self):
        kw = self._conn_kw()
        async def go():
            engine = await sa.create_engine(**kw)
            async with engine:
                async with engine.acquire() as conn:
                    await self._prepare(conn.connection)

                    ret = []
                    async for i in conn.execute(tbl.select()):
                        ret.append(i)
                    assert [(1, 'a'), (2, 'b'), (3, 'c')] == ret

        self.loop.run_until_complete(go())

    def test_transaction_context_manager(self):
        async def go():
            kw = self._conn_kw()
            async with sa.create_engine(**kw) as engine:
                async with engine.acquire() as conn:
                    await self._prepare(conn.connection)
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
                            cursor.close()
                    assert not tr2.is_active

            assert conn.closed
        self.loop.run_until_complete(go())

    def test_transaction_context_manager_error(self):
        async def go():
            kw = self._conn_kw()
            async with sa.create_engine(**kw) as engine:
                async with engine.acquire() as conn:
                    with pytest.raises(RuntimeError) as ctx:
                        async with conn.begin() as tr:
                            assert tr.is_active
                            raise RuntimeError('boom')
                    assert str(ctx.value) == 'boom'
                    assert not tr.is_active
            assert conn.closed
        self.loop.run_until_complete(go())

    def test_transaction_context_manager_commit_once(self):
        async def go():
            kw = self._conn_kw()
            async with sa.create_engine(**kw) as engine:
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
        self.loop.run_until_complete(go())
