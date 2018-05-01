import asyncio
from aiomysql import sa
# from aiomysql.connection import TIMEOUT

import os
import unittest

from sqlalchemy import MetaData, Table, Column, Integer, String

meta = MetaData()
tbl = Table('sa_tbl3', meta,
            Column('id', Integer, nullable=False,
                   primary_key=True),
            Column('name', String(255)))


class TestEngine(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        self.host = os.environ.get('MYSQL_HOST', 'localhost')
        self.port = int(os.environ.get('MYSQL_PORT', 3306))
        self.user = os.environ.get('MYSQL_USER', 'root')
        self.db = os.environ.get('MYSQL_DB', 'test_pymysql')
        self.password = os.environ.get('MYSQL_PASSWORD', '')
        self.engine = self.loop.run_until_complete(self.make_engine())
        self.loop.run_until_complete(self.start())

    def tearDown(self):
        self.engine.terminate()
        self.loop.run_until_complete(self.engine.wait_closed())
        self.loop.close()

    async def make_engine(self, use_loop=True, **kwargs):
        if use_loop:
            return (await sa.create_engine(db=self.db,
                                           user=self.user,
                                           password=self.password,
                                           host=self.host,
                                           port=self.port,
                                           loop=self.loop,
                                           minsize=10,
                                           **kwargs))
        else:
            return (await sa.create_engine(db=self.db,
                                           user=self.user,
                                           password=self.password,
                                           host=self.host,
                                           port=self.port,
                                           minsize=10,
                                           **kwargs))

    async def start(self):
        async with self.engine.acquire() as conn:
            await conn.execute("DROP TABLE IF EXISTS sa_tbl3")
            await conn.execute("CREATE TABLE sa_tbl3 "
                               "(id serial, name varchar(255))")

    def test_dialect(self):
        self.assertEqual(sa.engine._dialect, self.engine.dialect)

    def test_name(self):
        self.assertEqual('mysql', self.engine.name)

    def test_driver(self):
        self.assertEqual('pymysql', self.engine.driver)

    # def test_dsn(self):
    #     self.assertEqual(
    #         'dbname=aiomysql user=aiomysql password=xxxxxx host=127.0.0.1',
    #         self.engine.dsn)

    def test_minsize(self):
        self.assertEqual(10, self.engine.minsize)

    def test_maxsize(self):
        self.assertEqual(10, self.engine.maxsize)

    def test_size(self):
        self.assertEqual(10, self.engine.size)

    def test_freesize(self):
        self.assertEqual(10, self.engine.freesize)

    def test_make_engine_with_default_loop(self):

        async def go():
            engine = await self.make_engine(use_loop=False)
            engine.close()
            await engine.wait_closed()

        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(go())
        finally:
            asyncio.set_event_loop(None)

    def test_not_context_manager(self):
        async def go():
            with self.assertRaises(RuntimeError):
                with self.engine:
                    pass
        self.loop.run_until_complete(go())

    def test_release_transacted(self):
        async def go():
            conn = await self.engine.acquire()
            tr = await conn.begin()
            with self.assertRaises(sa.InvalidRequestError):
                self.engine.release(conn)
            del tr
        self.loop.run_until_complete(go())

    # def test_timeout(self):
    #     self.assertEqual(TIMEOUT, self.engine.timeout)

    # def test_timeout_override(self):
    #     @asyncio.coroutine
    #     def go():
    #         timeout = 1
    #         engine = yield from self.make_engine(timeout=timeout)
    #         self.assertEqual(timeout, engine.timeout)
    #         conn = yield from engine.acquire()
    #         with self.assertRaises(asyncio.TimeoutError):
    #             yield from conn.execute("SELECT pg_sleep(10)")
    #     self.loop.run_until_complete(go())

    def test_cannot_acquire_after_closing(self):
        async def go():
            engine = await self.make_engine()
            engine.close()

            with self.assertRaises(RuntimeError):
                await engine.acquire()
            await engine.wait_closed()
        self.loop.run_until_complete(go())

    def test_wait_closed(self):
        async def go():
            engine = await self.make_engine()

            c1 = await engine.acquire()
            c2 = await engine.acquire()
            self.assertEqual(10, engine.size)
            self.assertEqual(8, engine.freesize)

            ops = []

            async def do_release(conn):
                await asyncio.sleep(0, loop=self.loop)
                engine.release(conn)
                ops.append('release')

            async def wait_closed():
                await engine.wait_closed()
                ops.append('wait_closed')

            engine.close()
            await asyncio.gather(wait_closed(), do_release(c1),
                                 do_release(c2), loop=self.loop)
            self.assertEqual(['release', 'release', 'wait_closed'], ops)
            self.assertEqual(0, engine.freesize)
            engine.close()
            await engine.wait_closed()

        self.loop.run_until_complete(go())

    def test_terminate_with_acquired_connections(self):

        async def go():
            engine = await self.make_engine()
            conn = await engine.acquire()
            engine.terminate()
            await engine.wait_closed()

            self.assertTrue(conn.closed)

        self.loop.run_until_complete(go())
