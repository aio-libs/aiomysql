import asyncio
from aiomysql import connect, sa
import functools

import os
import unittest
from unittest import mock

from sqlalchemy import MetaData, Table, Column, Integer, String

meta = MetaData()
tbl = Table('sa_tbl2', meta,
            Column('id', Integer, nullable=False,
                   primary_key=True),
            Column('name', String(255)))


def check_prepared_transactions(func):
    @functools.wraps(func)
    async def wrapper(self):
        conn = await self.loop.run_until_complete(self._connect())
        val = await conn.scalar('show max_prepared_transactions')
        if not val:
            raise unittest.SkipTest('Twophase transacions are not supported. '
                                    'Set max_prepared_transactions to '
                                    'a nonzero value')
        return func(self)
    return wrapper


class TestTransaction(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        self.host = os.environ.get('MYSQL_HOST', 'localhost')
        self.port = int(os.environ.get('MYSQL_PORT', 3306))
        self.user = os.environ.get('MYSQL_USER', 'root')
        self.db = os.environ.get('MYSQL_DB', 'test_pymysql')
        self.password = os.environ.get('MYSQL_PASSWORD', '')
        self.loop.run_until_complete(self.start())

    def tearDown(self):
        self.loop.close()

    async def start(self, **kwargs):
        conn = await self.connect(**kwargs)
        await conn.execute("DROP TABLE IF EXISTS sa_tbl2")
        await conn.execute("CREATE TABLE sa_tbl2 "
                           "(id serial, name varchar(255))")
        await conn.execute("INSERT INTO sa_tbl2 (name)"
                           "VALUES ('first')")
        await conn._connection.commit()

    async def connect(self, **kwargs):
        conn = await connect(db=self.db,
                             user=self.user,
                             password=self.password,
                             host=self.host,
                             port=self.port,
                             loop=self.loop,
                             **kwargs)
        # TODO: fix this, should autocommit be enabled by default?
        await conn.autocommit(True)
        engine = mock.Mock()
        engine.dialect = sa.engine._dialect

        def release(*args):
            return
        engine.release = release

        ret = sa.SAConnection(conn, engine)
        return ret

    def test_without_transactions(self):
        async def go():
            conn1 = await self.connect()
            conn2 = await self.connect()
            res1 = await conn1.scalar(tbl.count())
            self.assertEqual(1, res1)

            await conn2.execute(tbl.delete())

            res2 = await conn1.scalar(tbl.count())
            self.assertEqual(0, res2)
            await conn1.close()
            await conn2.close()

        self.loop.run_until_complete(go())

    def test_connection_attr(self):
        async def go():
            conn = await self.connect()
            tr = await conn.begin()
            self.assertIs(tr.connection, conn)
            await conn.close()

        self.loop.run_until_complete(go())

    def test_root_transaction(self):
        async def go():
            conn1 = await self.connect()
            conn2 = await self.connect()

            tr = await conn1.begin()
            self.assertTrue(tr.is_active)
            await conn1.execute(tbl.delete())

            res1 = await conn2.scalar(tbl.count())
            self.assertEqual(1, res1)

            await tr.commit()

            self.assertFalse(tr.is_active)
            self.assertFalse(conn1.in_transaction)
            res2 = await conn2.scalar(tbl.count())
            self.assertEqual(0, res2)
            await conn1.close()
            await conn2.close()

        self.loop.run_until_complete(go())

    def test_root_transaction_rollback(self):
        async def go():
            conn1 = await self.connect()
            conn2 = await self.connect()

            tr = await conn1.begin()
            self.assertTrue(tr.is_active)
            await conn1.execute(tbl.delete())

            res1 = await conn2.scalar(tbl.count())
            self.assertEqual(1, res1)

            await tr.rollback()

            self.assertFalse(tr.is_active)
            res2 = await conn2.scalar(tbl.count())
            self.assertEqual(1, res2)
            await conn1.close()
            await conn2.close()

        self.loop.run_until_complete(go())

    def test_root_transaction_close(self):
        async def go():
            conn1 = await self.connect()
            conn2 = await self.connect()

            tr = await conn1.begin()
            self.assertTrue(tr.is_active)
            await conn1.execute(tbl.delete())

            res1 = await conn2.scalar(tbl.count())
            self.assertEqual(1, res1)

            await tr.close()

            self.assertFalse(tr.is_active)
            res2 = await conn2.scalar(tbl.count())
            self.assertEqual(1, res2)
            await conn1.close()
            await conn2.close()

        self.loop.run_until_complete(go())

    def test_rollback_on_connection_close(self):
        async def go():
            conn1 = await self.connect()
            conn2 = await self.connect()

            tr = await conn1.begin()
            await conn1.execute(tbl.delete())

            res1 = await conn2.scalar(tbl.count())
            self.assertEqual(1, res1)

            await conn1.close()

            res2 = await conn2.scalar(tbl.count())
            self.assertEqual(1, res2)
            del tr
            await conn1.close()
            await conn2.close()

        self.loop.run_until_complete(go())

    def test_root_transaction_commit_inactive(self):
        async def go():
            conn = await self.connect()
            tr = await conn.begin()
            self.assertTrue(tr.is_active)
            await tr.commit()
            self.assertFalse(tr.is_active)
            with self.assertRaises(sa.InvalidRequestError):
                await tr.commit()
            await conn.close()

        self.loop.run_until_complete(go())

    def test_root_transaction_rollback_inactive(self):
        async def go():
            conn = await self.connect()
            tr = await conn.begin()
            self.assertTrue(tr.is_active)
            await tr.rollback()
            self.assertFalse(tr.is_active)
            await tr.rollback()
            self.assertFalse(tr.is_active)
            await conn.close()

        self.loop.run_until_complete(go())

    def test_root_transaction_double_close(self):
        async def go():
            conn = await self.connect()
            tr = await conn.begin()
            self.assertTrue(tr.is_active)
            await tr.close()
            self.assertFalse(tr.is_active)
            await tr.close()
            self.assertFalse(tr.is_active)
            await conn.close()

        self.loop.run_until_complete(go())

    def test_inner_transaction_commit(self):
        async def go():
            conn = await self.connect()
            tr1 = await conn.begin()
            tr2 = await conn.begin()
            self.assertTrue(tr2.is_active)

            await tr2.commit()
            self.assertFalse(tr2.is_active)
            self.assertTrue(tr1.is_active)

            await tr1.commit()
            self.assertFalse(tr2.is_active)
            self.assertFalse(tr1.is_active)
            await conn.close()

        self.loop.run_until_complete(go())

    def test_inner_transaction_rollback(self):
        async def go():
            conn = await self.connect()
            tr1 = await conn.begin()
            tr2 = await conn.begin()
            self.assertTrue(tr2.is_active)
            await conn.execute(tbl.insert().values(name='aaaa'))

            await tr2.rollback()
            self.assertFalse(tr2.is_active)
            self.assertFalse(tr1.is_active)

            res = await conn.scalar(tbl.count())
            self.assertEqual(1, res)
            await conn.close()

        self.loop.run_until_complete(go())

    def test_inner_transaction_close(self):
        async def go():
            conn = await self.connect()
            tr1 = await conn.begin()
            tr2 = await conn.begin()
            self.assertTrue(tr2.is_active)
            await conn.execute(tbl.insert().values(name='aaaa'))

            await tr2.close()
            self.assertFalse(tr2.is_active)
            self.assertTrue(tr1.is_active)
            await tr1.commit()

            res = await conn.scalar(tbl.count())
            self.assertEqual(2, res)
            await conn.close()

        self.loop.run_until_complete(go())

    def test_nested_transaction_commit(self):
        async def go():
            conn = await self.connect()
            tr1 = await conn.begin_nested()
            tr2 = await conn.begin_nested()
            self.assertTrue(tr1.is_active)
            self.assertTrue(tr2.is_active)

            await conn.execute(tbl.insert().values(name='aaaa'))
            await tr2.commit()
            self.assertFalse(tr2.is_active)
            self.assertTrue(tr1.is_active)

            res = await conn.scalar(tbl.count())
            self.assertEqual(2, res)

            await tr1.commit()
            self.assertFalse(tr2.is_active)
            self.assertFalse(tr1.is_active)

            res = await conn.scalar(tbl.count())
            self.assertEqual(2, res)
            await conn.close()

        self.loop.run_until_complete(go())

    def test_nested_transaction_commit_twice(self):
        async def go():
            conn = await self.connect()
            tr1 = await conn.begin_nested()
            tr2 = await conn.begin_nested()

            await conn.execute(tbl.insert().values(name='aaaa'))
            await tr2.commit()
            self.assertFalse(tr2.is_active)
            self.assertTrue(tr1.is_active)

            await tr2.commit()
            self.assertFalse(tr2.is_active)
            self.assertTrue(tr1.is_active)

            res = await conn.scalar(tbl.count())
            self.assertEqual(2, res)

            await tr1.close()
            await conn.close()

        self.loop.run_until_complete(go())

    def test_nested_transaction_rollback(self):
        async def go():
            conn = await self.connect()
            tr1 = await conn.begin_nested()
            tr2 = await conn.begin_nested()
            self.assertTrue(tr1.is_active)
            self.assertTrue(tr2.is_active)

            await conn.execute(tbl.insert().values(name='aaaa'))
            await tr2.rollback()
            self.assertFalse(tr2.is_active)
            self.assertTrue(tr1.is_active)

            res = await conn.scalar(tbl.count())
            self.assertEqual(1, res)

            await tr1.commit()
            self.assertFalse(tr2.is_active)
            self.assertFalse(tr1.is_active)

            res = await conn.scalar(tbl.count())
            self.assertEqual(1, res)
            await conn.close()

        self.loop.run_until_complete(go())

    def test_nested_transaction_rollback_twice(self):
        async def go():
            conn = await self.connect()
            tr1 = await conn.begin_nested()
            tr2 = await conn.begin_nested()

            await conn.execute(tbl.insert().values(name='aaaa'))
            await tr2.rollback()
            self.assertFalse(tr2.is_active)
            self.assertTrue(tr1.is_active)

            await tr2.rollback()
            self.assertFalse(tr2.is_active)
            self.assertTrue(tr1.is_active)

            await tr1.commit()
            res = await conn.scalar(tbl.count())
            self.assertEqual(1, res)
            await conn.close()

        self.loop.run_until_complete(go())

    def test_twophase_transaction_commit(self):
        async def go():
            conn = await self.connect()
            tr = await conn.begin_twophase('sa_twophase')
            self.assertEqual(tr.xid, 'sa_twophase')
            await conn.execute(tbl.insert().values(name='aaaa'))

            await tr.prepare()
            self.assertTrue(tr.is_active)

            await tr.commit()
            self.assertFalse(tr.is_active)

            res = await conn.scalar(tbl.count())
            self.assertEqual(2, res)
            await conn.close()

        self.loop.run_until_complete(go())

    def test_twophase_transaction_twice(self):
        async def go():
            conn = await self.connect()
            tr = await conn.begin_twophase()
            with self.assertRaises(sa.InvalidRequestError):
                await conn.begin_twophase()

            self.assertTrue(tr.is_active)
            await tr.prepare()
            await tr.commit()
            await conn.close()

        self.loop.run_until_complete(go())

    def test_transactions_sequence(self):
        async def go():
            conn = await self.connect()

            await conn.execute(tbl.delete())

            self.assertIsNone(conn._transaction)

            tr1 = await conn.begin()
            self.assertIs(tr1, conn._transaction)
            await conn.execute(tbl.insert().values(name='a'))
            res1 = await conn.scalar(tbl.count())
            self.assertEqual(1, res1)

            await tr1.commit()
            self.assertIsNone(conn._transaction)

            tr2 = await conn.begin()
            self.assertIs(tr2, conn._transaction)
            await conn.execute(tbl.insert().values(name='b'))
            res2 = await conn.scalar(tbl.count())
            self.assertEqual(2, res2)
            await tr2.rollback()
            self.assertIsNone(conn._transaction)

            tr3 = await conn.begin()
            self.assertIs(tr3, conn._transaction)
            await conn.execute(tbl.insert().values(name='b'))
            res3 = await conn.scalar(tbl.count())
            self.assertEqual(2, res3)
            await tr3.commit()
            self.assertIsNone(conn._transaction)
            await conn.close()

        self.loop.run_until_complete(go())
