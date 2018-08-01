import asyncio
from aiomysql import sa
from sqlalchemy import bindparam

import os
import unittest

from sqlalchemy import MetaData, Table, Column, Integer, String

meta = MetaData()
tbl = Table('sa_tbl_cache_test', meta,
            Column('id', Integer, nullable=False,
                   primary_key=True),
            Column('val', String(255)))


class TestCompiledCache(unittest.TestCase):
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

    async def make_engine(self, **kwargs):
            return (await sa.create_engine(db=self.db,
                                           user=self.user,
                                           password=self.password,
                                           host=self.host,
                                           port=self.port,
                                           loop=self.loop,
                                           minsize=10,
                                           **kwargs))

    async def start(self):
        async with self.engine.acquire() as conn:
            tx = await conn.begin()
            await conn.execute("DROP TABLE IF EXISTS "
                               "sa_tbl_cache_test")
            await conn.execute("CREATE TABLE sa_tbl_cache_test"
                               "(id serial, val varchar(255))")
            await conn.execute(tbl.insert().values(val='some_val_1'))
            await conn.execute(tbl.insert().values(val='some_val_2'))
            await conn.execute(tbl.insert().values(val='some_val_3'))
            await tx.commit()

    def test_cache(self):
        async def go():
            cache = dict()
            engine = await self.make_engine(compiled_cache=cache)
            async with engine.acquire() as conn:
                # check select with params not added to cache
                q = tbl.select().where(tbl.c.val == 'some_val_1')
                cursor = await conn.execute(q)
                row = await cursor.fetchone()
                self.assertEqual('some_val_1', row.val)
                self.assertEqual(0, len(cache))

                # check select with bound params added to cache
                select_by_val = tbl.select().where(
                    tbl.c.val == bindparam('value')
                )
                cursor = await conn.execute(
                    select_by_val, {'value': 'some_val_3'}
                )
                row = await cursor.fetchone()
                self.assertEqual('some_val_3', row.val)
                self.assertEqual(1, len(cache))

                cursor = await conn.execute(
                    select_by_val, value='some_val_2'
                )
                row = await cursor.fetchone()
                self.assertEqual('some_val_2', row.val)
                self.assertEqual(1, len(cache))

                select_all = tbl.select()
                cursor = await conn.execute(select_all)
                rows = await cursor.fetchall()
                self.assertEqual(3, len(rows))
                self.assertEqual(2, len(cache))

                # check insert with bound params not added to cache
                await conn.execute(tbl.insert().values(val='some_val_4'))
                self.assertEqual(2, len(cache))

                # check insert with bound params added to cache
                q = tbl.insert().values(val=bindparam('value'))
                await conn.execute(q, value='some_val_5')
                self.assertEqual(3, len(cache))

                await conn.execute(q, value='some_val_6')
                self.assertEqual(3, len(cache))

                await conn.execute(q, {'value': 'some_val_7'})
                self.assertEqual(3, len(cache))

                cursor = await conn.execute(select_all)
                rows = await cursor.fetchall()
                self.assertEqual(7, len(rows))
                self.assertEqual(3, len(cache))

                # check update with params not added to cache
                q = tbl.update().where(
                    tbl.c.val == 'some_val_1'
                ).values(val='updated_val_1')
                await conn.execute(q)
                self.assertEqual(3, len(cache))
                cursor = await conn.execute(
                    select_by_val, value='updated_val_1'
                )
                row = await cursor.fetchone()
                self.assertEqual('updated_val_1', row.val)

                # check update with bound params added to cache
                q = tbl.update().where(
                    tbl.c.val == bindparam('value')
                ).values(val=bindparam('update'))
                await conn.execute(
                    q, value='some_val_2', update='updated_val_2'
                )
                self.assertEqual(4, len(cache))
                cursor = await conn.execute(
                    select_by_val, value='updated_val_2'
                )
                row = await cursor.fetchone()
                self.assertEqual('updated_val_2', row.val)

        self.loop.run_until_complete(go())
