import asyncio
import aiomysql
from aiomysql import connect, sa, Cursor

import os
import unittest
from unittest import mock

from sqlalchemy import MetaData, Table, Column, Integer, String
from sqlalchemy.schema import DropTable, CreateTable


meta = MetaData()
tbl = Table('sa_tbl', meta,
            Column('id', Integer, nullable=False,
                   primary_key=True),
            Column('name', String(255)))


class TestSAConnection(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        self.host = os.environ.get('MYSQL_HOST', 'localhost')
        self.port = int(os.environ.get('MYSQL_PORT', 3306))
        self.user = os.environ.get('MYSQL_USER', 'root')
        self.db = os.environ.get('MYSQL_DB', 'test_pymysql')
        self.password = os.environ.get('MYSQL_PASSWORD', '')

    def tearDown(self):
        self.loop.close()

    async def connect(self, **kwargs):
        conn = await connect(db=self.db,
                             user=self.user,
                             password=self.password,
                             host=self.host,
                             loop=self.loop,
                             port=self.port,
                             **kwargs)
        await conn.autocommit(True)
        cur = await conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS sa_tbl")
        await cur.execute("CREATE TABLE sa_tbl "
                          "(id serial, name varchar(255))")
        await cur.execute("INSERT INTO sa_tbl (name)"
                          "VALUES ('first')")

        await cur._connection.commit()
        # yield from cur.close()
        engine = mock.Mock()
        engine.dialect = sa.engine._dialect
        return sa.SAConnection(conn, engine)

    def test_execute_text_select(self):
        async def go():
            conn = await self.connect()
            res = await conn.execute("SELECT * FROM sa_tbl;")
            self.assertIsInstance(res.cursor, Cursor)
            self.assertEqual(('id', 'name'), res.keys())
            rows = await res.fetchall()
            self.assertTrue(res.closed)
            self.assertIsNone(res.cursor)
            self.assertEqual(1, len(rows))
            row = rows[0]
            self.assertEqual(1, row[0])
            self.assertEqual(1, row['id'])
            self.assertEqual(1, row.id)
            self.assertEqual('first', row[1])
            self.assertEqual('first', row['name'])
            self.assertEqual('first', row.name)
            # TODO: fix this
            await conn._connection.commit()
        self.loop.run_until_complete(go())

    def test_execute_sa_select(self):
        async def go():
            conn = await self.connect()
            res = await conn.execute(tbl.select())
            self.assertIsInstance(res.cursor, Cursor)
            self.assertEqual(('id', 'name'), res.keys())
            rows = await res.fetchall()
            self.assertTrue(res.closed)
            self.assertIsNone(res.cursor)
            self.assertTrue(res.returns_rows)

            self.assertEqual(1, len(rows))
            row = rows[0]
            self.assertEqual(1, row[0])
            self.assertEqual(1, row['id'])
            self.assertEqual(1, row.id)
            self.assertEqual('first', row[1])
            self.assertEqual('first', row['name'])
            self.assertEqual('first', row.name)
            # TODO: fix this
            await conn._connection.commit()

        self.loop.run_until_complete(go())

    def test_execute_sa_insert_with_dict(self):
        async def go():
            conn = await self.connect()
            await conn.execute(tbl.insert(), {"id": 2, "name": "second"})

            res = await conn.execute(tbl.select())
            rows = await res.fetchall()
            self.assertEqual(2, len(rows))
            self.assertEqual((1, 'first'), rows[0])
            self.assertEqual((2, 'second'), rows[1])

        self.loop.run_until_complete(go())

    def test_execute_sa_insert_with_tuple(self):
        async def go():
            conn = await self.connect()
            await conn.execute(tbl.insert(), (2, "second"))

            res = await conn.execute(tbl.select())
            rows = await res.fetchall()
            self.assertEqual(2, len(rows))
            self.assertEqual((1, 'first'), rows[0])
            self.assertEqual((2, 'second'), rows[1])

        self.loop.run_until_complete(go())

    def test_execute_sa_insert_named_params(self):
        async def go():
            conn = await self.connect()
            await conn.execute(tbl.insert(), id=2, name="second")

            res = await conn.execute(tbl.select())
            rows = await res.fetchall()
            self.assertEqual(2, len(rows))
            self.assertEqual((1, 'first'), rows[0])
            self.assertEqual((2, 'second'), rows[1])

        self.loop.run_until_complete(go())

    def test_execute_sa_insert_positional_params(self):
        async def go():
            conn = await self.connect()
            await conn.execute(tbl.insert(), 2, "second")

            res = await conn.execute(tbl.select())
            rows = await res.fetchall()
            self.assertEqual(2, len(rows))
            self.assertEqual((1, 'first'), rows[0])
            self.assertEqual((2, 'second'), rows[1])

        self.loop.run_until_complete(go())

    def test_scalar(self):
        async def go():
            conn = await self.connect()
            res = await conn.scalar(tbl.count())
            self.assertEqual(1, res)

        self.loop.run_until_complete(go())

    def test_scalar_None(self):
        async def go():
            conn = await self.connect()
            await conn.execute(tbl.delete())
            res = await conn.scalar(tbl.select())
            self.assertIsNone(res)
            # TODO: fix this
            await conn._connection.commit()

        self.loop.run_until_complete(go())

    def test_row_proxy(self):
        async def go():
            conn = await self.connect()
            res = await conn.execute(tbl.select())
            rows = await res.fetchall()
            row = rows[0]
            row2 = await (await conn.execute(tbl.select())).first()
            self.assertEqual(2, len(row))
            self.assertEqual(['id', 'name'], list(row))
            self.assertIn('id', row)
            self.assertNotIn('unknown', row)
            self.assertEqual('first', row.name)
            self.assertEqual('first', row[tbl.c.name])
            with self.assertRaises(AttributeError):
                row.unknown
            self.assertEqual("(1, 'first')", repr(row))
            self.assertEqual((1, 'first'), row.as_tuple())
            self.assertNotEqual((555, 'other'), row.as_tuple())
            self.assertEqual(row2, row)
            self.assertFalse(row2 != row)
            self.assertNotEqual(5, row)
            # TODO: fix this
            await conn._connection.commit()

        self.loop.run_until_complete(go())

    def test_insert(self):
        async def go():
            conn = await self.connect()
            res = await conn.execute(tbl.insert().values(name='second'))
            self.assertEqual(1, res.rowcount)
            self.assertEqual(2, res.lastrowid)

        self.loop.run_until_complete(go())

    def test_raw_insert(self):
        async def go():
            conn = await self.connect()
            await conn.execute(
                "INSERT INTO sa_tbl (name) VALUES ('third')")
            res = await conn.execute(tbl.select())
            self.assertEqual(2, res.rowcount)
            self.assertEqual(('id', 'name'), res.keys())
            self.assertTrue(res.returns_rows)

            rows = await res.fetchall()
            self.assertEqual(2, len(rows))
            self.assertEqual(2, rows[1].id)
        self.loop.run_until_complete(go())

    def test_raw_insert_with_params(self):
        async def go():
            conn = await self.connect()
            res = await conn.execute(
                "INSERT INTO sa_tbl (id, name) VALUES (%s, %s)",
                2, 'third')
            res = await conn.execute(tbl.select())
            self.assertEqual(2, res.rowcount)
            self.assertEqual(('id', 'name'), res.keys())
            self.assertTrue(res.returns_rows)

            rows = await res.fetchall()
            self.assertEqual(2, len(rows))
            self.assertEqual(2, rows[1].id)
        self.loop.run_until_complete(go())

    def test_raw_insert_with_params_dict(self):
        async def go():
            conn = await self.connect()
            res = await conn.execute(
                "INSERT INTO sa_tbl (id, name) VALUES (%(id)s, %(name)s)",
                {'id': 2, 'name': 'third'})
            res = await conn.execute(tbl.select())
            self.assertEqual(2, res.rowcount)
            self.assertEqual(('id', 'name'), res.keys())
            self.assertTrue(res.returns_rows)

            rows = await res.fetchall()
            self.assertEqual(2, len(rows))
            self.assertEqual(2, rows[1].id)
        self.loop.run_until_complete(go())

    def test_raw_insert_with_named_params(self):
        async def go():
            conn = await self.connect()
            res = await conn.execute(
                "INSERT INTO sa_tbl (id, name) VALUES (%(id)s, %(name)s)",
                id=2, name='third')
            res = await conn.execute(tbl.select())
            self.assertEqual(2, res.rowcount)
            self.assertEqual(('id', 'name'), res.keys())
            self.assertTrue(res.returns_rows)

            rows = await res.fetchall()
            self.assertEqual(2, len(rows))
            self.assertEqual(2, rows[1].id)
        self.loop.run_until_complete(go())

    def test_raw_insert_with_executemany(self):
        async def go():
            conn = await self.connect()
            with self.assertRaises(sa.ArgumentError):
                await conn.execute(
                    "INSERT INTO sa_tbl (id, name) VALUES (%(id)s, %(name)s)",
                    [(2, 'third'), (3, 'forth')])
        self.loop.run_until_complete(go())

    def test_raw_select_with_wildcard(self):
        async def go():
            conn = await self.connect()
            await conn.execute(
                'SELECT * FROM sa_tbl WHERE name LIKE "%test%"')
        self.loop.run_until_complete(go())

    def test_delete(self):
        async def go():
            conn = await self.connect()

            res = await conn.execute(tbl.delete().where(tbl.c.id == 1))

            self.assertEqual((), res.keys())
            self.assertEqual(1, res.rowcount)
            self.assertFalse(res.returns_rows)
            self.assertTrue(res.closed)
            self.assertIsNone(res.cursor)

        self.loop.run_until_complete(go())

    def test_double_close(self):
        async def go():
            conn = await self.connect()
            res = await conn.execute("SELECT 1")
            await res.close()
            self.assertTrue(res.closed)
            self.assertIsNone(res.cursor)
            await res.close()
            self.assertTrue(res.closed)
            self.assertIsNone(res.cursor)

        self.loop.run_until_complete(go())

    @unittest.skip("Find out how to close cursor on __del__ method")
    def test_weakrefs(self):
        async def go():
            conn = await self.connect()
            self.assertEqual(0, len(conn._weak_results))
            res = await conn.execute("SELECT 1")
            self.assertEqual(1, len(conn._weak_results))
            cur = res.cursor
            self.assertFalse(cur.closed)
            # TODO: fix this, how close cursor if result was deleted
            # yield from cur.close()
            del res
            self.assertTrue(cur.closed)
            self.assertEqual(0, len(conn._weak_results))

        self.loop.run_until_complete(go())

    def test_fetchall(self):
        async def go():
            conn = await self.connect()
            await conn.execute(tbl.insert().values(name='second'))

            res = await conn.execute(tbl.select())
            rows = await res.fetchall()
            self.assertEqual(2, len(rows))
            self.assertTrue(res.closed)
            self.assertTrue(res.returns_rows)
            self.assertEqual([(1, 'first'), (2, 'second')], rows)

        self.loop.run_until_complete(go())

    def test_fetchall_closed(self):
        async def go():
            conn = await self.connect()
            await conn.execute(tbl.insert().values(name='second'))

            res = await conn.execute(tbl.select())
            await res.close()
            with self.assertRaises(sa.ResourceClosedError):
                await res.fetchall()

        self.loop.run_until_complete(go())

    def test_fetchall_not_returns_rows(self):
        async def go():
            conn = await self.connect()
            res = await conn.execute(tbl.delete())
            with self.assertRaises(sa.ResourceClosedError):
                await res.fetchall()

        self.loop.run_until_complete(go())

    def test_fetchone_closed(self):
        async def go():
            conn = await self.connect()
            await conn.execute(tbl.insert().values(name='second'))

            res = await conn.execute(tbl.select())
            await res.close()
            with self.assertRaises(sa.ResourceClosedError):
                await res.fetchone()

        self.loop.run_until_complete(go())

    def test_first_not_returns_rows(self):
        async def go():
            conn = await self.connect()
            res = await conn.execute(tbl.delete())
            with self.assertRaises(sa.ResourceClosedError):
                await res.first()

        self.loop.run_until_complete(go())

    def test_fetchmany(self):
        async def go():
            conn = await self.connect()
            await conn.execute(tbl.insert().values(name='second'))

            res = await conn.execute(tbl.select())
            rows = await res.fetchmany()
            self.assertEqual(1, len(rows))
            self.assertFalse(res.closed)
            self.assertTrue(res.returns_rows)
            self.assertEqual([(1, 'first')], rows)

        self.loop.run_until_complete(go())

    def test_fetchmany_with_size(self):
        async def go():
            conn = await self.connect()
            await conn.execute(tbl.insert().values(name='second'))

            res = await conn.execute(tbl.select())
            rows = await res.fetchmany(100)
            self.assertEqual(2, len(rows))
            self.assertFalse(res.closed)
            self.assertTrue(res.returns_rows)
            self.assertEqual([(1, 'first'), (2, 'second')], rows)

        self.loop.run_until_complete(go())

    def test_fetchmany_closed(self):
        async def go():
            conn = await self.connect()
            await conn.execute(tbl.insert().values(name='second'))

            res = await conn.execute(tbl.select())
            await res.close()
            with self.assertRaises(sa.ResourceClosedError):
                await res.fetchmany()

        self.loop.run_until_complete(go())

    def test_fetchmany_with_size_closed(self):
        async def go():
            conn = await self.connect()
            await conn.execute(tbl.insert().values(name='second'))

            res = await conn.execute(tbl.select())
            await res.close()
            with self.assertRaises(sa.ResourceClosedError):
                await res.fetchmany(5555)

        self.loop.run_until_complete(go())

    def test_fetchmany_not_returns_rows(self):
        async def go():
            conn = await self.connect()
            res = await conn.execute(tbl.delete())
            with self.assertRaises(sa.ResourceClosedError):
                await res.fetchmany()

        self.loop.run_until_complete(go())

    def test_fetchmany_close_after_last_read(self):
        async def go():
            conn = await self.connect()

            res = await conn.execute(tbl.select())
            rows = await res.fetchmany()
            self.assertEqual(1, len(rows))
            self.assertFalse(res.closed)
            self.assertTrue(res.returns_rows)
            self.assertEqual([(1, 'first')], rows)
            rows2 = await res.fetchmany()
            self.assertEqual(0, len(rows2))
            self.assertTrue(res.closed)

        self.loop.run_until_complete(go())

    def test_create_table(self, **kwargs):
        async def go():
            conn = await self.connect()
            res = await conn.execute(DropTable(tbl))
            with self.assertRaises(sa.ResourceClosedError):
                await res.fetchmany()

            with self.assertRaises(aiomysql.ProgrammingError):
                await conn.execute("SELECT * FROM sa_tbl")

            res = await conn.execute(CreateTable(tbl))
            with self.assertRaises(sa.ResourceClosedError):
                await res.fetchmany()

            res = await conn.execute("SELECT * FROM sa_tbl")
            self.assertEqual(0, len(await res.fetchall()))

        self.loop.run_until_complete(go())
