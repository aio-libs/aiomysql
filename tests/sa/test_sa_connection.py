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
        self.port = os.environ.get('MYSQL_PORT', 3306)
        self.user = os.environ.get('MYSQL_USER', 'root')
        self.db = os.environ.get('MYSQL_DB', 'test_pymysql')
        self.password = os.environ.get('MYSQL_PASSWORD', '')

    def tearDown(self):
        self.loop.close()

    @asyncio.coroutine
    def connect(self, **kwargs):
        conn = yield from connect(db=self.db,
                                  user=self.user,
                                  password=self.password,
                                  host=self.host,
                                  loop=self.loop,
                                  **kwargs)
        yield from conn.autocommit(True)
        cur = yield from conn.cursor()
        yield from cur.execute("DROP TABLE IF EXISTS sa_tbl")
        yield from cur.execute("CREATE TABLE sa_tbl "
                               "(id serial, name varchar(255))")
        yield from cur.execute("INSERT INTO sa_tbl (name)"
                               "VALUES ('first')")

        yield from cur._connection.commit()
        # yield from cur.close()
        engine = mock.Mock()
        engine.dialect = sa.engine._dialect
        return sa.SAConnection(conn, engine)

    def test_execute_text_select(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            res = yield from conn.execute("SELECT * FROM sa_tbl;")
            self.assertIsInstance(res.cursor, Cursor)
            self.assertEqual(('id', 'name'), res.keys())
            rows = [r for r in res]
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
            yield from conn._connection.commit()
        self.loop.run_until_complete(go())

    def test_execute_sa_select(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            res = yield from conn.execute(tbl.select())
            self.assertIsInstance(res.cursor, Cursor)
            self.assertEqual(('id', 'name'), res.keys())
            rows = [r for r in res]
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
            yield from conn._connection.commit()

        self.loop.run_until_complete(go())

    def test_execute_sa_insert_with_dict(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            yield from conn.execute(tbl.insert(), {"id": 2, "name": "second"})

            res = yield from conn.execute(tbl.select())
            rows = list(res)
            self.assertEqual(2, len(rows))
            self.assertEqual((1, 'first'), rows[0])
            self.assertEqual((2, 'second'), rows[1])

        self.loop.run_until_complete(go())

    def test_execute_sa_insert_with_tuple(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            yield from conn.execute(tbl.insert(), (2, "second"))

            res = yield from conn.execute(tbl.select())
            rows = list(res)
            self.assertEqual(2, len(rows))
            self.assertEqual((1, 'first'), rows[0])
            self.assertEqual((2, 'second'), rows[1])

        self.loop.run_until_complete(go())

    def test_execute_sa_insert_named_params(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            yield from conn.execute(tbl.insert(), id=2, name="second")

            res = yield from conn.execute(tbl.select())
            rows = list(res)
            self.assertEqual(2, len(rows))
            self.assertEqual((1, 'first'), rows[0])
            self.assertEqual((2, 'second'), rows[1])

        self.loop.run_until_complete(go())

    def test_execute_sa_insert_positional_params(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            yield from conn.execute(tbl.insert(), 2, "second")

            res = yield from conn.execute(tbl.select())
            rows = list(res)
            self.assertEqual(2, len(rows))
            self.assertEqual((1, 'first'), rows[0])
            self.assertEqual((2, 'second'), rows[1])

        self.loop.run_until_complete(go())

    def test_scalar(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            res = yield from conn.scalar(tbl.count())
            self.assertEqual(1, res)

        self.loop.run_until_complete(go())

    def test_scalar_None(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            yield from conn.execute(tbl.delete())
            res = yield from conn.scalar(tbl.select())
            self.assertIsNone(res)
            # TODO: fix this
            yield from conn._connection.commit()

        self.loop.run_until_complete(go())

    def test_row_proxy(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            res = yield from conn.execute(tbl.select())
            rows = [r for r in res]
            row = rows[0]
            row2 = yield from (yield from conn.execute(tbl.select())).first()
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
            yield from conn._connection.commit()

        self.loop.run_until_complete(go())

    def test_insert(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            res = yield from conn.execute(tbl.insert().values(name='second'))
            self.assertEqual(1, res.rowcount)
            self.assertEqual(2, res.lastrowid)

        self.loop.run_until_complete(go())

    def test_raw_insert(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            yield from conn.execute(
                "INSERT INTO sa_tbl (name) VALUES ('third')")
            res = yield from conn.execute(tbl.select())
            self.assertEqual(2, res.rowcount)
            self.assertEqual(('id', 'name'), res.keys())
            self.assertTrue(res.returns_rows)

            rows = [r for r in res]
            self.assertEqual(2, len(rows))
            self.assertEqual(2, rows[1].id)
        self.loop.run_until_complete(go())

    def test_raw_insert_with_params(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            res = yield from conn.execute(
                "INSERT INTO sa_tbl (id, name) VALUES (%s, %s)",
                2, 'third')
            res = yield from conn.execute(tbl.select())
            self.assertEqual(2, res.rowcount)
            self.assertEqual(('id', 'name'), res.keys())
            self.assertTrue(res.returns_rows)

            rows = [r for r in res]
            self.assertEqual(2, len(rows))
            self.assertEqual(2, rows[1].id)
        self.loop.run_until_complete(go())

    def test_raw_insert_with_params_dict(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            res = yield from conn.execute(
                "INSERT INTO sa_tbl (id, name) VALUES (%(id)s, %(name)s)",
                {'id': 2, 'name': 'third'})
            res = yield from conn.execute(tbl.select())
            self.assertEqual(2, res.rowcount)
            self.assertEqual(('id', 'name'), res.keys())
            self.assertTrue(res.returns_rows)

            rows = [r for r in res]
            self.assertEqual(2, len(rows))
            self.assertEqual(2, rows[1].id)
        self.loop.run_until_complete(go())

    def test_raw_insert_with_named_params(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            res = yield from conn.execute(
                "INSERT INTO sa_tbl (id, name) VALUES (%(id)s, %(name)s)",
                id=2, name='third')
            res = yield from conn.execute(tbl.select())
            self.assertEqual(2, res.rowcount)
            self.assertEqual(('id', 'name'), res.keys())
            self.assertTrue(res.returns_rows)

            rows = [r for r in res]
            self.assertEqual(2, len(rows))
            self.assertEqual(2, rows[1].id)
        self.loop.run_until_complete(go())

    def test_raw_insert_with_executemany(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            with self.assertRaises(sa.ArgumentError):
                yield from conn.execute(
                    "INSERT INTO sa_tbl (id, name) VALUES (%(id)s, %(name)s)",
                    [(2, 'third'), (3, 'forth')])
        self.loop.run_until_complete(go())

    def test_raw_select_with_wildcard(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            yield from conn.execute(
                'SELECT * FROM sa_tbl WHERE name LIKE "%test%"')
        self.loop.run_until_complete(go())

    def test_delete(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()

            res = yield from conn.execute(tbl.delete().where(tbl.c.id == 1))

            self.assertEqual((), res.keys())
            self.assertEqual(1, res.rowcount)
            self.assertFalse(res.returns_rows)
            self.assertTrue(res.closed)
            self.assertIsNone(res.cursor)

        self.loop.run_until_complete(go())

    def test_double_close(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            res = yield from conn.execute("SELECT 1")
            yield from res.close()
            self.assertTrue(res.closed)
            self.assertIsNone(res.cursor)
            yield from res.close()
            self.assertTrue(res.closed)
            self.assertIsNone(res.cursor)

        self.loop.run_until_complete(go())

    @unittest.skip("Find out how to close cursor on __del__ method")
    def test_weakrefs(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            self.assertEqual(0, len(conn._weak_results))
            res = yield from conn.execute("SELECT 1")
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
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            yield from conn.execute(tbl.insert().values(name='second'))

            res = yield from conn.execute(tbl.select())
            rows = yield from res.fetchall()
            self.assertEqual(2, len(rows))
            self.assertTrue(res.closed)
            self.assertTrue(res.returns_rows)
            self.assertEqual([(1, 'first'), (2, 'second')], rows)

        self.loop.run_until_complete(go())

    def test_fetchall_closed(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            yield from conn.execute(tbl.insert().values(name='second'))

            res = yield from conn.execute(tbl.select())
            yield from res.close()
            with self.assertRaises(sa.ResourceClosedError):
                yield from res.fetchall()

        self.loop.run_until_complete(go())

    def test_fetchall_not_returns_rows(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            res = yield from conn.execute(tbl.delete())
            with self.assertRaises(sa.ResourceClosedError):
                yield from res.fetchall()

        self.loop.run_until_complete(go())

    def test_fetchone_closed(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            yield from conn.execute(tbl.insert().values(name='second'))

            res = yield from conn.execute(tbl.select())
            yield from res.close()
            with self.assertRaises(sa.ResourceClosedError):
                yield from res.fetchone()

        self.loop.run_until_complete(go())

    def test_first_not_returns_rows(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            res = yield from conn.execute(tbl.delete())
            with self.assertRaises(sa.ResourceClosedError):
                yield from res.first()

        self.loop.run_until_complete(go())

    def test_fetchmany(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            yield from conn.execute(tbl.insert().values(name='second'))

            res = yield from conn.execute(tbl.select())
            rows = yield from res.fetchmany()
            self.assertEqual(1, len(rows))
            self.assertFalse(res.closed)
            self.assertTrue(res.returns_rows)
            self.assertEqual([(1, 'first')], rows)

        self.loop.run_until_complete(go())

    def test_fetchmany_with_size(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            yield from conn.execute(tbl.insert().values(name='second'))

            res = yield from conn.execute(tbl.select())
            rows = yield from res.fetchmany(100)
            self.assertEqual(2, len(rows))
            self.assertFalse(res.closed)
            self.assertTrue(res.returns_rows)
            self.assertEqual([(1, 'first'), (2, 'second')], rows)

        self.loop.run_until_complete(go())

    def test_fetchmany_closed(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            yield from conn.execute(tbl.insert().values(name='second'))

            res = yield from conn.execute(tbl.select())
            yield from res.close()
            with self.assertRaises(sa.ResourceClosedError):
                yield from res.fetchmany()

        self.loop.run_until_complete(go())

    def test_fetchmany_with_size_closed(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            yield from conn.execute(tbl.insert().values(name='second'))

            res = yield from conn.execute(tbl.select())
            yield from res.close()
            with self.assertRaises(sa.ResourceClosedError):
                yield from res.fetchmany(5555)

        self.loop.run_until_complete(go())

    def test_fetchmany_not_returns_rows(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            res = yield from conn.execute(tbl.delete())
            with self.assertRaises(sa.ResourceClosedError):
                yield from res.fetchmany()

        self.loop.run_until_complete(go())

    def test_fetchmany_close_after_last_read(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()

            res = yield from conn.execute(tbl.select())
            rows = yield from res.fetchmany()
            self.assertEqual(1, len(rows))
            self.assertFalse(res.closed)
            self.assertTrue(res.returns_rows)
            self.assertEqual([(1, 'first')], rows)
            rows2 = yield from res.fetchmany()
            self.assertEqual(0, len(rows2))
            self.assertTrue(res.closed)

        self.loop.run_until_complete(go())

    def test_create_table(self, **kwargs):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            res = yield from conn.execute(DropTable(tbl))
            with self.assertRaises(sa.ResourceClosedError):
                yield from res.fetchmany()

            with self.assertRaises(aiomysql.ProgrammingError):
                yield from conn.execute("SELECT * FROM sa_tbl")

            res = yield from conn.execute(CreateTable(tbl))
            with self.assertRaises(sa.ResourceClosedError):
                yield from res.fetchmany()

            res = yield from conn.execute("SELECT * FROM sa_tbl")
            self.assertEqual(0, len(list(res)))

        self.loop.run_until_complete(go())
