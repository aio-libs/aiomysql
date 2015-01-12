import asyncio
from tests import base
from tests._testutils import run_until_complete

from aiomysql import ProgrammingError


class TestCursor(base.AIOPyMySQLTestCase):
    # this test case does not work quite right yet, however,
    # we substitute in None for the erroneous field which is
    # compatible with the DB-API 2.0 spec and has not broken
    # any unit tests for anything we've tried.

    @asyncio.coroutine
    def _prepare(self, conn):
        cur = conn.cursor()
        yield from cur.execute("DROP TABLE IF EXISTS tbl;")

        yield from cur.execute("""CREATE TABLE tbl (
                 id MEDIUMINT NOT NULL AUTO_INCREMENT,
                 name VARCHAR(255) NOT NULL,
                 PRIMARY KEY (id));""")

        for i in [(1, 'a'), (2, 'b'), (3, 'c')]:
            yield from cur.execute("INSERT INTO tbl VALUES(%s, %s)", i)
        yield from cur.execute("DROP TABLE IF EXISTS tbl2")
        yield from cur.execute("""CREATE TABLE tbl2
                                  (id int, name varchar(255))""")
        yield from conn.commit()

    @asyncio.coroutine
    def _prepare_procedure(self, conn):
        cur = conn.cursor()
        yield from cur.execute("DROP PROCEDURE IF EXISTS myinc;")
        yield from cur.execute("""CREATE PROCEDURE myinc(p1 INT)
                               BEGIN
                                   SELECT p1 + 1;
                               END
                               """)
        yield from conn.commit()

    @run_until_complete
    def test_description(self):
        conn = self.connections[0]
        yield from self._prepare(conn)
        cur = conn.cursor()
        self.assertEqual(None, cur.description)
        yield from cur.execute('SELECT * from tbl;')

        self.assertEqual(len(cur.description), 2,
                         'cursor.description describes too many columns')

        self.assertEqual(len(cur.description[0]), 7,
                         'cursor.description[x] tuples must have '
                         '7 elements')

        self.assertEqual(cur.description[0][0].lower(), 'id',
                         'cursor.description[x][0] must return column '
                         'name')

        self.assertEqual(cur.description[1][0].lower(), 'name',
                         'cursor.description[x][0] must return column '
                         'name')

        # Make sure self.description gets reset, cursor should be
        # set to None in case of none resulting queries like DDL
        yield from cur.execute('DROP TABLE IF EXISTS foobar;')
        self.assertEqual(None, cur.description)

    def test_connection_property(self):
        conn = self.connections[0]
        cur = conn.cursor()
        self.assertIs(cur.connection, conn)

    @run_until_complete
    def test_scroll_relative(self):
        conn = self.connections[0]
        yield from self._prepare(conn)
        cur = conn.cursor()
        yield from cur.execute('SELECT * FROM tbl;')
        cur.scroll(1)
        ret = cur.fetchone()
        self.assertEqual((2, 'b'), ret)

    @run_until_complete
    def test_scroll_absolute(self):
        conn = self.connections[0]
        yield from self._prepare(conn)
        cur = conn.cursor()
        yield from cur.execute('SELECT * FROM tbl;')
        cur.scroll(2, mode='absolute')
        ret = cur.fetchone()
        self.assertEqual((3, 'c'), ret)

    @run_until_complete
    def test_scroll_errors(self):
        conn = self.connections[0]
        cur = conn.cursor()

        with self.assertRaises(ProgrammingError):
            cur.scroll(2, mode='absolute')

        cur = conn.cursor()
        yield from cur.execute('SELECT * FROM tbl;')

        with self.assertRaises(ProgrammingError):
            cur.scroll(2, mode='not_valid_mode')

    @run_until_complete
    def test_scroll_index_error(self):
        conn = self.connections[0]
        yield from self._prepare(conn)
        cur = conn.cursor()
        yield from cur.execute('SELECT * FROM tbl;')
        with self.assertRaises(IndexError):
            cur.scroll(1000)

    @run_until_complete
    def test_close(self):
        conn = self.connections[0]
        cur = conn.cursor()
        yield from cur.close()
        self.assertTrue(cur.closed)
        with self.assertRaises(ProgrammingError):
            yield from cur.execute('SELECT 1')
        # try to close for second time
        yield from cur.close()

    def test_arraysize(self):
        conn = self.connections[0]
        cur = conn.cursor()
        self.assertEqual(1, cur.arraysize)
        cur.arraysize = 10
        self.assertEqual(10, cur.arraysize)

    @run_until_complete
    def test_rows(self):
        conn = self.connections[0]
        yield from self._prepare(conn)

        cur = conn.cursor()
        yield from cur.execute('SELECT * from tbl')
        self.assertEqual(3, cur.rowcount)
        self.assertEqual(0, cur.rownumber)
        cur.fetchone()
        self.assertEqual(1, cur.rownumber)
        self.assertEqual(None, cur.lastrowid)
        yield from cur.execute('INSERT INTO tbl VALUES (%s, %s)', (4, 'd'))
        self.assertNotEqual(0, cur.lastrowid)
        yield from conn.commit()

    @run_until_complete
    def test_callproc(self):
        conn = yield from self.connect()
        yield from self._prepare_procedure(conn)
        cur = conn.cursor()
        yield from cur.callproc('myinc', [1])
        ret = cur.fetchone()
        self.assertEqual((2,), ret)
        yield from cur.close()
        with self.assertRaises(ProgrammingError):
            yield from cur.callproc('myinc', [1])
        conn.close()

    @run_until_complete
    def test_fetch_no_result(self):
        # test a fetchone() with no rows
        conn = self.connections[0]
        c = conn.cursor()
        yield from c.execute("create table test_nr (b varchar(32))")
        try:
            data = "pymysql"
            yield from c.execute("insert into test_nr (b) values (%s)",
                                 (data,))
            self.assertEqual(None, c.fetchone())
        finally:
            yield from c.execute("drop table test_nr")

    @run_until_complete
    def test_aggregates(self):
        """ test aggregate functions """
        conn = self.connections[0]
        c = conn.cursor()
        try:
            yield from c.execute('create table test_aggregates (i integer)')
            for i in range(0, 10):
                yield from c.execute(
                    'insert into test_aggregates (i) values (%s)', (i,))
            yield from c.execute('select sum(i) from test_aggregates')
            r, = c.fetchone()
            self.assertEqual(sum(range(0, 10)), r)
        finally:
            yield from c.execute('drop table test_aggregates')

    @run_until_complete
    def test_single_tuple(self):
        """ test a single tuple """
        conn = self.connections[0]
        c = conn.cursor()
        try:
            yield from c.execute(
                "create table mystuff (id integer primary key)")
            yield from c.execute("insert into mystuff (id) values (1)")
            yield from c.execute("insert into mystuff (id) values (2)")
            yield from c.execute("select id from mystuff where id in %s",
                                 ((1,),))
            self.assertEqual([(1,)], list(c.fetchall()))
        finally:
            yield from c.execute("drop table mystuff")
