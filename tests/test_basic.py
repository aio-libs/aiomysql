import asyncio
import time
import datetime

from tests import base
from pymysql import util
from pymysql.err import ProgrammingError

import aiomysql.cursors
from tests._testutils import run_until_complete


class TestConversion(base.AIOPyMySQLTestCase):
    @run_until_complete
    def test_datatypes(self):
        """ test every data type """
        conn = self.connections[0]
        c = conn.cursor()
        yield from c.execute(
            "create table test_datatypes (b bit, i int, l bigint, f real, s "
            "varchar(32), u varchar(32), bb blob, d date, dt datetime, "
            "ts timestamp, td time, t time, st datetime)")
        try:
            # insert values
            v = (
                True, -3, 123456789012, 5.7, "hello'\" world",
                u"Espa\xc3\xb1ol",
                "binary\x00data".encode(conn.charset),
                datetime.date(1988, 2, 2),
                datetime.datetime.now().replace(microsecond=0),
                datetime.timedelta(5, 6), datetime.time(16, 32),
                time.localtime())
            yield from c.execute(
                "insert into test_datatypes (b,i,l,f,s,u,bb,d,dt,td,t,st) "
                "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                v)
            yield from c.execute(
                "select b,i,l,f,s,u,bb,d,dt,td,t,st from test_datatypes")
            r = c.fetchone()
            self.assertEqual(util.int2byte(1), r[0])
            # self.assertEqual(v[1:8], r[1:8])
            self.assertEqual(v[1:9], r[1:9])
            # mysql throws away microseconds so we need to check datetimes
            # specially. additionally times are turned into timedeltas.
            # self.assertEqual(datetime.datetime(*v[8].timetuple()[:6]), r[8])
            self.assertEqual(v[9], r[9])  # just timedeltas
            self.assertEqual(
                datetime.timedelta(0, 60 * (v[10].hour * 60 + v[10].minute)),
                r[10])
            self.assertEqual(datetime.datetime(*v[-1][:6]), r[-1])

            yield from c.execute("delete from test_datatypes")

            # check nulls
            yield from c.execute(
                "insert into test_datatypes (b,i,l,f,s,u,bb,d,dt,td,t,st) "
                "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                [None] * 12)
            yield from c.execute(
                "select b,i,l,f,s,u,bb,d,dt,td,t,st from test_datatypes")
            r = c.fetchone()
            self.assertEqual(tuple([None] * 12), r)

            yield from c.execute("delete from test_datatypes")

            # check sequence type
            yield from c.execute(
                "insert into test_datatypes (i, l) values (2,4), (6,8), "
                "(10,12)")
            yield from c.execute(
                "select l from test_datatypes where i in %s order by i",
                ((2, 6),))
            r = c.fetchall()
            self.assertEqual(((4,), (8,)), r)
        finally:
            yield from c.execute("drop table test_datatypes")

    @run_until_complete
    def test_dict(self):
        """ test dict escaping """
        conn = self.connections[0]
        c = conn.cursor()
        yield from c.execute(
            "create table test_dict (a integer, b integer, c integer)")
        try:
            yield from c.execute(
                "insert into test_dict (a,b,c) values (%(a)s, %(b)s, %(c)s)",
                {"a": 1, "b": 2, "c": 3})
            yield from c.execute("select a,b,c from test_dict")
            self.assertEqual((1, 2, 3), c.fetchone())
        finally:
            yield from c.execute("drop table test_dict")

    @run_until_complete
    def test_string(self):
        conn = self.connections[0]
        c = conn.cursor()
        yield from c.execute("create table test_dict (a text)")
        test_value = "I am a test string"
        try:
            yield from c.execute("insert into test_dict (a) values (%s)",
                                 test_value)
            yield from c.execute("select a from test_dict")
            self.assertEqual((test_value,), c.fetchone())
        finally:
            yield from c.execute("drop table test_dict")

    @run_until_complete
    def test_integer(self):
        conn = self.connections[0]
        c = conn.cursor()
        yield from c.execute("create table test_dict (a integer)")
        test_value = 12345
        try:
            yield from c.execute("insert into test_dict (a) values (%s)",
                                 test_value)
            yield from c.execute("select a from test_dict")
            self.assertEqual((test_value,), c.fetchone())
        finally:
            yield from c.execute("drop table test_dict")

    @run_until_complete
    def test_big_blob(self):
        """ test tons of data """
        conn = self.connections[0]
        c = conn.cursor()
        yield from c.execute("create table test_big_blob (b blob)")
        try:
            data = "pymysql" * 1024
            yield from c.execute("insert into test_big_blob (b) values (%s)",
                                 (data,))
            yield from c.execute("select b from test_big_blob")
            self.assertEqual(data.encode(conn.charset), c.fetchone()[0])
        finally:
            yield from c.execute("drop table test_big_blob")

    @run_until_complete
    def test_untyped(self):
        """ test conversion of null, empty string """
        conn = self.connections[0]
        c = conn.cursor()
        yield from c.execute("select null,''")
        self.assertEqual((None, u''), c.fetchone())
        yield from c.execute("select '',null")
        self.assertEqual((u'', None), c.fetchone())

    @run_until_complete
    def test_timedelta(self):
        """ test timedelta conversion """
        conn = self.connections[0]
        c = conn.cursor()
        yield from c.execute(
            "select time('12:30'), time('23:12:59'), time('23:12:59.05100'), "
            "time('-12:30'), time('-23:12:59'), time('-23:12:59.05100'), "
            "time('-00:30')")
        self.assertEqual((datetime.timedelta(0, 45000),
                          datetime.timedelta(0, 83579),
                          datetime.timedelta(0, 83579, 51000),
                          -datetime.timedelta(0, 45000),
                          -datetime.timedelta(0, 83579),
                          -datetime.timedelta(0, 83579, 51000),
                          -datetime.timedelta(0, 1800)),
                         c.fetchone())

    @run_until_complete
    def test_datetime(self):
        """ test datetime conversion """
        conn = self.connections[0]
        c = conn.cursor()
        dt = datetime.datetime(2013, 11, 12, 9, 9, 9, 123450)
        try:
            yield from c.execute(
                "create table test_datetime (id int, ts datetime(6))")
            yield from c.execute(
                "insert into test_datetime values "
                "(1,'2013-11-12 09:09:09.12345')")
            yield from c.execute("select ts from test_datetime")
            self.assertEqual((dt,), c.fetchone())
        except ProgrammingError:
            # User is running a version of MySQL that doesn't support
            # msecs within datetime
            pass
        finally:
            yield from c.execute("drop table if exists test_datetime")

    @run_until_complete
    def test_get_transaction_status(self):
        conn = self.connections[0]
        #  make sure that connection is clean without transactions
        transaction_flag = conn.get_transaction_status()
        self.assertFalse(transaction_flag)
        # start transaction
        yield from conn.begin()
        # make sure transaction flag is up
        transaction_flag = conn.get_transaction_status()
        self.assertTrue(transaction_flag)
        cursor = conn.cursor()
        yield from cursor.execute('SELECT 1;')
        (r, ) = cursor.fetchone()
        self.assertEqual(r, 1)
        yield from conn.commit()
        # make sure that transaction flag is down
        transaction_flag = conn.get_transaction_status()
        self.assertFalse(transaction_flag)


class TestCursor(base.AIOPyMySQLTestCase):
    # this test case does not work quite right yet, however,
    # we substitute in None for the erroneous field which is
    # compatible with the DB-API 2.0 spec and has not broken
    # any unit tests for anything we've tried.

    # def test_description(self):
    #    """ test description attribute """
    #    # result is from MySQLdb module
    #    r = (('Host', 254, 11, 60, 60, 0, 0),
    #         ('User', 254, 16, 16, 16, 0, 0),
    #         ('Password', 254, 41, 41, 41, 0, 0),
    #         ('Select_priv', 254, 1, 1, 1, 0, 0),
    #         ('Insert_priv', 254, 1, 1, 1, 0, 0),
    #         ('Update_priv', 254, 1, 1, 1, 0, 0),
    #         ('Delete_priv', 254, 1, 1, 1, 0, 0),
    #         ('Create_priv', 254, 1, 1, 1, 0, 0),
    #         ('Drop_priv', 254, 1, 1, 1, 0, 0),
    #         ('Reload_priv', 254, 1, 1, 1, 0, 0),
    #         ('Shutdown_priv', 254, 1, 1, 1, 0, 0),
    #         ('Process_priv', 254, 1, 1, 1, 0, 0),
    #         ('File_priv', 254, 1, 1, 1, 0, 0),
    #         ('Grant_priv', 254, 1, 1, 1, 0, 0),
    #         ('References_priv', 254, 1, 1, 1, 0, 0),
    #         ('Index_priv', 254, 1, 1, 1, 0, 0),
    #         ('Alter_priv', 254, 1, 1, 1, 0, 0),
    #         ('Show_db_priv', 254, 1, 1, 1, 0, 0),
    #         ('Super_priv', 254, 1, 1, 1, 0, 0),
    #         ('Create_tmp_table_priv', 254, 1, 1, 1, 0, 0),
    #         ('Lock_tables_priv', 254, 1, 1, 1, 0, 0),
    #         ('Execute_priv', 254, 1, 1, 1, 0, 0),
    #         ('Repl_slave_priv', 254, 1, 1, 1, 0, 0),
    #         ('Repl_client_priv', 254, 1, 1, 1, 0, 0),
    #         ('Create_view_priv', 254, 1, 1, 1, 0, 0),
    #         ('Show_view_priv', 254, 1, 1, 1, 0, 0),
    #         ('Create_routine_priv', 254, 1, 1, 1, 0, 0),
    #         ('Alter_routine_priv', 254, 1, 1, 1, 0, 0),
    #         ('Create_user_priv', 254, 1, 1, 1, 0, 0),
    #         ('Event_priv', 254, 1, 1, 1, 0, 0),
    #         ('Trigger_priv', 254, 1, 1, 1, 0, 0),
    #         ('ssl_type', 254, 0, 9, 9, 0, 0),
    #         ('ssl_cipher', 252, 0, 65535, 65535, 0, 0),
    #         ('x509_issuer', 252, 0, 65535, 65535, 0, 0),
    #         ('x509_subject', 252, 0, 65535, 65535, 0, 0),
    #         ('max_questions', 3, 1, 11, 11, 0, 0),
    #         ('max_updates', 3, 1, 11, 11, 0, 0),
    #         ('max_connections', 3, 1, 11, 11, 0, 0),
    #         ('max_user_connections', 3, 1, 11, 11, 0, 0))
    #    conn = self.connections[0]
    #    c = conn.cursor()
    #    c.execute("select * from mysql.user")
    #
    #    self.assertEqual(r, c.description)

    @run_until_complete
    def test_fetch_no_result(self):
        """ test a fetchone() with no rows """
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


class TestBulkInserts(base.AIOPyMySQLTestCase):
    cursor_type = aiomysql.cursors.DictCursor

    def setUp(self):
        super(TestBulkInserts, self).setUp()
        self.conn = conn = self.connections[0]
        c = conn.cursor(self.cursor_type)

        @asyncio.coroutine
        def prepare():
            # create a table ane some data to query
            yield from c.execute("drop table if exists bulkinsert;")
            yield from c.execute(
                """CREATE TABLE bulkinsert
                (
                id int(11),
                name char(20),
                age int,
                height int,
                PRIMARY KEY (id)
                )
                """)

        self.loop.run_until_complete(prepare())

    @asyncio.coroutine
    def _verify_records(self, data):
        conn = self.connections[0]
        cursor = conn.cursor()
        yield from cursor.execute(
            "SELECT id, name, age, height from bulkinsert")
        result = cursor.fetchall()
        yield from cursor.execute('commit')
        self.assertEqual(sorted(data), sorted(result))

    @run_until_complete
    def test_bulk_insert(self):
        conn = self.connections[0]
        cursor = conn.cursor()

        data = [(0, "bob", 21, 123), (1, "jim", 56, 45), (2, "fred", 100, 180)]
        yield from cursor.executemany(
            "insert into bulkinsert (id, name, age, height) "
            "values (%s,%s,%s,%s)", data)
        self.assertEqual(
            cursor._last_executed, bytearray(
                b"insert into bulkinsert (id, name, age, height) values "
                b"(0,'bob',21,123),(1,'jim',56,45),(2,'fred',100,180)"))
        yield from cursor.execute('commit')
        yield from self._verify_records(data)

    @run_until_complete
    def test_bulk_insert_multiline_statement(self):
        conn = self.connections[0]
        cursor = conn.cursor()
        data = [(0, "bob", 21, 123), (1, "jim", 56, 45), (2, "fred", 100, 180)]
        yield from cursor.executemany("""insert
            into bulkinsert (id, name,
            age, height)
            values (%s,
            %s , %s,
            %s )
             """, data)
        self.assertEqual(cursor._last_executed, bytearray(b"""insert
            into bulkinsert (id, name,
            age, height)
            values (0,
            'bob' , 21,
            123 ),(1,
            'jim' , 56,
            45 ),(2,
            'fred' , 100,
            180 )"""))
        yield from cursor.execute('commit')
        yield from self._verify_records(data)

    @run_until_complete
    def test_bulk_insert_single_record(self):
        conn = self.connections[0]
        cursor = conn.cursor()
        data = [(0, "bob", 21, 123)]
        yield from cursor.executemany(
            "insert into bulkinsert (id, name, age, height) "
            "values (%s,%s,%s,%s)", data)
        yield from cursor.execute('commit')
        yield from self._verify_records(data)
