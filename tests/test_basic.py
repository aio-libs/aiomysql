import time
import datetime

from tests import base
from pymysql import util
from pymysql.err import ProgrammingError

from tests._testutils import run_until_complete


class TestConversion(base.AIOPyMySQLTestCase):

    @run_until_complete
    def test_datatypes(self):
        """ test every data type """
        conn = self.connections[0]
        c = yield from conn.cursor()
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
            r = yield from c.fetchone()
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
            r = yield from c.fetchone()
            self.assertEqual(tuple([None] * 12), r)

            yield from c.execute("delete from test_datatypes")

            # check sequence type
            yield from c.execute(
                "insert into test_datatypes (i, l) values (2,4), (6,8), "
                "(10,12)")
            yield from c.execute(
                "select l from test_datatypes where i in %s order by i",
                ((2, 6),))
            r = yield from c.fetchall()
            self.assertEqual(((4,), (8,)), r)
        finally:
            yield from c.execute("drop table test_datatypes")

    @run_until_complete
    def test_dict(self):
        """ test dict escaping """
        conn = self.connections[0]
        c = yield from conn.cursor()
        yield from c.execute(
            "create table test_dict (a integer, b integer, c integer)")
        try:
            yield from c.execute(
                "insert into test_dict (a,b,c) values (%(a)s, %(b)s, %(c)s)",
                {"a": 1, "b": 2, "c": 3})
            yield from c.execute("select a,b,c from test_dict")
            r = yield from c.fetchone()
            self.assertEqual((1, 2, 3), r)
        finally:
            yield from c.execute("drop table test_dict")

    @run_until_complete
    def test_string(self):
        conn = self.connections[0]
        c = yield from conn.cursor()
        yield from c.execute("DROP TABLE IF EXISTS test_dict;")
        yield from c.execute("create table test_dict (a text)")
        test_value = "I am a test string"
        try:
            yield from c.execute("insert into test_dict (a) values (%s)",
                                 test_value)
            yield from c.execute("select a from test_dict")
            r = yield from c.fetchone()
            self.assertEqual((test_value,), r)
        finally:
            yield from c.execute("drop table test_dict")

    @run_until_complete
    def test_integer(self):
        conn = self.connections[0]
        c = yield from conn.cursor()
        yield from c.execute("create table test_dict (a integer)")
        test_value = 12345
        try:
            yield from c.execute("insert into test_dict (a) values (%s)",
                                 test_value)
            yield from c.execute("select a from test_dict")
            r = yield from c.fetchone()
            self.assertEqual((test_value,), r)
        finally:
            yield from c.execute("drop table test_dict")

    @run_until_complete
    def test_big_blob(self):
        """ test tons of data """
        conn = self.connections[0]
        c = yield from conn.cursor()
        yield from c.execute("create table test_big_blob (b blob)")
        try:
            data = "pymysql" * 1024
            yield from c.execute("insert into test_big_blob (b) values (%s)",
                                 (data,))
            yield from c.execute("select b from test_big_blob")
            r = yield from c.fetchone()
            self.assertEqual(data.encode(conn.charset), r[0])
        finally:
            yield from c.execute("drop table test_big_blob")

    @run_until_complete
    def test_untyped(self):
        """ test conversion of null, empty string """
        conn = self.connections[0]
        c = yield from conn.cursor()
        yield from c.execute("select null,''")
        r = yield from c.fetchone()
        self.assertEqual((None, u''), r)
        yield from c.execute("select '',null")
        r = yield from c.fetchone()
        self.assertEqual((u'', None), r)

    @run_until_complete
    def test_timedelta(self):
        """ test timedelta conversion """
        conn = self.connections[0]
        c = yield from conn.cursor()
        yield from c.execute(
            "select time('12:30'), time('23:12:59'), time('23:12:59.05100'), "
            "time('-12:30'), time('-23:12:59'), time('-23:12:59.05100'), "
            "time('-00:30')")
        r = yield from c.fetchone()
        self.assertEqual((datetime.timedelta(0, 45000),
                          datetime.timedelta(0, 83579),
                          datetime.timedelta(0, 83579, 51000),
                          -datetime.timedelta(0, 45000),
                          -datetime.timedelta(0, 83579),
                          -datetime.timedelta(0, 83579, 51000),
                          -datetime.timedelta(0, 1800)),
                         r)

    @run_until_complete
    def test_datetime(self):
        """ test datetime conversion """
        conn = self.connections[0]
        c = yield from conn.cursor()
        dt = datetime.datetime(2013, 11, 12, 9, 9, 9, 123450)
        try:
            yield from c.execute(
                "create table test_datetime (id int, ts datetime(6))")
            yield from c.execute(
                "insert into test_datetime values "
                "(1,'2013-11-12 09:09:09.12345')")
            yield from c.execute("select ts from test_datetime")
            r = yield from c.fetchone()
            self.assertEqual((dt,), r)
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
        cursor = yield from conn.cursor()
        yield from cursor.execute('SELECT 1;')
        (r, ) = yield from cursor.fetchone()
        self.assertEqual(r, 1)
        yield from conn.commit()
        # make sure that transaction flag is down
        transaction_flag = conn.get_transaction_status()
        self.assertFalse(transaction_flag)

    @run_until_complete
    def test_rollback(self):
        conn = self.connections[0]
        cursor = yield from conn.cursor()

        yield from cursor.execute('DROP TABLE IF EXISTS tz_data;')
        yield from cursor.execute('CREATE TABLE tz_data ('
                                  'region VARCHAR(64),'
                                  'zone VARCHAR(64),'
                                  'name VARCHAR(64))')
        yield from conn.commit()

        args = ('America', '', 'America/New_York')
        yield from cursor.execute('INSERT INTO tz_data VALUES (%s, %s, %s)',
                                  args)
        yield from cursor.execute('SELECT * FROM tz_data;')
        data = yield from cursor.fetchall()
        self.assertEqual(len(data), 1)

        yield from conn.rollback()
        yield from cursor.execute('SELECT * FROM tz_data;')
        data = yield from cursor.fetchall()
        self.assertEqual(len(data), 0, 'should not return any rows since no '
                                       'inserts was commited')
