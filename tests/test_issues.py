import datetime
import unittest
import aiomysql

from tests import base
from tests._testutils import run_until_complete


class TestOldIssues(base.AIOPyMySQLTestCase):
    @run_until_complete
    def test_issue_3(self):
        """ undefined methods datetime_or_None, date_or_None """
        conn = self.connections[0]
        c = yield from conn.cursor()
        yield from c.execute("drop table if exists issue3")
        yield from c.execute(
            "create table issue3 (d date, t time, dt datetime, ts timestamp)")
        try:
            yield from c.execute(
                "insert into issue3 (d, t, dt, ts) values (%s,%s,%s,%s)",
                (None, None, None, None))
            yield from c.execute("select d from issue3")
            r = yield from c.fetchone()
            self.assertEqual(None, r[0])
            yield from c.execute("select t from issue3")
            r = yield from c.fetchone()
            self.assertEqual(None, r[0])
            yield from c.execute("select dt from issue3")
            r = yield from c.fetchone()
            self.assertEqual(None, r[0])
            yield from c.execute("select ts from issue3")
            r = yield from c.fetchone()
            self.assertTrue(isinstance(r[0], datetime.datetime))
        finally:
            yield from c.execute("drop table issue3")

    @run_until_complete
    def test_issue_4(self):
        """ can't retrieve TIMESTAMP fields """
        conn = self.connections[0]
        c = yield from conn.cursor()
        yield from c.execute("drop table if exists issue4")
        yield from c.execute("create table issue4 (ts timestamp)")
        try:
            yield from c.execute("insert into issue4 (ts) values (now())")
            yield from c.execute("select ts from issue4")
            r = yield from c.fetchone()
            self.assertTrue(isinstance(r[0], datetime.datetime))
        finally:
            yield from c.execute("drop table issue4")

    @run_until_complete
    def test_issue_5(self):
        """ query on information_schema.tables fails """
        con = self.connections[0]
        cur = yield from con.cursor()
        yield from cur.execute("select * from information_schema.tables")

    @run_until_complete
    def test_issue_6(self):
        # test for exception: TypeError: ord() expected a character,
        # but string of length 0 found
        conn = yield from self.connect(db='mysql')
        c = yield from conn.cursor()
        self.assertEqual(conn.db, 'mysql')
        yield from c.execute("select * from user")
        yield from conn.ensure_closed()

    @run_until_complete
    def test_issue_8(self):
        """ Primary Key and Index error when selecting data """
        conn = self.connections[0]
        c = yield from conn.cursor()
        yield from c.execute("drop table if exists test")
        yield from c.execute("""CREATE TABLE `test` (`station` int(10) NOT
            NULL DEFAULT '0', `dh`
            datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
            `echeance` int(1) NOT NULL DEFAULT '0', `me` double DEFAULT NULL,
            `mo` double DEFAULT NULL, PRIMARY
            KEY (`station`,`dh`,`echeance`)) ENGINE=MyISAM DEFAULT
            CHARSET=latin1;""")
        try:
            yield from c.execute("SELECT * FROM test")
            self.assertEqual(0, c.rowcount)
            yield from c.execute(
                "ALTER TABLE `test` ADD INDEX `idx_station` (`station`)")
            yield from c.execute("SELECT * FROM test")
            self.assertEqual(0, c.rowcount)
        finally:
            yield from c.execute("drop table test")

    @run_until_complete
    def test_issue_13(self):
        """ can't handle large result fields """
        conn = self.connections[0]
        cur = yield from conn.cursor()
        yield from cur.execute("drop table if exists issue13")
        try:
            yield from cur.execute("create table issue13 (t text)")
            # ticket says 18k
            size = 18 * 1024
            yield from cur.execute("insert into issue13 (t) values (%s)",
                                   ("x" * size,))
            yield from cur.execute("select t from issue13")
            # use assertTrue so that obscenely huge error messages don't print
            r = yield from cur.fetchone()
            self.assertTrue("x" * size == r[0])
        finally:
            yield from cur.execute("drop table issue13")

    @run_until_complete
    def test_issue_15(self):
        """ query should be expanded before perform character encoding """
        conn = self.connections[0]
        c = yield from conn.cursor()
        yield from c.execute("drop table if exists issue15")
        yield from c.execute("create table issue15 (t varchar(32))")
        try:
            yield from c.execute("insert into issue15 (t) values (%s)",
                                 (u'\xe4\xf6\xfc',))
            yield from c.execute("select t from issue15")
            r = yield from c.fetchone()
            self.assertEqual(u'\xe4\xf6\xfc', r[0])
        finally:
            yield from c.execute("drop table issue15")

    @run_until_complete
    def test_issue_16(self):
        """ Patch for string and tuple escaping """
        conn = self.connections[0]
        c = yield from conn.cursor()
        yield from c.execute("drop table if exists issue16")
        yield from c.execute("create table issue16 (name varchar(32) "
                             "primary key, email varchar(32))")
        try:
            yield from c.execute("insert into issue16 (name, email) values "
                                 "('pete', 'floydophone')")
            yield from c.execute("select email from issue16 where name=%s",
                                 ("pete",))
            r = yield from c.fetchone()
            self.assertEqual("floydophone", r[0])
        finally:
            yield from c.execute("drop table issue16")

    @unittest.skip(
        "test_issue_17() requires a custom, legacy MySQL configuration and "
        "will not be run.")
    @run_until_complete
    def test_issue_17(self):
        """ could not connect mysql use passwod """
        conn = self.connections[0]
        host = self.host
        db = self.db
        c = yield from conn.cursor()
        # grant access to a table to a user with a password
        try:
            yield from c.execute("drop table if exists issue17")
            yield from c.execute(
                "create table issue17 (x varchar(32) primary key)")
            yield from c.execute(
                "insert into issue17 (x) values ('hello, world!')")
            yield from c.execute("grant all privileges on %s.issue17 to "
                                 "'issue17user'@'%%' identified by '1234'"
                                 % db)
            yield from conn.commit()

            conn2 = yield from aiomysql.connect(host=host, user="issue17user",
                                                passwd="1234", db=db,
                                                loop=self.loop)
            c2 = yield from conn2.cursor()
            yield from c2.execute("select x from issue17")
            r = yield from c2.fetchone()
            self.assertEqual("hello, world!", r[0])
        finally:
            yield from c.execute("drop table issue17")


class TestNewIssues(base.AIOPyMySQLTestCase):
    @run_until_complete
    def test_issue_34(self):
        try:
            yield from aiomysql.connect(host="localhost", port=1237,
                                        user="root", loop=self.loop)
            self.fail()
        except aiomysql.OperationalError as e:
            self.assertEqual(2003, e.args[0])
        except Exception:
            self.fail()

    @run_until_complete
    def test_issue_33(self):
        conn = yield from self.connect(charset='utf8')
        c = yield from conn.cursor()
        try:
            yield from c.execute(
                b"drop table if exists hei\xc3\x9fe".decode("utf8"))
            yield from c.execute(
                b"create table hei\xc3\x9fe (name varchar(32))".decode("utf8"))
            yield from c.execute(b"insert into hei\xc3\x9fe (name) "
                                 b"values ('Pi\xc3\xb1ata')".
                                 decode("utf8"))
            yield from c.execute(
                b"select name from hei\xc3\x9fe".decode("utf8"))
            r = yield from c.fetchone()
            self.assertEqual(b"Pi\xc3\xb1ata".decode("utf8"), r[0])
        finally:
            yield from c.execute(b"drop table hei\xc3\x9fe".decode("utf8"))

    @unittest.skip("This test requires manual intervention")
    @run_until_complete
    def test_issue_35(self):
        conn = self.connections[0]
        c = yield from conn.cursor()
        print("sudo killall -9 mysqld within the next 10 seconds")
        try:
            yield from c.execute("select sleep(10)")
            self.fail()
        except aiomysql.OperationalError as e:
            self.assertEqual(2013, e.args[0])

    @run_until_complete
    def test_issue_36(self):
        conn = self.connections[0]
        c = yield from conn.cursor()
        # kill connections[0]
        yield from c.execute("show processlist")
        kill_id = None
        rows = yield from c.fetchall()
        for row in rows:
            id = row[0]
            info = row[7]
            if info == "show processlist":
                kill_id = id
                break
        try:
            # now nuke the connection
            yield from conn.kill(kill_id)
            # make sure this connection has broken
            yield from c.execute("show tables")
            self.fail()
        except Exception:
            pass
        # check the process list from the other connection
        try:
            c = yield from self.connections[1].cursor()
            yield from c.execute("show processlist")
            rows = yield from c.fetchall()
            ids = [row[0] for row in rows]

            self.assertFalse(kill_id in ids)
        finally:
            del self.connections[0]

    @run_until_complete
    def test_issue_37(self):
        conn = self.connections[0]
        c = yield from conn.cursor()
        self.assertEqual(1, (yield from c.execute("SELECT @foo")))

        r = yield from c.fetchone()
        self.assertEqual((None,), r)
        self.assertEqual(0, (yield from c.execute("SET @foo = 'bar'")))
        yield from c.execute("set @foo = 'bar'")

    @run_until_complete
    def test_issue_38(self):
        conn = self.connections[0]
        c = yield from conn.cursor()
        # reduced size for most default mysql installs
        datum = "a" * 1024 * 1023

        try:
            yield from c.execute("drop table if exists issue38")
            yield from c.execute(
                "create table issue38 (id integer, data mediumblob)")
            yield from c.execute("insert into issue38 values (1, %s)",
                                 (datum,))
        finally:
            yield from c.execute("drop table issue38")

    @run_until_complete
    def disabled_test_issue_54(self):
        conn = self.connections[0]
        c = yield from conn.cursor()
        yield from c.execute("drop table if exists issue54")
        big_sql = "select * from issue54 where "
        big_sql += " and ".join("%d=%d" % (i, i) for i in range(0, 100000))

        try:
            yield from c.execute(
                "create table issue54 (id integer primary key)")
            yield from c.execute("insert into issue54 (id) values (7)")
            yield from c.execute(big_sql)

            r = yield from c.fetchone()
            self.assertEqual(7, r[0])
        finally:
            yield from c.execute("drop table issue54")


class TestGitHubIssues(base.AIOPyMySQLTestCase):
    @run_until_complete
    def test_issue_66(self):
        """ 'Connection' object has no attribute 'insert_id' """
        conn = self.connections[0]
        c = yield from conn.cursor()
        self.assertEqual(0, conn.insert_id())
        try:
            yield from c.execute("drop table if exists issue66")
            yield from c.execute("create table issue66 (id integer primary "
                                 "key auto_increment, x integer)")
            yield from c.execute("insert into issue66 (x) values (1)")
            yield from c.execute("insert into issue66 (x) values (1)")
            self.assertEqual(2, conn.insert_id())
        finally:
            yield from c.execute("drop table issue66")

    @run_until_complete
    def test_issue_79(self):
        """ Duplicate field overwrites the previous one in the result
        of DictCursor """
        conn = self.connections[0]
        c = yield from conn.cursor(aiomysql.cursors.DictCursor)

        yield from c.execute("drop table if exists a")
        yield from c.execute("drop table if exists b")
        yield from c.execute("""CREATE TABLE a (id int, value int)""")
        yield from c.execute("""CREATE TABLE b (id int, value int)""")

        a = (1, 11)
        b = (1, 22)
        try:
            yield from c.execute("insert into a values (%s, %s)", a)
            yield from c.execute("insert into b values (%s, %s)", b)

            yield from c.execute("SELECT * FROM a inner join b on a.id = b.id")
            r, *_ = yield from c.fetchall()
            self.assertEqual(r['id'], 1)
            self.assertEqual(r['value'], 11)
            self.assertEqual(r['b.value'], 22)
        finally:
            yield from c.execute("drop table a")
            yield from c.execute("drop table b")

    @run_until_complete
    def test_issue_95(self):
        """ Leftover trailing OK packet for "CALL my_sp" queries """
        conn = self.connections[0]
        cur = yield from conn.cursor()
        yield from cur.execute("DROP PROCEDURE IF EXISTS `foo`")
        yield from cur.execute("""CREATE PROCEDURE `foo` ()
        BEGIN
            SELECT 1;
        END""")
        try:
            yield from cur.execute("""CALL foo()""")
            yield from cur.execute("""SELECT 1""")
            r = yield from cur.fetchone()
            self.assertEqual(r[0], 1)
        finally:
            yield from cur.execute("DROP PROCEDURE IF EXISTS `foo`")

    @run_until_complete
    def test_issue_114(self):
        """ autocommit is not set after reconnecting with ping() """
        conn = yield from self.connect(charset="utf8")
        yield from conn.autocommit(False)
        c = yield from conn.cursor()
        yield from c.execute("""select @@autocommit;""")
        r = yield from c.fetchone()
        self.assertFalse(r[0])
        yield from conn.ensure_closed()
        yield from conn.ping()
        yield from c.execute("""select @@autocommit;""")
        r = yield from c.fetchone()
        self.assertFalse(r[0])
        yield from conn.ensure_closed()

        # Ensure autocommit() is still working
        conn = yield from self.connect(charset="utf8")
        c = yield from conn.cursor()
        yield from c.execute("""select @@autocommit;""")
        r = yield from c.fetchone()
        self.assertFalse(r[0])
        yield from conn.ensure_closed()
        yield from conn.ping()
        yield from conn.autocommit(True)
        yield from c.execute("""select @@autocommit;""")
        r = yield from c.fetchone()
        self.assertTrue(r[0])
        yield from conn.ensure_closed()

    @run_until_complete
    def test_issue_175(self):
        """ The number of fields returned by server is read in wrong way """
        conn = self.connections[0]
        cur = yield from conn.cursor()
        for length in (200, 300):
            cols = ', '.join('c{0} integer'.format(i) for i in range(length))
            sql = 'create table test_field_count ({0})'.format(cols)
            try:
                yield from cur.execute(sql)
                yield from cur.execute('select * from test_field_count')
                assert len(cur.description) == length
            finally:
                yield from cur.execute('drop table if exists test_field_count')
