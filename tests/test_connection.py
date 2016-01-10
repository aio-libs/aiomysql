import asyncio
import gc
import os
import sys
import unittest
import aiomysql
from tests._testutils import run_until_complete
from tests.base import AIOPyMySQLTestCase


PY_341 = sys.version_info >= (3, 4, 1)


class TestConnection(AIOPyMySQLTestCase):

    def fill_my_cnf(self):
        tests_root = os.path.abspath(os.path.dirname(__file__))
        path1 = os.path.join(tests_root, 'fixtures/my.cnf.tmpl')
        path2 = os.path.join(tests_root, 'fixtures/my.cnf')
        with open(path1) as f1:
            tmpl = f1.read()
        with open(path2, 'w') as f2:
            f2.write(tmpl.format_map(self.__dict__))

    @run_until_complete
    def test_config_file(self):
        self.fill_my_cnf()
        tests_root = os.path.abspath(os.path.dirname(__file__))
        path = os.path.join(tests_root, 'fixtures/my.cnf')
        conn = yield from self.connect(read_default_file=path)

        self.assertEqual(conn.host, self.host)
        self.assertEqual(conn.port, self.port)
        self.assertEqual(conn.user, self.user)

        # make sure connection is working
        cur = yield from conn.cursor()
        yield from cur.execute('SELECT 42;')
        (r, ) = yield from cur.fetchone()
        self.assertEqual(r, 42)
        conn.close()

    @run_until_complete
    def test_config_file_with_different_group(self):
        self.fill_my_cnf()
        # same test with config file but actual settings
        # located in not default group.
        tests_root = os.path.abspath(os.path.dirname(__file__))
        path = os.path.join(tests_root, 'fixtures/my.cnf')
        group = 'client_with_unix_socket'
        conn = yield from self.connect(read_default_file=path,
                                       read_default_group=group)

        self.assertEqual(conn.charset, 'utf8')
        self.assertEqual(conn.user, 'root')
        self.assertEqual(conn.unix_socket, '/var/run/mysqld/mysqld.sock')

        # make sure connection is working
        cur = yield from conn.cursor()
        yield from cur.execute('SELECT 42;')
        (r, ) = yield from cur.fetchone()
        self.assertEqual(r, 42)
        conn.close()

    @run_until_complete
    def test_connect_using_unix_socket(self):
        sock = '/var/run/mysqld/mysqld.sock'
        conn = yield from self.connect(unix_socket=sock)
        self.assertEqual(conn.unix_socket, sock)

        cur = yield from conn.cursor()
        yield from cur.execute('SELECT 42;')
        (r, ) = yield from cur.fetchone()
        self.assertEqual(r, 42)
        conn.close()

    @run_until_complete
    def test_utf8mb4(self):
        """This test requires MySQL >= 5.5"""
        charset = 'utf8mb4'
        conn = yield from self.connect(charset=charset)
        self.assertEqual(conn.charset, charset)
        conn.close()

    @run_until_complete
    def test_largedata(self):
        """Large query and response (>=16MB)"""
        cur = yield from self.connections[0].cursor()
        yield from cur.execute("SELECT @@max_allowed_packet")
        r = yield from cur.fetchone()
        if r[0] < 16 * 1024 * 1024 + 10:
            self.skipTest('Set max_allowed_packet to bigger than 17MB')
        else:
            t = 'a' * (16 * 1024 * 1024)
            yield from cur.execute("SELECT '" + t + "'")
            r = yield from cur.fetchone()
            self.assertEqual(r[0], t)

    @run_until_complete
    def test_escape_string(self):
        con = self.connections[0]
        cur = yield from con.cursor()

        self.assertEqual(con.escape("foo'bar"), "'foo\\'bar'")
        # literal is alias for escape
        self.assertEqual(con.literal("foo'bar"), "'foo\\'bar'")
        yield from cur.execute("SET sql_mode='NO_BACKSLASH_ESCAPES'")
        self.assertEqual(con.escape("foo'bar"), "'foo''bar'")

    @run_until_complete
    def test_sql_mode_param(self):
        con = yield from self.connect(sql_mode='NO_BACKSLASH_ESCAPES')
        self.assertEqual(con.escape("foo'bar"), "'foo''bar'")

    @run_until_complete
    def test_init_param(self):
        init_command = "SET sql_mode='NO_BACKSLASH_ESCAPES';"
        con = yield from self.connect(init_command=init_command)
        self.assertEqual(con.escape("foo'bar"), "'foo''bar'")

    @run_until_complete
    def test_autocommit(self):
        con = self.connections[0]
        self.assertFalse(con.get_autocommit())

        cur = yield from con.cursor()
        yield from cur.execute("SET AUTOCOMMIT=1")
        self.assertTrue(con.get_autocommit())

        yield from con.autocommit(False)
        self.assertFalse(con.get_autocommit())
        yield from cur.execute("SELECT @@AUTOCOMMIT")
        r = yield from cur.fetchone()
        self.assertEqual(r[0], 0)

    @run_until_complete
    def test_select_db(self):
        con = self.connections[0]
        current_db = self.db
        other_db = self.other_db
        cur = yield from con.cursor()
        yield from cur.execute('SELECT database()')
        r = yield from cur.fetchone()
        self.assertEqual(r[0], current_db)

        yield from con.select_db(other_db)
        yield from cur.execute('SELECT database()')
        r = yield from cur.fetchone()
        self.assertEqual(r[0], other_db)

    @run_until_complete
    def test_connection_gone_away(self):
        # test
        # http://dev.mysql.com/doc/refman/5.0/en/gone-away.html
        # http://dev.mysql.com/doc/refman/5.0/en/error-messages-client.html
        # error_cr_server_gone_error
        conn = yield from self.connect()
        cur = yield from conn.cursor()
        yield from cur.execute("SET wait_timeout=1")
        yield from asyncio.sleep(2, loop=self.loop)
        with self.assertRaises(aiomysql.OperationalError) as cm:
            yield from cur.execute("SELECT 1+1")
        # error occures while reading, not writing because of socket buffer.
        # self.assertEqual(cm.exception.args[0], 2006)
        self.assertIn(cm.exception.args[0], (2006, 2013))
        conn.close()

    @run_until_complete
    def test_connection_info_methods(self):
        conn = yield from self.connect()
        # trhead id is int
        self.assertIsInstance(conn.thread_id(), int)
        self.assertEqual(conn.character_set_name(), 'latin1')
        self.assertTrue(str(conn.port) in conn.get_host_info())
        self.assertIsInstance(conn.get_server_info(), str)
        # protocol id is int
        self.assertIsInstance(conn.get_proto_info(), int)
        conn.close()

    @run_until_complete
    def test_connection_set_charset(self):
        conn = yield from self.connect()
        self.assertEqual(conn.character_set_name(), 'latin1')
        yield from conn.set_charset('utf8')
        self.assertEqual(conn.character_set_name(), 'utf8')

    @run_until_complete
    def test_connection_ping(self):
        conn = yield from self.connect()
        yield from conn.ping()
        self.assertEqual(conn.closed, False)
        conn.close()
        yield from conn.ping()
        self.assertEqual(conn.closed, False)

    @run_until_complete
    def test_connection_properties(self):
        conn = yield from self.connect()
        self.assertEqual(conn.host, self.host)
        self.assertEqual(conn.port, self.port)
        self.assertEqual(conn.user, self.user)
        self.assertEqual(conn.db, self.db)
        self.assertEqual(conn.echo, False)
        conn.close()

    @run_until_complete
    def test_connection_double_ensure_closed(self):
        conn = yield from self.connect()
        self.assertFalse(conn.closed)
        yield from conn.ensure_closed()
        self.assertTrue(conn.closed)
        yield from conn.ensure_closed()
        self.assertTrue(conn.closed)

    @unittest.skipIf(not PY_341,
                     "Python 3.3 doesnt support __del__ calls from GC")
    @run_until_complete
    def test___del__(self):
        conn = yield from aiomysql.connect(loop=self.loop, host=self.host,
                                           port=self.port, db=self.db,
                                           user=self.user,
                                           password=self.password)
        with self.assertWarns(ResourceWarning):
            del conn
            gc.collect()

    @run_until_complete
    def test_no_delay_warning(self):
        with self.assertWarns(DeprecationWarning):
            conn = yield from self.connect(no_delay=True)
        conn.close()

    @run_until_complete
    def test_no_delay_default_arg(self):
        conn = yield from self.connect()
        self.assertTrue(conn._no_delay)
        conn.close()

    @run_until_complete
    def test_previous_cursor_not_closed(self):
        conn = yield from self.connect()
        cur1 = yield from conn.cursor()
        yield from cur1.execute("SELECT 1; SELECT 2")
        cur2 = yield from conn.cursor()
        yield from cur2.execute("SELECT 3;")
        resp = yield from cur2.fetchone()
        self.assertEqual(resp[0], 3)

    @run_until_complete
    def test_commit_during_multi_result(self):
        conn = yield from self.connect()
        cur = yield from conn.cursor()
        yield from cur.execute("SELECT 1; SELECT 2;")
        yield from conn.commit()
        yield from cur.execute("SELECT 3;")
        resp = yield from cur.fetchone()
        self.assertEqual(resp[0], 3)
