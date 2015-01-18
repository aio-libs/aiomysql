import asyncio
import aiomysql
from tests._testutils import run_until_complete
from tests.base import AIOPyMySQLTestCase


class TestConnection(AIOPyMySQLTestCase):
    @run_until_complete
    def test_utf8mb4(self):
        """This test requires MySQL >= 5.5"""
        conn = yield from aiomysql.connect(loop=self.loop, charset='utf8mb4')
        assert conn  # pyflakes

    @run_until_complete
    def test_largedata(self):
        """Large query and response (>=16MB)"""
        cur = self.connections[0].cursor()
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
        cur = con.cursor()

        self.assertEqual(con.escape("foo'bar"), "'foo\\'bar'")
        # literal is alias for escape
        self.assertEqual(con.literal("foo'bar"), "'foo\\'bar'")
        yield from cur.execute("SET sql_mode='NO_BACKSLASH_ESCAPES'")
        self.assertEqual(con.escape("foo'bar"), "'foo''bar'")

    @run_until_complete
    def test_autocommit(self):
        con = self.connections[0]
        self.assertFalse(con.get_autocommit())

        cur = con.cursor()
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
        cur = con.cursor()
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
        cur = conn.cursor()
        yield from cur.execute("SET wait_timeout=1")
        yield from asyncio.sleep(2, loop=self.loop)
        with self.assertRaises(aiomysql.OperationalError) as cm:
            yield from cur.execute("SELECT 1+1")
        # error occures while reading, not writing because of socket buffer.
        # self.assertEquals(cm.exception.args[0], 2006)
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
        self.assertEqual(conn.open, True)
        conn.close()
        yield from conn.ping()
        self.assertEqual(conn.open, True)

    @run_until_complete
    def test_connection_set_nodelay_option(self):
        conn = yield from self.connect(no_delay=True)
        cur = conn.cursor()
        yield from cur.execute("SELECT 1;")
        (r, ) = yield from cur.fetchone()
        self.assertEqual(r, 1)

    @run_until_complete
    def test_connection_properties(self):
        conn = yield from self.connect(no_delay=True)
        self.assertEqual(conn.host, self.host)
        self.assertEqual(conn.port, self.port)
        self.assertEqual(conn.user, self.user)
        self.assertEqual(conn.db, self.db)
        self.assertEqual(conn.echo, False)
        conn.close()
