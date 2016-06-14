import asyncio
import json
import os
import unittest

import aiomysql
from aiomysql.connection import Connection
from aiomysql.pool import Pool


class TestPool(unittest.TestCase):

    fname = os.path.join(os.path.dirname(__file__), "databases.json")

    if os.path.exists(fname):
        with open(fname) as f:
            databases = json.load(f)
    else:
        databases = [
            {"host": "localhost", "user": "root", "password": "",
             "db": "test_pymysql", "use_unicode": True},
            {"host": "localhost", "user": "root", "password": "",
             "db": "test_pymysql2"}]

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        self.pool = None

        self.host = os.environ.get('MYSQL_HOST', 'localhost')
        self.port = os.environ.get('MYSQL_PORT', 3306)
        self.user = os.environ.get('MYSQL_USER', 'root')
        self.db = os.environ.get('MYSQL_DB', 'test_pymysql')
        self.password = os.environ.get('MYSQL_PASSWORD', '')

    def tearDown(self):
        if self.pool is not None:
            self.pool.terminate()
            self.loop.run_until_complete(self.pool.wait_closed())
        self.loop.close()
        self.loop = None

    @asyncio.coroutine
    def connect(self, host=None, user=None, password=None,
                db=None, use_unicode=True, no_delay=None, **kwargs):
        if host is None:
            host = self.host
        if user is None:
            user = self.user
        if password is None:
            password = self.password
        if db is None:
            db = self.db
        conn = yield from aiomysql.connect(loop=self.loop, host=host,
                                           user=user, password=password,
                                           db=db, use_unicode=use_unicode,
                                           no_delay=no_delay, **kwargs)
        return conn

    @asyncio.coroutine
    def create_pool(self, no_loop=False, use_unicode=True, **kwargs):
        kwargs.setdefault("minsize", 10)
        loop = None if no_loop else self.loop
        pool = yield from aiomysql.create_pool(loop=loop,
                                               host=self.host,
                                               port=self.port,
                                               user=self.user,
                                               db=self.db,
                                               password=self.password,
                                               use_unicode=use_unicode,
                                               **kwargs)
        self.pool = pool
        return pool

    def test_create_pool(self):

        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            self.assertIsInstance(pool, Pool)
            self.assertEqual(10, pool.minsize)
            self.assertEqual(10, pool.maxsize)
            self.assertEqual(10, pool.size)
            self.assertEqual(10, pool.freesize)
            self.assertFalse(pool.echo)

        self.loop.run_until_complete(go())

    def test_create_pool2(self):

        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool(minsize=10, maxsize=20)
            self.assertIsInstance(pool, Pool)
            self.assertEqual(10, pool.minsize)
            self.assertEqual(20, pool.maxsize)
            self.assertEqual(10, pool.size)
            self.assertEqual(10, pool.freesize)

        self.loop.run_until_complete(go())

    def test_acquire(self):

        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            conn = yield from pool.acquire()
            self.assertIsInstance(conn, Connection)
            self.assertFalse(conn.closed)
            cur = yield from conn.cursor()
            yield from cur.execute('SELECT 1')
            val = yield from cur.fetchone()
            self.assertEqual((1,), val)
            pool.release(conn)

        self.loop.run_until_complete(go())

    def test_release(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            conn = yield from pool.acquire()
            self.assertEqual(9, pool.freesize)
            self.assertEqual({conn}, pool._used)
            pool.release(conn)
            self.assertEqual(10, pool.freesize)
            self.assertFalse(pool._used)

        self.loop.run_until_complete(go())

    def test_release_closed(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            conn = yield from pool.acquire()
            self.assertEqual(9, pool.freesize)
            yield from conn.ensure_closed()
            pool.release(conn)
            self.assertEqual(9, pool.freesize)
            self.assertFalse(pool._used)
            self.assertEqual(9, pool.size)

            conn2 = yield from pool.acquire()
            self.assertEqual(9, pool.freesize)
            self.assertEqual(10, pool.size)
            pool.release(conn2)

        self.loop.run_until_complete(go())

    def test_bad_context_manager_usage(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            with self.assertRaises(RuntimeError):
                with pool:
                    pass

        self.loop.run_until_complete(go())

    def test_context_manager(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            with (yield from pool) as conn:
                self.assertIsInstance(conn, Connection)
                self.assertEqual(9, pool.freesize)
                self.assertEqual({conn}, pool._used)
            self.assertEqual(10, pool.freesize)

        self.loop.run_until_complete(go())

    def test_clear(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            yield from pool.clear()
            self.assertEqual(0, pool.freesize)

        self.loop.run_until_complete(go())

    def test_initial_empty(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool(minsize=0)
            self.assertEqual(10, pool.maxsize)
            self.assertEqual(0, pool.minsize)
            self.assertEqual(0, pool.size)
            self.assertEqual(0, pool.freesize)

            with (yield from pool):
                self.assertEqual(1, pool.size)
                self.assertEqual(0, pool.freesize)
            self.assertEqual(1, pool.size)
            self.assertEqual(1, pool.freesize)

            conn1 = yield from pool.acquire()
            self.assertEqual(1, pool.size)
            self.assertEqual(0, pool.freesize)

            conn2 = yield from pool.acquire()
            self.assertEqual(2, pool.size)
            self.assertEqual(0, pool.freesize)

            pool.release(conn1)
            self.assertEqual(2, pool.size)
            self.assertEqual(1, pool.freesize)

            pool.release(conn2)
            self.assertEqual(2, pool.size)
            self.assertEqual(2, pool.freesize)

        self.loop.run_until_complete(go())

    def test_parallel_tasks(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool(minsize=0, maxsize=2)
            self.assertEqual(2, pool.maxsize)
            self.assertEqual(0, pool.minsize)
            self.assertEqual(0, pool.size)
            self.assertEqual(0, pool.freesize)

            fut1 = pool.acquire()
            fut2 = pool.acquire()

            conn1, conn2 = yield from asyncio.gather(fut1, fut2,
                                                     loop=self.loop)
            self.assertEqual(2, pool.size)
            self.assertEqual(0, pool.freesize)
            self.assertEqual({conn1, conn2}, pool._used)

            pool.release(conn1)
            self.assertEqual(2, pool.size)
            self.assertEqual(1, pool.freesize)
            self.assertEqual({conn2}, pool._used)

            pool.release(conn2)
            self.assertEqual(2, pool.size)
            self.assertEqual(2, pool.freesize)
            self.assertFalse(conn1.closed)
            self.assertFalse(conn2.closed)

            conn3 = yield from pool.acquire()
            self.assertIs(conn3, conn1)
            pool.release(conn3)

        self.loop.run_until_complete(go())

    def test_parallel_tasks_more(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool(minsize=0, maxsize=3)

            fut1 = pool.acquire()
            fut2 = pool.acquire()
            fut3 = pool.acquire()

            conn1, conn2, conn3 = yield from asyncio.gather(fut1, fut2, fut3,
                                                            loop=self.loop)
            self.assertEqual(3, pool.size)
            self.assertEqual(0, pool.freesize)
            self.assertEqual({conn1, conn2, conn3}, pool._used)

            pool.release(conn1)
            self.assertEqual(3, pool.size)
            self.assertEqual(1, pool.freesize)
            self.assertEqual({conn2, conn3}, pool._used)

            pool.release(conn2)
            self.assertEqual(3, pool.size)
            self.assertEqual(2, pool.freesize)
            self.assertEqual({conn3}, pool._used)
            self.assertFalse(conn1.closed)
            self.assertFalse(conn2.closed)

            pool.release(conn3)
            self.assertEqual(3, pool.size)
            self.assertEqual(3, pool.freesize)
            self.assertFalse(pool._used)
            self.assertFalse(conn1.closed)
            self.assertFalse(conn2.closed)
            self.assertFalse(conn3.closed)

            conn4 = yield from pool.acquire()
            self.assertIs(conn4, conn1)
            pool.release(conn4)

        self.loop.run_until_complete(go())

    def test_default_event_loop(self):
        asyncio.set_event_loop(self.loop)

        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool(no_loop=True)
            self.assertIs(pool._loop, self.loop)

        self.loop.run_until_complete(go())

    # @mock.patch("aiopg.pool.logger")
    def test_release_with_invalid_status(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            conn = yield from pool.acquire()
            self.assertEqual(9, pool.freesize)
            self.assertEqual({conn}, pool._used)
            cur = yield from conn.cursor()
            yield from cur.execute('BEGIN')
            yield from cur.close()

            pool.release(conn)
            self.assertEqual(9, pool.freesize)
            self.assertFalse(pool._used)
            self.assertTrue(conn.closed)

        self.loop.run_until_complete(go())

    def test_release_with_invalid_status_wait_release(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            conn = yield from pool.acquire()
            self.assertEqual(9, pool.freesize)
            self.assertEqual({conn}, pool._used)
            cur = yield from conn.cursor()
            yield from cur.execute('BEGIN')
            yield from cur.close()

            yield from pool.release(conn)
            self.assertEqual(9, pool.freesize)
            self.assertFalse(pool._used)
            self.assertTrue(conn.closed)

        self.loop.run_until_complete(go())

    def test__fill_free(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool(minsize=1)
            with (yield from pool):
                self.assertEqual(0, pool.freesize)
                self.assertEqual(1, pool.size)

                conn = yield from asyncio.wait_for(pool.acquire(),
                                                   timeout=0.5,
                                                   loop=self.loop)
                self.assertEqual(0, pool.freesize)
                self.assertEqual(2, pool.size)
                pool.release(conn)
                self.assertEqual(1, pool.freesize)
                self.assertEqual(2, pool.size)
            self.assertEqual(2, pool.freesize)
            self.assertEqual(2, pool.size)

        self.loop.run_until_complete(go())

    def test_connect_from_acquire(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool(minsize=0)
            self.assertEqual(0, pool.freesize)
            self.assertEqual(0, pool.size)
            with (yield from pool):
                self.assertEqual(1, pool.size)
                self.assertEqual(0, pool.freesize)
            self.assertEqual(1, pool.size)
            self.assertEqual(1, pool.freesize)
        self.loop.run_until_complete(go())

    @unittest.skip('Not implemented')
    def test_create_pool_with_timeout(self):

        @asyncio.coroutine
        def go():
            timeout = 0.1
            pool = yield from self.create_pool(timeout=timeout)
            self.assertEqual(timeout, pool.timeout)
            conn = yield from pool.acquire()
            self.assertEqual(timeout, conn.timeout)
            pool.release(conn)

        self.loop.run_until_complete(go())

    def test_concurrency(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool(minsize=2, maxsize=4)
            c1 = yield from pool.acquire()
            c2 = yield from pool.acquire()
            self.assertEqual(0, pool.freesize)
            self.assertEqual(2, pool.size)
            pool.release(c1)
            pool.release(c2)

        self.loop.run_until_complete(go())

    def test_invalid_minsize_and_maxsize(self):

        @asyncio.coroutine
        def go():
            with self.assertRaises(ValueError):
                yield from self.create_pool(minsize=-1)

            with self.assertRaises(ValueError):
                yield from self.create_pool(minsize=5, maxsize=2)

        self.loop.run_until_complete(go())

    def test_true_parallel_tasks(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool(minsize=0, maxsize=1)
            self.assertEqual(1, pool.maxsize)
            self.assertEqual(0, pool.minsize)
            self.assertEqual(0, pool.size)
            self.assertEqual(0, pool.freesize)

            maxsize = 0
            minfreesize = 100

            def inner():
                nonlocal maxsize, minfreesize
                maxsize = max(maxsize, pool.size)
                minfreesize = min(minfreesize, pool.freesize)
                conn = yield from pool.acquire()
                maxsize = max(maxsize, pool.size)
                minfreesize = min(minfreesize, pool.freesize)
                yield from asyncio.sleep(0.01, loop=self.loop)
                pool.release(conn)
                maxsize = max(maxsize, pool.size)
                minfreesize = min(minfreesize, pool.freesize)

            yield from asyncio.gather(inner(), inner(),
                                      loop=self.loop)

            self.assertEqual(1, maxsize)
            self.assertEqual(0, minfreesize)

        self.loop.run_until_complete(go())

    def test_cannot_acquire_after_closing(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            pool.close()

            with self.assertRaises(RuntimeError):
                yield from pool.acquire()

        self.loop.run_until_complete(go())

    def test_wait_closed(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()

            c1 = yield from pool.acquire()
            c2 = yield from pool.acquire()
            self.assertEqual(10, pool.size)
            self.assertEqual(8, pool.freesize)

            ops = []

            @asyncio.coroutine
            def do_release(conn):
                yield from asyncio.sleep(0, loop=self.loop)
                pool.release(conn)
                ops.append('release')

            @asyncio.coroutine
            def wait_closed():
                yield from pool.wait_closed()
                ops.append('wait_closed')

            pool.close()
            yield from asyncio.gather(wait_closed(),
                                      do_release(c1),
                                      do_release(c2),
                                      loop=self.loop)
            self.assertEqual(['release', 'release', 'wait_closed'], ops)
            self.assertEqual(0, pool.freesize)

        self.loop.run_until_complete(go())

    def test_echo(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool(echo=True)
            self.assertTrue(pool.echo)

            with (yield from pool) as conn:
                self.assertTrue(conn.echo)

        self.loop.run_until_complete(go())

    def test_terminate_with_acquired_connections(self):

        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            conn = yield from pool.acquire()
            pool.terminate()
            yield from pool.wait_closed()

            self.assertTrue(conn.closed)

        self.loop.run_until_complete(go())

    def test_release_closed_connection(self):

        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            conn = yield from pool.acquire()
            conn.close()

            pool.release(conn)
            pool.close()

        self.loop.run_until_complete(go())

    def test_wait_closing_on_not_closed(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()

            with self.assertRaises(RuntimeError):
                yield from pool.wait_closed()
            pool.close()
        self.loop.run_until_complete(go())

    def test_release_terminated_pool(self):

        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            conn = yield from pool.acquire()
            pool.terminate()
            yield from pool.wait_closed()

            pool.release(conn)
            pool.close()

        self.loop.run_until_complete(go())

    def test_release_terminated_pool_wait_release(self):

        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            conn = yield from pool.acquire()
            pool.terminate()
            yield from pool.wait_closed()

            yield from pool.release(conn)
            pool.close()

        self.loop.run_until_complete(go())

    def test_close_with_acquired_connections(self):

        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            yield from pool.acquire()
            pool.close()

            with self.assertRaises(asyncio.TimeoutError):
                yield from asyncio.wait_for(pool.wait_closed(),
                                            0.1, loop=self.loop)

        self.loop.run_until_complete(go())

    @asyncio.coroutine
    def _set_global_conn_timeout(self, t):
        # create separate connection to setup global connection timeouts
        # https://dev.mysql.com/doc/refman/5.1/en/server-system-variables
        # .html#sysvar_interactive_timeout
        conn = yield from self.connect()
        cur = yield from conn.cursor()
        yield from cur.execute('SET GLOBAL wait_timeout=%s;', t)
        yield from cur.execute('SET GLOBAL interactive_timeout=%s;', t)
        conn.close()

    def test_drop_connection_if_timedout(self):
        @asyncio.coroutine
        def go():

            yield from self._set_global_conn_timeout(2)
            try:
                pool = yield from self.create_pool(minsize=3, maxsize=3)
                # sleep, more then connection timeout
                yield from asyncio.sleep(3, loop=self.loop)
                conn = yield from pool.acquire()
                cur = yield from conn.cursor()
                # query should not throw exception OperationalError
                yield from cur.execute('SELECT 1;')
                pool.release(conn)
                pool.close()
                yield from pool.wait_closed()
            finally:
                # setup default timeouts
                yield from self._set_global_conn_timeout(28800)

        self.loop.run_until_complete(go())

    def test_cancelled_connection(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool(minsize=0, maxsize=1)

            try:
                with (yield from pool) as conn:
                    curs = yield from conn.cursor()
                    # Cancel a cursor in the middle of execution, before it
                    # could read even the first packet (SLEEP assures the
                    # timings)
                    task = self.loop.create_task(curs.execute(
                        "SELECT 1 as id, SLEEP(0.1) as xxx"))
                    yield from asyncio.sleep(0.05, loop=self.loop)
                    task.cancel()
                    yield from task
            except asyncio.CancelledError:
                pass

            with (yield from pool) as conn:
                cur2 = yield from conn.cursor()
                res = yield from cur2.execute("SELECT 2 as value, 0 as xxx")
                names = [x[0] for x in cur2.description]
                # If we receive ["id", "xxx"] - we corrupted the connection
                self.assertEqual(names, ["value", "xxx"])
                res = yield from cur2.fetchall()
                # If we receive [(1, 0)] - we retrieved old cursor's values
                self.assertEqual(list(res), [(2, 0)])

        self.loop.run_until_complete(go())
