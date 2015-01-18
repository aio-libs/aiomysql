# copied from aiopg
# https://github.com/aio-libs/aiopg/blob/master/aiopg/pool.py

import asyncio
import collections

from .connection import connect


@asyncio.coroutine
def create_pool(minsize=10, maxsize=10, echo=False, loop=None, **kwargs):
    if loop is None:
        loop = asyncio.get_event_loop()

    pool = Pool(minsize=minsize, maxsize=maxsize, echo=echo, loop=loop,
                **kwargs)
    if minsize > 0:
        with (yield from pool._cond):
            yield from pool._fill_free_pool(False)
    return pool


class Pool(asyncio.AbstractServer):
    """Connection pool"""

    def __init__(self, minsize, maxsize, echo, loop, **kwargs):
        if minsize < 0:
            raise ValueError("minsize should be zero or greater")
        if maxsize < minsize:
            raise ValueError("maxsize should be not less than minsize")
        self._minsize = minsize
        self._loop = loop
        self._conn_kwargs = kwargs
        self._acquiring = 0
        self._free = collections.deque(maxlen=maxsize)
        self._cond = asyncio.Condition(loop=loop)
        self._used = set()
        self._terminated = set()
        self._closing = False
        self._closed = False
        self._echo = echo

    @property
    def echo(self):
        return self._echo

    @property
    def minsize(self):
        return self._minsize

    @property
    def maxsize(self):
        return self._free.maxlen

    @property
    def size(self):
        return self.freesize + len(self._used) + self._acquiring

    @property
    def freesize(self):
        return len(self._free)

    @asyncio.coroutine
    def clear(self):
        """Close all free connections in pool."""
        with (yield from self._cond):
            while self._free:
                conn = self._free.popleft()
                yield from conn.wait_closed()
            self._cond.notify()

    def close(self):
        """Close pool.

        Mark all pool connections to be closed on getting back to pool.
        Closed pool doesn't allow to acquire new connections.
        """
        if self._closed:
            return
        self._closing = True

    def terminate(self):
        """Terminate pool.

        Close pool with instantly closing all acquired connections also.
        """

        self.close()

        for conn in list(self._used):
            conn.close()
            self._terminated.add(conn)

        self._used.clear()

    @asyncio.coroutine
    def wait_closed(self):
        """Wait for closing all pool's connections."""

        if self._closed:
            return
        if not self._closing:
            raise RuntimeError(".wait_closed() should be called "
                               "after .close()")

        while self._free:
            conn = self._free.popleft()
            conn.close()

        with (yield from self._cond):
            while self.size > self.freesize:
                yield from self._cond.wait()

        self._closed = True

    @asyncio.coroutine
    def acquire(self):
        """Acquire free connection from the pool."""
        if self._closing:
            raise RuntimeError("Cannot acquire connection after closing pool")
        with (yield from self._cond):
            while True:
                yield from self._fill_free_pool(True)
                if self._free:
                    conn = self._free.popleft()
                    assert not conn.closed, conn
                    assert conn not in self._used, (conn, self._used)
                    self._used.add(conn)
                    return conn
                else:
                    yield from self._cond.wait()

    @asyncio.coroutine
    def _fill_free_pool(self, override_min):
        while self.size < self.minsize:
            self._acquiring += 1
            try:
                conn = yield from connect(echo=self._echo, loop=self._loop,
                                          **self._conn_kwargs)
                # raise exception if pool is closing
                self._free.append(conn)
                self._cond.notify()
            finally:
                self._acquiring -= 1
        if self._free:
            return

        if override_min and self.size < self.maxsize:
            self._acquiring += 1
            try:
                conn = yield from connect(echo=self._echo, loop=self._loop,
                                          **self._conn_kwargs)
                # raise exception if pool is closing
                self._free.append(conn)
                self._cond.notify()
            finally:
                self._acquiring -= 1

    @asyncio.coroutine
    def _wakeup(self):
        with (yield from self._cond):
            self._cond.notify()

    def release(self, conn):
        """Release free connection back to the connection pool.

        This is **NOT** a coroutine.
        """
        if conn in self._terminated:
            assert conn.closed, conn
            self._terminated.remove(conn)
            return
        assert conn in self._used, (conn, self._used)
        self._used.remove(conn)
        if not conn.closed:
            in_trans = conn.get_transaction_status()
            if in_trans:
                conn.close()
                return
            if self._closing:
                conn.close()
            else:
                self._free.append(conn)
            asyncio.Task(self._wakeup(), loop=self._loop)

    def __enter__(self):
        raise RuntimeError(
            '"yield from" should be used as context manager expression')

    def __exit__(self, *args):
        # This must exist because __enter__ exists, even though that
        # always raises; that's how the with-statement works.
        pass  # pragma: nocover

    def __iter__(self):
        # This is not a coroutine.  It is meant to enable the idiom:
        #
        #     with (yield from pool) as conn:
        #         <block>
        #
        # as an alternative to:
        #
        #     conn = yield from pool.acquire()
        #     try:
        #         <block>
        #     finally:
        #         conn.release()
        conn = yield from self.acquire()
        return _ConnectionContextManager(self, conn)


class _ConnectionContextManager:
    """Context manager.

    This enables the following idiom for acquiring and releasing a
    connection around a block:

        with (yield from pool) as conn:
            cur = yield from conn.cursor()

    while failing loudly when accidentally using:

        with pool:
            <block>
    """

    __slots__ = ('_pool', '_conn')

    def __init__(self, pool, conn):
        self._pool = pool
        self._conn = conn

    def __enter__(self):
        return self._conn

    def __exit__(self, *args):
        try:
            self._pool.release(self._conn)
        finally:
            self._pool = None
            self._conn = None
