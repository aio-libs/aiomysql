# copied from aiopg
# https://github.com/aio-libs/aiopg/blob/master/aiopg/pool.py

import asyncio
import collections
import warnings

from .connection import connect
from .utils import (PY_35, _PoolContextManager, _PoolConnectionContextManager,
                    _PoolAcquireContextManager, create_future, create_task)


def create_pool(minsize=1, maxsize=10, echo=False, pool_recycle=-1,
                loop=None, **kwargs):
    coro = _create_pool(minsize=minsize, maxsize=maxsize, echo=echo,
                        pool_recycle=pool_recycle, loop=loop, **kwargs)
    return _PoolContextManager(coro)


@asyncio.coroutine
def _create_pool(minsize=1, maxsize=10, echo=False, pool_recycle=-1,
                 loop=None, **kwargs):
    if loop is None:
        loop = asyncio.get_event_loop()

    pool = Pool(minsize=minsize, maxsize=maxsize, echo=echo,
                pool_recycle=pool_recycle, loop=loop, **kwargs)
    if minsize > 0:
        with (yield from pool._cond):
            yield from pool._fill_free_pool(False)
    return pool


class Pool(asyncio.AbstractServer):
    """Connection pool"""

    def __init__(self, minsize, maxsize, echo, pool_recycle, loop, **kwargs):
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
        self._recycle = pool_recycle

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
                yield from conn.ensure_closed()
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

    def acquire(self):
        """Acquire free connection from the pool."""
        coro = self._acquire()
        return _PoolAcquireContextManager(coro, self)

    @asyncio.coroutine
    def _acquire(self):
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
        # iterate over free connections and remove timeouted ones
        free_size = len(self._free)
        n = 0
        while n < free_size:
            conn = self._free[-1]
            if conn._reader.at_eof():
                self._free.pop()
                conn.close()

            elif (self._recycle > -1 and
                  self._loop.time() - conn.last_usage > self._recycle):
                self._free.pop()
                conn.close()

            else:
                self._free.rotate()
            n += 1

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
        fut = create_future(self._loop)
        fut.set_result(None)

        if conn in self._terminated:
            assert conn.closed, conn
            self._terminated.remove(conn)
            return fut
        assert conn in self._used, (conn, self._used)
        self._used.remove(conn)
        if not conn.closed:
            in_trans = conn.get_transaction_status()
            if in_trans:
                conn.close()
                return fut
            if self._closing:
                conn.close()
            else:
                self._free.append(conn)
            fut = create_task(self._wakeup(), self._loop)
        return fut

    def get(self):
        warnings.warn("pool.get deprecated use pool.acquire instead",
                      DeprecationWarning,
                      stacklevel=2)
        return _PoolConnectionContextManager(self, None)

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
        return _PoolConnectionContextManager(self, conn)

    if PY_35:  # pragma: no branch
        def __await__(self):
            msg = "with await pool as conn deprecated, use" \
                  "async with pool.acquire() as conn instead"
            warnings.warn(msg, DeprecationWarning, stacklevel=2)
            conn = yield from self.acquire()
            return _PoolConnectionContextManager(self, conn)

        @asyncio.coroutine
        def __aenter__(self):
            return self

        @asyncio.coroutine
        def __aexit__(self, exc_type, exc_val, exc_tb):
            self.close()
            yield from self.wait_closed()
