# based on aiopg pool
# https://github.com/aio-libs/aiopg/blob/master/aiopg/pool.py

import asyncio
import collections
import warnings
from types import TracebackType
from typing import (
    Optional,
    Any,
    Deque,
    Type
)

from aiomysql.connection import (
    connect,
    Connection
)
from aiomysql.utils import (
    _ContextManager
)


# todo: Update Any to stricter kwarg
# https://github.com/python/mypy/issues/4441
def create_pool(
        minsize: int = 1,
        maxsize: int = 10,
        echo: bool = False,
        pool_recycle: int = -1,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        **kwargs: Any) -> _ContextManager["Pool"]:
    coro = _create_pool(minsize=minsize, maxsize=maxsize, echo=echo,
                        pool_recycle=pool_recycle, loop=loop, **kwargs)
    return _ContextManager[Pool](coro, _destroy_pool)


async def _destroy_pool(pool: "Pool") -> None:
    pool.close()
    await pool.wait_closed()


# todo: Update Any to stricter kwarg
# https://github.com/python/mypy/issues/4441
async def _create_pool(
        minsize: int = 1,
        maxsize: int = 10,
        echo: bool = False,
        pool_recycle: int = -1,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        **kwargs: Any
) -> 'Pool':
    if loop is None:
        loop = asyncio.get_event_loop()

    pool: Pool = Pool(minsize=minsize, maxsize=maxsize, echo=echo,
                      pool_recycle=pool_recycle, loop=loop, **kwargs)

    if minsize > 0:
        async with pool._cond:
            await pool._fill_free_pool(False)

    return pool


class Pool(asyncio.AbstractServer):
    """Connection pool"""

    def __init__(
            self,
            minsize: int,
            maxsize: int,
            echo: bool,
            pool_recycle: int,
            loop: asyncio.AbstractEventLoop,
            **kwargs: Any
    ) -> None:
        if minsize < 0:
            raise ValueError("minsize should be zero or greater")
        if maxsize < minsize and maxsize != 0:
            raise ValueError("maxsize should be not less than minsize")
        self._minsize: int = minsize
        self._loop: asyncio.AbstractEventLoop = loop
        self._conn_kwargs: dict[str, Any] = kwargs
        self._acquiring: int = 0
        self._free: Deque[Any] = collections.deque(maxlen=maxsize or None)
        self._cond: asyncio.Condition = asyncio.Condition()
        self._used: set[Any] = set()
        self._terminated: set[Any] = set()
        self._closing: bool = False
        self._closed: bool = False
        self._echo: bool = echo
        self._recycle: int = pool_recycle

    @property
    def echo(self) -> bool:
        return self._echo

    @property
    def minsize(self) -> int:
        return self._minsize

    @property
    def maxsize(self) -> int:
        return self._free.maxlen

    @property
    def size(self) -> int:
        return self.freesize + len(self._used) + self._acquiring

    @property
    def freesize(self) -> int:
        return len(self._free)

    async def clear(self) -> None:
        """Close all free connections in pool."""
        async with self._cond:
            while self._free:
                conn = self._free.popleft()
                await conn.ensure_closed()
            self._cond.notify()

    @property
    def closed(self) -> bool:
        """
        The readonly property that returns ``True`` if connections is closed.
        """
        return self._closed

    def close(self) -> None:
        """Close pool.

        Mark all pool connections to be closed on getting back to pool.
        Closed pool doesn't allow acquiring new connections.
        """
        if self._closed:
            return
        self._closing = True

    def terminate(self) -> None:
        """Terminate pool.

        Close pool with instantly closing all acquired connections also.
        """
        self.close()

        for conn in list(self._used):
            conn.close()
            self._terminated.add(conn)

        self._used.clear()

    async def wait_closed(self) -> None:
        """Wait for closing all pool's connections."""
        if self._closed:
            return
        if not self._closing:
            raise RuntimeError(".wait_closed() should be called "
                               "after .close()")

        while self._free:
            conn = self._free.popleft()
            conn.close()

        async with self._cond:
            while self.size > self.freesize:
                await self._cond.wait()

        self._closed = True

    async def acquire(self) -> _ContextManager:
        """Acquire free connection from the pool."""
        coro = self._acquire()
        return _ContextManager[Connection](coro, self.release)

    async def _acquire(self) -> Connection:
        if self._closing:
            raise RuntimeError("Cannot acquire connection after closing pool")
        async with self._cond:
            while True:
                await self._fill_free_pool(True)
                if self._free:
                    conn = self._free.popleft()
                    assert not conn.closed, conn
                    assert conn not in self._used, (conn, self._used)
                    self._used.add(conn)
                    return conn
                else:
                    await self._cond.wait()

    async def _fill_free_pool(self, override_min: bool) -> None:
        # iterate over free connections and remove timed out ones
        free_size = len(self._free)
        n = 0
        while n < free_size:
            conn = self._free[-1]
            if conn._reader.at_eof() or conn._reader.exception():
                self._free.pop()
                conn.close()

            # On MySQL 8.0 a timed out connection sends an error packet before
            # closing the connection, preventing us from relying on at_eof().
            # This relies on our custom StreamReader, as eof_received is not
            # present in asyncio.StreamReader.
            elif conn._reader.eof_received:
                self._free.pop()
                conn.close()

            elif -1 < self._recycle < self._loop.time() - conn.last_usage:
                self._free.pop()
                conn.close()

            else:
                self._free.rotate()
            n += 1

        while self.size < self.minsize:
            self._acquiring += 1
            try:
                conn = await connect(echo=self._echo, loop=self._loop,
                                     **self._conn_kwargs)
                # raise exception if pool is closing
                self._free.append(conn)
                self._cond.notify()
            finally:
                self._acquiring -= 1
        if self._free:
            return

        if override_min and (not self.maxsize or self.size < self.maxsize):
            self._acquiring += 1
            try:
                conn = await connect(echo=self._echo, loop=self._loop,
                                     **self._conn_kwargs)
                # raise exception if pool is closing
                self._free.append(conn)
                self._cond.notify()
            finally:
                self._acquiring -= 1

    async def _wakeup(self) -> None:
        async with self._cond:
            self._cond.notify()

    def release(self, conn: Any) -> asyncio.Future:
        """Release free connection back to the connection pool.

        This is **NOT** a coroutine.
        """
        fut = self._loop.create_future()
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
            fut = self._loop.create_task(self._wakeup())
        return fut

    def __enter__(self) -> None:
        raise RuntimeError(
            '"yield from" should be used as context manager expression')

    # todo: Update Any to stricter kwarg
    # https://github.com/python/mypy/issues/4441
    def __exit__(self, *args: Any) -> None:
        # This must exist because __enter__ exists, even though that
        # always raises; that's how the with-statement works.
        pass  # pragma: nocover

    def __iter__(self) -> _ContextManager:
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
        return _ContextManager[Connection](conn, self.release)

    def __await__(self) -> _ContextManager:
        msg = "with await pool as conn deprecated, use" \
              "async with pool.acquire() as conn instead"
        warnings.warn(msg, DeprecationWarning, stacklevel=2)
        conn = yield from self.acquire()
        return _ContextManager[Connection](conn, self.release)

    async def __aenter__(self) -> 'Pool':
        return self

    async def __aexit__(self, exc_type: Optional[Type[BaseException]],
                        exc_val: Optional[BaseException],
                        exc_tb: Optional[TracebackType]) -> None:
        self.close()
        await self.wait_closed()
