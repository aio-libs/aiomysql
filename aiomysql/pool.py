# based on aiopg pool
# https://github.com/aio-libs/aiopg/blob/master/aiopg/pool.py

import asyncio
import collections
import sys
import warnings

from pymysql import OperationalError

from .log import logger
from .connection import connect
from .utils import (_PoolContextManager, _PoolConnectionContextManager,
                    _PoolAcquireContextManager, TaskTransactionContextManager)


def create_pool(minsize=1, maxsize=10, echo=False, pool_recycle=-1,
                loop=None, **kwargs):
    coro = _create_pool(minsize=minsize, maxsize=maxsize, echo=echo,
                        pool_recycle=pool_recycle, loop=loop, **kwargs)
    return _PoolContextManager(coro)


async def _create_pool(minsize=1, maxsize=10, echo=False, pool_recycle=-1,
                       loop=None, **kwargs):
    if loop is None:
        loop = asyncio.get_event_loop()

    pool = Pool(minsize=minsize, maxsize=maxsize, echo=echo,
                pool_recycle=pool_recycle, loop=loop, **kwargs)
    if minsize > 0:
        async with pool._cond:
            await pool._fill_free_pool(False)
    return pool


class Pool(asyncio.AbstractServer):
    """Connection pool"""

    def __init__(self, minsize, maxsize, echo, pool_recycle, loop, **kwargs):
        if minsize < 0:
            raise ValueError("minsize should be zero or greater")
        if maxsize < minsize and maxsize != 0:
            raise ValueError("maxsize should be not less than minsize")
        self._minsize = minsize
        self._loop = loop
        self._conn_kwargs = kwargs
        self._acquiring = 0
        self._free = collections.deque(maxlen=maxsize or None)
        self._cond = asyncio.Condition()
        self._used = set()
        self._terminated = set()
        self._closing = False
        self._closed = False
        self._echo = echo
        self._recycle = pool_recycle
        self._db = kwargs.get('db')

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

    @property
    def db_name(self):
        return self._db

    async def clear(self):
        """Close all free connections in pool."""
        async with self._cond:
            while self._free:
                conn = self._free.popleft()
                await conn.ensure_closed()
            self._cond.notify()

    @property
    def closed(self):
        """
        The readonly property that returns ``True`` if connections is closed.
        """
        return self._closed

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

    async def wait_closed(self):
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

    def acquire(self):
        """Acquire free connection from the pool."""
        if sys.version_info < (3, 7):
            o_transaction_context_manager = TaskTransactionContextManager.get_transaction_context_manager(asyncio.Task.current_task())

        else:
            o_transaction_context_manager = TaskTransactionContextManager.get_transaction_context_manager(asyncio.current_task())

        if o_transaction_context_manager:
            return o_transaction_context_manager

        coro = self._acquire()
        return _PoolAcquireContextManager(coro, self)

    def acquire_with_transaction(self):
        """Acquire free connection from the pool for transaction"""
        if sys.version_info < (3, 7):
            o_transaction_context_manager = TaskTransactionContextManager.get_transaction_context_manager(asyncio.Task.current_task())

        else:
            o_transaction_context_manager = TaskTransactionContextManager.get_transaction_context_manager(asyncio.current_task())

        if o_transaction_context_manager:
            return o_transaction_context_manager

        coro = self._acquire()
        return TaskTransactionContextManager(coro, self)

    async def _acquire(self):
        if self._closing:
            raise RuntimeError("Cannot acquire connection after closing pool")
        async with self._cond:
            while True:
                await self._fill_free_pool(True)
                if self._free:
                    conn = self._free.popleft()
                    # assert not conn.closed, conn
                    # assert conn not in self._used, (conn, self._used)
                    self._used.add(conn)
                    return conn
                else:
                    logger.debug('%s - All connections (%d) are busy. Waiting for release connection', self._db, self.freesize)
                    await self._cond.wait()

    async def _fill_free_pool(self, override_min):
        # iterate over free connections and remove timed out ones
        free_size = len(self._free)
        n = 0
        while n < free_size:
            conn = self._free[-1]
            if conn._reader.at_eof() or conn._reader.exception():
                logger.debug('%s - Connection (%d) is removed from pool because of at_eof or exception', self._db, id(conn))
                self._free.pop()
                conn.close()

            # On MySQL 8.0 a timed out connection sends an error packet before
            # closing the connection, preventing us from relying on at_eof().
            # This relies on our custom StreamReader, as eof_received is not
            # present in asyncio.StreamReader.
            elif conn._reader.eof_received:
                self._free.pop()
                conn.close()

            elif self._recycle > -1 and self._loop.time() - conn.last_usage > self._recycle:
                logger.debug('%s - Connection (%d) is removed from pool because of recycle time %d', self._db, id(conn), self._recycle)
                self._free.pop()
                conn.close()

            else:
                self._free.rotate()

            n += 1

        while self.size < self.minsize:
            await self.__create_new_connection()

        if self._free:
            return

        if override_min and (not self.maxsize or self.size < self.maxsize):
            await self.__create_new_connection()

    async def __create_new_connection(self):
        logger.debug('%s - Try to create new connection', self._db)
        self._acquiring += 1
        try:
            try:
                conn = await connect(echo=self._echo, loop=self._loop, **self._conn_kwargs)

            except OperationalError as error:
                logger.error(error)
                sleep_time_list = [3] * 20
                for attempt, sleep_time in enumerate(sleep_time_list):
                    try:
                        logger.warning('%s - Connect to MySQL failed. Attempt %d of 20', self._db, attempt + 1)
                        conn = await connect(echo=self._echo, loop=self._loop, **self._conn_kwargs)
                        logger.info('%s - Successfully connect to MySQL after error', self._db)
                        break

                    except OperationalError as e:
                        logger.error(e)
                        await asyncio.sleep(sleep_time)

                else:
                    logger.error('%s - Connect to MySQL failed', self._db)
                    raise error

            # raise exception if pool is closing
            self._free.append(conn)
            self._cond.notify()

        finally:
            self._acquiring -= 1

    async def _wakeup(self):
        async with self._cond:
            self._cond.notify()

    def release(self, conn):
        """Release free connection back to the connection pool.

        This is **NOT** a coroutine.
        """
        fut = self._loop.create_future()
        fut.set_result(None)

        if conn in self._terminated:
            # assert conn.closed, conn
            self._terminated.remove(conn)
            return fut
        # assert conn in self._used, (conn, self._used)
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

    def __await__(self):
        msg = "with await pool as conn deprecated, use" \
              "async with pool.acquire() as conn instead"
        warnings.warn(msg, DeprecationWarning, stacklevel=2)
        conn = yield from self.acquire()
        return _PoolConnectionContextManager(self, conn)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()
        await self.wait_closed()
