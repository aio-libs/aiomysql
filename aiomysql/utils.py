import asyncio
import sys
from collections.abc import Coroutine

import struct

from .log import logger


def _pack_int24(n):
    return struct.pack("<I", n)[:3]


def _lenenc_int(i):
    if i < 0:
        raise ValueError(
            "Encoding %d is less than 0 - no representation in LengthEncodedInteger" % i
        )
    elif i < 0xFB:
        return bytes([i])
    elif i < (1 << 16):
        return b"\xfc" + struct.pack("<H", i)
    elif i < (1 << 24):
        return b"\xfd" + struct.pack("<I", i)[:3]
    elif i < (1 << 64):
        return b"\xfe" + struct.pack("<Q", i)
    else:
        raise ValueError(
            "Encoding %x is larger than %x - no representation in LengthEncodedInteger"
            % (i, (1 << 64))
        )


class _ContextManager(Coroutine):
    __slots__ = ('_coro', '_obj')

    def __init__(self, coro):
        self._coro = coro
        self._obj = None

    def send(self, value):
        return self._coro.send(value)

    def throw(self, typ, val=None, tb=None):
        if val is None:
            return self._coro.throw(typ)
        elif tb is None:
            return self._coro.throw(typ, val)
        else:
            return self._coro.throw(typ, val, tb)

    def close(self):
        return self._coro.close()

    @property
    def gi_frame(self):
        return self._coro.gi_frame

    @property
    def gi_running(self):
        return self._coro.gi_running

    @property
    def gi_code(self):
        return self._coro.gi_code

    def __next__(self):
        return self.send(None)

    def __iter__(self):
        return self._coro.__await__()

    def __await__(self):
        return self._coro.__await__()

    async def __aenter__(self):
        self._obj = await self._coro
        return self._obj

    async def __aexit__(self, exc_type, exc, tb):
        await self._obj.close()
        self._obj = None


class _ConnectionContextManager(_ContextManager):
    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is not None:
            self._obj.close()
        else:
            await self._obj.ensure_closed()
        self._obj = None


class _PoolContextManager(_ContextManager):
    async def __aexit__(self, exc_type, exc, tb):
        self._obj.close()
        await self._obj.wait_closed()
        self._obj = None


class _SAConnectionContextManager(_ContextManager):
    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._obj is None:
            self._obj = await self._coro

        try:
            return await self._obj.__anext__()
        except StopAsyncIteration:
            await self._obj.close()
            self._obj = None
            raise


class _TransactionContextManager(_ContextManager):
    async def __aexit__(self, exc_type, exc, tb):
        if exc_type:
            await self._obj.rollback()
        else:
            if self._obj.is_active:
                await self._obj.commit()
        self._obj = None


class _PoolAcquireContextManager(_ContextManager):
    __slots__ = ('_coro', '_conn', '_pool')

    def __init__(self, coro, pool):
        self._coro = coro
        self._conn = None
        self._pool = pool

    async def __aenter__(self):
        self._conn = await self._coro
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        try:
            await self._pool.release(self._conn)
        finally:
            self._pool = None
            self._conn = None


class _PoolConnectionContextManager:
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
        assert self._conn
        return self._conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self._pool.release(self._conn)
        finally:
            self._pool = None
            self._conn = None

    async def __aenter__(self):
        assert not self._conn
        self._conn = await self._pool.acquire()
        return self._conn

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            await self._pool.release(self._conn)
        finally:
            self._pool = None
            self._conn = None


class TaskTransactionContextManager:
    __task_storage = dict()
    __slots__ = ('_coro', '_conn', '_pool', '_counter', '_callback_list', '__connection_transaction_begin', '__connection_committed', '__connection_rollbacked')

    def __init__(self, coro, pool):
        self._coro = coro
        self._conn = None
        self._pool = pool
        self._counter = 0
        self._callback_list = list()
        if sys.version_info < (3, 7):
            task = asyncio.Task.current_task()

        else:
            task = asyncio.current_task()

        if task in self.__task_storage:
            raise Exception('task already in task_storage')

        self.__task_storage[task] = self
        self.__connection_transaction_begin = False
        self.__connection_committed = False
        self.__connection_rollbacked = False

    @classmethod
    def create(cls, coro, pool) -> 'TaskTransactionContextManager':
        if sys.version_info < (3, 7):
            task = asyncio.Task.current_task()

        else:
            task = asyncio.current_task()

        if task in cls.__task_storage:
            return cls.__task_storage[task]

        return TaskTransactionContextManager(coro, pool)

    @classmethod
    def get_transaction_context_manager(cls, task=None) -> 'TaskTransactionContextManager':
        if not task:
            if sys.version_info < (3, 7):
                task = asyncio.Task.current_task()

            else:
                task = asyncio.current_task()

        return cls.__task_storage.get(task)

    def add_callback_on_commit(self, callback_func, **kwargs):
        self._callback_list.append((callback_func, kwargs))

    async def connection_begin(self):
        if not self.__connection_transaction_begin:
            await self._conn.begin()
            self.__connection_transaction_begin = True

    async def connection_commit(self):
        if self._counter <= 1:
            await self._conn.commit()
            for callback_func, kwargs in self._callback_list:
                try:
                    if asyncio.iscoroutine(callback_func):
                        await callback_func(**kwargs)

                    else:
                        callback_func(**kwargs)

                except Exception as e:
                    logger.exception(e)

            self._callback_list.clear()
            self.__connection_committed = True

    async def connection_rollback(self):
        await self._conn.rollback()
        self._callback_list.clear()
        self.__connection_rollbacked = True

    async def __aenter__(self):
        self._counter += 1
        if not self._conn:
            self._conn = await self._coro

        return TransactionConnection(self._conn)

    async def __aexit__(self, exc_type, exc, tb):
        self._counter -= 1
        if self._counter <= 0:
            if self.__connection_transaction_begin and not self.__connection_committed and not self.__connection_rollbacked:
                if not exc_type:
                    logger.warning('sql operation was not committed. Try to commit by TaskTransactionContextManager')
                    await self.connection_commit()

            if sys.version_info < (3, 7):
                self.__task_storage.pop(asyncio.Task.current_task(), None)

            else:
                self.__task_storage.pop(asyncio.current_task(), None)

            try:
                await self._pool.release(self._conn)

            finally:
                self._pool = None
                self._conn = None
                self.__connection_transaction_begin = False
                self.__connection_committed = False
                self.__connection_rollbacked = False


class TransactionConnection:

    def __init__(self, conn):
        self.__conn = conn

    def __str__(self):
        return 'TransactionConnection ' + str(self.__conn)

    async def begin(self):
        """Begin transaction."""
        if TaskTransactionContextManager.get_transaction_context_manager():
            await TaskTransactionContextManager.get_transaction_context_manager().connection_begin()

        else:
            await self.__conn.begin()

    async def commit(self):
        """Commit changes to stable storage."""
        if TaskTransactionContextManager.get_transaction_context_manager():
            await TaskTransactionContextManager.get_transaction_context_manager().connection_commit()

        else:
            await self.__conn.commit()

    async def rollback(self):
        """Roll back the current transaction."""
        if TaskTransactionContextManager.get_transaction_context_manager():
            await TaskTransactionContextManager.get_transaction_context_manager().connection_rollback()

        else:
            await self.__conn.rollback()

    def __getattr__(self, item):
        return getattr(self.__conn, item)

    @property
    def conn(self):
        return self.__conn
