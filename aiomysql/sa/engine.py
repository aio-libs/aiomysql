# ported from:
# https://github.com/aio-libs/aiopg/blob/master/aiopg/sa/engine.py
import asyncio
from types import TracebackType
from typing import (
    Optional,
    Dict,
    Any,
    MutableMapping,
    Union
)

import aiomysql
from .connection import SAConnection
from .exc import (
    InvalidRequestError,
    ArgumentError
)
from ..connection import (
    Cursor,
    DeserializationCursor,
    DictCursor,
    SSCursor,
    SSDictCursor
)
from ..utils import _ContextManager

try:
    from sqlalchemy.dialects.mysql.pymysql import MySQLDialect_pymysql
    from sqlalchemy.dialects.mysql.mysqldb import MySQLCompiler_mysqldb
except ImportError:  # pragma: no cover
    raise ImportError('aiomysql.sa requires sqlalchemy')


# noinspection PyPep8Naming,PyAbstractClass
class MySQLCompiler_pymysql(MySQLCompiler_mysqldb):
    def construct_params(
            self,
            params: Optional[Dict[str, Any]] = None,
            _group_number: Optional[int] = None,
            _check: bool = True,
            **kwargs: Any
    ) -> MutableMapping[str, Any]:
        pd = super().construct_params(params, _group_number, _check)

        for column in self.prefetch:
            pd[column.key] = self._exec_default(column.default)

        return pd

    def _exec_default(self, default: Any) -> Any:
        if default.is_callable:
            return default.arg(self.dialect)
        else:
            return default.arg


_dialect = MySQLDialect_pymysql(paramstyle='pyformat')
_dialect.statement_compiler = MySQLCompiler_pymysql
_dialect.default_paramstyle = 'pyformat'


def create_engine(
        minsize: int = 1,
        maxsize: int = 10,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        dialect=_dialect,
        pool_recycle: int = -1,
        compiled_cache: Optional[Dict[str, Any]] = None,
        **kwargs: Union[str, int, bool, Any]
):
    """
    A coroutine for Engine creation.

    Returns Engine instance with embedded connection pool.

    The pool has *minsize* opened connections to MySQL server.
    """
    deprecated_cursor_classes = [
        DeserializationCursor, DictCursor, SSCursor, SSDictCursor,
    ]

    cursorclass = kwargs.get('cursorclass', Cursor)
    if not issubclass(cursorclass, Cursor) or any(
            issubclass(cursorclass, cursor_class)
            for cursor_class in deprecated_cursor_classes
    ):
        raise ArgumentError(f"The cursor class '{cursorclass.__name__}' is not supported by the SQLAlchemy engine.")

    coro = _create_engine(minsize=minsize, maxsize=maxsize, loop=loop,
                          dialect=dialect, pool_recycle=pool_recycle,
                          compiled_cache=compiled_cache, **kwargs)
    return _ContextManager(coro, _close_engine)


async def _close_engine(engine: 'Engine') -> None:
    engine.close()
    await engine.wait_closed()


async def _close_connection(c: SAConnection) -> None:
    await c.close()


async def _create_engine(
        minsize: int = 1,
        maxsize: int = 10,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        dialect=_dialect,
        pool_recycle: int = -1,
        compiled_cache: Optional[Dict[str, Any]] = None,
        **kwargs: Any
):
    if loop is None:
        loop = asyncio.get_event_loop()
    pool = await aiomysql.create_pool(minsize=minsize, maxsize=maxsize,
                                      loop=loop,
                                      pool_recycle=pool_recycle, **kwargs)
    conn = await pool.acquire()
    try:
        return Engine(dialect, pool, compiled_cache=compiled_cache, **kwargs)
    finally:
        await pool.release(conn)


class Engine:
    """
    Connects a aiomysql.Pool and
    sqlalchemy.engine.interfaces.Dialect together to provide a
    source of database connectivity and behavior.

    An Engine object is instantiated publicly using the
    create_engine coroutine.
    """

    def __init__(
            self,
            dialect,
            pool: Any,
            compiled_cache: Any = None,
            **kwargs: Any
    ) -> None:
        self._dialect = dialect
        self._pool = pool
        self._compiled_cache = compiled_cache
        self._conn_kw = kwargs

    @property
    def dialect(self):
        """A dialect for engine."""
        return self._dialect

    @property
    def name(self):
        """A name of the dialect."""
        return self._dialect.name

    @property
    def driver(self):
        """A driver of the dialect."""
        return self._dialect.driver

    @property
    def minsize(self) -> int:
        return self._pool.minsize

    @property
    def maxsize(self) -> int:
        return self._pool.maxsize

    @property
    def size(self) -> int:
        return self._pool.size

    @property
    def freesize(self) -> int:
        return self._pool.freesize

    def close(self) -> None:
        """
        Close engine.

        Mark all engine connections to be closed on getting back to pool.
        Closed engine doesn't allow acquiring new connections.
        """
        self._pool.close()

    def terminate(self) -> None:
        """
        Terminate engine.

        Terminate engine pool with instantly closing all acquired
        connections also.
        """
        self._pool.terminate()

    async def wait_closed(self) -> None:
        """Wait for closing all engine's connections."""
        await self._pool.wait_closed()

    def acquire(self) -> _ContextManager:
        """Get a connection from pool."""
        coro = self._acquire()
        return _ContextManager[SAConnection](coro, _close_connection)

    async def _acquire(self) -> SAConnection:
        raw = await self._pool.acquire()
        return SAConnection(raw, self, compiled_cache=self._compiled_cache)

    def release(self, conn: SAConnection):
        """Revert connection to pool."""
        if conn.in_transaction:
            raise InvalidRequestError("Cannot release a connection with "
                                      "not finished transaction")
        return self._pool.release(conn.connection)

    def __enter__(self):
        raise RuntimeError(
            '"await" should be used as context manager expression')

    def __exit__(
            self,
            exc_type: Optional[type],
            exc_val: Optional[BaseException],
            exc_tb: Optional[TracebackType]
    ) -> None:
        # This must exist because __enter__ exists, even though that
        # always raises; that's how the with-statement works.
        pass  # pragma: nocover

    async def __aiter__(self) -> '_ConnectionContextManager':
        # This is not a coroutine. It is meant to enable the idiom:
        #
        #     async with engine as conn:
        #         <block>
        #
        # as an alternative to:
        #
        #     conn = await engine.acquire()
        #     try:
        #         <block>
        #     finally:
        #         engine.release(conn)
        conn = await self.acquire()
        return _ConnectionContextManager(self, conn)

    async def __aenter__(self) -> 'Engine':
        return self

    async def __aexit__(
            self,
            exc_type: Optional[type],
            exc_val: Optional[BaseException],
            exc_tb: Optional[Any]
    ) -> None:
        self.close()
        await self.wait_closed()


class _ConnectionContextManager:
    """
    Context manager.

    This enables the following idiom for acquiring and releasing a
    connection around a block:

        with (yield from engine) as conn:
            cur = yield from conn.cursor()

    while failing loudly when accidentally using:

        with engine:
            <block>
    """

    def __init__(
            self,
            engine: Engine,
            conn: SAConnection
    ):
        self._engine = engine
        self._conn = conn

    async def __aenter__(self) -> SAConnection:
        assert self._conn is not None
        return self._conn

    async def __aexit__(self, *args: Any) -> None:
        try:
            await self._engine.release(self._conn)
        finally:
            self._engine = None
            self._conn = None
