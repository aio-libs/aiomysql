# ported from:
# https://github.com/aio-libs/aiopg/blob/master/aiopg/sa/connection.py
import weakref
from types import TracebackType
from typing import (
    Any,
    Dict,
    Union,
    List,
    Tuple,
    Optional,
    Type
)

from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.ddl import DDLElement
from sqlalchemy.sql.dml import UpdateBase

from . import exc, Engine
from .result import create_result_proxy, ResultProxy
from .transaction import (
    RootTransaction,
    Transaction,
    NestedTransaction,
    TwoPhaseTransaction
)
from .. import Cursor, Connection
from ..utils import _IterableContextManager


async def _commit_transaction_if_active(t: Transaction) -> None:
    if t.is_active:
        await t.commit()


async def _rollback_transaction(t: Transaction) -> None:
    await t.rollback()


async def _close_result_proxy(c: 'ResultProxy') -> None:
    await c.close()


class SAConnection:

    def __init__(
            self,
            connection: Connection,
            engine: Engine,
            compiled_cache: Optional[Any] = None,
    ) -> None:
        self._connection = connection
        self._transaction = None
        self._savepoint_seq = 0
        self._weak_results = weakref.WeakSet()
        self._engine = engine
        self._dialect = engine.dialect
        self._compiled_cache = compiled_cache

    # todo: Update Any to stricter kwarg
    # https://github.com/python/mypy/issues/4441
    def execute(
            self,
            query: Union[str, DDLElement, ClauseElement],
            *multiparams: Any,
            **params: Any
    ) -> _IterableContextManager:
        """Executes a SQL query with optional parameters.

        query - a SQL query string or any sqlalchemy expression.

        *multiparams/**params - represent bound parameter values to be
        used in the execution.  Typically, the format is a dictionary
        passed to *multiparams:

            await conn.execute(
                table.insert(),
                {"id":1, "value":"v1"},
            )

        ...or individual key/values interpreted by **params::

            await conn.execute(
                table.insert(), id=1, value="v1"
            )

        In the case that a plain SQL string is passed, a tuple or
        individual values in *multiparams may be passed::

            await conn.execute(
                "INSERT INTO table (id, value) VALUES (%d, %s)",
                (1, "v1")
            )

            await conn.execute(
                "INSERT INTO table (id, value) VALUES (%s, %s)",
                1, "v1"
            )

        Returns ResultProxy instance with results of SQL query
        execution.

        """
        coro = self._execute(query, *multiparams, **params)
        return _IterableContextManager[ResultProxy](coro, _close_result_proxy)

    def _base_params(
            self,
            query: Union[str, DDLElement, ClauseElement],
            dp: Any,
            compiled: Any,
            is_update: bool,
    ) -> Dict[str, Any]:
        """
        handle params
        """
        if dp and isinstance(dp, (list, tuple)):
            if is_update:
                dp = {c.key: pval for c, pval in zip(query.table.c, dp)}
            else:
                raise exc.ArgumentError(
                    "Don't mix sqlalchemy SELECT "
                    "clause with positional "
                    "parameters"
                )
        compiled_params = compiled.construct_params(dp)
        processors = compiled._bind_processors
        params = [{
            key: processors.get(key, lambda val: val)(compiled_params[key])
            for key in compiled_params
        }]
        post_processed_params = self._dialect.execute_sequence_format(params)
        return post_processed_params[0]

    async def _executemany(
            self,
            query: Union[str, DDLElement, ClauseElement],
            dps: Any,
            cursor: Cursor,
    ) -> ResultProxy:
        """
        executemany
        """
        result_map: Optional[Dict[str, Any]] = None
        if isinstance(query, str):
            await cursor.executemany(query, dps)
        elif isinstance(query, DDLElement):
            raise exc.ArgumentError(
                "Don't mix sqlalchemy DDL clause "
                "and execution with parameters"
            )
        elif isinstance(query, ClauseElement):
            compiled = query.compile(dialect=self._dialect)
            params = []
            is_update = isinstance(query, UpdateBase)
            for dp in dps:
                params.append(
                    self._base_params(
                        query,
                        dp,
                        compiled,
                        is_update,
                    )
                )
            await cursor.executemany(str(compiled), params)
            result_map = compiled._result_columns
        else:
            raise exc.ArgumentError(
                "sql statement should be str or "
                "SQLAlchemy data "
                "selection/modification clause"
            )
        ret = await create_result_proxy(
            self,
            cursor,
            self._dialect,
            result_map
        )
        self._weak_results.add(ret)
        return ret

    # todo: Update Any to stricter kwarg
    # https://github.com/python/mypy/issues/4441
    async def _execute(
            self,
            query: Union[str, DDLElement, ClauseElement],
            *multiparams: Any,
            **params: Any
    ):
        cursor = await self._connection.cursor()
        dp = _distill_params(multiparams, params)
        if len(dp) > 1:
            return await self._executemany(query, dp, cursor)
        elif dp:
            dp = dp[0]

        result_map = None
        if isinstance(query, str):
            await cursor.execute(query, dp or None)
        elif isinstance(query, ClauseElement):
            if self._compiled_cache is not None:
                key = query
                compiled = self._compiled_cache.get(key)
                if not compiled:
                    compiled = query.compile(dialect=self._dialect)
                    if dp and dp.keys() == compiled.params.keys() \
                            or not (dp or compiled.params):
                        # we only want queries with bound params in cache
                        self._compiled_cache[key] = compiled
            else:
                compiled = query.compile(dialect=self._dialect)

            if not isinstance(query, DDLElement):
                post_processed_params = self._base_params(
                    query,
                    dp,
                    compiled,
                    isinstance(query, UpdateBase)
                )
                result_map = compiled._result_columns
            else:
                if dp:
                    raise exc.ArgumentError("Don't mix sqlalchemy DDL clause "
                                            "and execution with parameters")
                post_processed_params = compiled.construct_params()
                result_map = None
            await cursor.execute(str(compiled), post_processed_params)
        else:
            raise exc.ArgumentError("sql statement should be str or "
                                    "SQLAlchemy data "
                                    "selection/modification clause")

        ret = await create_result_proxy(
            self, cursor, self._dialect, result_map
        )
        self._weak_results.add(ret)
        return ret

    # todo: Update Any to stricter kwarg
    # https://github.com/python/mypy/issues/4441
    async def scalar(
            self,
            query: Union[str, DDLElement, ClauseElement],
            *multiparams: Any,
            **params: Any
    ) -> Optional[Any]:
        """Executes a SQL query and returns a scalar value."""
        res = await self.execute(query, *multiparams, **params)
        return await res.scalar()

    @property
    def closed(self):
        """The readonly property that returns True if connections is closed."""
        return self._connection is None or self._connection.closed

    @property
    def connection(self):
        return self._connection

    def begin(self):
        """Begin a transaction and return a transaction handle.

        The returned object is an instance of Transaction.  This
        object represents the "scope" of the transaction, which
        completes when either the .rollback or .commit method is
        called.

        Nested calls to .begin on the same SAConnection instance will
        return new Transaction objects that represent an emulated
        transaction within the scope of the enclosing transaction,
        that is::

            trans = await conn.begin()   # outermost transaction
            trans2 = await conn.begin()  # "nested"
            await trans2.commit()        # does nothing
            await trans.commit()         # actually commits

        Calls to .commit only have an effect when invoked via the
        outermost Transaction object, though the .rollback method of
        the Transaction objects will roll back the transaction.

        See also:
          .begin_nested - use a SAVEPOINT
          .begin_twophase - use a two phase/XA transaction

        """
        coro = self._begin()
        return _IterableContextManager(coro)

    async def _begin(self) -> 'Transaction':
        if self._transaction is None:
            self._transaction = RootTransaction(self)
            await self._begin_impl()
            return self._transaction
        else:
            return Transaction(self, self._transaction)

    async def _begin_impl(self) -> None:
        cur = await self._connection.cursor()
        try:
            await cur.execute('BEGIN')
        finally:
            await cur.close()

    async def _commit_impl(self) -> None:
        cur = await self._connection.cursor()
        try:
            await cur.execute('COMMIT')
        finally:
            await cur.close()
            self._transaction = None

    async def _rollback_impl(self) -> None:
        cur = await self._connection.cursor()
        try:
            await cur.execute('ROLLBACK')
        finally:
            await cur.close()
            self._transaction = None

    async def begin_nested(self) -> 'Transaction':
        """Begin a nested transaction and return a transaction handle.

        The returned object is an instance of :class:`.NestedTransaction`.

        Nested transactions require SAVEPOINT support in the
        underlying database.  Any transaction in the hierarchy may
        .commit() and .rollback(), however the outermost transaction
        still controls the overall .commit() or .rollback() of the
        transaction of a whole.
        """
        if self._transaction is None:
            self._transaction = RootTransaction(self)
            await self._begin_impl()
        else:
            self._transaction = NestedTransaction(self, self._transaction)
            self._transaction._savepoint = await self._savepoint_impl()
        return self._transaction

    async def _savepoint_impl(self, name: Optional[str] = None) -> str:
        self._savepoint_seq += 1
        name = f'aiomysql_sa_savepoint_{self._savepoint_seq}'

        cur = await self._connection.cursor()
        try:
            await cur.execute(f'SAVEPOINT {name}')
            return name
        finally:
            await cur.close()

    async def _rollback_to_savepoint_impl(self, name: str, parent: Optional[Transaction] = None) -> None:
        cur = await self._connection.cursor()
        try:
            await cur.execute(f'ROLLBACK TO SAVEPOINT {name}')
        finally:
            await cur.close()
        self._transaction = parent

    async def _release_savepoint_impl(self, name: str, parent: Optional[Transaction] = None) -> None:
        cur = await self._connection.cursor()
        try:
            await cur.execute(f'RELEASE SAVEPOINT {name}')
        finally:
            await cur.close()
        self._transaction = parent

    async def begin_twophase(self, xid: Optional[str] = None) -> TwoPhaseTransaction:
        """Begin a two-phase or XA transaction and return a transaction
        handle.

        The returned object is an instance of
        TwoPhaseTransaction, which in addition to the
        methods provided by Transaction, also provides a
        TwoPhaseTransaction.prepare() method.

        xid - the two phase transaction id.  If not supplied, a
        random id will be generated.
        """

        if self._transaction is not None:
            raise exc.InvalidRequestError(
                "Cannot start a two phase transaction when a transaction "
                "is already in progress.")
        if xid is None:
            xid = self._dialect.create_xid()
        self._transaction = TwoPhaseTransaction(self, xid)
        await self.execute(f"XA START {xid}")
        return self._transaction

    async def _prepare_twophase_impl(self, xid: str) -> None:
        await self.execute(f"XA END '{xid}'")
        await self.execute(f"XA PREPARE '{xid}'")

    async def recover_twophase(self) -> List[str]:
        """Return a list of prepared twophase transaction ids."""
        result = await self.execute("XA RECOVER;")
        return [row[0] for row in result]

    async def rollback_prepared(self, xid: str, *, is_prepared: bool = True) -> None:
        """Rollback prepared twophase transaction."""
        if not is_prepared:
            await self.execute(f"XA END '{xid}'")
        await self.execute(f"XA ROLLBACK '{xid}'")

    async def commit_prepared(self, xid: str, *, is_prepared: bool = True) -> None:
        """Commit prepared twophase transaction."""
        if not is_prepared:
            await self.execute(f"XA END '{xid}'")
        await self.execute(f"XA COMMIT '{xid}'")

    @property
    def in_transaction(self) -> bool:
        """Return True if a transaction is in progress."""
        return self._transaction is not None and self._transaction.is_active

    async def close(self) -> None:
        """Close this SAConnection.

        This results in a release of the underlying database
        resources, that is, the underlying connection referenced
        internally. The underlying connection is typically restored
        back to the connection-holding Pool referenced by the Engine
        that produced this SAConnection. Any transactional state
        present on the underlying connection is also unconditionally
        released via calling Transaction.rollback() method.

        After .close() is called, the SAConnection is permanently in a
        closed state, and will allow no further operations.
        """
        if self._connection is None:
            return

        if self._transaction is not None:
            await self._transaction.rollback()
            self._transaction = None
        # don't close underlying connection, it can be reused by pool
        # conn.close()
        self._engine.release(self)
        self._connection = None
        self._engine = None

    async def __aenter__(self) -> 'SAConnection':
        return self

    async def __aexit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[TracebackType],
    ) -> None:
        await self.close()


def _distill_params(multiparams: Any, params: Any) -> List[Union[Dict[str, Any], List[Tuple[Any, ...]]]]:
    """Given arguments from the calling form *multiparams, **params,
    return a list of bind parameter structures, usually a list of
    dictionaries.

    In the case of 'raw' execution which accepts positional parameters,
    it may be a list of tuples or lists.

    """

    if not multiparams:
        if params:
            return [params]
        else:
            return []
    elif len(multiparams) == 1:
        zero = multiparams[0]
        if isinstance(zero, (list, tuple)):
            if not zero or hasattr(zero[0], '__iter__') and \
                    not hasattr(zero[0], 'strip'):
                # execute(stmt, [{}, {}, {}, ...])
                # execute(stmt, [(), (), (), ...])
                return zero
            else:
                # execute(stmt, ("value", "value"))
                return [zero]
        elif hasattr(zero, 'keys'):
            # execute(stmt, {"key":"value"})
            return [zero]
        else:
            # execute(stmt, "value")
            return [[zero]]
    else:
        if hasattr(multiparams[0], '__iter__') and not hasattr(multiparams[0], 'strip'):
            return multiparams
        else:
            return [multiparams]
