# ported from:
# https://github.com/aio-libs/aiopg/blob/master/aiopg/sa/transaction.py
from types import TracebackType
from typing import (
    Any,
    Optional,
    Type
)

from . import exc


class Transaction(object):
    """Represent a database transaction in progress.

    The Transaction object is procured by
    calling the SAConnection.begin() method of
    SAConnection:

        with (yield from engine) as conn:
            trans = yield from conn.begin()
            try:
                yield from conn.execute("insert into x (a, b) values (1, 2)")
            except Exception:
                yield from trans.rollback()
            else:
                yield from trans.commit()

    The object provides .rollback() and .commit()
    methods in order to control transaction boundaries.

    See also:  SAConnection.begin(), SAConnection.begin_twophase(),
    SAConnection.begin_nested().
    """

    def __init__(
            self,
            connection: Any,
            parent: Optional['Transaction']
    ) -> None:
        self._connection = connection
        self._parent = parent or self
        self._is_active = True

    @property
    def is_active(self) -> bool:
        """Return ``True`` if a transaction is active."""
        return self._is_active

    @property
    def connection(self) -> Any:
        """Return transaction's connection (SAConnection instance)."""
        return self._connection

    async def close(self) -> None:
        """Close this transaction.

        If this transaction is the base transaction in a begin/commit
        nesting, the transaction will rollback().  Otherwise, the
        method returns.

        This is used to cancel a Transaction without affecting the scope of
        an enclosing transaction.
        """
        if not self._parent._is_active:
            return
        if self._parent is self:
            await self.rollback()
        else:
            self._is_active = False

    async def rollback(self) -> None:
        """Roll back this transaction."""
        if not self._parent._is_active:
            return
        await self._do_rollback()
        self._is_active = False

    async def _do_rollback(self) -> None:
        await self._parent.rollback()

    async def commit(self) -> None:
        """Commit this transaction."""

        if not self._parent._is_active:
            raise exc.InvalidRequestError("This transaction is inactive")
        await self._do_commit()
        self._is_active = False

    async def _do_commit(self) -> None:
        pass

    async def __aenter__(self) -> 'Transaction':
        return self

    async def __aexit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[TracebackType]
    ) -> None:
        if exc_type:
            await self.rollback()
        else:
            if self._is_active:
                await self.commit()


class RootTransaction(Transaction):

    def __init__(
            self,
            connection: Any
    ) -> None:
        super().__init__(connection, None)

    async def _do_rollback(self) -> None:
        await self._connection._rollback_impl()

    async def _do_commit(self) -> None:
        await self._connection._commit_impl()


class NestedTransaction(Transaction):
    """Represent a 'nested', or SAVEPOINT transaction.

    A new NestedTransaction object may be procured
    using the SAConnection.begin_nested() method.

    The interface is the same as that of Transaction class.
    """

    _savepoint: Optional[Any] = None

    def __init__(
            self,
            connection: Any,
            parent: Optional['Transaction']
    ):
        super(NestedTransaction, self).__init__(connection, parent)

    async def _do_rollback(self) -> None:
        assert self._savepoint is not None, "Broken transaction logic"
        if self._is_active:
            await self._connection._rollback_to_savepoint_impl(
                self._savepoint, self._parent)

    async def _do_commit(self) -> None:
        assert self._savepoint is not None, "Broken transaction logic"
        if self._is_active:
            await self._connection._release_savepoint_impl(
                self._savepoint, self._parent)


class TwoPhaseTransaction(Transaction):
    """Represent a two-phase transaction.

    A new TwoPhaseTransaction object may be procured
    using the SAConnection.begin_twophase() method.

    The interface is the same as that of Transaction class
    with the addition of the .prepare() method.
    """

    def __init__(
            self,
            connection: Any,
            xid: Any
    ) -> None:
        super().__init__(connection, None)
        self._is_prepared = False
        self._xid = xid

    @property
    def xid(self) -> 'xid':
        """Returns twophase transaction id."""
        return self._xid

    async def prepare(self) -> None:
        """Prepare this TwoPhaseTransaction.

        After a PREPARE, the transaction can be committed.
        """

        if not self._parent.is_active:
            raise exc.InvalidRequestError("This transaction is inactive")
        await self._connection._prepare_twophase_impl(self._xid)
        self._is_prepared = True

    async def _do_rollback(self) -> None:
        await self._connection.rollback_prepared(
            self._xid, is_prepared=self._is_prepared)

    async def _do_commit(self) -> None:
        await self._connection.commit_prepared(
            self._xid, is_prepared=self._is_prepared)
