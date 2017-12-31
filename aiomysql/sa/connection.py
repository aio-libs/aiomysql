# ported from:
# https://github.com/aio-libs/aiopg/blob/master/aiopg/sa/connection.py
import asyncio
import weakref

from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.dml import UpdateBase
from sqlalchemy.sql.ddl import DDLElement

from . import exc
from .result import create_result_proxy
from .transaction import (RootTransaction, Transaction,
                          NestedTransaction, TwoPhaseTransaction)
from ..utils import _TransactionContextManager, _SAConnectionContextManager


class SAConnection:

    def __init__(self, connection, engine):
        self._connection = connection
        self._transaction = None
        self._savepoint_seq = 0
        self._weak_results = weakref.WeakSet()
        self._engine = engine
        self._dialect = engine.dialect

    def execute(self, query, *multiparams, **params):
        """Executes a SQL query with optional parameters.

        query - a SQL query string or any sqlalchemy expression.

        *multiparams/**params - represent bound parameter values to be
        used in the execution.  Typically, the format is a dictionary
        passed to *multiparams:

            yield from conn.execute(
                table.insert(),
                {"id":1, "value":"v1"},
            )

        ...or individual key/values interpreted by **params::

            yield from conn.execute(
                table.insert(), id=1, value="v1"
            )

        In the case that a plain SQL string is passed, a tuple or
        individual values in \*multiparams may be passed::

            yield from conn.execute(
                "INSERT INTO table (id, value) VALUES (%d, %s)",
                (1, "v1")
            )

            yield from conn.execute(
                "INSERT INTO table (id, value) VALUES (%s, %s)",
                1, "v1"
            )

        Returns ResultProxy instance with results of SQL query
        execution.

        """
        coro = self._execute(query, *multiparams, **params)
        return _SAConnectionContextManager(coro)

    @asyncio.coroutine
    def _execute(self, query, *multiparams, **params):
        cursor = yield from self._connection.cursor()
        dp = _distill_params(multiparams, params)
        if len(dp) > 1:
            raise exc.ArgumentError("aiomysql doesn't support executemany")
        elif dp:
            dp = dp[0]

        if isinstance(query, str):
            yield from cursor.execute(query, dp or None)
        elif isinstance(query, ClauseElement):
            compiled = query.compile(dialect=self._dialect)
            # parameters = compiled.params
            if not isinstance(query, DDLElement):
                if dp and isinstance(dp, (list, tuple)):
                    if isinstance(query, UpdateBase):
                        dp = {c.key: pval
                              for c, pval in zip(query.table.c, dp)}
                    else:
                        raise exc.ArgumentError("Don't mix sqlalchemy SELECT "
                                                "clause with positional "
                                                "parameters")
                compiled_parameters = [compiled.construct_params(
                    dp)]
                processed_parameters = []
                processors = compiled._bind_processors
                for compiled_params in compiled_parameters:
                    params = {key: (processors[key](compiled_params[key])
                                    if key in processors
                                    else compiled_params[key])
                              for key in compiled_params}
                    processed_parameters.append(params)
                post_processed_params = self._dialect.execute_sequence_format(
                    processed_parameters)
            else:
                if dp:
                    raise exc.ArgumentError("Don't mix sqlalchemy DDL clause "
                                            "and execution with parameters")
                post_processed_params = [compiled.construct_params()]
            yield from cursor.execute(str(compiled), post_processed_params[0])
        else:
            raise exc.ArgumentError("sql statement should be str or "
                                    "SQLAlchemy data "
                                    "selection/modification clause")

        ret = yield from create_result_proxy(self, cursor, self._dialect)
        self._weak_results.add(ret)
        return ret

    @asyncio.coroutine
    def scalar(self, query, *multiparams, **params):
        """Executes a SQL query and returns a scalar value."""
        res = yield from self.execute(query, *multiparams, **params)
        return (yield from res.scalar())

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

            trans = yield from conn.begin()   # outermost transaction
            trans2 = yield from conn.begin()  # "nested"
            yield from trans2.commit()        # does nothing
            yield from trans.commit()         # actually commits

        Calls to .commit only have an effect when invoked via the
        outermost Transaction object, though the .rollback method of
        any of the Transaction objects will roll back the transaction.

        See also:
          .begin_nested - use a SAVEPOINT
          .begin_twophase - use a two phase/XA transaction

        """
        coro = self._begin()
        return _TransactionContextManager(coro)

    @asyncio.coroutine
    def _begin(self):
        if self._transaction is None:
            self._transaction = RootTransaction(self)
            yield from self._begin_impl()
            return self._transaction
        else:
            return Transaction(self, self._transaction)

    @asyncio.coroutine
    def _begin_impl(self):
        cur = yield from self._connection.cursor()
        try:
            yield from cur.execute('BEGIN')
        finally:
            yield from cur.close()

    @asyncio.coroutine
    def _commit_impl(self):
        cur = yield from self._connection.cursor()
        try:
            yield from cur.execute('COMMIT')
        finally:
            yield from cur.close()
            self._transaction = None

    @asyncio.coroutine
    def _rollback_impl(self):
        cur = yield from self._connection.cursor()
        try:
            yield from cur.execute('ROLLBACK')
        finally:
            yield from cur.close()
            self._transaction = None

    @asyncio.coroutine
    def begin_nested(self):
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
            yield from self._begin_impl()
        else:
            self._transaction = NestedTransaction(self, self._transaction)
            self._transaction._savepoint = yield from self._savepoint_impl()
        return self._transaction

    @asyncio.coroutine
    def _savepoint_impl(self, name=None):
        self._savepoint_seq += 1
        name = 'aiomysql_sa_savepoint_%s' % self._savepoint_seq

        cur = yield from self._connection.cursor()
        try:
            yield from cur.execute('SAVEPOINT ' + name)
            return name
        finally:
            yield from cur.close()

    @asyncio.coroutine
    def _rollback_to_savepoint_impl(self, name, parent):
        cur = yield from self._connection.cursor()
        try:
            yield from cur.execute('ROLLBACK TO SAVEPOINT ' + name)
        finally:
            yield from cur.close()
        self._transaction = parent

    @asyncio.coroutine
    def _release_savepoint_impl(self, name, parent):
        cur = yield from self._connection.cursor()
        try:
            yield from cur.execute('RELEASE SAVEPOINT ' + name)
        finally:
            yield from cur.close()
        self._transaction = parent

    @asyncio.coroutine
    def begin_twophase(self, xid=None):
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
        yield from self.execute("XA START %s", xid)
        return self._transaction

    @asyncio.coroutine
    def _prepare_twophase_impl(self, xid):
        yield from self.execute("XA END '%s'" % xid)
        yield from self.execute("XA PREPARE '%s'" % xid)

    @asyncio.coroutine
    def recover_twophase(self):
        """Return a list of prepared twophase transaction ids."""
        result = yield from self.execute("XA RECOVER;")
        return [row[0] for row in result]

    @asyncio.coroutine
    def rollback_prepared(self, xid, *, is_prepared=True):
        """Rollback prepared twophase transaction."""
        if not is_prepared:
            yield from self.execute("XA END '%s'" % xid)
        yield from self.execute("XA ROLLBACK '%s'" % xid)

    @asyncio.coroutine
    def commit_prepared(self, xid, *, is_prepared=True):
        """Commit prepared twophase transaction."""
        if not is_prepared:
            yield from self.execute("XA END '%s'" % xid)
        yield from self.execute("XA COMMIT '%s'" % xid)

    @property
    def in_transaction(self):
        """Return True if a transaction is in progress."""
        return self._transaction is not None and self._transaction.is_active

    @asyncio.coroutine
    def close(self):
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
            yield from self._transaction.rollback()
            self._transaction = None
        # don't close underlying connection, it can be reused by pool
        # conn.close()
        self._engine.release(self)
        self._connection = None
        self._engine = None

    @asyncio.coroutine
    def __aenter__(self):
        return self

    @asyncio.coroutine
    def __aexit__(self, exc_type, exc_val, exc_tb):
        yield from self.close()


def _distill_params(multiparams, params):
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
        if (hasattr(multiparams[0], '__iter__') and
                not hasattr(multiparams[0], 'strip')):
            return multiparams
        else:
            return [multiparams]
