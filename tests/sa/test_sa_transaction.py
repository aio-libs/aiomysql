import functools
import unittest
from unittest import mock

import pytest
from sqlalchemy import MetaData, Table, Column, Integer, String

from aiomysql import sa


meta = MetaData()
tbl = Table('sa_tbl2', meta,
            Column('id', Integer, nullable=False,
                   primary_key=True),
            Column('name', String(255)))


def check_prepared_transactions(func):
    @functools.wraps(func)
    async def wrapper(self):
        conn = await self.loop.run_until_complete(self._connect())
        val = await conn.scalar('show max_prepared_transactions')
        if not val:
            raise unittest.SkipTest('Twophase transacions are not supported. '
                                    'Set max_prepared_transactions to '
                                    'a nonzero value')
        return func(self)
    return wrapper


async def start(conn):
    await conn.execute("DROP TABLE IF EXISTS sa_tbl2")
    await conn.execute("CREATE TABLE sa_tbl2 "
                       "(id serial, name varchar(255))")
    await conn.execute("INSERT INTO sa_tbl2 (name)"
                       "VALUES ('first')")
    await conn._connection.commit()


@pytest.fixture()
def sa_connect(connection, connection_creator):
    async def _connect(**kwargs):
        conn = await connection_creator(**kwargs)
        # TODO: fix this, should autocommit be enabled by default?
        await conn.autocommit(True)
        engine = mock.Mock()
        engine.dialect = sa.engine._dialect

        def release(*args):
            return
        engine.release = release

        ret = sa.SAConnection(conn, engine)
        return ret
    return _connect


async def test_without_transactions(sa_connect):
    conn1 = await sa_connect()
    await start(conn1)

    conn2 = await sa_connect()
    res1 = await conn1.scalar(tbl.count())
    assert 1 == res1

    await conn2.execute(tbl.delete())

    res2 = await conn1.scalar(tbl.count())
    assert 0 == res2
    await conn1.close()
    await conn2.close()


async def test_connection_attr(sa_connect):
    conn = await sa_connect()
    await start(conn)
    tr = await conn.begin()
    assert tr.connection is conn
    await conn.close()


async def test_root_transaction(sa_connect):
    conn1 = await sa_connect()
    await start(conn1)
    conn2 = await sa_connect()

    tr = await conn1.begin()
    assert tr.is_active
    await conn1.execute(tbl.delete())

    res1 = await conn2.scalar(tbl.count())
    assert 1 == res1

    await tr.commit()

    assert not tr.is_active
    assert not conn1.in_transaction
    res2 = await conn2.scalar(tbl.count())
    assert 0 == res2
    await conn1.close()
    await conn2.close()


async def test_root_transaction_rollback(sa_connect):
    conn1 = await sa_connect()
    await start(conn1)
    conn2 = await sa_connect()

    tr = await conn1.begin()
    assert tr.is_active
    await conn1.execute(tbl.delete())

    res1 = await conn2.scalar(tbl.count())
    assert 1 == res1

    await tr.rollback()

    assert not tr.is_active
    res2 = await conn2.scalar(tbl.count())
    assert 1 == res2
    await conn1.close()
    await conn2.close()


async def test_root_transaction_close(sa_connect):
    conn1 = await sa_connect()
    await start(conn1)
    conn2 = await sa_connect()

    tr = await conn1.begin()
    assert tr.is_active
    await conn1.execute(tbl.delete())

    res1 = await conn2.scalar(tbl.count())
    assert 1 == res1

    await tr.close()

    assert not tr.is_active
    res2 = await conn2.scalar(tbl.count())
    assert 1 == res2
    await conn1.close()
    await conn2.close()


async def test_rollback_on_connection_close(sa_connect):
    conn1 = await sa_connect()
    await start(conn1)
    conn2 = await sa_connect()

    tr = await conn1.begin()
    await conn1.execute(tbl.delete())

    res1 = await conn2.scalar(tbl.count())
    assert 1 == res1

    await conn1.close()

    res2 = await conn2.scalar(tbl.count())
    assert 1 == res2
    del tr
    await conn1.close()
    await conn2.close()


async def test_root_transaction_commit_inactive(sa_connect):
    conn = await sa_connect()
    await start(conn)
    tr = await conn.begin()
    assert tr.is_active
    await tr.commit()
    assert not tr.is_active
    with pytest.raises(sa.InvalidRequestError):
        await tr.commit()
    await conn.close()


async def test_root_transaction_rollback_inactive(sa_connect):
    conn = await sa_connect()
    await start(conn)
    tr = await conn.begin()
    assert tr.is_active
    await tr.rollback()
    assert not tr.is_active
    await tr.rollback()
    assert not tr.is_active
    await conn.close()


async def test_root_transaction_double_close(sa_connect):
    conn = await sa_connect()
    await start(conn)
    tr = await conn.begin()
    assert tr.is_active
    await tr.close()
    assert not tr.is_active
    await tr.close()
    assert not tr.is_active
    await conn.close()


async def test_inner_transaction_commit(sa_connect):
    conn = await sa_connect()
    await start(conn)
    tr1 = await conn.begin()
    tr2 = await conn.begin()
    assert tr2.is_active

    await tr2.commit()
    assert not tr2.is_active
    assert tr1.is_active

    await tr1.commit()
    assert not tr2.is_active
    assert not tr1.is_active
    await conn.close()


async def test_inner_transaction_rollback(sa_connect):
    conn = await sa_connect()
    await start(conn)
    tr1 = await conn.begin()
    tr2 = await conn.begin()
    assert tr2.is_active
    await conn.execute(tbl.insert().values(name='aaaa'))

    await tr2.rollback()
    assert not tr2.is_active
    assert not tr1.is_active

    res = await conn.scalar(tbl.count())
    assert 1 == res
    await conn.close()


async def test_inner_transaction_close(sa_connect):
    conn = await sa_connect()
    await start(conn)
    tr1 = await conn.begin()
    tr2 = await conn.begin()
    assert tr2.is_active
    await conn.execute(tbl.insert().values(name='aaaa'))

    await tr2.close()
    assert not tr2.is_active
    assert tr1.is_active
    await tr1.commit()

    res = await conn.scalar(tbl.count())
    assert 2 == res
    await conn.close()


async def test_nested_transaction_commit(sa_connect):
    conn = await sa_connect()
    await start(conn)
    tr1 = await conn.begin_nested()
    tr2 = await conn.begin_nested()
    assert tr1.is_active
    assert tr2.is_active

    await conn.execute(tbl.insert().values(name='aaaa'))
    await tr2.commit()
    assert not tr2.is_active
    assert tr1.is_active

    res = await conn.scalar(tbl.count())
    assert 2 == res

    await tr1.commit()
    assert not tr2.is_active
    assert not tr1.is_active

    res = await conn.scalar(tbl.count())
    assert 2 == res
    await conn.close()


async def test_nested_transaction_commit_twice(sa_connect):
    conn = await sa_connect()
    await start(conn)
    tr1 = await conn.begin_nested()
    tr2 = await conn.begin_nested()

    await conn.execute(tbl.insert().values(name='aaaa'))
    await tr2.commit()
    assert not tr2.is_active
    assert tr1.is_active

    await tr2.commit()
    assert not tr2.is_active
    assert tr1.is_active

    res = await conn.scalar(tbl.count())
    assert 2 == res

    await tr1.close()
    await conn.close()


async def test_nested_transaction_rollback(sa_connect):
    conn = await sa_connect()
    await start(conn)
    tr1 = await conn.begin_nested()
    tr2 = await conn.begin_nested()
    assert tr1.is_active
    assert tr2.is_active

    await conn.execute(tbl.insert().values(name='aaaa'))
    await tr2.rollback()
    assert not tr2.is_active
    assert tr1.is_active

    res = await conn.scalar(tbl.count())
    assert 1 == res

    await tr1.commit()
    assert not tr2.is_active
    assert not tr1.is_active

    res = await conn.scalar(tbl.count())
    assert 1 == res
    await conn.close()


async def test_nested_transaction_rollback_twice(sa_connect):
    conn = await sa_connect()
    await start(conn)
    tr1 = await conn.begin_nested()
    tr2 = await conn.begin_nested()

    await conn.execute(tbl.insert().values(name='aaaa'))
    await tr2.rollback()
    assert not tr2.is_active
    assert tr1.is_active

    await tr2.rollback()
    assert not tr2.is_active
    assert tr1.is_active

    await tr1.commit()
    res = await conn.scalar(tbl.count())
    assert 1 == res
    await conn.close()


async def test_twophase_transaction_commit(sa_connect):
    conn = await sa_connect()
    await start(conn)
    tr = await conn.begin_twophase('sa_twophase')
    assert tr.xid == 'sa_twophase'
    await conn.execute(tbl.insert().values(name='aaaa'))

    await tr.prepare()
    assert tr.is_active

    await tr.commit()
    assert not tr.is_active

    res = await conn.scalar(tbl.count())
    assert 2 == res
    await conn.close()


async def test_twophase_transaction_twice(sa_connect):
    conn = await sa_connect()
    await start(conn)
    tr = await conn.begin_twophase()
    with pytest.raises(sa.InvalidRequestError):
        await conn.begin_twophase()

    assert tr.is_active
    await tr.prepare()
    await tr.commit()
    await conn.close()


async def test_transactions_sequence(sa_connect):
    conn = await sa_connect()
    await start(conn)

    await conn.execute(tbl.delete())

    assert conn._transaction is None

    tr1 = await conn.begin()
    assert tr1 is conn._transaction
    await conn.execute(tbl.insert().values(name='a'))
    res1 = await conn.scalar(tbl.count())
    assert 1 == res1

    await tr1.commit()
    assert conn._transaction is None

    tr2 = await conn.begin()
    assert tr2 is conn._transaction
    await conn.execute(tbl.insert().values(name='b'))
    res2 = await conn.scalar(tbl.count())
    assert 2 == res2
    await tr2.rollback()
    assert conn._transaction is None

    tr3 = await conn.begin()
    assert tr3 is conn._transaction
    await conn.execute(tbl.insert().values(name='b'))
    res3 = await conn.scalar(tbl.count())
    assert 2 == res3
    await tr3.commit()
    assert conn._transaction is None
    await conn.close()
