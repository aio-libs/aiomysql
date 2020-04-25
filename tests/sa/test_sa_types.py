from enum import IntEnum
from unittest import mock

import pytest
from sqlalchemy import MetaData, Table, Column, Integer, TypeDecorator

from aiomysql import sa


class UserDefinedEnum(IntEnum):
    Value1 = 111
    Value2 = 222


class IntEnumField(TypeDecorator):
    impl = Integer

    def __init__(self, enum_class, *arg, **kw):
        TypeDecorator.__init__(self, *arg, **kw)
        self.enum_class = enum_class

    def process_bind_param(self, value, dialect):
        """ From python to DB """
        if value is None:
            return None
        elif not isinstance(value, self.enum_class):
            return self.enum_class(value).value
        else:
            return value.value

    def process_result_value(self, value, dialect):
        """ From DB to Python """
        if value is None:
            return None

        return self.enum_class(value)


meta = MetaData()
tbl = Table('sa_test_type_tbl', meta,
            Column('id', Integer, nullable=False,
                   primary_key=True),
            Column('val', IntEnumField(enum_class=UserDefinedEnum)))


@pytest.fixture()
def sa_connect(connection_creator):
    async def connect(**kwargs):
        conn = await connection_creator()
        await conn.autocommit(True)
        cur = await conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS sa_test_type_tbl")
        await cur.execute("CREATE TABLE sa_test_type_tbl "
                          "(id serial, val bigint)")
        await cur._connection.commit()
        engine = mock.Mock()
        engine.dialect = sa.engine._dialect
        return sa.SAConnection(conn, engine)
    return connect


@pytest.mark.run_loop
async def test_values(sa_connect):
    conn = await sa_connect()

    await conn.execute(tbl.insert().values(
        val=UserDefinedEnum.Value1)
    )
    result = await conn.execute(tbl.select().where(
        tbl.c.val == UserDefinedEnum.Value1)
    )
    data = await result.fetchone()
    assert data['val'] == UserDefinedEnum.Value1
