import asyncio
from aiomysql import connect, sa
from enum import IntEnum

import os
import unittest
from unittest import mock

from sqlalchemy import MetaData, Table, Column, Integer, TypeDecorator


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


class TestSAConnection(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        self.host = os.environ.get('MYSQL_HOST', 'localhost')
        self.port = int(os.environ.get('MYSQL_PORT', 3306))
        self.user = os.environ.get('MYSQL_USER', 'root')
        self.db = os.environ.get('MYSQL_DB', 'test_pymysql')
        self.password = os.environ.get('MYSQL_PASSWORD', '')

    def tearDown(self):
        self.loop.close()

    async def connect(self, **kwargs):
        conn = await connect(db=self.db,
                             user=self.user,
                             password=self.password,
                             host=self.host,
                             loop=self.loop,
                             port=self.port,
                             **kwargs)
        await conn.autocommit(True)
        cur = await conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS sa_test_type_tbl")
        await cur.execute("CREATE TABLE sa_test_type_tbl "
                          "(id serial, val bigint)")
        await cur._connection.commit()
        engine = mock.Mock()
        engine.dialect = sa.engine._dialect
        return sa.SAConnection(conn, engine)

    def test_values(self):
        async def go():
            conn = await self.connect()

            await conn.execute(tbl.insert().values(
                val=UserDefinedEnum.Value1)
            )
            result = await conn.execute(tbl.select().where(
                tbl.c.val == UserDefinedEnum.Value1)
            )
            data = await result.fetchone()
            self.assertEqual(
                data['val'], UserDefinedEnum.Value1
            )

        self.loop.run_until_complete(go())
