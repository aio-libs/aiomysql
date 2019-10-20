from aiomysql.cursors import Cursor
from .table import Table
from .query import AsyncQuery


class PyDALCursor:
    def __init__(self, pydal, pydal_connection, aiomysql_cursor: Cursor) -> None:
        super().__init__()
        self._aiomysql_cursor = aiomysql_cursor
        self._pydal = pydal
        self._pydal_connection = pydal_connection
        self._table_map = {}

    @property
    def aiomysql_cursor(self) -> Cursor:
        return self._aiomysql_cursor

    @property
    def pydal_connection(self):
        return self._pydal_connection

    @property
    def description(self):
        return self._aiomysql_cursor.description

    async def execute(self, sql, args=None):
        await self.aiomysql_cursor.execute(sql, args)

    async def executemany(self, sql, args=None):
        await self.aiomysql_cursor.executemany(sql, args)

    async def fetchall(self):
        return await self.aiomysql_cursor.fetchall()

    async def fetchone(self):
        return await self.aiomysql_cursor.fetchone()

    async def fetchmany(self, size=None):
        return await self.aiomysql_cursor.fetchmany(size)

    async def close(self):
        await self.aiomysql_cursor.close()

    @property
    def rowcount(self) -> int:
        return self.aiomysql_cursor.rowcount

    def __getitem__(self, item):
        return self.__getattr__(item)

    def __getattr__(self, item):
        pydal_table = self._pydal.__getattr__(item)
        if pydal_table in self._table_map:
            async_table = self._table_map[pydal_table]
        else:
            async_table = Table(self._pydal, self, pydal_table)
            self._table_map[pydal_table] = async_table
        return async_table

    def __call__(self, *args) -> AsyncQuery:
        return AsyncQuery(self, self._pydal(*args))
