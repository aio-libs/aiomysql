from aiomysql.connection import Connection
from .cursor import PyDALCursor
from .table import Table


class PyDALConnection:
    def __init__(self, pydal, aiomysql_conn: Connection) -> None:
        super().__init__()
        self._pydal = pydal
        self._aiomysql_conn = aiomysql_conn

    @property
    def aiomysql_conn(self) -> Connection:
        return self._aiomysql_conn

    async def commit(self):
        await self.aiomysql_conn.commit()

    def close(self):
        self.aiomysql_conn.close()

    async def autocommit(self, mode: bool):
        await self.aiomysql_conn.autocommit(mode)

    def autocommit_mode(self):
        return self.aiomysql_conn.autocommit_mode

    async def cursor(self) -> PyDALCursor:
        return PyDALCursor(self._pydal, self, await self.aiomysql_conn.cursor())
