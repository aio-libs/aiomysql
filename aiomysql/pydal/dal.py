from pydal import DAL as PY_DAL

import aiomysql
from aiomysql.pool import Pool
from .connection import PyDALConnection


class AsyncDAL:
    """
    @see https://aiomysql.readthedocs.io/en/latest/pool.html
    """

    def __init__(self) -> None:
        super().__init__()
        self._pydal = None
        self._aiomysql_conn_pool = None

    @staticmethod
    async def create(host, user, password, db, port=3306, pool_max_size=10, echo=True) -> "AsyncDAL":
        return await AsyncDAL().async_init(host, user, password, db, port, pool_max_size, echo)

    async def async_init(self, host, user, password, db, port, pool_max_size, echo) -> "AsyncDAL":
        self._pydal = PY_DAL(
            uri=f"mysql://{user}:{password}@{host}:{port}/{db}", migrate=False, migrate_enabled=False,
            do_connect=False, bigint_id=True
        )
        self._aiomysql_conn_pool = await aiomysql.create_pool(
            0, pool_max_size, echo, host=host, port=port, user=user,
            password=password, db=db
        )
        return self

    @property
    def aiomysql_conn_pool(self) -> Pool:
        return self._aiomysql_conn_pool

    def close(self):
        self.aiomysql_conn_pool.close()

    async def wait_closed(self):
        await self.aiomysql_conn_pool.wait_closed()

    def terminate(self):
        self.aiomysql_conn_pool.terminate()

    async def acquire(self) -> PyDALConnection:
        return PyDALConnection(self._pydal, await self.aiomysql_conn_pool.acquire())

    async def release(self, conn: PyDALConnection):
        await self.aiomysql_conn_pool.release(conn.aiomysql_conn)

    def define_table(self, tablename, *fields, **kwargs):
        self._pydal.define_table(tablename, *fields, **kwargs)
