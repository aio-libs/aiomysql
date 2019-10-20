from . import row


class AsyncQuery:
    def __init__(self, pydal_cursor, dal_query) -> None:
        super().__init__()
        self._pydal_cursor = pydal_cursor
        self._dal_query = dal_query

    def _select(self, *args, **kwargs):
        return self._dal_query._select(*args, **kwargs)

    async def select(self, *args, **kwargs):
        sql: str = self._select(*args, **kwargs)
        sql = sql.replace("\\", "")
        # print(sql)
        await self._pydal_cursor.execute(sql)
        result = await self._pydal_cursor.fetchall()
        return row.Rows(self._pydal_cursor.description, result)

    def _update(self, **kwargs):
        return self._dal_query._update(**kwargs)

    async def update(self, **kwargs) -> int:
        sql = self._update(**kwargs)
        # print(sql)
        await self._pydal_cursor.execute(sql)
        return self._pydal_cursor.rowcount

    def _delete(self):
        return self._dal_query._delete()

    async def delete(self):
        sql = self._delete()
        # print(sql)
        await self._pydal_cursor.execute(sql)
        return self._pydal_cursor.rowcount

    def _count(self):
        return self._dal_query._count()

    async def count(self) -> int:
        sql = self._count()
        # print(sql)
        await self._pydal_cursor.execute(sql)
        result = await self._pydal_cursor.fetchone()
        return result[0]

    async def isempty(self):
        return await self.count() == 0
