import asyncio

import aiomysql


async def main():
    conn = await aiomysql.connect(user='', password='', db='')
    async with conn.cursor() as cursor:
        stmt_select = "SELECT 42;"
        await cursor.execute(stmt_select)
        resp = await cursor.fetchall()
        print(resp)
    conn.close()


if __name__ == '__main__':
    asyncio.run(main())
