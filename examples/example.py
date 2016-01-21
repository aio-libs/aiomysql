import asyncio
import aiomysql


async def test_example(loop):
    conn = await aiomysql.connect(host='127.0.0.1', port=3306,
                                  user='root', password='', db='mysql',
                                  loop=loop)

    async with conn.cursor() as cur:
        await cur.execute("SELECT Host,User FROM user")
        print(cur.description)
        r = await cur.fetchall()
        print(r)
    conn.close()


loop = asyncio.get_event_loop()
loop.run_until_complete(test_example(loop))
