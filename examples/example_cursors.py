import asyncio
import aiomysql


async def test_example(loop):
    conn = await aiomysql.connect(host='127.0.0.1', port=3306,
                                  user='root', password='', db='mysql',
                                  loop=loop)
    sql = "SELECT 1 `id`, JSON_OBJECT('key1', 1, 'key2', 'abc') obj"
    async with conn.cursor(aiomysql.cursors.DeserializationCursor,
                           aiomysql.cursors.DictCursor) as cur:
        await cur.execute(sql)
        print(cur.description)
        r = await cur.fetchall()
        print(r)
    conn.close()


loop = asyncio.get_event_loop()
loop.run_until_complete(test_example(loop))
