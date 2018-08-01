import asyncio
import aiomysql


async def test_example(loop):
    conn = await aiomysql.connect(host='127.0.0.1', port=3306,
                                  user='root', password='',
                                  db='test_pymysql', loop=loop)

    async with conn.cursor() as cur:
        await cur.execute('DROP PROCEDURE IF EXISTS myinc;')
        await cur.execute("""CREATE PROCEDURE myinc(p1 INT)
                             BEGIN
                                 SELECT p1 + 1;
                             END""")

        await cur.callproc('myinc', [1])
        (ret, ) = await cur.fetchone()
        assert 2, ret
        print(ret)

    conn.close()


loop = asyncio.get_event_loop()
loop.run_until_complete(test_example(loop))
