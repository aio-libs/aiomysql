import asyncio
import aiomysql


loop = asyncio.get_event_loop()


@asyncio.coroutine
def test_example():
    conn = yield from aiomysql.connect(host='127.0.0.1', port=3306,
                                       user='root', password='',
                                       db='test_pymysql', loop=loop)

    cur = yield from conn.cursor()
    yield from cur.execute("DROP PROCEDURE IF EXISTS myinc;")
    yield from cur.execute("""CREATE PROCEDURE myinc(p1 INT)
                           BEGIN
                               SELECT p1 + 1;
                           END
                           """)

    yield from cur.callproc('myinc', [1])
    (ret, ) = yield from cur.fetchone()
    assert 2, ret
    print(ret)

    yield from cur.close()
    conn.close()


loop.run_until_complete(test_example())
