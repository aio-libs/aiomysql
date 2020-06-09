import asyncio
import aiomysql


loop = asyncio.get_event_loop()


@asyncio.coroutine
def test_example():
    pool = yield from aiomysql.create_pool(
        host='127.0.0.1',
        port=3306,
        user='root',
        password='',
        db='mysql',
        loop=loop
    )
    with (yield from pool) as conn:
        cur = yield from conn.cursor()
        yield from cur.execute("SELECT 10")
        # print(cur.description)
        (r,) = yield from cur.fetchone()
        assert r == 10
    pool.close()
    yield from pool.wait_closed()


loop.run_until_complete(test_example())
