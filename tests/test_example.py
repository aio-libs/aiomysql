import aiomysql
from tests._testutils import run_until_complete
from tests.base import AIOPyMySQLTestCase


class TestExample(AIOPyMySQLTestCase):

    @run_until_complete
    def test_example(self):
        conn = yield from aiomysql.connect(host='127.0.0.1', port=3306,
                                           user='root', password='',
                                           db='mysql', loop=self.loop)

        cur = conn.cursor()
        yield from cur.execute("SELECT Host,User FROM user")
        # print(cur.description)
        r = cur.fetchall()
        self.assertTrue(r)
        # print(r)
        cur.close()
        conn.close()

    @run_until_complete
    def test_pool_example(self):
        pool = yield from aiomysql.create_pool(host='127.0.0.1', port=3306,
                                           user='root', password='',
                                           db='mysql', loop=self.loop)

        with (yield from pool) as conn:
            cur = conn.cursor()
            yield from cur.execute("SELECT 10")
            # print(cur.description)
            (r,) = cur.fetchone()
            self.assertTrue(r, 10)
        pool.close()
        yield from pool.wait_closed()
