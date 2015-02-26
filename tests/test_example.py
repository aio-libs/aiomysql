import aiomysql
from tests._testutils import run_until_complete
from tests.base import AIOPyMySQLTestCase


class TestExample(AIOPyMySQLTestCase):
    @run_until_complete
    def test_example(self):
        conn = yield from aiomysql.connect(host='127.0.0.1', port=3306,
                                           user='root', password='',
                                           db='mysql', loop=self.loop)

        cur = yield from conn.cursor()
        yield from cur.execute("SELECT Host,User FROM user")
        # print(cur.description)
        r = yield from cur.fetchall()
        self.assertTrue(r)
        # print(r)
        yield from cur.close()
        conn.close()

    @run_until_complete
    def test_pool_example(self):
        pool = yield from aiomysql.create_pool(host='127.0.0.1', port=3306,
                                               user='root', password='',
                                               db='mysql', loop=self.loop)

        with (yield from pool) as conn:
            cur = yield from conn.cursor()
            yield from cur.execute("SELECT 10")
            # print(cur.description)
            (r,) = yield from cur.fetchone()
            self.assertTrue(r, 10)
        pool.close()
        yield from pool.wait_closed()

    @run_until_complete
    def test_callproc(self):

        conn = yield from aiomysql.connect(host='127.0.0.1', port=3306,
                                           user='root', password='',
                                           db='mysql', loop=self.loop)

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

        yield from cur.close()
        conn.close()
