import aiomysql
from tests._testutils import run_until_complete
from tests.base import AIOPyMySQLTestCase

class TestExample(AIOPyMySQLTestCase):

    @run_until_complete
    def test_example(self):
        conn = yield from aiomysql.connect(host='127.0.0.1', port=3306,
                                           user='root', passwd='', db='mysql',
                                           loop=self.loop)

        cur = conn.cursor()
        yield from cur.execute("SELECT Host,User FROM user")
        print(cur.description)
        r = cur.fetchall()
        print(r)
        cur.close()
        conn.close()
