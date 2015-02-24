import asyncio
import datetime

import aiomysql.cursors
from tests import base
from tests._testutils import run_until_complete


class TestDictCursor(base.AIOPyMySQLTestCase):
    bob = {'name': 'bob', 'age': 21,
           'DOB': datetime.datetime(1990, 2, 6, 23, 4, 56)}
    jim = {'name': 'jim', 'age': 56,
           'DOB': datetime.datetime(1955, 5, 9, 13, 12, 45)}
    fred = {'name': 'fred', 'age': 100,
            'DOB': datetime.datetime(1911, 9, 12, 1, 1, 1)}

    cursor_type = aiomysql.cursors.DictCursor

    def setUp(self):
        super(TestDictCursor, self).setUp()
        self.conn = conn = self.connections[0]

        @asyncio.coroutine
        def prepare():
            c = yield from conn.cursor(self.cursor_type)

            # create a table ane some data to query
            yield from c.execute("drop table if exists dictcursor")
            yield from c.execute(
                """CREATE TABLE dictcursor (name char(20), age int ,
                DOB datetime)""")
            data = [("bob", 21, "1990-02-06 23:04:56"),
                    ("jim", 56, "1955-05-09 13:12:45"),
                    ("fred", 100, "1911-09-12 01:01:01")]
            yield from c.executemany("insert into dictcursor values "
                                     "(%s,%s,%s)",
                                     data)

        self.loop.run_until_complete(prepare())

    def tearDown(self):
        @asyncio.coroutine
        def shutdown():
            c = yield from self.conn.cursor()
            yield from c.execute("drop table dictcursor;")

        self.loop.run_until_complete(shutdown())
        super(TestDictCursor, self).tearDown()

    @run_until_complete
    def test_dictcursor(self):
        bob, jim, fred = self.bob.copy(), self.jim.copy(), self.fred.copy()
        # all assert test compare to the structure as would come
        # out from MySQLdb
        conn = self.conn
        c = yield from conn.cursor(self.cursor_type)

        # try an update which should return no rows
        yield from c.execute("update dictcursor set age=20 where name='bob'")
        bob['age'] = 20
        # pull back the single row dict for bob and check
        yield from c.execute("SELECT * from dictcursor where name='bob'")
        r = yield from c.fetchone()
        self.assertEqual(bob, r, "fetchone via DictCursor failed")
        # same again, but via fetchall => tuple)
        yield from c.execute("SELECT * from dictcursor where name='bob'")
        r = yield from c.fetchall()
        self.assertEqual([bob], r,
                         "fetch a 1 row result via fetchall failed via "
                         "DictCursor")

        # get all 3 row via fetchall
        yield from c.execute("SELECT * from dictcursor")
        r = yield from c.fetchall()
        self.assertEqual([bob, jim, fred], r, "fetchall failed via DictCursor")

        # get all 2 row via fetchmany
        yield from c.execute("SELECT * from dictcursor")
        r = yield from c.fetchmany(2)
        self.assertEqual([bob, jim], r, "fetchmany failed via DictCursor")
        yield from c.execute('commit')

    @run_until_complete
    def test_custom_dict(self):
        class MyDict(dict):
            pass

        class MyDictCursor(self.cursor_type):
            dict_type = MyDict

        keys = ['name', 'age', 'DOB']
        bob = MyDict([(k, self.bob[k]) for k in keys])
        jim = MyDict([(k, self.jim[k]) for k in keys])
        fred = MyDict([(k, self.fred[k]) for k in keys])

        cur = yield from self.conn.cursor(MyDictCursor)
        yield from cur.execute("SELECT * FROM dictcursor WHERE name='bob'")
        r = yield from cur.fetchone()
        self.assertEqual(bob, r, "fetchone() returns MyDictCursor")

        yield from cur.execute("SELECT * FROM dictcursor")
        r = yield from cur.fetchall()
        self.assertEqual([bob, jim, fred], r,
                         "fetchall failed via MyDictCursor")

        yield from cur.execute("SELECT * FROM dictcursor")
        r = yield from cur.fetchmany(2)
        self.assertEqual([bob, jim], r,
                         "list failed via MyDictCursor")

    @run_until_complete
    def test_ssdictcursor(self):
        conn = self.conn
        c = yield from conn.cursor(aiomysql.cursors.SSDictCursor)
        yield from c.execute("SELECT * from dictcursor where name='bob'")
        r = yield from c.fetchall()
        self.assertEqual([self.bob], r,
                         "fetch a 1 row result via fetchall failed via "
                         "DictCursor")
