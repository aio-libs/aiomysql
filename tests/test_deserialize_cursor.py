import copy
import asyncio

import aiomysql.cursors
from tests import base
from tests._testutils import run_until_complete


class TestDeserializeCursor(base.AIOPyMySQLTestCase):
    bob = ("bob", 21, {"k1": "pretty", "k2": [18, 25]})
    jim = ("jim", 56, {"k1": "rich", "k2": [20, 60]})
    fred = ("fred", 100, {"k1": "longevity", "k2": [100, 160]})
    havejson = True

    cursor_type = aiomysql.cursors.DeserializationCursor

    def setUp(self):
        super(TestDeserializeCursor, self).setUp()
        self.conn = conn = self.connections[0]

        @asyncio.coroutine
        def prepare():
            c = yield from conn.cursor(self.cursor_type)

            # create a table ane some data to query
            yield from c.execute("drop table if exists deserialize_cursor")
            yield from c.execute("select VERSION()")
            v = yield from c.fetchone()
            version, *db_type = v[0].split('-', 1)
            version = float(".".join(version.split('.', 2)[:2]))
            ismariadb = db_type and 'mariadb' in db_type[0].lower()
            if ismariadb or version < 5.7:
                yield from c.execute(
                    """CREATE TABLE deserialize_cursor
                     (name char(20), age int , claim text)""")
                self.havejson = False
            else:
                yield from c.execute(
                    """CREATE TABLE deserialize_cursor
                     (name char(20), age int , claim json)""")
            data = [("bob", 21, '{"k1": "pretty", "k2": [18, 25]}'),
                    ("jim", 56, '{"k1": "rich", "k2": [20, 60]}'),
                    ("fred", 100, '{"k1": "longevity", "k2": [100, 160]}')]
            yield from c.executemany("insert into deserialize_cursor values "
                                     "(%s,%s,%s)",
                                     data)

        self.loop.run_until_complete(prepare())

    def tearDown(self):
        @asyncio.coroutine
        def shutdown():
            c = yield from self.conn.cursor()
            yield from c.execute("drop table deserialize_cursor;")

        self.loop.run_until_complete(shutdown())
        super(TestDeserializeCursor, self).tearDown()

    @run_until_complete
    def test_deserialize_cursor(self):
        if not self.havejson:
            return
        bob, jim, fred = copy.deepcopy(self.bob), copy.deepcopy(
            self.jim), copy.deepcopy(self.fred)
        # all assert test compare to the structure as would come
        # out from MySQLdb
        conn = self.conn
        c = yield from conn.cursor(self.cursor_type)

        # pull back the single row dict for bob and check
        yield from c.execute("SELECT * from deserialize_cursor "
                             "where name='bob'")
        r = yield from c.fetchone()
        self.assertEqual(bob, r, "fetchone via DeserializeCursor failed")
        # same again, but via fetchall => tuple)
        yield from c.execute("SELECT * from deserialize_cursor "
                             "where name='bob'")
        r = yield from c.fetchall()
        self.assertEqual([bob], r,
                         "fetch a 1 row result via fetchall failed via "
                         "DeserializeCursor")
        # get all 3 row via fetchall
        yield from c.execute("SELECT * from deserialize_cursor")
        r = yield from c.fetchall()
        self.assertEqual([bob, jim, fred], r,
                         "fetchall failed via DictCursor")

        # get all 2 row via fetchmany
        yield from c.execute("SELECT * from deserialize_cursor")
        r = yield from c.fetchmany(2)
        self.assertEqual([bob, jim], r, "fetchmany failed via DictCursor")
        yield from c.execute('commit')

    @run_until_complete
    def test_deserialize_cursor_low_version(self):
        if self.havejson:
            return
        bob = ("bob", 21, '{"k1": "pretty", "k2": [18, 25]}')
        jim = ("jim", 56, '{"k1": "rich", "k2": [20, 60]}')
        fred = ("fred", 100, '{"k1": "longevity", "k2": [100, 160]}')
        # all assert test compare to the structure as would come
        # out from MySQLdb
        conn = self.conn
        c = yield from conn.cursor(self.cursor_type)

        # pull back the single row dict for bob and check
        yield from c.execute("SELECT * from deserialize_cursor "
                             "where name='bob'")
        r = yield from c.fetchone()
        self.assertEqual(bob, r, "fetchone via DeserializeCursor failed")
        # same again, but via fetchall => tuple)
        yield from c.execute("SELECT * from deserialize_cursor "
                             "where name='bob'")
        r = yield from c.fetchall()
        self.assertEqual([bob], r,
                         "fetch a 1 row result via fetchall failed via "
                         "DeserializeCursor")
        # get all 3 row via fetchall
        yield from c.execute("SELECT * from deserialize_cursor")
        r = yield from c.fetchall()
        self.assertEqual([bob, jim, fred], r,
                         "fetchall failed via DictCursor")

        # get all 2 row via fetchmany
        yield from c.execute("SELECT * from deserialize_cursor")
        r = yield from c.fetchmany(2)
        self.assertEqual([bob, jim], r, "fetchmany failed via DictCursor")
        yield from c.execute('commit')

    @run_until_complete
    def test_deserializedictcursor(self):
        if not self.havejson:
            return
        bob = {'name': 'bob', 'age': 21,
               'claim': {"k1": "pretty", "k2": [18, 25]}}
        conn = self.conn
        c = yield from conn.cursor(aiomysql.cursors.DeserializationCursor,
                                   aiomysql.cursors.DictCursor)
        yield from c.execute("SELECT * from deserialize_cursor "
                             "where name='bob'")
        r = yield from c.fetchall()
        self.assertEqual([bob], r,
                         "fetch a 1 row result via fetchall failed via "
                         "DeserializationCursor")

    @run_until_complete
    def test_ssdeserializecursor(self):
        if not self.havejson:
            return
        conn = self.conn
        c = yield from conn.cursor(aiomysql.cursors.SSCursor,
                                   aiomysql.cursors.DeserializationCursor)
        yield from c.execute("SELECT * from deserialize_cursor "
                             "where name='bob'")
        r = yield from c.fetchall()
        self.assertEqual([self.bob], r,
                         "fetch a 1 row result via fetchall failed via "
                         "DeserializationCursor")

    @run_until_complete
    def test_ssdeserializedictcursor(self):
        if not self.havejson:
            return
        bob = {'name': 'bob', 'age': 21,
               'claim': {"k1": "pretty", "k2": [18, 25]}}
        conn = self.conn
        c = yield from conn.cursor(aiomysql.cursors.SSCursor,
                                   aiomysql.cursors.DeserializationCursor,
                                   aiomysql.cursors.DictCursor)
        yield from c.execute("SELECT * from deserialize_cursor "
                             "where name='bob'")
        r = yield from c.fetchall()
        self.assertEqual([bob], r,
                         "fetch a 1 row result via fetchall failed via "
                         "DeserializationCursor")
