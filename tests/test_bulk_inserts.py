import asyncio
import aiomysql
from tests import base
from tests._testutils import run_until_complete


class TestBulkInserts(base.AIOPyMySQLTestCase):
    cursor_type = aiomysql.cursors.DictCursor

    def setUp(self):
        super(TestBulkInserts, self).setUp()

        @asyncio.coroutine
        def prepare(self):
            # create a table ane some data to query
            self.conn = conn = self.connections[0]
            c = yield from conn.cursor(self.cursor_type)
            yield from c.execute("drop table if exists bulkinsert;")
            yield from c.execute(
                """CREATE TABLE bulkinsert
                (
                id int(11),
                name char(20),
                age int,
                height int,
                PRIMARY KEY (id)
                )
                """)

        self.loop.run_until_complete(prepare(self))

    @asyncio.coroutine
    def _verify_records(self, data):
        conn = self.connections[0]
        cursor = yield from conn.cursor()
        yield from cursor.execute(
            "SELECT id, name, age, height from bulkinsert")
        result = yield from cursor.fetchall()
        yield from cursor.execute('commit')
        self.assertEqual(sorted(data), sorted(result))

    @run_until_complete
    def test_bulk_insert(self):
        conn = self.connections[0]
        cursor = yield from conn.cursor()

        data = [(0, "bob", 21, 123), (1, "jim", 56, 45), (2, "fred", 100, 180)]
        yield from cursor.executemany(
            "insert into bulkinsert (id, name, age, height) "
            "values (%s,%s,%s,%s)", data)
        self.assertEqual(
            cursor._last_executed, bytearray(
                b"insert into bulkinsert (id, name, age, height) values "
                b"(0,'bob',21,123),(1,'jim',56,45),(2,'fred',100,180)"))
        yield from cursor.execute('commit')
        yield from self._verify_records(data)

    @run_until_complete
    def test_bulk_insert_multiline_statement(self):
        conn = self.connections[0]
        cursor = yield from conn.cursor()
        data = [(0, "bob", 21, 123), (1, "jim", 56, 45), (2, "fred", 100, 180)]
        yield from cursor.executemany("""insert
            into bulkinsert (id, name,
            age, height)
            values (%s,
            %s , %s,
            %s )
             """, data)
        self.assertEqual(cursor._last_executed, bytearray(b"""insert
            into bulkinsert (id, name,
            age, height)
            values (0,
            'bob' , 21,
            123 ),(1,
            'jim' , 56,
            45 ),(2,
            'fred' , 100,
            180 )"""))
        yield from cursor.execute('commit')
        yield from self._verify_records(data)

    @run_until_complete
    def test_bulk_insert_single_record(self):
        conn = self.connections[0]
        cursor = yield from conn.cursor()
        data = [(0, "bob", 21, 123)]
        yield from cursor.executemany(
            "insert into bulkinsert (id, name, age, height) "
            "values (%s,%s,%s,%s)", data)
        yield from cursor.execute('commit')
        yield from self._verify_records(data)
