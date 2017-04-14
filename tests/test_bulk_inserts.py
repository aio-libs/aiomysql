import asyncio

import pytest
from aiomysql import DictCursor


@pytest.fixture
def table(loop, connection, table_cleanup):
    @asyncio.coroutine
    def f():
        cursor = yield from connection.cursor(DictCursor)
        sql = """CREATE TABLE bulkinsert (id INT(11), name CHAR(20),
                 age INT, height INT, PRIMARY KEY (id))"""
        yield from cursor.execute(sql)
    table_cleanup('bulkinsert')
    loop.run_until_complete(f())


@pytest.fixture
def assert_records(cursor):
    @asyncio.coroutine
    def f(data):
        yield from cursor.execute(
            "SELECT id, name, age, height FROM bulkinsert")
        result = yield from cursor.fetchall()
        yield from cursor.execute('COMMIT')
        assert sorted(data) == sorted(result)
    return f


@pytest.fixture
def assert_dict_records(connection):
    @asyncio.coroutine
    def f(data):
        cursor = yield from connection.cursor(DictCursor)
        yield from cursor.execute(
            "SELECT id, name, age, height FROM bulkinsert")
        result = yield from cursor.fetchall()
        yield from cursor.execute('COMMIT')
        assert sorted(data, key=lambda k: k['id']) == \
            sorted(result, key=lambda k: k['id'])
    return f


@pytest.mark.run_loop
def test_bulk_insert(cursor, table, assert_records):
    data = [(0, "bob", 21, 123), (1, "jim", 56, 45), (2, "fred", 100, 180)]
    yield from cursor.executemany(
        "INSERT INTO bulkinsert (id, name, age, height) "
        "VALUES (%s,%s,%s,%s)", data)
    expected = bytearray(b"INSERT INTO bulkinsert (id, name, age, height) "
                         b"VALUES (0,'bob',21,123),(1,'jim',56,45),"
                         b"(2,'fred',100,180)")
    assert cursor._last_executed == expected
    yield from cursor.execute('commit')
    yield from assert_records(data)


@pytest.mark.run_loop
def test_bulk_insert_multiline_statement(cursor, table, assert_records):
    data = [(0, "bob", 21, 123), (1, "jim", 56, 45), (2, "fred", 100, 180)]
    yield from cursor.executemany("""insert
        into bulkinsert (id, name,
        age, height)
        values (%s,
        %s , %s,
        %s )
         """, data)
    assert cursor._last_executed.strip() == bytearray(b"""insert
        into bulkinsert (id, name,
        age, height)
        values (0,
        'bob' , 21,
        123 ),(1,
        'jim' , 56,
        45 ),(2,
        'fred' , 100,
        180 )""")
    yield from cursor.execute('COMMIT')
    yield from assert_records(data)


@pytest.mark.run_loop
def test_bulk_insert_single_record(cursor, table, assert_records):
    data = [(0, "bob", 21, 123)]
    yield from cursor.executemany(
        "insert into bulkinsert (id, name, age, height) "
        "values (%s,%s,%s,%s)", data)
    yield from cursor.execute('COMMIT')
    yield from assert_records(data)


@pytest.mark.run_loop
def test_insert_on_duplicate_key_update(cursor, table, assert_records):
    # executemany should work with "insert ... on update" "
    data = [(0, "bob", 21, 123), (1, "jim", 56, 45), (2, "fred", 100, 180)]
    yield from cursor.executemany("""insert
        into bulkinsert (id, name,
        age, height)
        values (%s,
        %s , %s,
        %s ) on duplicate key update
        age = values(age)
         """, data)
    assert cursor._last_executed.strip() == bytearray(b"""insert
        into bulkinsert (id, name,
        age, height)
        values (0,
        'bob' , 21,
        123 ),(1,
        'jim' , 56,
        45 ),(2,
        'fred' , 100,
        180 ) on duplicate key update
        age = values(age)""")
    yield from cursor.execute('COMMIT')
    yield from assert_records(data)


@pytest.mark.run_loop
def test_bulk_insert_with_params_as_dict(cursor, table, assert_dict_records):
    data = [
        {
            'id': 0,
            'name': "bob",
            'age': 21,
            'height': 123
        },
        {
            'id': 1,
            'name': "jim",
            'age': 56,
            'height': 45
        },
        {
            'id': 2,
            'name': "fred",
            'age': 100,
            'height': 180
        },
    ]
    yield from cursor.executemany(
        "INSERT INTO bulkinsert (id, name, age, height) "
        "VALUES (%(id)s,%(name)s,%(age)s,%(height)s)", data)
    expected = bytearray(b"INSERT INTO bulkinsert (id, name, age, height) "
                         b"VALUES (0,'bob',21,123),(1,'jim',56,45),"
                         b"(2,'fred',100,180)")
    assert cursor._last_executed == expected
    yield from cursor.execute('commit')
    yield from assert_dict_records(data)


@pytest.mark.run_loop
def test_bulk_insert_with_precedence_spaces(cursor, table, assert_records):
    data = [(0, "bob", 21, 123), (1, "jim", 56, 45)]
    yield from cursor.executemany("""
        INSERT INTO bulkinsert (id, name, age, height)
        VALUES (%s,%s,%s,%s)
    """, data)
    expected = bytearray(b"INSERT INTO bulkinsert (id, name, age, height)"
                         b"\n        VALUES (0,\'bob\',21,123),"
                         b"(1,\'jim\',56,45)\n    ")
    assert cursor._last_executed == expected
    yield from cursor.execute('commit')
    yield from assert_records(data)


@pytest.mark.run_loop
def test_bulk_replace(cursor, table, assert_records):
    data = [(0, "bob", 21, 123), (0, "jim", 56, 45)]
    sql = ("REPLACE INTO bulkinsert (id, name, age, height) " +
           "VALUES (%s,%s,%s,%s)")
    yield from cursor.executemany(sql, data)
    assert cursor._last_executed.strip() == bytearray(
        b"REPLACE INTO bulkinsert (id, name, age, height) " +
        b"VALUES (0,'bob',21,123),(0,'jim',56,45)"
    )
    yield from cursor.execute('COMMIT')
    yield from assert_records([(0, "jim", 56, 45)])


@pytest.mark.run_loop
def test_bulk_insert_with_semicolon_at_the_end(cursor, table, assert_records):
    data = [(0, "bob", 21, 123), (1, "jim", 56, 45)]
    yield from cursor.executemany(
        "INSERT INTO bulkinsert (id, name, age, height) "
        "VALUES (%s,%s,%s,%s);", data)
    expected = bytearray(b"INSERT INTO bulkinsert (id, name, age, height) "
                         b"VALUES (0,'bob',21,123),(1,'jim',56,45)")
    assert cursor._last_executed == expected
    yield from cursor.execute('commit')
    yield from assert_records(data)
