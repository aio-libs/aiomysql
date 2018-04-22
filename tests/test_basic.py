import datetime
import json
import re
import time

import pytest
from pymysql import util
from pymysql.err import ProgrammingError


@pytest.fixture
def datatype_table(loop, cursor, table_cleanup):
    async def f():
        await cursor.execute(
            "CREATE TABLE test_datatypes (b bit, i int, l bigint, f real, s "
            "varchar(32), u varchar(32), bb blob, d date, dt datetime, "
            "ts timestamp, td time, t time, st datetime)")
        table_cleanup('test_datatypes')
    loop.run_until_complete(f())
    table_cleanup('test_datatypes')


@pytest.mark.run_loop
async def test_datatypes(connection, cursor, datatype_table):
    # insert values
    v = (
        True, -3, 123456789012, 5.7, "hello'\" world",
        u"Espa\xc3\xb1ol",
        "binary\x00data".encode(connection.charset),
        datetime.date(1988, 2, 2),
        datetime.datetime.now().replace(microsecond=0),
        datetime.timedelta(5, 6), datetime.time(16, 32),
        time.localtime())
    await cursor.execute(
        "INSERT INTO test_datatypes (b,i,l,f,s,u,bb,d,dt,td,t,st) "
        "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        v)
    await cursor.execute(
        "select b,i,l,f,s,u,bb,d,dt,td,t,st from test_datatypes")
    r = await cursor.fetchone()
    assert util.int2byte(1) == r[0]
    # assert v[1:8] == r[1:8])
    assert v[1:9] == r[1:9]
    # mysql throws away microseconds so we need to check datetimes
    # specially. additionally times are turned into timedeltas.
    # self.assertEqual(datetime.datetime(*v[8].timetuple()[:6]), r[8])

    # TODO: figure out why this assert fails
    # assert [9] == r[9]  # just timedeltas
    expected = datetime.timedelta(0, 60 * (v[10].hour * 60 + v[10].minute))
    assert expected == r[10]
    assert datetime.datetime(*v[-1][:6]) == r[-1]


@pytest.mark.run_loop
async def test_datatypes_nulls(cursor, datatype_table):
    # check nulls
    await cursor.execute(
        "insert into test_datatypes (b,i,l,f,s,u,bb,d,dt,td,t,st) "
        "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        [None] * 12)
    await cursor.execute(
        "select b,i,l,f,s,u,bb,d,dt,td,t,st from test_datatypes")
    r = await cursor.fetchone()
    assert tuple([None] * 12) == r


@pytest.mark.run_loop
async def test_datatypes_sequence_types(cursor, datatype_table):
    # check sequence type
    await cursor.execute(
        "INSERT INTO test_datatypes (i, l) VALUES (2,4), (6,8), "
        "(10,12)")
    await cursor.execute(
        "select l from test_datatypes where i in %s order by i",
        ((2, 6),))
    r = await cursor.fetchall()
    assert ((4,), (8,)) == r


@pytest.mark.run_loop
async def test_dict_escaping(cursor, table_cleanup):
    sql = "CREATE TABLE test_dict (a INTEGER, b INTEGER, c INTEGER)"
    await cursor.execute(sql)
    table_cleanup('test_dict')
    sql = "INSERT INTO test_dict (a,b,c) VALUES (%(a)s, %(b)s, %(c)s)"
    await cursor.execute(sql, {"a": 1, "b": 2, "c": 3})
    await cursor.execute("SELECT a,b,c FROM test_dict")
    r = await cursor.fetchone()
    assert (1, 2, 3) == r


@pytest.mark.run_loop
async def test_string(cursor, table_cleanup):
    await cursor.execute("DROP TABLE IF EXISTS test_string;")
    await cursor.execute("CREATE TABLE test_string (a text)")
    test_value = "I am a test string"
    table_cleanup('test_string')
    await cursor.execute("INSERT INTO test_string (a) VALUES (%s)",
                         test_value)
    await cursor.execute("SELECT a FROM test_string")
    r = await cursor.fetchone()
    assert (test_value,) == r


@pytest.mark.run_loop
async def test_integer(cursor, table_cleanup):
    await cursor.execute("CREATE TABLE test_integer (a INTEGER)")
    table_cleanup('test_integer')
    test_value = 12345
    await cursor.execute("INSERT INTO test_integer (a) VALUES (%s)",
                         test_value)
    await cursor.execute("SELECT a FROM test_integer")
    r = await cursor.fetchone()
    assert (test_value,) == r


@pytest.mark.run_loop
async def test_binary_data(cursor, table_cleanup):
    data = bytes(bytearray(range(256)) * 4)
    await cursor.execute("CREATE TABLE test_blob (b blob)")
    table_cleanup('test_blob')
    await cursor.execute("INSERT INTO test_blob (b) VALUES (%s)",
                         (data,))
    await cursor.execute("SELECT b FROM test_blob")
    (r,) = await cursor.fetchone()
    assert data == r


@pytest.mark.run_loop
async def test_untyped_convertion_to_null_and_empty_string(cursor):
    await cursor.execute("select null,''")
    r = await cursor.fetchone()
    assert (None, u'') == r
    await cursor.execute("select '',null")
    r = await cursor.fetchone()
    assert (u'', None) == r


@pytest.mark.run_loop
async def test_timedelta_conversion(cursor):
    await cursor.execute(
        "select time('12:30'), time('23:12:59'), time('23:12:59.05100'), "
        "time('-12:30'), time('-23:12:59'), time('-23:12:59.05100'), "
        "time('-00:30')")
    r = await cursor.fetchone()
    assert (datetime.timedelta(0, 45000),
            datetime.timedelta(0, 83579),
            datetime.timedelta(0, 83579, 51000),
            -datetime.timedelta(0, 45000),
            -datetime.timedelta(0, 83579),
            -datetime.timedelta(0, 83579, 51000),
            -datetime.timedelta(0, 1800)) == r


@pytest.mark.run_loop
async def test_datetime_conversion(cursor, table_cleanup):
    dt = datetime.datetime(2013, 11, 12, 9, 9, 9, 123450)
    try:
        await cursor.execute("CREATE TABLE test_datetime"
                             "(id INT, ts DATETIME(6))")
        table_cleanup('test_datetime')
        await cursor.execute("INSERT INTO test_datetime VALUES "
                             "(1,'2013-11-12 09:09:09.12345')")
        await cursor.execute("SELECT ts FROM test_datetime")
        r = await cursor.fetchone()
        assert (dt,) == r
    except ProgrammingError:
        # User is running a version of MySQL that doesn't support
        # msecs within datetime
        pass


@pytest.mark.run_loop
async def test_get_transaction_status(connection, cursor):
    #  make sure that connection is clean without transactions
    transaction_flag = connection.get_transaction_status()
    assert not transaction_flag

    # start transaction
    await connection.begin()
    # make sure transaction flag is up
    transaction_flag = connection.get_transaction_status()
    assert transaction_flag

    await cursor.execute('SELECT 1;')
    (r, ) = await cursor.fetchone()
    assert r == 1
    await connection.commit()
    # make sure that transaction flag is down
    transaction_flag = connection.get_transaction_status()
    assert not transaction_flag


@pytest.mark.run_loop
async def test_rollback(connection, cursor):
    await cursor.execute('DROP TABLE IF EXISTS tz_data;')
    await cursor.execute('CREATE TABLE tz_data ('
                         'region VARCHAR(64),'
                         'zone VARCHAR(64),'
                         'name VARCHAR(64))')
    await connection.commit()

    args = ('America', '', 'America/New_York')
    await cursor.execute('INSERT INTO tz_data VALUES (%s, %s, %s)',
                         args)
    await cursor.execute('SELECT * FROM tz_data;')
    data = await cursor.fetchall()
    assert len(data) == 1

    await connection.rollback()
    await cursor.execute('SELECT * FROM tz_data;')
    data = await cursor.fetchall()

    # should not return any rows since no inserts was commited
    assert len(data) == 0


def mysql_server_is(server_version, version_tuple):
    """Return True if the given connection is on the version given or
    greater.
    e.g.::
        if self.mysql_server_is(conn, (5, 6, 4)):
            # do something for MySQL 5.6.4 and above
    """
    server_version_tuple = tuple(
        (int(dig) if dig is not None else 0)
        for dig in
        re.match(r'(\d+)\.(\d+)\.(\d+)', server_version).group(1, 2, 3)
    )
    return server_version_tuple >= version_tuple


@pytest.mark.run_loop
async def test_json(connection_creator, table_cleanup):
    connection = await connection_creator(
        charset="utf8mb4", autocommit=True)
    server_info = connection.get_server_info()
    if not mysql_server_is(server_info, (5, 7, 0)):
        raise pytest.skip("JSON type is not supported on MySQL <= 5.6")

    cursor = await connection.cursor()
    await cursor.execute("""\
    CREATE TABLE test_json (
        id INT NOT NULL,
        json JSON NOT NULL,
        PRIMARY KEY (id)
    );""")
    table_cleanup("test_json")
    json_str = '{"hello": "こんにちは"}'
    await cursor.execute(
        "INSERT INTO test_json (id, `json`) values (42, %s)", (json_str,))
    await cursor.execute("SELECT `json` from `test_json` WHERE `id`=42")

    r = await cursor.fetchone()
    assert json.loads(r[0]) == json.loads(json_str)

    await cursor.execute("SELECT CAST(%s AS JSON) AS x", (json_str,))
    r = await cursor.fetchone()
    assert json.loads(r[0]) == json.loads(json_str)
