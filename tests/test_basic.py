import time
import datetime

import pytest
from pymysql import util
from pymysql.err import ProgrammingError


@pytest.mark.run_loop
def test_datatypes(connection, cursor):
    """ test every data type """
    yield from cursor.execute(
        "create table test_datatypes (b bit, i int, l bigint, f real, s "
        "varchar(32), u varchar(32), bb blob, d date, dt datetime, "
        "ts timestamp, td time, t time, st datetime)")
    try:
        # insert values
        v = (
            True, -3, 123456789012, 5.7, "hello'\" world",
            u"Espa\xc3\xb1ol",
            "binary\x00data".encode(connection.charset),
            datetime.date(1988, 2, 2),
            datetime.datetime.now().replace(microsecond=0),
            datetime.timedelta(5, 6), datetime.time(16, 32),
            time.localtime())
        yield from cursor.execute(
            "insert into test_datatypes (b,i,l,f,s,u,bb,d,dt,td,t,st) "
            "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            v)
        yield from cursor.execute(
            "select b,i,l,f,s,u,bb,d,dt,td,t,st from test_datatypes")
        r = yield from cursor.fetchone()
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

        yield from cursor.execute("delete from test_datatypes")

        # check nulls
        yield from cursor.execute(
            "insert into test_datatypes (b,i,l,f,s,u,bb,d,dt,td,t,st) "
            "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            [None] * 12)
        yield from cursor.execute(
            "select b,i,l,f,s,u,bb,d,dt,td,t,st from test_datatypes")
        r = yield from cursor.fetchone()
        assert tuple([None] * 12) == r

        yield from cursor.execute("delete from test_datatypes")

        # check sequence type
        yield from cursor.execute(
            "insert into test_datatypes (i, l) values (2,4), (6,8), "
            "(10,12)")
        yield from cursor.execute(
            "select l from test_datatypes where i in %s order by i",
            ((2, 6),))
        r = yield from cursor.fetchall()
        assert ((4,), (8,)) == r
    finally:
        yield from cursor.execute("drop table test_datatypes")


@pytest.mark.run_loop
def test_dict_escaping(cursor):
    yield from cursor.execute(
        "create table test_dict (a integer, b integer, c integer)")
    try:
        yield from cursor.execute(
            "insert into test_dict (a,b,c) values (%(a)s, %(b)s, %(c)s)",
            {"a": 1, "b": 2, "c": 3})
        yield from cursor.execute("select a,b,c from test_dict")
        r = yield from cursor.fetchone()
        assert (1, 2, 3) == r
    finally:
        yield from cursor.execute("drop table test_dict")


@pytest.mark.run_loop
def test_string(cursor):
    yield from cursor.execute("DROP TABLE IF EXISTS test_dict;")
    yield from cursor.execute("create table test_dict (a text)")
    test_value = "I am a test string"
    try:
        yield from cursor.execute("insert into test_dict (a) values (%s)",
                                  test_value)
        yield from cursor.execute("select a from test_dict")
        r = yield from cursor.fetchone()
        assert (test_value,) == r
    finally:
        yield from cursor.execute("drop table test_dict")


@pytest.mark.run_loop
def test_integer(cursor):
    yield from cursor.execute("create table test_dict (a integer)")
    test_value = 12345
    try:
        yield from cursor.execute("insert into test_dict (a) values (%s)",
                                  test_value)
        yield from cursor.execute("select a from test_dict")
        r = yield from cursor.fetchone()
        assert (test_value,) == r
    finally:
        yield from cursor.execute("drop table test_dict")


@pytest.mark.run_loop
def test_binary_data(cursor):
    data = bytes(bytearray(range(256)) * 4)
    try:
        yield from cursor.execute("create table test_blob (b blob)")
        yield from cursor.execute("insert into test_blob (b) values (%s)",
                                  (data,))
        yield from cursor.execute("select b from test_blob")
        (r,) = yield from cursor.fetchone()
        assert data == r
    finally:
        yield from cursor.execute("drop table test_blob")


@pytest.mark.run_loop
def test_untyped_convertion_to_null_and_empty_string(cursor):
    yield from cursor.execute("select null,''")
    r = yield from cursor.fetchone()
    assert (None, u'') == r
    yield from cursor.execute("select '',null")
    r = yield from cursor.fetchone()
    assert (u'', None) == r


@pytest.mark.run_loop
def test_timedelta_conversion(cursor):
    yield from cursor.execute(
        "select time('12:30'), time('23:12:59'), time('23:12:59.05100'), "
        "time('-12:30'), time('-23:12:59'), time('-23:12:59.05100'), "
        "time('-00:30')")
    r = yield from cursor.fetchone()
    assert (datetime.timedelta(0, 45000),
            datetime.timedelta(0, 83579),
            datetime.timedelta(0, 83579, 51000),
            -datetime.timedelta(0, 45000),
            -datetime.timedelta(0, 83579),
            -datetime.timedelta(0, 83579, 51000),
            -datetime.timedelta(0, 1800)) == r


@pytest.mark.run_loop
def test_datetime_conversion(cursor):
    c = cursor
    dt = datetime.datetime(2013, 11, 12, 9, 9, 9, 123450)
    try:
        yield from c.execute(
            "create table test_datetime (id int, ts datetime(6))")
        yield from c.execute(
            "insert into test_datetime values "
            "(1,'2013-11-12 09:09:09.12345')")
        yield from c.execute("select ts from test_datetime")
        r = yield from c.fetchone()
        assert (dt,) == r
    except ProgrammingError:
        # User is running a version of MySQL that doesn't support
        # msecs within datetime
        pass
    finally:
        yield from c.execute("drop table if exists test_datetime")


@pytest.mark.run_loop
def test_get_transaction_status(connection, cursor):
    #  make sure that connection is clean without transactions
    transaction_flag = connection.get_transaction_status()
    assert not transaction_flag

    # start transaction
    yield from connection.begin()
    # make sure transaction flag is up
    transaction_flag = connection.get_transaction_status()
    assert transaction_flag

    yield from cursor.execute('SELECT 1;')
    (r, ) = yield from cursor.fetchone()
    assert r == 1
    yield from connection.commit()
    # make sure that transaction flag is down
    transaction_flag = connection.get_transaction_status()
    assert not transaction_flag


@pytest.mark.run_loop
def test_rollback(connection, cursor):

    yield from cursor.execute('DROP TABLE IF EXISTS tz_data;')
    yield from cursor.execute('CREATE TABLE tz_data ('
                              'region VARCHAR(64),'
                              'zone VARCHAR(64),'
                              'name VARCHAR(64))')
    yield from connection.commit()

    args = ('America', '', 'America/New_York')
    yield from cursor.execute('INSERT INTO tz_data VALUES (%s, %s, %s)',
                              args)
    yield from cursor.execute('SELECT * FROM tz_data;')
    data = yield from cursor.fetchall()
    assert len(data) == 1

    yield from connection.rollback()
    yield from cursor.execute('SELECT * FROM tz_data;')
    data = yield from cursor.fetchall()

    # should not return any rows since no inserts was commited
    assert len(data) == 0
