import datetime

import pytest

import aiomysql.cursors


BOB = {'name': 'bob', 'age': 21,
       'DOB': datetime.datetime(1990, 2, 6, 23, 4, 56)}
JIM = {'name': 'jim', 'age': 56,
       'DOB': datetime.datetime(1955, 5, 9, 13, 12, 45)}
FRED = {'name': 'fred', 'age': 100,
        'DOB': datetime.datetime(1911, 9, 12, 1, 1, 1)}

CURSOR_TYPE = aiomysql.cursors.DictCursor


async def prepare(conn):
    c = await conn.cursor(CURSOR_TYPE)

    # create a table ane some data to query
    await c.execute("drop table if exists dictcursor")
    await c.execute(
        """CREATE TABLE dictcursor (name char(20), age int ,
        DOB datetime)""")
    data = [("bob", 21, "1990-02-06 23:04:56"),
            ("jim", 56, "1955-05-09 13:12:45"),
            ("fred", 100, "1911-09-12 01:01:01")]
    await c.executemany("insert into dictcursor values "
                        "(%s,%s,%s)", data)


@pytest.mark.run_loop
async def test_dictcursor(connection):
    conn = connection
    await prepare(connection)

    bob, jim, fred = BOB.copy(), JIM.copy(), FRED.copy()
    # all assert test compare to the structure as would come
    # out from MySQLdb
    c = await conn.cursor(CURSOR_TYPE)

    # try an update which should return no rows
    await c.execute("update dictcursor set age=20 where name='bob'")
    bob['age'] = 20
    # pull back the single row dict for bob and check
    await c.execute("SELECT * from dictcursor where name='bob'")
    r = await c.fetchone()
    assert bob == r, "fetchone via DictCursor failed"
    # same again, but via fetchall => tuple)
    await c.execute("SELECT * from dictcursor where name='bob'")
    r = await c.fetchall()
    assert [bob] == r, \
        "fetch a 1 row result via fetchall failed via DictCursor"

    # get all 3 row via fetchall
    await c.execute("SELECT * from dictcursor")
    r = await c.fetchall()
    assert [bob, jim, fred] == r, "fetchall failed via DictCursor"

    # get all 2 row via fetchmany
    await c.execute("SELECT * from dictcursor")
    r = await c.fetchmany(2)
    assert [bob, jim] == r, "fetchmany failed via DictCursor"
    await c.execute('commit')


@pytest.mark.run_loop
async def test_custom_dict(connection):
    conn = connection
    await prepare(connection)

    class MyDict(dict):
        pass

    class MyDictCursor(CURSOR_TYPE):
        dict_type = MyDict

    keys = ['name', 'age', 'DOB']
    bob = MyDict([(k, BOB[k]) for k in keys])
    jim = MyDict([(k, JIM[k]) for k in keys])
    fred = MyDict([(k, FRED[k]) for k in keys])

    cur = await conn.cursor(MyDictCursor)
    await cur.execute("SELECT * FROM dictcursor WHERE name='bob'")
    r = await cur.fetchone()
    assert bob == r, "fetchone() returns MyDictCursor"

    await cur.execute("SELECT * FROM dictcursor")
    r = await cur.fetchall()
    assert [bob, jim, fred] == r, "fetchall failed via MyDictCursor"

    await cur.execute("SELECT * FROM dictcursor")
    r = await cur.fetchmany(2)
    assert [bob, jim] == r, "list failed via MyDictCursor"


@pytest.mark.run_loop
async def test_ssdictcursor(connection):
    conn = connection
    await prepare(connection)

    c = await conn.cursor(aiomysql.cursors.SSDictCursor)
    await c.execute("SELECT * from dictcursor where name='bob'")
    r = await c.fetchall()
    assert [BOB] == r,\
        "fetch a 1 row result via fetchall failed via DictCursor"
