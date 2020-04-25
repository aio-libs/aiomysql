import copy

import aiomysql.cursors

import pytest


BOB = ("bob", 21, {"k1": "pretty", "k2": [18, 25]})
JIM = ("jim", 56, {"k1": "rich", "k2": [20, 60]})
FRED = ("fred", 100, {"k1": "longevity", "k2": [100, 160]})


@pytest.fixture()
async def prepare(connection):

    havejson = True

    c = await connection.cursor(aiomysql.cursors.DeserializationCursor)

    # create a table ane some data to query
    await c.execute("drop table if exists deserialize_cursor")
    await c.execute("select VERSION()")
    v = await c.fetchone()
    version, *db_type = v[0].split('-', 1)
    version = float(".".join(version.split('.', 2)[:2]))
    ismariadb = db_type and 'mariadb' in db_type[0].lower()
    if ismariadb or version < 5.7:
        await c.execute(
            """CREATE TABLE deserialize_cursor
             (name char(20), age int , claim text)""")
        havejson = False
    else:
        await c.execute(
            """CREATE TABLE deserialize_cursor
             (name char(20), age int , claim json)""")
    data = [("bob", 21, '{"k1": "pretty", "k2": [18, 25]}'),
            ("jim", 56, '{"k1": "rich", "k2": [20, 60]}'),
            ("fred", 100, '{"k1": "longevity", "k2": [100, 160]}')]
    await c.executemany("insert into deserialize_cursor values "
                        "(%s,%s,%s)", data)

    return havejson


@pytest.mark.run_loop
async def test_deserialize_cursor(prepare, connection):
    havejson = await prepare
    if not havejson:
        return
    bob, jim, fred = copy.deepcopy(BOB), copy.deepcopy(
        JIM), copy.deepcopy(FRED)
    # all assert test compare to the structure as would come
    # out from MySQLdb
    conn = connection
    c = await conn.cursor(aiomysql.cursors.DeserializationCursor)

    # pull back the single row dict for bob and check
    await c.execute("SELECT * from deserialize_cursor "
                    "where name='bob'")
    r = await c.fetchone()
    assert bob == r, "fetchone via DeserializeCursor failed"
    # same again, but via fetchall => tuple)
    await c.execute("SELECT * from deserialize_cursor "
                    "where name='bob'")
    r = await c.fetchall()
    assert [bob] == r, \
        "fetch a 1 row result via fetchall failed via DeserializeCursor"
    # get all 3 row via fetchall
    await c.execute("SELECT * from deserialize_cursor")
    r = await c.fetchall()
    assert [bob, jim, fred] == r, "fetchall failed via DictCursor"

    # get all 2 row via fetchmany
    await c.execute("SELECT * from deserialize_cursor")
    r = await c.fetchmany(2)
    assert [bob, jim] == r, "fetchmany failed via DictCursor"
    await c.execute('commit')


@pytest.mark.run_loop
async def test_deserialize_cursor_low_version(prepare, connection):
    havejson = await prepare
    if havejson:
        return
    bob = ("bob", 21, '{"k1": "pretty", "k2": [18, 25]}')
    jim = ("jim", 56, '{"k1": "rich", "k2": [20, 60]}')
    fred = ("fred", 100, '{"k1": "longevity", "k2": [100, 160]}')
    # all assert test compare to the structure as would come
    # out from MySQLdb
    conn = connection
    c = await conn.cursor(aiomysql.cursors.DeserializationCursor)

    # pull back the single row dict for bob and check
    await c.execute("SELECT * from deserialize_cursor where name='bob'")
    r = await c.fetchone()
    assert bob == r, "fetchone via DeserializeCursor failed"
    # same again, but via fetchall => tuple)
    await c.execute("SELECT * from deserialize_cursor "
                    "where name='bob'")
    r = await c.fetchall()
    assert [bob] == r, \
        "fetch a 1 row result via fetchall failed via DeserializeCursor"
    # get all 3 row via fetchall
    await c.execute("SELECT * from deserialize_cursor")
    r = await c.fetchall()
    assert [bob, jim, fred] == r, "fetchall failed via DictCursor"

    # get all 2 row via fetchmany
    await c.execute("SELECT * from deserialize_cursor")
    r = await c.fetchmany(2)
    assert [bob, jim] == r, "fetchmany failed via DictCursor"
    await c.execute('commit')


@pytest.mark.run_loop
async def test_deserializedictcursor(prepare, connection):
    havejson = await prepare
    if not havejson:
        return
    bob = {'name': 'bob', 'age': 21,
           'claim': {"k1": "pretty", "k2": [18, 25]}}
    conn = connection
    c = await conn.cursor(aiomysql.cursors.DeserializationCursor,
                          aiomysql.cursors.DictCursor)
    await c.execute("SELECT * from deserialize_cursor "
                    "where name='bob'")
    r = await c.fetchall()
    assert [bob] == r, \
        "fetch a 1 row result via fetchall failed via DeserializationCursor"


@pytest.mark.run_loop
async def test_ssdeserializecursor(prepare, connection):
    havejson = await prepare
    if not havejson:
        return
    conn = connection
    c = await conn.cursor(aiomysql.cursors.SSCursor,
                          aiomysql.cursors.DeserializationCursor)
    await c.execute("SELECT * from deserialize_cursor "
                    "where name='bob'")
    r = await c.fetchall()
    assert [BOB] == r, \
        "fetch a 1 row result via fetchall failed via DeserializationCursor"


@pytest.mark.run_loop
async def test_ssdeserializedictcursor(prepare, connection):
    havejson = await prepare
    if not havejson:
        return
    bob = {'name': 'bob', 'age': 21,
           'claim': {"k1": "pretty", "k2": [18, 25]}}
    conn = connection
    c = await conn.cursor(aiomysql.cursors.SSCursor,
                          aiomysql.cursors.DeserializationCursor,
                          aiomysql.cursors.DictCursor)
    await c.execute("SELECT * from deserialize_cursor "
                    "where name='bob'")
    r = await c.fetchall()
    assert [bob] == r, \
        "fetch a 1 row result via fetchall failed via DeserializationCursor"
