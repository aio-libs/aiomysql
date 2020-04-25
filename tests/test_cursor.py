import asyncio

import pytest

from aiomysql import ProgrammingError, Cursor, InterfaceError


async def _prepare(conn):
    cur = await conn.cursor()
    await cur.execute("DROP TABLE IF EXISTS tbl;")

    await cur.execute("""CREATE TABLE tbl (
             id MEDIUMINT NOT NULL AUTO_INCREMENT,
             name VARCHAR(255) NOT NULL,
             PRIMARY KEY (id));""")

    for i in [(1, 'a'), (2, 'b'), (3, 'c')]:
        await cur.execute("INSERT INTO tbl VALUES(%s, %s)", i)
    await cur.execute("DROP TABLE IF EXISTS tbl2")
    await cur.execute("""CREATE TABLE tbl2
                              (id int, name varchar(255))""")
    await conn.commit()


async def _prepare_procedure(conn):
    cur = await conn.cursor()
    await cur.execute("DROP PROCEDURE IF EXISTS myinc;")
    await cur.execute("""CREATE PROCEDURE myinc(p1 INT)
                           BEGIN
                               SELECT p1 + 1;
                           END
                           """)
    await conn.commit()


@pytest.mark.run_loop
async def test_description(connection_creator):
    conn = await connection_creator()
    await _prepare(conn)
    cur = await conn.cursor()
    assert cur.description is None
    await cur.execute('SELECT * from tbl;')

    assert len(cur.description) == 2, \
        'cursor.description describes too many columns'

    assert len(cur.description[0]) == 7, \
        'cursor.description[x] tuples must have 7 elements'

    assert cur.description[0][0].lower() == 'id', \
        'cursor.description[x][0] must return column name'

    assert cur.description[1][0].lower() == 'name', \
        'cursor.description[x][0] must return column name'

    # Make sure self.description gets reset, cursor should be
    # set to None in case of none resulting queries like DDL
    await cur.execute('DROP TABLE IF EXISTS foobar;')
    assert cur.description is None


@pytest.mark.run_loop
async def test_cursor_properties(connection_creator):
    conn = await connection_creator()
    cur = await conn.cursor()
    assert cur.connection is conn
    cur.setinputsizes()
    cur.setoutputsizes()
    assert cur.echo == conn.echo


@pytest.mark.run_loop
async def test_scroll_relative(connection_creator):
    conn = await connection_creator()
    await _prepare(conn)
    cur = await conn.cursor()
    await cur.execute('SELECT * FROM tbl;')
    await cur.scroll(1)
    ret = await cur.fetchone()
    assert (2, 'b') == ret


@pytest.mark.run_loop
async def test_scroll_absolute(connection_creator):
    conn = await connection_creator()
    await _prepare(conn)
    cur = await conn.cursor()
    await cur.execute('SELECT * FROM tbl;')
    await cur.scroll(2, mode='absolute')
    ret = await cur.fetchone()
    assert (3, 'c') == ret


@pytest.mark.run_loop
async def test_scroll_errors(connection_creator):
    conn = await connection_creator()
    cur = await conn.cursor()

    with pytest.raises(ProgrammingError):
        await cur.scroll(2, mode='absolute')

    cur = await conn.cursor()
    await cur.execute('SELECT * FROM tbl;')

    with pytest.raises(ProgrammingError):
        await cur.scroll(2, mode='not_valid_mode')


@pytest.mark.run_loop
async def test_scroll_index_error(connection_creator):
    conn = await connection_creator()
    await _prepare(conn)
    cur = await conn.cursor()
    await cur.execute('SELECT * FROM tbl;')
    with pytest.raises(IndexError):
        await cur.scroll(1000)


@pytest.mark.run_loop
async def test_close(connection_creator):
    conn = await connection_creator()
    cur = await conn.cursor()
    await cur.close()
    assert cur.closed is True
    with pytest.raises(ProgrammingError):
        await cur.execute('SELECT 1')
    # try to close for second time
    await cur.close()


@pytest.mark.run_loop
async def test_arraysize(connection_creator):
    conn = await connection_creator()
    cur = await conn.cursor()
    assert 1 == cur.arraysize
    cur.arraysize = 10
    assert 10 == cur.arraysize


@pytest.mark.run_loop
async def test_rows(connection_creator):
    conn = await connection_creator()
    await _prepare(conn)

    cur = await conn.cursor()
    await cur.execute('SELECT * from tbl')
    assert 3 == cur.rowcount
    assert 0 == cur.rownumber
    await cur.fetchone()
    assert 1 == cur.rownumber
    assert cur.lastrowid is None
    await cur.execute('INSERT INTO tbl VALUES (%s, %s)', (4, 'd'))
    assert 0 != cur.lastrowid
    await conn.commit()


@pytest.mark.run_loop
async def test_callproc(connection_creator):
    conn = await connection_creator()
    await _prepare_procedure(conn)
    cur = await conn.cursor()
    await cur.callproc('myinc', [1])
    ret = await cur.fetchone()
    assert (2,) == ret
    await cur.close()
    with pytest.raises(ProgrammingError):
        await cur.callproc('myinc', [1])
    conn.close()


@pytest.mark.run_loop
async def test_fetchone_no_result(connection_creator):
    # test a fetchone() with no rows
    conn = await connection_creator()
    c = await conn.cursor()
    await c.execute("create table test_nr (b varchar(32))")
    try:
        data = "pymysql"
        await c.execute("insert into test_nr (b) values (%s)", (data,))
        r = await c.fetchone()
        assert r is None
    finally:
        await c.execute("drop table test_nr")


@pytest.mark.run_loop
async def test_fetchmany_no_result(connection_creator):
    conn = await connection_creator()
    cur = await conn.cursor()
    await cur.execute('DROP TABLE IF EXISTS foobar;')
    r = await cur.fetchmany()
    assert [] == r


@pytest.mark.run_loop
async def test_fetchall_no_result(connection_creator):
    # test a fetchone() with no rows
    conn = await connection_creator()
    cur = await conn.cursor()
    await cur.execute('DROP TABLE IF EXISTS foobar;')
    r = await cur.fetchall()
    assert [] == r


@pytest.mark.run_loop
async def test_fetchall_with_scroll(connection_creator):
    conn = await connection_creator()
    await _prepare(conn)
    cur = await conn.cursor()
    await cur.execute('SELECT * FROM tbl;')
    await cur.scroll(1)
    ret = await cur.fetchall()
    assert ((2, 'b'), (3, 'c')) == ret


@pytest.mark.run_loop
async def test_aggregates(connection_creator):
    """ test aggregate functions """
    conn = await connection_creator()
    c = await conn.cursor()
    try:
        await c.execute('create table test_aggregates (i integer)')
        for i in range(0, 10):
            await c.execute(
                'insert into test_aggregates (i) values (%s)', (i,))
        await c.execute('select sum(i) from test_aggregates')
        r, = await c.fetchone()
        assert sum(range(0, 10)) == r
    finally:
        await c.execute('drop table test_aggregates')


@pytest.mark.run_loop
async def test_single_tuple(connection_creator):
    """ test a single tuple """
    conn = await connection_creator()
    c = await conn.cursor()
    try:
        await c.execute(
            "create table mystuff (id integer primary key)")
        await c.execute("insert into mystuff (id) values (1)")
        await c.execute("insert into mystuff (id) values (2)")
        await c.execute("select id from mystuff where id in %s", ((1,),))
        r = await c.fetchall()
        assert [(1,)] == list(r)
    finally:
        await c.execute("drop table mystuff")


@pytest.mark.run_loop
async def test_executemany(connection_creator):
    conn = await connection_creator()
    await _prepare(conn)
    cur = await conn.cursor()
    assert cur.description is None
    args = [1, 2, 3]
    row_count = await cur.executemany(
        'SELECT * FROM tbl WHERE id  = %s;', args)
    assert row_count == 3
    r = await cur.fetchall()
    # TODO: if this right behaviour
    assert ((3, 'c'),) == r

    # calling execute many without args
    row_count = await cur.executemany('SELECT 1;', ())
    assert row_count is None


@pytest.mark.run_loop
async def test_custom_cursor(connection_creator):
    class MyCursor(Cursor):
        pass
    conn = await connection_creator()
    cur = await conn.cursor(MyCursor)
    assert isinstance(cur, MyCursor)
    await cur.execute("SELECT 42;")
    (r, ) = await cur.fetchone()
    assert r == 42


@pytest.mark.run_loop
async def test_custom_cursor_not_cursor_subclass(connection_creator):
    class MyCursor2:
        pass
    conn = await connection_creator()
    with pytest.raises(TypeError):
        await conn.cursor(MyCursor2)


@pytest.mark.run_loop
async def test_morgify(connection_creator):
    conn = await connection_creator()
    cur = await conn.cursor()
    pairs = [(1, 'a'), (2, 'b'), (3, 'c')]
    sql = "INSERT INTO tbl VALUES(%s, %s)"
    results = [cur.mogrify(sql, p) for p in pairs]
    expected = ["INSERT INTO tbl VALUES(1, 'a')",
                "INSERT INTO tbl VALUES(2, 'b')",
                "INSERT INTO tbl VALUES(3, 'c')"]
    assert results == expected


@pytest.mark.run_loop
async def test_execute_cancel(connection_creator):
    conn = await connection_creator()
    cur = await conn.cursor()
    # Cancel a cursor in the middle of execution, before it could
    # read even the first packet (SLEEP assures the timings)
    task = asyncio.ensure_future(cur.execute(
        "SELECT 1 as id, SLEEP(0.1) as xxx"))
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    with pytest.raises(InterfaceError):
        await conn.cursor()
