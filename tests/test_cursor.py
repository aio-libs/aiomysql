import asyncio

import pytest

from aiomysql import ProgrammingError, Cursor, InterfaceError, OperationalError
from aiomysql.cursors import RE_INSERT_VALUES


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
    await _prepare(conn)
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


@pytest.mark.run_loop
async def test_execute_percentage(connection_creator):
    # %% in column set
    conn = await connection_creator()
    async with conn.cursor() as cur:
        await cur.execute("DROP TABLE IF EXISTS percent_test")
        await cur.execute("""\
            CREATE TABLE percent_test (
                `A%` INTEGER,
                `B%` INTEGER)""")

        q = "INSERT INTO percent_test (`A%%`, `B%%`) VALUES (%s, %s)"

        await cur.execute(q, (3, 4))


@pytest.mark.run_loop
async def test_executemany_percentage(connection_creator):
    # %% in column set
    conn = await connection_creator()
    async with conn.cursor() as cur:
        await cur.execute("DROP TABLE IF EXISTS percent_test")
        await cur.execute("""\
            CREATE TABLE percent_test (
                `A%` INTEGER,
                `B%` INTEGER)""")

        q = "INSERT INTO percent_test (`A%%`, `B%%`) VALUES (%s, %s)"

        assert RE_INSERT_VALUES.match(q) is not None
        await cur.executemany(q, [(3, 4), (5, 6)])
        assert cur._last_executed.endswith(b"(3, 4),(5, 6)"), \
            "executemany with %% not in one query"


@pytest.mark.run_loop
async def test_max_execution_time(mysql_server, connection_creator):
    conn = await connection_creator()
    await _prepare(conn)
    async with conn.cursor() as cur:
        # MySQL MAX_EXECUTION_TIME takes ms
        # MariaDB max_statement_time takes seconds as int/float, introduced in 10.1

        # this will sleep 0.01 seconds per row
        if mysql_server["db_type"] == "mysql":
            sql = """
                  SELECT /*+ MAX_EXECUTION_TIME(2000) */
                  name, sleep(0.01) FROM tbl
                  """
        else:
            sql = """
                  SET STATEMENT max_statement_time=2 FOR
                  SELECT name, sleep(0.01) FROM tbl
                  """

        await cur.execute(sql)
        # unlike SSCursor, Cursor returns a tuple of tuples here
        assert (await cur.fetchall()) == (
            ("a", 0),
            ("b", 0),
            ("c", 0),
        )

        if mysql_server["db_type"] == "mysql":
            sql = """
                  SELECT /*+ MAX_EXECUTION_TIME(2000) */
                  name, sleep(0.01) FROM tbl
                  """
        else:
            sql = """
                  SET STATEMENT max_statement_time=2 FOR
                  SELECT name, sleep(0.01) FROM tbl
                  """
        await cur.execute(sql)
        assert (await cur.fetchone()) == ("a", 0)

        # this discards the previous unfinished query
        await cur.execute("SELECT 1")
        assert (await cur.fetchone()) == (1,)

        if mysql_server["db_type"] == "mysql":
            sql = """
                  SELECT /*+ MAX_EXECUTION_TIME(1) */
                  name, sleep(1) FROM tbl
                  """
        else:
            sql = """
                  SET STATEMENT max_statement_time=0.001 FOR
                  SELECT name, sleep(1) FROM tbl
                  """
        with pytest.raises(OperationalError) as cm:
            # in a buffered cursor this should reliably raise an
            # OperationalError
            await cur.execute(sql)

        if mysql_server["db_type"] == "mysql":
            # this constant was only introduced in MySQL 5.7, not sure
            # what was returned before, may have been ER_QUERY_INTERRUPTED

            # this constant is pending a new PyMySQL release
            # assert cm.value.args[0] == pymysql.constants.ER.QUERY_TIMEOUT
            assert cm.value.args[0] == 3024
        else:
            # this constant is pending a new PyMySQL release
            # assert cm.value.args[0] == pymysql.constants.ER.STATEMENT_TIMEOUT
            assert cm.value.args[0] == 1969

        # connection should still be fine at this point
        await cur.execute("SELECT 1")
        assert (await cur.fetchone()) == (1,)
