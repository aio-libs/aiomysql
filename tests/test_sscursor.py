import asyncio

import pytest
from pymysql import NotSupportedError

from aiomysql import ProgrammingError, InterfaceError
from aiomysql.cursors import SSCursor


DATA = [
    ('America', '', 'America/Jamaica'),
    ('America', '', 'America/Los_Angeles'),
    ('America', '', 'America/Lima'),
    ('America', '', 'America/New_York'),
    ('America', '', 'America/Menominee'),
    ('America', '', 'America/Havana'),
    ('America', '', 'America/El_Salvador'),
    ('America', '', 'America/Costa_Rica'),
    ('America', '', 'America/Denver'),
    ('America', '', 'America/Detroit'), ]


async def _prepare(conn):
    cursor = await conn.cursor()
    # Create table
    await cursor.execute('DROP TABLE IF EXISTS tz_data;')
    await cursor.execute('CREATE TABLE tz_data ('
                         'region VARCHAR(64),'
                         'zone VARCHAR(64),'
                         'name VARCHAR(64))')

    await cursor.executemany(
        'INSERT INTO tz_data VALUES (%s, %s, %s)', DATA)
    await conn.commit()
    await cursor.close()


@pytest.mark.run_loop
async def test_ssursor(connection):
    # affected_rows = 18446744073709551615
    conn = connection
    cursor = await conn.cursor(SSCursor)
    # Create table
    await cursor.execute('DROP TABLE IF EXISTS tz_data;')
    await cursor.execute(('CREATE TABLE tz_data ('
                          'region VARCHAR(64),'
                          'zone VARCHAR(64),'
                          'name VARCHAR(64))'))

    # Test INSERT
    for i in DATA:
        await cursor.execute(
            'INSERT INTO tz_data VALUES (%s, %s, %s)', i)
        assert conn.affected_rows() == 1, 'affected_rows does not match'
    await conn.commit()

    # Test update, affected_rows()
    await cursor.execute('UPDATE tz_data SET zone = %s', ['Foo'])
    await conn.commit()

    assert cursor.rowcount == len(DATA), \
        'Update failed. affected_rows != %s' % (str(len(DATA)))

    await cursor.close()
    await cursor.close()


@pytest.mark.run_loop
async def test_sscursor_fetchall(connection):
    conn = connection
    cursor = await conn.cursor(SSCursor)

    await _prepare(conn)
    await cursor.execute('SELECT * FROM tz_data')
    fetched_data = await cursor.fetchall()
    assert len(fetched_data) == len(DATA), \
        'fetchall failed. Number of rows does not match'


@pytest.mark.run_loop
async def test_sscursor_fetchmany(connection):
    conn = connection
    cursor = await conn.cursor(SSCursor)
    await _prepare(conn)
    await cursor.execute('SELECT * FROM tz_data')
    fetched_data = await cursor.fetchmany(2)
    assert len(fetched_data) == 2, \
        'fetchmany failed. Number of rows does not match'

    await cursor.close()
    # test default fetchmany size
    cursor = await conn.cursor(SSCursor)
    await cursor.execute('SELECT * FROM tz_data;')
    fetched_data = await cursor.fetchmany()
    assert len(fetched_data) == 1


@pytest.mark.run_loop
async def test_sscursor_executemany(connection):
    conn = connection
    await _prepare(conn)
    cursor = await conn.cursor(SSCursor)
    # Test executemany
    await cursor.executemany(
        'INSERT INTO tz_data VALUES (%s, %s, %s)', DATA)
    msg = 'executemany failed. cursor.rowcount != %s'
    assert cursor.rowcount == len(DATA), msg % (str(len(DATA)))


@pytest.mark.run_loop
async def test_sscursor_scroll_relative(connection):
    conn = connection
    await _prepare(conn)
    cursor = await conn.cursor(SSCursor)
    await cursor.execute('SELECT * FROM tz_data;')
    await cursor.scroll(1)
    ret = await cursor.fetchone()
    assert ('America', '', 'America/Los_Angeles') == ret


@pytest.mark.run_loop
async def test_sscursor_scroll_absolute(connection):
    conn = connection
    await _prepare(conn)
    cursor = await conn.cursor(SSCursor)
    await cursor.execute('SELECT * FROM tz_data;')
    await cursor.scroll(2, mode='absolute')
    ret = await cursor.fetchone()
    assert ('America', '', 'America/Lima') == ret


@pytest.mark.run_loop
async def test_sscursor_scroll_errors(connection):
    conn = connection
    cursor = await conn.cursor(SSCursor)

    await cursor.execute('SELECT * FROM tz_data;')

    with pytest.raises(NotSupportedError):
        await cursor.scroll(-2, mode='relative')

    await cursor.scroll(2, mode='absolute')

    with pytest.raises(NotSupportedError):
        await cursor.scroll(1, mode='absolute')
    with pytest.raises(ProgrammingError):
        await cursor.scroll(2, mode='not_valid_mode')


@pytest.mark.run_loop
async def test_sscursor_cancel(connection):
    conn = connection
    cur = await conn.cursor(SSCursor)
    # Prepare ALOT of data

    await cur.execute('DROP TABLE IF EXISTS long_seq;')
    await cur.execute(
        """ CREATE TABLE long_seq (
              id int(11)
            )
        """)

    ids = [(x) for x in range(100000)]
    await cur.executemany('INSERT INTO long_seq VALUES (%s)', ids)

    # Will return several results. All we need at this point
    big_str = "x" * 10000
    await cur.execute(
        """SELECT '{}' as id FROM long_seq;
        """.format(big_str))
    first = await cur.fetchone()
    assert first == (big_str,)

    async def read_cursor():
        while True:
            res = await cur.fetchone()
            if res is None:
                break
    task = asyncio.ensure_future(read_cursor())
    await asyncio.sleep(0)
    assert not task.done(), "Test failed to produce needed condition."
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    with pytest.raises(InterfaceError):
        await conn.cursor(SSCursor)
