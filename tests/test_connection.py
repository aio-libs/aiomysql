import asyncio
import gc
import os

import pytest
from pymysql.constants import COMMAND

import aiomysql


@pytest.fixture()
def fill_my_cnf(mysql_params):
    tests_root = os.path.abspath(os.path.dirname(__file__))

    if "unix_socket" in mysql_params:
        tmpl_path = "fixtures/my.cnf.unix.tmpl"
    else:
        tmpl_path = "fixtures/my.cnf.tcp.tmpl"

    path1 = os.path.join(tests_root, tmpl_path)
    path2 = os.path.join(tests_root, 'fixtures/my.cnf')
    with open(path1) as f1:
        tmpl = f1.read()
    with open(path2, 'w') as f2:
        f2.write(tmpl.format_map(mysql_params))


@pytest.mark.run_loop
async def test_connect_timeout(connection_creator):
    # OSErrors and asyncio.TimeoutError are caught and raised as operational
    # errors
    with pytest.raises(aiomysql.OperationalError):
        await connection_creator(connect_timeout=0.000000000001)


@pytest.mark.run_loop
async def test_config_file(fill_my_cnf, connection_creator, mysql_params):
    tests_root = os.path.abspath(os.path.dirname(__file__))
    path = os.path.join(tests_root, 'fixtures/my.cnf')
    conn = await connection_creator(read_default_file=path)

    if "unix_socket" in mysql_params:
        assert conn.unix_socket == mysql_params["unix_socket"]
    else:
        assert conn.host == mysql_params['host']
        assert conn.port == mysql_params['port']
    assert conn.user, mysql_params['user']

    # make sure connection is working
    cur = await conn.cursor()
    await cur.execute('SELECT 42;')
    (r, ) = await cur.fetchone()
    assert r == 42
    conn.close()


@pytest.mark.run_loop
async def test_config_file_with_different_group(fill_my_cnf,
                                                connection_creator,
                                                mysql_params):
    # same test with config file but actual settings
    # located in not default group.
    tests_root = os.path.abspath(os.path.dirname(__file__))
    path = os.path.join(tests_root, 'fixtures/my.cnf')
    group = 'client_with_unix_socket'
    conn = await connection_creator(read_default_file=path,
                                    read_default_group=group)

    assert conn.charset == 'utf8'
    assert conn.user == 'root'

    # make sure connection is working
    cur = await conn.cursor()
    await cur.execute('SELECT 42;')
    (r, ) = await cur.fetchone()
    assert r == 42
    conn.close()


@pytest.mark.run_loop
async def test_utf8mb4(connection_creator):
    """This test requires MySQL >= 5.5"""
    charset = 'utf8mb4'
    conn = await connection_creator(charset=charset)
    assert conn.charset == charset
    conn.close()


@pytest.mark.run_loop
async def test_largedata(connection_creator):
    """Large query and response (>=16MB)"""
    conn = await connection_creator()
    cur = await conn.cursor()
    await cur.execute("SELECT @@max_allowed_packet")
    r = await cur.fetchone()
    if r[0] < 16 * 1024 * 1024 + 10:
        pytest.skip('Set max_allowed_packet to bigger than 17MB')
    else:
        t = 'a' * (16 * 1024 * 1024)
        await cur.execute("SELECT '" + t + "'")
        r = await cur.fetchone()
        assert r[0] == t


@pytest.mark.run_loop
async def test_escape_string(connection_creator):
    con = await connection_creator()
    cur = await con.cursor()

    assert con.escape("foo'bar") == "'foo\\'bar'"
    # literal is alias for escape
    assert con.literal("foo'bar") == "'foo\\'bar'"
    await cur.execute("SET sql_mode='NO_BACKSLASH_ESCAPES'")
    assert con.escape("foo'bar") == "'foo''bar'"


@pytest.mark.run_loop
async def test_sql_mode_param(connection_creator):
    con = await connection_creator(sql_mode='NO_BACKSLASH_ESCAPES')
    assert con.escape("foo'bar") == "'foo''bar'"


@pytest.mark.run_loop
async def test_init_param(connection_creator):
    init_command = "SET sql_mode='NO_BACKSLASH_ESCAPES';"
    con = await connection_creator(init_command=init_command)
    assert con.escape("foo'bar") == "'foo''bar'"


@pytest.mark.run_loop
async def test_autocommit(connection_creator):
    con = await connection_creator()
    assert con.get_autocommit() is False

    cur = await con.cursor()
    await cur.execute("SET AUTOCOMMIT=1")
    assert con.get_autocommit() is True

    await con.autocommit(False)
    assert con.get_autocommit() is False
    await cur.execute("SELECT @@AUTOCOMMIT")
    r = await cur.fetchone()
    assert r[0] == 0


@pytest.mark.run_loop
async def test_select_db(connection_creator):
    con = await connection_creator()
    current_db = 'test_pymysql'
    other_db = 'test_pymysql2'
    cur = await con.cursor()
    await cur.execute('SELECT database()')
    r = await cur.fetchone()
    assert r[0] == current_db

    await con.select_db(other_db)
    await cur.execute('SELECT database()')
    r = await cur.fetchone()
    assert r[0] == other_db


@pytest.mark.run_loop
async def test_connection_gone_away(connection_creator):
    # test
    # http://dev.mysql.com/doc/refman/5.0/en/gone-away.html
    # http://dev.mysql.com/doc/refman/5.0/en/error-messages-client.html
    # error_cr_server_gone_error
    conn = await connection_creator()
    cur = await conn.cursor()
    await cur.execute("SET wait_timeout=1")
    await asyncio.sleep(2)
    with pytest.raises(aiomysql.OperationalError) as cm:
        await cur.execute("SELECT 1+1")
    # error occures while reading, not writing because of socket buffer.
    # assert cm.exception.args[0] == 2006
    assert cm.value.args[0] in (2006, 2013)
    conn.close()


@pytest.mark.run_loop
async def test_connection_info_methods(connection_creator, mysql_params):
    conn = await connection_creator()
    # trhead id is int
    assert isinstance(conn.thread_id(), int)
    assert conn.character_set_name() in ('latin1', 'utf8mb4')
    if "unix_socket" in mysql_params:
        assert mysql_params["unix_socket"] in conn.get_host_info()
    else:
        assert str(conn.port) in conn.get_host_info()
    assert isinstance(conn.get_server_info(), str)
    # protocol id is int
    assert isinstance(conn.get_proto_info(), int)
    conn.close()


@pytest.mark.run_loop
async def test_connection_set_charset(connection_creator):
    conn = await connection_creator()
    assert conn.character_set_name(), ('latin1' in 'utf8mb4')
    await conn.set_charset('utf8')
    assert conn.character_set_name() == 'utf8'


@pytest.mark.run_loop
async def test_connection_ping(connection_creator):
    conn = await connection_creator()
    await conn.ping()
    assert conn.closed is False
    conn.close()
    await conn.ping()
    assert conn.closed is False


@pytest.mark.run_loop
async def test_connection_properties(connection_creator, mysql_params):
    conn = await connection_creator()
    if "unix_socket" in mysql_params:
        assert conn.unix_socket == mysql_params["unix_socket"]
    else:
        assert conn.host == mysql_params['host']
        assert conn.port == mysql_params['port']
    assert conn.user == mysql_params['user']
    assert conn.db == mysql_params['db']
    assert conn.echo is False
    conn.close()


@pytest.mark.run_loop
async def test_connection_double_ensure_closed(connection_creator):
    conn = await connection_creator()
    assert conn.closed is False
    await conn.ensure_closed()
    assert conn.closed is True
    await conn.ensure_closed()
    assert conn.closed is True


@pytest.mark.run_loop
@pytest.mark.usefixtures("disable_gc")
async def test___del__(connection_creator):
    conn = await connection_creator()
    with pytest.warns(ResourceWarning):
        del conn
        gc.collect()


@pytest.mark.run_loop
async def test_previous_cursor_not_closed(connection_creator):
    conn = await connection_creator()
    cur1 = await conn.cursor()
    await cur1.execute("SELECT 1; SELECT 2")
    cur2 = await conn.cursor()
    await cur2.execute("SELECT 3;")
    resp = await cur2.fetchone()
    assert resp[0] == 3


@pytest.mark.run_loop
async def test_commit_during_multi_result(connection_creator):
    conn = await connection_creator()
    cur = await conn.cursor()
    await cur.execute("SELECT 1; SELECT 2;")
    await conn.commit()
    await cur.execute("SELECT 3;")
    resp = await cur.fetchone()
    assert resp[0] == 3


@pytest.mark.run_loop
async def test_escape_bytes_binary_prefix_default(connection_creator):
    conn = await connection_creator()
    assert conn.escape(b"x'y") == r"_binary'x\'y'"


@pytest.mark.run_loop
async def test_escape_bytes_without_binary_prefix(connection_creator):
    conn = await connection_creator(binary_prefix=False)
    assert conn.escape(b"x'y") == r"'x\'y'"
    assert conn.escape("x'y") == r"'x\'y'"


@pytest.mark.run_loop
async def test_insert_bytes_json_without_binary_prefix(connection_creator):
    # MySQL 8 rejects ``_binary`` strings for JSON columns (error 3144).
    conn = await connection_creator(binary_prefix=False)
    async with conn.cursor() as cur:
        await cur.execute("DROP TABLE IF EXISTS test_binary_prefix_json")
        await cur.execute(
            "CREATE TABLE test_binary_prefix_json (j JSON)")
        try:
            await cur.execute(
                "INSERT INTO test_binary_prefix_json (j) VALUES (%s)",
                (b'{"a": 1}',))
            await cur.execute("SELECT j FROM test_binary_prefix_json")
            (row,) = await cur.fetchall()
            assert row[0] in ('{"a": 1}', {"a": 1})
        finally:
            await cur.execute("DROP TABLE test_binary_prefix_json")


_RAW_VALUES = [
    b'\xff\x00\xe9',  # invalid UTF-8 and a NUL byte
    b'caf\xc3\xa9',  # valid non-ASCII UTF-8
    b"it's \\ \"quoted\"",
]


async def _make_raw_table(cur):
    await cur.execute("DROP TABLE IF EXISTS test_binary_prefix_raw")
    await cur.execute(
        "CREATE TABLE test_binary_prefix_raw (id INT, b VARBINARY(64))")


@pytest.mark.run_loop
async def test_binary_prefix_false_raw_bytes_execute(connection_creator):
    conn = await connection_creator(binary_prefix=False)
    async with conn.cursor() as cur:
        await _make_raw_table(cur)
        try:
            for i, value in enumerate(_RAW_VALUES):
                await cur.execute(
                    "INSERT INTO test_binary_prefix_raw VALUES (%s, %s)",
                    (i, value))
            await cur.execute(
                "SELECT b FROM test_binary_prefix_raw ORDER BY id")
            assert [r[0] for r in await cur.fetchall()] == _RAW_VALUES
        finally:
            await cur.execute("DROP TABLE test_binary_prefix_raw")


@pytest.mark.run_loop
async def test_binary_prefix_false_raw_bytes_executemany(connection_creator):
    conn = await connection_creator(binary_prefix=False)
    async with conn.cursor() as cur:
        await _make_raw_table(cur)
        try:
            await cur.executemany(
                "INSERT INTO test_binary_prefix_raw VALUES (%s, %s)",
                list(enumerate(_RAW_VALUES)))
            await cur.execute(
                "SELECT b FROM test_binary_prefix_raw ORDER BY id")
            assert [r[0] for r in await cur.fetchall()] == _RAW_VALUES
        finally:
            await cur.execute("DROP TABLE test_binary_prefix_raw")


@pytest.mark.run_loop
async def test_execute_command_str_with_surrogateescape(connection_creator):
    # str built from undecodable bytes (as produced by escape() with
    # binary_prefix=False) must not raise UnicodeEncodeError when sent.
    conn = await connection_creator(binary_prefix=False)
    sql = "SELECT 1 /* " + b'\xff\xe9'.decode('ascii', 'surrogateescape') \
        + " */"
    await conn._execute_command(COMMAND.COM_QUERY, sql)
    await conn._read_query_result()
    assert conn._result.rows == ((1,),)


@pytest.mark.run_loop
async def test_pool_binary_prefix_false_raw_bytes(pool_creator):
    pool = await pool_creator(binary_prefix=False)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await _make_raw_table(cur)
            try:
                await cur.execute(
                    "INSERT INTO test_binary_prefix_raw VALUES (1, %s)",
                    (_RAW_VALUES[0],))
                await cur.executemany(
                    "INSERT INTO test_binary_prefix_raw VALUES (%s, %s)",
                    [(2, _RAW_VALUES[1]), (3, _RAW_VALUES[2])])
                await cur.execute(
                    "SELECT b FROM test_binary_prefix_raw ORDER BY id")
                assert [r[0] for r in await cur.fetchall()] == _RAW_VALUES
            finally:
                await cur.execute("DROP TABLE test_binary_prefix_raw")


@pytest.mark.run_loop
async def test_binary_prefix_false_no_backslash_escapes(connection_creator):
    conn = await connection_creator(binary_prefix=False)
    async with conn.cursor() as cur:
        await cur.execute("SET SESSION sql_mode = 'NO_BACKSLASH_ESCAPES'")
        assert conn.escape(b"a'b\\c") == "'a''b\\c'"
        await _make_raw_table(cur)
        try:
            value = b"x' OR '1'='1 \\"
            await cur.execute(
                "INSERT INTO test_binary_prefix_raw VALUES (1, %s)", (value,))
            await cur.execute("SELECT b FROM test_binary_prefix_raw")
            assert (await cur.fetchone())[0] == value
        finally:
            await cur.execute("DROP TABLE test_binary_prefix_raw")
