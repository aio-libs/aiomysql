import asyncio
import gc
import os

import pytest

import aiomysql


@pytest.fixture()
def fill_my_cnf(mysql_params):
    tests_root = os.path.abspath(os.path.dirname(__file__))
    path1 = os.path.join(tests_root, 'fixtures/my.cnf.tmpl')
    path2 = os.path.join(tests_root, 'fixtures/my.cnf')
    with open(path1) as f1:
        tmpl = f1.read()
    with open(path2, 'w') as f2:
        f2.write(tmpl.format_map(mysql_params))


@pytest.mark.run_loop
async def test_connect_timeout(connection_creator):
    # All exceptions are caught and raised as operational errors
    with pytest.raises(aiomysql.OperationalError):
        await connection_creator(connect_timeout=0.000000000001)


@pytest.mark.run_loop
async def test_config_file(fill_my_cnf, connection_creator, mysql_params):
    tests_root = os.path.abspath(os.path.dirname(__file__))
    path = os.path.join(tests_root, 'fixtures/my.cnf')
    conn = await connection_creator(read_default_file=path)

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
async def test_connection_info_methods(connection_creator):
    conn = await connection_creator()
    # trhead id is int
    assert isinstance(conn.thread_id(), int)
    assert conn.character_set_name() in ('latin1', 'utf8mb4')
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
async def test_no_delay_warning(connection_creator):
    with pytest.warns(DeprecationWarning):
        conn = await connection_creator(no_delay=True)
    conn.close()


@pytest.mark.run_loop
async def test_no_delay_default_arg(connection_creator):
    conn = await connection_creator()
    assert conn._no_delay is True
    conn.close()


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
