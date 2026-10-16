import copy
from unittest.mock import AsyncMock, patch

from aiomysql import Connection, create_pool
from pymysql.connections import MysqlPacket, _auth

import pytest


@pytest.mark.run_loop
@pytest.mark.parametrize('terminator', [b'\0', b''],
                         ids=['nul-terminated', 'unterminated'])
async def test_caching_sha2_auth_switch_salt(terminator, loop):
    nonce = b'0123456789abcdefghij'
    password = b'pass_caching_sha2'
    conn = Connection(password=password.decode('latin1'), loop=loop)
    auth_switch = MysqlPacket(
        b'\xfe' + b'caching_sha2_password\0' + nonce + terminator, 'utf8'
    )
    # The caller consumes the packet identifier and plugin name first.
    auth_switch.read_uint8()
    auth_switch.read_string()
    fast_auth = MysqlPacket(b'\x01\x03', 'utf8')
    ok_packet = MysqlPacket(b'\x00\x00\x00\x02\x00\x00\x00', 'utf8')

    with patch.object(conn, 'write_packet') as write_packet, \
            patch.object(conn, '_read_packet', new_callable=AsyncMock,
                         side_effect=[fast_auth, ok_packet]) as read_packet:
        result = await conn.caching_sha2_password_auth(auth_switch)

    assert conn.salt == nonce
    write_packet.assert_called_once_with(
        _auth.scramble_caching_sha2(password, nonce)
    )
    assert result is ok_packet
    assert read_packet.await_count == 2


# You could parameterise these tests with this, but then pytest
# does some funky stuff and spins up and tears down containers
# per function call.  Remember it would be
# mysql_versions * event_loops * 4 auth tests ~= 3*2*4 ~= 24 tests

# As the MySQL daemon restarts at least 3 times in the container
# before it becomes stable, there's a sleep(10) so that's
# around a 4min wait time.

# @pytest.mark.parametrize("user,password,plugin", [
#     ("nopass_sha256", None, 'sha256_password'),
#     ("user_sha256", 'pass_sha256', 'sha256_password'),
#     ("nopass_caching_sha2", None, 'caching_sha2_password'),
#     ("user_caching_sha2", 'pass_caching_sha2', 'caching_sha2_password'),
# ])


def ensure_mysql_version(mysql_server):
    if mysql_server["db_type"] != "mysql" \
            or mysql_server["server_version_tuple_short"] != (8, 0):
        pytest.skip("Not applicable for {} version: {}"
                    .format(mysql_server["db_type"],
                            mysql_server["server_version_tuple_short"]))


@pytest.mark.run_loop
async def test_sha256_nopw(mysql_server, loop):
    ensure_mysql_version(mysql_server)

    connection_data = copy.copy(mysql_server['conn_params'])
    connection_data['user'] = 'nopass_sha256'
    connection_data['password'] = None

    async with create_pool(**connection_data,
                           loop=loop) as pool:
        async with pool.acquire() as conn:
            # User doesnt have any permissions to look at DBs
            # But as 8.0 will default to caching_sha2_password
            assert conn._auth_plugin_used == 'sha256_password'


@pytest.mark.run_loop
async def test_sha256_pw(mysql_server, loop):
    ensure_mysql_version(mysql_server)

    # https://dev.mysql.com/doc/refman/8.0/en/sha256-pluggable-authentication.html
    # Unlike caching_sha2_password, the sha256_password plugin does not treat
    # shared-memory connections as secure, even though share-memory transport
    # is secure by default.
    if "unix_socket" in mysql_server['conn_params']:
        pytest.skip("sha256_password is not supported on unix sockets")

    connection_data = copy.copy(mysql_server['conn_params'])
    connection_data['user'] = 'user_sha256'
    connection_data['password'] = 'pass_sha256'

    async with create_pool(**connection_data,
                           loop=loop) as pool:
        async with pool.acquire() as conn:
            # User doesnt have any permissions to look at DBs
            # But as 8.0 will default to caching_sha2_password
            assert conn._auth_plugin_used == 'sha256_password'


@pytest.mark.run_loop
async def test_cached_sha256_nopw(mysql_server, loop):
    ensure_mysql_version(mysql_server)

    connection_data = copy.copy(mysql_server['conn_params'])
    connection_data['user'] = 'nopass_caching_sha2'
    connection_data['password'] = None

    async with create_pool(**connection_data,
                           loop=loop) as pool:
        async with pool.acquire() as conn:
            # User doesnt have any permissions to look at DBs
            # But as 8.0 will default to caching_sha2_password
            assert conn._auth_plugin_used == 'caching_sha2_password'


@pytest.mark.run_loop
async def test_cached_sha256_pw(mysql_server, loop):
    ensure_mysql_version(mysql_server)

    connection_data = copy.copy(mysql_server['conn_params'])
    connection_data['user'] = 'user_caching_sha2'
    connection_data['password'] = 'pass_caching_sha2'

    async with create_pool(**connection_data,
                           loop=loop) as pool:
        async with pool.acquire() as conn:
            # User doesnt have any permissions to look at DBs
            # But as 8.0 will default to caching_sha2_password
            assert conn._auth_plugin_used == 'caching_sha2_password'
