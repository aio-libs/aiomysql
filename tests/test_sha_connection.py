import copy
from aiomysql import create_pool

import pytest


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


@pytest.mark.mysql_verison('8.0')
@pytest.mark.run_loop
async def test_sha256_nopw(mysql_server, loop):
    connection_data = copy.copy(mysql_server['conn_params'])
    connection_data['user'] = 'nopass_sha256'
    connection_data['password'] = None

    async with create_pool(**connection_data,
                           loop=loop) as pool:
        async with pool.get() as conn:
            # User doesnt have any permissions to look at DBs
            # But as 8.0 will default to caching_sha2_password
            assert conn._auth_plugin_used == 'sha256_password'


@pytest.mark.mysql_verison('8.0')
@pytest.mark.run_loop
async def test_sha256_pw(mysql_server, loop):
    connection_data = copy.copy(mysql_server['conn_params'])
    connection_data['user'] = 'user_sha256'
    connection_data['password'] = 'pass_sha256'

    async with create_pool(**connection_data,
                           loop=loop) as pool:
        async with pool.get() as conn:
            # User doesnt have any permissions to look at DBs
            # But as 8.0 will default to caching_sha2_password
            assert conn._auth_plugin_used == 'sha256_password'


@pytest.mark.mysql_verison('8.0')
@pytest.mark.run_loop
async def test_cached_sha256_nopw(mysql_server, loop):
    connection_data = copy.copy(mysql_server['conn_params'])
    connection_data['user'] = 'nopass_caching_sha2'
    connection_data['password'] = None

    async with create_pool(**connection_data,
                           loop=loop) as pool:
        async with pool.get() as conn:
            # User doesnt have any permissions to look at DBs
            # But as 8.0 will default to caching_sha2_password
            assert conn._auth_plugin_used == 'caching_sha2_password'


@pytest.mark.mysql_verison('8.0')
@pytest.mark.run_loop
async def test_cached_sha256_pw(mysql_server, loop):
    connection_data = copy.copy(mysql_server['conn_params'])
    connection_data['user'] = 'user_caching_sha2'
    connection_data['password'] = 'pass_caching_sha2'

    async with create_pool(**connection_data,
                           loop=loop) as pool:
        async with pool.get() as conn:
            # User doesnt have any permissions to look at DBs
            # But as 8.0 will default to caching_sha2_password
            assert conn._auth_plugin_used == 'caching_sha2_password'
