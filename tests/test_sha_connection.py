import copy
from aiomysql import create_pool

import pytest


@pytest.mark.mysql_verison('8.0')
@pytest.mark.run_loop
@pytest.mark.parametrize("user,password,plugin", [
    ("nopass_sha256", None, 'sha256_password'),
    ("user_sha256", 'pass_sha256', 'sha256_password'),
    ("nopass_caching_sha2", None, 'caching_sha2_password'),
    ("user_caching_sha2", 'pass_caching_sha2', 'caching_sha2_password'),
])
async def test_sha(mysql_server, loop, user, password, plugin):
    connection_data = copy.copy(mysql_server['conn_params'])
    connection_data['user'] = user
    connection_data['password'] = password

    async with create_pool(**connection_data,
                           loop=loop) as pool:
        async with pool.get() as conn:
            # User doesnt have any permissions to look at DBs
            # But as 8.0 will default to caching_sha2_password
            assert conn._auth_plugin_used == plugin
