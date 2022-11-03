from aiomysql import create_pool

import pytest


@pytest.mark.run_loop
async def test_tls_connect(mysql_server, loop, mysql_params):
    if "unix_socket" in mysql_params:
        pytest.skip("TLS is not supported on unix sockets")

    async with create_pool(**mysql_server['conn_params'],
                           loop=loop) as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                # Run simple command
                await cur.execute("SHOW DATABASES;")
                value = await cur.fetchall()

                values = [item[0] for item in value]
                # Spot check the answers, we should at least have mysql
                # and information_schema
                assert 'mysql' in values, \
                    'Could not find the "mysql" table'
                assert 'information_schema' in values, \
                    'Could not find the "mysql" table'

                # Check TLS variables
                await cur.execute("SHOW STATUS LIKE 'Ssl_version%';")
                value = await cur.fetchone()

                # The context has TLS
                assert value[1].startswith('TLS'), \
                    'Not connected to the database with TLS'


# MySQL will get you to renegotiate if sent a cleartext password
@pytest.mark.run_loop
async def test_auth_plugin_renegotiation(mysql_server, loop, mysql_params):
    if "unix_socket" in mysql_params:
        pytest.skip("TLS is not supported on unix sockets")

    async with create_pool(**mysql_server['conn_params'],
                           auth_plugin='mysql_clear_password',
                           loop=loop) as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                # Run simple command
                await cur.execute("SHOW DATABASES;")
                value = await cur.fetchall()

                assert len(value), 'No databases found'

                # Check we tried to use the cleartext plugin
                assert conn._client_auth_plugin == 'mysql_clear_password', \
                    'Client did not try clear password auth'

                # Check the server asked us to use MySQL's default plugin
                assert conn._server_auth_plugin in (
                    'mysql_native_password', 'caching_sha2_password'), \
                    'Server did not ask for native auth'
                # Check we actually used the servers default plugin
                assert conn._auth_plugin_used in (
                    'mysql_native_password', 'caching_sha2_password'), \
                    'Client did not renegotiate with server\'s default auth'
