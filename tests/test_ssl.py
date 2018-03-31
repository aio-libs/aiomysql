from aiomysql import create_pool

import pytest


@pytest.mark.run_loop
async def test_tls_connect(mysql_server, loop):

    async with create_pool(**mysql_server['conn_params'], loop=loop) as pool:
        async with pool.get() as conn:
            async with conn.cursor() as cur:
                # Run simple command
                await cur.execute("SHOW DATABASES;")
                value = await cur.fetchall()

                values = [item[0] for item in value]
                # Spot check the answers, we should at least have mysql
                # and information_schema
                assert 'mysql' in values, 'Could not find the "mysql" table'
                assert 'information_schema' in values, \
                    'Could not find the "mysql" table'

                # Check TLS variables
                await cur.execute("SHOW STATUS LIKE '%Ssl_version%';")
                value = await cur.fetchone()

                # The context has TLS
                assert value[1].startswith('TLS'), \
                    'Not connected to the database with TLS'
