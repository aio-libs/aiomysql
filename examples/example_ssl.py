import asyncio
import ssl
import aiomysql

ctx = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
ctx.check_hostname = False
ctx.load_verify_locations(cafile='../tests/ssl_resources/ssl/ca.pem')


async def main():
    async with aiomysql.create_pool(
            host='localhost', port=3306, user='root',
            password='rootpw', ssl=ctx,
            auth_plugin='mysql_clear_password') as pool:

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

asyncio.get_event_loop().run_until_complete(main())
