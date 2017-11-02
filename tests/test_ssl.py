# import asyncio
# import ssl
#
# from aiomysql import create_pool
#
#
# async def main():
#     ctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
#     ctx.check_hostname = False
#     ctx.verify_mode = ssl.CERT_NONE
#
#     # mysql_clear_password for connecting to rds
#     async with create_pool(host='127.0.0.1', port=3308, user='root',
#                            password='test1234',
#                            db='testdb', loop=loop, ssl=ctx,
#                            auth_plugin='mysql_clear_password') as pool:
#         async with pool.get() as conn:
#             async with conn.cursor() as cur:
#                 await cur.execute("SHOW TABLES;")
#                 value = await cur.fetchone()
#                 print(value)
#
#
# loop = asyncio.get_event_loop()
# loop.run_until_complete(main())
