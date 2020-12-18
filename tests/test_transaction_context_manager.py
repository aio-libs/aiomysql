import asyncio
import logging

from pymysql import InterfaceError, OperationalError

import aiomysql


async def start_test():
    logging.getLogger().setLevel(logging.DEBUG)
    logging.info('start_test')
    host = '10.20.166.2'
    port = 3306
    user = 'root'
    password = 'Kub@Rozpruw@cz007'
    db = 'manager'
    pool: aiomysql.Pool = await aiomysql.create_pool(
        host=host, user=user, password=password, db=db,
        port=port, minsize=1, maxsize=2
    )

    for x in range(2):
        loop.create_task(test_connection1(f'name{x}', pool))


async def test_connection1(name, pool: aiomysql.Pool):
    sql = """
    SELECT 1;
    """
    while True:
        logging.info(f"{name}: pool_freesize %d", pool.freesize)
        async with pool.acquire_with_transaction() as connection:
            logging.info(connection)
            await connection.begin()
            async with connection.cursor() as cursor:
                try:
                    await cursor.execute(sql)
                    await cursor.fetchone()
                    logging.info(f"{name}: OK")

                except asyncio.CancelledError:
                    raise

                except Exception as e:
                    logging.error(f"{name}: {e}")
                    # raise

            await test_connection2('test_connection2', pool)
            await test_connection3('test_connection3', pool)

            await connection.rollback()

        await asyncio.sleep(1)


async def test_connection2(name, pool: aiomysql.Pool):
    sql = """
    SELECT 1;
    """
    async with pool.acquire_with_transaction() as connection:
        logging.info(connection)
        await connection.begin()
        async with connection.cursor() as cursor:
            try:
                await cursor.execute(sql)
                await cursor.fetchone()
                logging.info(f"{name}: OK")

            except asyncio.CancelledError:
                raise

            except Exception as e:
                logging.error(f"{name}: {e}")
                # raise


async def test_connection3(name, pool: aiomysql.Pool):
    sql = """
    SELECT 1;
    """
    async with pool.acquire() as connection:
        logging.info(connection)
        async with connection.cursor() as cursor:
            try:
                await cursor.execute(sql)
                await cursor.fetchone()
                logging.info(f"{name}: OK")

            except asyncio.CancelledError:
                raise

            except Exception as e:
                logging.error(f"{name}: {e}")
                # raise


loop = asyncio.get_event_loop()
loop.run_until_complete(start_test())
loop.run_forever()
