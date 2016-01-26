import asyncio
import aiomysql


async def test_example_transaction(loop):
    conn = await aiomysql.connect(host='127.0.0.1', port=3306,
                                  user='root', password='',
                                  db='test_pymysql', autocommit=False,
                                  loop=loop)

    async with conn.cursor() as cursor:
        stmt_drop = "DROP TABLE IF EXISTS names"
        await cursor.execute(stmt_drop)
        await cursor.execute("""
            CREATE TABLE names (
            id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
            name VARCHAR(30) DEFAULT '' NOT NULL,
            cnt TINYINT UNSIGNED DEFAULT 0,
            PRIMARY KEY (id))""")
        await conn.commit()

        # Insert 3 records
        names = (('Geert',), ('Jan',), ('Michel',))
        stmt_insert = "INSERT INTO names (name) VALUES (%s)"
        await cursor.executemany(stmt_insert, names)

        # Roll back!!!!
        await conn.rollback()

        # There should be no data!
        stmt_select = "SELECT id, name FROM names ORDER BY id"
        await cursor.execute(stmt_select)
        resp = await cursor.fetchall()
        # Check there is no data
        assert not resp

        # Do the insert again.
        await cursor.executemany(stmt_insert, names)

        # Data should be already there
        await cursor.execute(stmt_select)
        resp = await cursor.fetchall()
        print(resp)
        # Do a commit
        await conn.commit()

        await cursor.execute(stmt_select)
        print(resp)

        # Cleaning up, dropping the table again
        await cursor.execute(stmt_drop)
        await cursor.close()
        conn.close()


loop = asyncio.get_event_loop()
loop.run_until_complete(test_example_transaction(loop))
