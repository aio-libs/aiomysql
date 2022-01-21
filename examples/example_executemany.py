import asyncio
import aiomysql


async def test_example_executemany(loop):
    conn = await aiomysql.connect(host='127.0.0.1', port=3306,
                                       user='root', password='',
                                       db='test_pymysql', loop=loop)

    cur = await conn.cursor()
    async with conn.cursor() as cur:
        await cur.execute("DROP TABLE IF EXISTS music_style;")
        await cur.execute("""CREATE TABLE music_style
                                  (id INT,
                                  name VARCHAR(255),
                                  PRIMARY KEY (id));""")
        await conn.commit()

        # insert 3 rows one by one
        await cur.execute("INSERT INTO music_style VALUES(1,'heavy metal')")
        await cur.execute("INSERT INTO music_style VALUES(2,'death metal');")
        await cur.execute("INSERT INTO music_style VALUES(3,'power metal');")
        await conn.commit()

        # insert 3 row by one long query using *executemany* method
        data = [(4, 'gothic metal'), (5, 'doom metal'), (6, 'post metal')]
        await cur.executemany(
            "INSERT INTO music_style (id, name)"
            "values (%s,%s)", data)
        await conn.commit()

        # fetch all insert row from table music_style
        await cur.execute("SELECT * FROM music_style;")
        result = await cur.fetchall()
        print(result)

    conn.close()


loop = asyncio.get_event_loop()
loop.run_until_complete(test_example_executemany(loop))
