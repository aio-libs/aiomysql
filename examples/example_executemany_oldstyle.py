import asyncio
import aiomysql


loop = asyncio.get_event_loop()


@asyncio.coroutine
def test_example_executemany():
    conn = yield from aiomysql.connect(host='127.0.0.1', port=3306,
                                       user='root', password='',
                                       db='test_pymysql', loop=loop)

    cur = yield from conn.cursor()
    yield from cur.execute("DROP TABLE IF EXISTS music_style;")
    yield from cur.execute("""CREATE TABLE music_style
                              (id INT,
                              name VARCHAR(255),
                              PRIMARY KEY (id));""")
    yield from conn.commit()

    # insert 3 rows one by one
    yield from cur.execute("INSERT INTO music_style VALUES(1,'heavy metal')")
    yield from cur.execute("INSERT INTO music_style VALUES(2,'death metal');")
    yield from cur.execute("INSERT INTO music_style VALUES(3,'power metal');")
    yield from conn.commit()

    # insert 3 row by one long query using *executemane* method
    data = [(4, 'gothic metal'), (5, 'doom metal'), (6, 'post metal')]
    yield from cur.executemany(
        "INSERT INTO music_style (id, name)"
        "values (%s,%s)", data)
    yield from conn.commit()

    # fetch all insert row from table music_style
    yield from cur.execute("SELECT * FROM music_style;")
    result = yield from cur.fetchall()
    print(result)

    yield from cur.close()
    conn.close()


loop.run_until_complete(test_example_executemany())
