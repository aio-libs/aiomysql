import asyncio
import aiomysql


loop = asyncio.get_event_loop()


@asyncio.coroutine
def test_example_transaction():
    conn = yield from aiomysql.connect(host='127.0.0.1', port=3306,
                                       user='root', password='',
                                       db='test_pymysql', autocommit=False,
                                       loop=loop)

    cursor = yield from conn.cursor()
    stmt_drop = "DROP TABLE IF EXISTS names"
    yield from cursor.execute(stmt_drop)
    yield from cursor.execute("""
        CREATE TABLE names (
        id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
        name VARCHAR(30) DEFAULT '' NOT NULL,
        cnt TINYINT UNSIGNED DEFAULT 0,
        PRIMARY KEY (id))""")
    yield from conn.commit()

    # Insert 3 records
    names = (('Geert',), ('Jan',), ('Michel',))
    stmt_insert = "INSERT INTO names (name) VALUES (%s)"
    yield from cursor.executemany(stmt_insert, names)

    # Roll back!!!!
    yield from conn.rollback()

    # There should be no data!
    stmt_select = "SELECT id, name FROM names ORDER BY id"
    yield from cursor.execute(stmt_select)
    resp = yield from cursor.fetchall()
    # Check there is no data
    assert not resp

    # Do the insert again.
    yield from cursor.executemany(stmt_insert, names)

    # Data should be already there
    yield from cursor.execute(stmt_select)
    resp = yield from cursor.fetchall()
    print(resp)
    # Do a commit
    yield from conn.commit()

    yield from cursor.execute(stmt_select)
    print(resp)

    # Cleaning up, dropping the table again
    yield from cursor.execute(stmt_drop)
    yield from cursor.close()
    conn.close()


loop.run_until_complete(test_example_transaction())
