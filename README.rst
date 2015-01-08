aiomysql (work in progress)
===========================
.. image:: https://travis-ci.org/jettify/aiomysql.svg
    :target: https://travis-ci.org/jettify/aiomysql

Fork of https://github.com/PyMySQL/Tornado-MySQL

Basic Example
-------------

.. code:: python

    import asyncio
    import aiomysql

    loop = asyncio.get_event_loop()

    @asyncio.coroutine
    def test_example(self):
        conn = yield from aiomysql.connect(host='127.0.0.1', port=3306,
                                           user='root', passwd='', db='mysql',
                                           loop=loop)

        cur = conn.cursor()
        yield from cur.execute("SELECT Host,User FROM user")
        print(cur.description)
        r = cur.fetchall()
        print(r)
        cur.close()
        conn.close()

    loop.run_until_complete(test_example())
