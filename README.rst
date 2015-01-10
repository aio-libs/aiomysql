aiomysql (work in progress)
===========================
.. image:: https://travis-ci.org/jettify/aiomysql.svg
    :target: https://travis-ci.org/jettify/aiomysql
.. image:: https://coveralls.io/repos/jettify/aiomysql/badge.png?branch=master
    :target: https://coveralls.io/r/jettify/aiomysql?branch=master

Fork of https://github.com/PyMySQL/Tornado-MySQL

Basic Example
-------------

.. code:: python

    import asyncio
    import aiomysql

    loop = asyncio.get_event_loop()

    @asyncio.coroutine
    def test_example():
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

Connection Pool
---------------
Connection pooling ported from aiopg_ :

.. code:: python

    import asyncio
    import aiomysql


    loop = asyncio.get_event_loop()


    @asyncio.coroutine
    def test_example():
            pool = yield from aiomysql.create_pool(host='127.0.0.1', port=3306,
                                           user='root', passwd='', db='mysql',
                                           loop=loop)
            with (yield from pool) as conn:
                cur = conn.cursor()
                yield from cur.execute("SELECT 10")
                # print(cur.description)
                (r,) = cur.fetchone()
                assert r == 10
            pool.close()
            yield from pool.wait_closed()

    loop.run_until_complete(test_example())


Requirements
------------

* Python_ 3.3+
* asyncio_ or Python_ 3.4+
* PyMySQL_


.. _Python: https://www.python.org
.. _asyncio: http://docs.python.org/3.4/library/asyncio.html
.. _aiopg: https://github.com/aio-libs/aiopg
.. _PyMySQL: https://github.com/PyMySQL/PyMySQL