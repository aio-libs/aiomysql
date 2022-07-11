.. _aiomysql-pool:

Pool
====

The library provides *connection pool* as well as plain
:class:`Connection` objects.


The basic usage is::

    import asyncio
    import aiomysql

    loop = asyncio.get_event_loop()

    async def go():
        pool = await aiomysql.create_pool(host='127.0.0.1', port=3306,
                                          user='root', password='',
                                          db='mysql', loop=loop, autocommit=False)

        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 10")
                # print(cur.description)
                (r,) = await cur.fetchone()
                assert r == 10
        pool.close()
        await pool.wait_closed()

    loop.run_until_complete(go())


.. function:: create_pool(minsize=1, maxsize=10, loop=None, **kwargs)

    A :ref:`coroutine <coroutine>` that creates a pool of connections to
    :term:`MySQL` database.

    :param int minsize: minimum sizes of the *pool*.
    :param int maxsize: maximum sizes of the *pool*.
    :param loop: is an optional *event loop* instance,
        :func:`asyncio.get_event_loop` is used if *loop* is not specified.
    :param bool echo: -- executed log SQL queryes (``False`` by default).
    :param kwargs: The function accepts all parameters that
        :func:`aiomysql.connect` does plus optional keyword-only parameters
        *loop*, *minsize*, *maxsize*.
    :param float pool_recycle: number of seconds after which connection is
         recycled, helps to deal with stale connections in pool, default
         value is -1, means recycling logic is disabled.
    :returns: :class:`Pool` instance.


.. class:: Pool

    A connection pool.

    After creation pool has *minsize* free connections and can grow up
    to *maxsize* ones.

    If *minsize* is ``0`` the pool doesn't creates any connection on startup.

    If *maxsize* is ``0`` than size of pool is unlimited (but it
    recycles used connections of course).

    The most important way to use it is getting connection in *with statement*::

        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                pass


    See also :meth:`Pool.acquire` and :meth:`Pool.release` for acquiring
    :class:`Connection` without *with statement*.

    .. attribute:: echo

        Return *echo mode* status. Log all executed queries to logger
        named ``aiomysql`` if ``True``

    .. attribute:: minsize

        A minimal size of the pool (*read-only*), ``1`` by default.

    .. attribute:: maxsize

        A maximal size of the pool (*read-only*), ``10`` by default.

    .. attribute:: size

        A current size of the pool (*readonly*). Includes used and free
        connections.

    .. attribute:: freesize

        A count of free connections in the pool (*readonly*).

    .. method:: clear()

       A :ref:`coroutine <coroutine>` that closes all *free* connections
       in the pool. At next connection acquiring at least :attr:`minsize` of
       them will be recreated.

   .. method:: close()

      Close pool.

      Mark all pool connections to be closed on getting back to pool.
      Closed pool doesn't allow to acquire new connections.

      If you want to wait for actual closing of acquired connection please
      call :meth:`wait_closed` after :meth:`close`.

      .. warning:: The method is not a :ref:`coroutine <coroutine>`.

   .. method:: terminate()

      Terminate pool.

      Close pool with instantly closing all acquired connections also.

      :meth:`wait_closed` should be called after :meth:`terminate` for
      waiting for actual finishing.

      .. warning:: The method is not a :ref:`coroutine <coroutine>`.

   .. method:: wait_closed()

      A :ref:`coroutine <coroutine>` that waits for releasing and
      closing all acquired connections.

      Should be called after :meth:`close` for waiting for actual pool
      closing.

   .. method:: acquire()

      A :ref:`coroutine <coroutine>` that acquires a connection from
      *free pool*. Creates new connection if needed and :attr:`size`
      of pool is less than :attr:`maxsize`.

      Returns a :class:`Connection` instance.

   .. method:: release(conn)

      Reverts connection *conn* to *free pool* for future recycling.

      .. warning:: The method is not a :ref:`coroutine <coroutine>`.
