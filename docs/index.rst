.. _aiomysql-index:

.. aiomysql documentation master file, created by
   sphinx-quickstart on Sun Jun 11 16:24:33 2023.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to aiomysql's documentation!
====================================

.. _GitHub: https://github.com/aio-libs/aiomysql
.. _asyncio: http://docs.python.org/3.5/library/asyncio.html
.. _aiopg: https://github.com/aio-libs/aiopg
.. _Tornado-MySQL: https://github.com/PyMySQL/Tornado-MySQL
.. _aio-libs: https://github.com/aio-libs


**aiomysql** is a library for accessing a :term:`MySQL` database
from the asyncio_ (PEP-3156/tulip) framework. It depends and reuses most parts
of :term:`PyMySQL` . **aiomysql** tries to be like awesome aiopg_ library and preserve
same api, look and feel.

Internally **aiomysql** is copy of PyMySQL, underlying io calls switched
to async, basically ``await`` and ``async def coroutine`` added in
proper places. :term:`sqlalchemy` support ported from aiopg_.


Features
--------

* Implements *asyncio* :term:`DBAPI` *like* interface for
  :term:`MySQL`.  It includes :ref:`aiomysql-connection`,
  :ref:`aiomysql-cursors` and :ref:`aiomysql-pool` objects.
* Implements *optional* support for charming :term:`sqlalchemy`
  functional sql layer.

Basics
------

**aiomysql** based on :term:`PyMySQL` , and provides same api, you just need
to use  ``await conn.f()`` instead of just call ``conn.f()`` for
every method.

Properties are unchanged, so ``conn.prop`` is correct as well as
``conn.prop = val``.

See example:

.. code:: python

    import asyncio
    import aiomysql

    loop = asyncio.get_event_loop()


    async def test_example():
        conn = await aiomysql.connect(host='127.0.0.1', port=3306,
                                           user='root', password='', db='mysql',
                                           loop=loop)

        cur = await conn.cursor()
        await cur.execute("SELECT Host,User FROM user")
        print(cur.description)
        r = await cur.fetchall()
        print(r)
        await cur.close()
        conn.close()

    loop.run_until_complete(test_example())


Installation
------------

.. code::

   pip3 install aiomysql

.. note:: :mod:`aiomysql` requires :term:`PyMySQL` library.


Also you probably want to use :mod:`aiomysql.sa`.

.. _aiomysql-install-sqlalchemy:

:mod:`aiomysql.sa` module is **optional** and requires
:term:`sqlalchemy`. You can install *sqlalchemy* by running::

  pip3 install sqlalchemy

Source code
-----------

The project is hosted on GitHub_

Please feel free to file an issue on `bug tracker
<https://github.com/aio-libs/aiomysql/issues>`_ if you have found a bug
or have some suggestion for library improvement.

The library uses `GitHub Actions
<https://github.com/aio-libs/aiomysql/actions>`_ for Continuous Integration
and `Codecov <https://app.codecov.io/gh/aio-libs/aiomysql/branch/main>`_ for
coverage reports.


Dependencies
------------

- Python 3.9+
- :term:`PyMySQL`
- aiomysql.sa requires :term:`sqlalchemy`.


Authors and License
-------------------

The ``aiomysql`` package is written by Nikolay Novik, :term:`PyMySQL` and
aio-libs_ contributors. It's MIT licensed (same as PyMySQL).

Feel free to improve this package and send a pull request to GitHub_.

Contents:
---------

.. toctree::
   :maxdepth: 2
   :titlesonly:

   connection
   cursors
   pool
   tutorial
   sa
   examples
   glossary
   contributing

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
