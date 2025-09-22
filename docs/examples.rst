Examples of aiomysql usage
==========================

Below is a list of examples from `aiomysql/examples
<https://github.com/aio-libs/aiomysql/tree/main/examples>`_

Every example is a correct tiny python program that demonstrates specific
feature of library.

.. _aiomysql-examples-simple:

Low-level API
-------------
Basic example, fetch host and user information from internal table: user.

.. literalinclude:: ../examples/example.py

Example of stored procedure, which just increments input value.

.. literalinclude:: ../examples/example_callproc.py

Example of using `executemany` method:

.. literalinclude:: ../examples/example_executemany.py

Example of using transactions `rollback` and `commit` methods:

.. literalinclude:: ../examples/example_transaction.py

Example of using connection pool:

.. literalinclude:: ../examples/example_pool.py


sqlalchemy usage
----------------

.. literalinclude:: ../examples/example_simple_sa.py
