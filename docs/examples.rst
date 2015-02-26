Examples of aiomysql usage
==========================

Below is a list of examples from `aiomysql/examples
<https://github.com/aio-libs/aiomysql/tree/master/examples>`_

Every example is a correct tiny python program.

.. _aiomysql-examples-simple:

Low-level API
-------------
Bassic example, fetch host and user information from internal table: user.

.. literalinclude:: ../examples/example.py

Example of stored procedure, which just increments input value.

.. literalinclude:: ../examples/example_callproc.py

Simple sqlalchemy usage
-----------------------

.. literalinclude:: ../examples/example_sa.py
