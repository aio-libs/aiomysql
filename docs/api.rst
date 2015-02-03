.. _api:

:mod:`aiomysql` --- API Reference
=================================

.. module:: aiomysql
   :synopsis: A library for accessing a MySQL database from the asyncio
.. currentmodule:: aiomysql


Connection
==========

The library provides a way to connect to MySQL database with simple factory
function :func:`aiomysql.connnect`. Use this function if you want just one
connection to the database, consider connection pool for multiple connections.

Example::

  import asyncio
  import aiomysql


  @asyncio.coroutine
  def go():
      conn = yield from aiomysql.connect(database='aiomysql',
                                      user='root',
                                      password='secret',
                                      host='127.0.0.1')
      cur = yield from conn.cursor()
      yield from cur.execute("SELECT * FROM tbl")
      ret = yield from cur.fetchall()


.. function:: connect(host="localhost", user=None, password="",
            db=None, port=3306, unix_socket=None,
            charset='', sql_mode=None,
            read_default_file=None, conv=decoders, use_unicode=None,
            client_flag=0, cursorclass=Cursor, init_command=None,
            connect_timeout=None, read_default_group=None,
            no_delay=False, autocommit=False, echo=False, loop=None)

    A :ref:`coroutine <coroutine>` that connects to MySQL.

    The function accepts all parameters that :func:`pymysql.connect`
    does plus optional keyword-only *loop* and *timeout* parameters.

    :param str host: host where the database server is located,
        default: `localhost`.
    :param str user: username to log in as.
    :param str password: password to use.
    :param str db: database to use, None to not use a particular one.
    :param int port: MySQL port to use, default is usually OK.
    :param str unix_socket: optionally, you can use a unix socket rather
        than TCP/IP.
    :param str charset: charset you want to use, for example 'utf8'.
    :param sql_mode: default sql-mode_ to use, like 'NO_BACKSLASH_ESCAPES'
    :param read_default_file: specifies  my.cnf file to read these
        parameters from under the [client] section.
    :param conv: decoders dictionary to use instead of the default one.
        This is used to provide custom marshalling of types.
        See `pymysql.converters`.
    :param use_unicode: whether or not to default to unicode strings.
    :param  client_flag: custom flags to send to MySQL. Find
        potential values in `pymysql.constants.CLIENT`.
    :param cursorclass: custom cursor class to use.
    :param str init_command: initial SQL statement to run when connection is
        established.
    :param connect_timeout: Timeout before throwing an exception
        when connecting.
    :param str read_default_group: Group to read from in the configuration
        file.
    :param bool no_delay: disable Nagle's algorithm on the socket
    :param autocommit: Autocommit mode. None means use server default.
        (default: ``False``)
    :param loop: asyncio event loop instance or ``None`` for default one.
    :returns: :class:`Connection` instance.


.. _sql-mode: http://dev.mysql.com/doc/refman/5.0/en/sql-mode.html
