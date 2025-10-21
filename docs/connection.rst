.. _api:

:mod:`aiomysql` --- API Reference
=================================

.. module:: aiomysql
   :synopsis: A library for accessing a MySQL database from the asyncio
.. currentmodule:: aiomysql

.. _aiomysql-connection:

Connection
==========

The library provides a way to connect to MySQL database with simple factory
function :func:`aiomysql.connnect`. Use this function if you want just one
connection to the database, consider connection pool for multiple connections.

Example::

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


.. function:: connect(host="localhost", user=None, password="",
            db=None, port=3306, unix_socket=None,
            charset='', sql_mode=None,
            read_default_file=None, conv=decoders, use_unicode=None,
            client_flag=0, cursorclass=Cursor, init_command=None,
            connect_timeout=None, read_default_group=None,
            autocommit=False, echo=False
            local_infile=False, loop=None, ssl=None, auth_plugin='',
            program_name='', server_public_key=None)

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
        potential values in `pymysql.constants.CLIENT`. Refer to the
        `local_infile` parameter for enabling loading of local data.
    :param cursorclass: custom cursor class to use.
    :param str init_command: initial SQL statement to run when connection is
        established.
    :param connect_timeout: Timeout in seconds before throwing an exception
        when connecting.
    :param str read_default_group: Group to read from in the configuration
        file.
    :param autocommit: Autocommit mode. None means use server default.
        (default: ``False``)
    :param local_infile: Boolean to enable the use of `LOAD DATA LOCAL`
        command. This also enables the corresponding `client_flag`. aiomysql
        does not perform any validation of files requested by the server. Do
        not use this with untrusted servers. (default: ``False``)
    :param ssl: Optional SSL Context to force SSL
    :param auth_plugin: String to manually specify the authentication
        plugin to use, i.e you will want to use mysql_clear_password
        when using IAM authentication with Amazon RDS.
        (default: Server Default)
    :param program_name: Program name string to provide when
        handshaking with MySQL. (omitted by default)

        .. versionchanged:: 1.0
            ``sys.argv[0]`` is no longer passed by default
    :param server_public_key: SHA256 authenticaiton plugin public key value.
    :param loop: asyncio event loop instance or ``None`` for default one.
    :returns: :class:`Connection` instance.


    Representation of a socket with a mysql server. The proper way to get an
    instance of this class is to call :func:`aiomysql.connnect`.

   Its insterface is almost the same as `pymysql.connection` except all methods
   are :ref:`coroutines <coroutine>`.


   The most important methods are:

   .. method:: cursor(cursor=None)

        A :ref:`coroutine <coroutine>` that creates a new cursor object
        using the connection.

        By default, :class:`Cursor` is returned. It is possible to also give a
        custom cursor through the `cursor` parameter, but it needs to
        be a subclass of :class:`Cursor`

        :param cursor: subclass of :class:`Cursor` or ``None`` for default
            cursor.
        :returns: :class:`Cursor` instance.

   .. method:: close()

        Immediately close the connection.

        Close the connection now (rather than whenever `del` is executed).
        The connection will be unusable from this point forward.

   .. method:: ensure_closed()

        A :ref:`coroutine <coroutine>` ends quit command and then closes
        socket connection.

   .. method:: autocommit(value)

        A :ref:`coroutine <coroutine>` to enable/disable autocommit mode for
        current MySQL session.
        :param bool value: toggle atutocommit mode.

   .. method:: get_autocommit()

        Returns autocommit status for current MySQL sesstion.
        :returns bool: current autocommit status.

   .. method:: begin()

        A :ref:`coroutine <coroutine>` to begin transaction.

   .. method:: commit()

        Commit changes to stable storage :ref:`coroutine <coroutine>`.

   .. method:: rollback()

        Roll back the current transaction :ref:`coroutine <coroutine>`.

   .. method:: select_db(db)

        A :ref:`coroutine <coroutine>` to set current db.

        :param str db: database name

   .. attribute:: closed

        The readonly property that returns ``True`` if connections is closed.

   .. attribute:: host

        MySQL server IP address or name.

   .. attribute:: port

        MySQL server TCP/IP port.


   .. attribute:: unix_socket

        ySQL Unix socket file location.

   .. attribute:: db

        Current database name.

   .. attribute:: user

        User used while connecting to MySQL

   .. attribute:: echo

        Return echo mode status.

   .. attribute:: encoding

        Encoding employed for this connection.


   .. attribute:: charset

        Returns the character set for current connection.


.. _sql-mode: http://dev.mysql.com/doc/refman/5.0/en/sql-mode.html
