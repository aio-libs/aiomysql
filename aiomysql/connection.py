# Python implementation of the MySQL client-server protocol
# http://dev.mysql.com/doc/internals/en/client-server-protocol.html

import asyncio
import os
import socket
import struct
import sys
import warnings
import configparser
import getpass
from functools import partial
from asyncio import sslproto

from pymysql.charset import charset_by_name, charset_by_id
from pymysql.constants import SERVER_STATUS
from pymysql.constants import CLIENT
from pymysql.constants import COMMAND
from pymysql.constants import FIELD_TYPE
from pymysql.util import byte2int, int2byte
from pymysql.converters import (escape_item, encoders, decoders,
                                escape_string, through)
from pymysql.err import (Warning, Error,
                         InterfaceError, DataError, DatabaseError,
                         OperationalError,
                         IntegrityError, InternalError, NotSupportedError,
                         ProgrammingError)

from pymysql.connections import TEXT_TYPES, MAX_PACKET_LEN, DEFAULT_CHARSET
# from pymysql.connections import dump_packet
from pymysql.connections import _scramble
from pymysql.connections import _scramble_323
from pymysql.connections import pack_int24

from pymysql.connections import MysqlPacket
from pymysql.connections import FieldDescriptorPacket
from pymysql.connections import EOFPacketWrapper
from pymysql.connections import OKPacketWrapper
from pymysql.connections import LoadLocalPacketWrapper
from pymysql.connections import lenenc_int


# from aiomysql.utils import _convert_to_str
from .cursors import Cursor
from .utils import (PY_35, _ConnectionContextManager, _ContextManager,
                    create_future)
# from .log import logger

DEFAULT_USER = getpass.getuser()
PY_341 = sys.version_info >= (3, 4, 1)


def connect(host="localhost", user=None, password="",
            db=None, port=3306, unix_socket=None,
            charset='', sql_mode=None,
            read_default_file=None, conv=decoders, use_unicode=None,
            client_flag=0, cursorclass=Cursor, init_command=None,
            connect_timeout=None, read_default_group=None,
            no_delay=None, autocommit=False, echo=False,
            local_infile=False, loop=None, ssl=None, auth_plugin=''):
    """See connections.Connection.__init__() for information about
    defaults."""
    coro = _connect(host=host, user=user, password=password, db=db,
                    port=port, unix_socket=unix_socket, charset=charset,
                    sql_mode=sql_mode, read_default_file=read_default_file,
                    conv=conv, use_unicode=use_unicode,
                    client_flag=client_flag, cursorclass=cursorclass,
                    init_command=init_command,
                    connect_timeout=connect_timeout,
                    read_default_group=read_default_group,
                    no_delay=no_delay, autocommit=autocommit, echo=echo,
                    local_infile=local_infile, loop=loop, ssl=ssl,
                    auth_plugin=auth_plugin)
    return _ConnectionContextManager(coro)


@asyncio.coroutine
def _connect(*args, **kwargs):
    conn = Connection(*args, **kwargs)
    yield from conn._connect()
    return conn


class Connection:
    """Representation of a socket with a mysql server.

    The proper way to get an instance of this class is to call
    connect().
    """

    def __init__(self, host="localhost", user=None, password="",
                 db=None, port=3306, unix_socket=None,
                 charset='', sql_mode=None,
                 read_default_file=None, conv=decoders, use_unicode=None,
                 client_flag=0, cursorclass=Cursor, init_command=None,
                 connect_timeout=None, read_default_group=None,
                 no_delay=None, autocommit=False, echo=False,
                 local_infile=False, loop=None, ssl=None, auth_plugin=''):
        """
        Establish a connection to the MySQL database. Accepts several
        arguments:

        :param host: Host where the database server is located
        :param user: Username to log in as
        :param password: Password to use.
        :param db: Database to use, None to not use a particular one.
        :param port: MySQL port to use, default is usually OK.
        :param unix_socket: Optionally, you can use a unix socket rather
        than TCP/IP.
        :param charset: Charset you want to use.
        :param sql_mode: Default SQL_MODE to use.
        :param read_default_file: Specifies  my.cnf file to read these
            parameters from under the [client] section.
        :param conv: Decoders dictionary to use instead of the default one.
            This is used to provide custom marshalling of types.
            See converters.
        :param use_unicode: Whether or not to default to unicode strings.
        :param  client_flag: Custom flags to send to MySQL. Find
            potential values in constants.CLIENT.
        :param cursorclass: Custom cursor class to use.
        :param init_command: Initial SQL statement to run when connection is
            established.
        :param connect_timeout: Timeout before throwing an exception
            when connecting.
        :param read_default_group: Group to read from in the configuration
            file.
        :param no_delay: Disable Nagle's algorithm on the socket
        :param autocommit: Autocommit mode. None means use server default.
            (default: False)
        :param local_infile: boolean to enable the use of LOAD DATA LOCAL
            command. (default: False)
        :param loop: asyncio loop
        """
        self._loop = loop or asyncio.get_event_loop()

        if use_unicode is None and sys.version_info[0] > 2:
            use_unicode = True

        if read_default_file:
            if not read_default_group:
                read_default_group = "client"
            cfg = configparser.RawConfigParser()
            cfg.read(os.path.expanduser(read_default_file))
            _config = partial(cfg.get, read_default_group)

            user = _config("user", fallback=user)
            password = _config("password", fallback=password)
            host = _config("host", fallback=host)
            db = _config("database", fallback=db)
            unix_socket = _config("socket", fallback=unix_socket)
            port = int(_config("port", fallback=port))
            charset = _config("default-character-set", fallback=charset)

        # pymysql port
        if no_delay is not None:
            warnings.warn("no_delay option is deprecated", DeprecationWarning)
            no_delay = bool(no_delay)
        else:
            no_delay = True

        self._host = host
        self._port = port
        self._user = user or DEFAULT_USER
        self._password = password or ""
        self._db = db
        self._no_delay = no_delay
        self._echo = echo
        self._last_usage = self._loop.time()
        self._auth_plugin = auth_plugin

        self._unix_socket = unix_socket
        if charset:
            self._charset = charset
            self.use_unicode = True
        else:
            self._charset = DEFAULT_CHARSET
            self.use_unicode = False

        if use_unicode is not None:
            self.use_unicode = use_unicode

        self._ssl_context = ssl
        if ssl:
            client_flag |= CLIENT.SSL

        if local_infile:
            client_flag |= CLIENT.LOCAL_FILES

        client_flag |= CLIENT.CAPABILITIES
        client_flag |= CLIENT.MULTI_STATEMENTS
        if self._db:
            client_flag |= CLIENT.CONNECT_WITH_DB
        self.client_flag = client_flag

        self.cursorclass = cursorclass
        self.connect_timeout = connect_timeout

        self._result = None
        self.host_info = "Not connected"

        #: specified autocommit mode. None means use server default.
        self.autocommit_mode = autocommit

        self.encoders = encoders  # Need for MySQLdb compatibility.
        self.sql_mode = sql_mode
        self.init_command = init_command

        # asyncio StreamReader, StreamWriter
        self._original_transport = None
        self._tls_protocol = None
        self._protocol = None
        self._socket = None
        # If connection was closed for specific reason, we should show that to
        # user
        self._close_reason = None

    @property
    def host(self):
        """MySQL server IP address or name"""
        return self._host

    @property
    def port(self):
        """MySQL server TCP/IP port"""
        return self._port

    @property
    def unix_socket(self):
        """MySQL Unix socket file location"""
        return self._unix_socket

    @property
    def db(self):
        """Current database name."""
        return self._db

    @property
    def user(self):
        """User used while connecting to MySQL"""
        return self._user

    @property
    def echo(self):
        """Return echo mode status."""
        return self._echo

    @property
    def last_usage(self):
        """Return time() when connection was used."""
        return self._last_usage

    @property
    def loop(self):
        return self._loop

    @property
    def closed(self):
        """The readonly property that returns ``True`` if connections is
        closed.
        """
        return self._protocol is None

    @property
    def encoding(self):
        """Encoding employed for this connection."""
        return self._protocol.encoding

    @property
    def charset(self):
        """Returns the character set for current connection."""
        return self._protocol._charset

    def close(self):
        """Close socket connection"""
        if self._protocol is not None and not self._protocol.closed and \
                self._protocol.transport is not None:
            try:
                self._protocol.transport.close()
            except Exception:
                pass
        self._protocol = None

    @asyncio.coroutine
    def ensure_closed(self):
        """Send quit command and then close socket connection"""
        if not self.closed:
            if self._protocol is None:
                # connection has been closed
                return
            send_data = struct.pack('<i', 1) + int2byte(COMMAND.COM_QUIT)
            yield from self._protocol.write(send_data)
            self.close()

    @asyncio.coroutine
    def autocommit(self, value):
        """Enable/disable autocommit mode for current MySQL session.

        :param value: ``bool``, toggle autocommit
        """
        self.autocommit_mode = bool(value)
        current = self.get_autocommit()
        if value != current:
            yield from self._send_autocommit_mode()

    def get_autocommit(self):
        """Returns autocommit status for current MySQL session.

        :returns bool: current autocommit status."""

        status = self._protocol.server_status & \
            SERVER_STATUS.SERVER_STATUS_AUTOCOMMIT
        return bool(status)

    @asyncio.coroutine
    def _send_autocommit_mode(self):
        """Set whether or not to commit after every execute() """
        yield from self._protocol.execute_command(
            COMMAND.COM_QUERY,
            "SET AUTOCOMMIT = %s" % self.escape(self.autocommit_mode))
        yield from self._protocol.read_ok_packet()

    @asyncio.coroutine
    def begin(self):
        """Begin transaction."""
        yield from self._protocol.execute_command(COMMAND.COM_QUERY, "BEGIN")
        yield from self._protocol.read_ok_packet()

    @asyncio.coroutine
    def commit(self):
        """Commit changes to stable storage."""
        yield from self._protocol.execute_command(COMMAND.COM_QUERY, "COMMIT")
        yield from self._protocol.read_ok_packet()

    @asyncio.coroutine
    def rollback(self):
        """Roll back the current transaction."""
        yield from self._protocol.execute_command(
            COMMAND.COM_QUERY, "ROLLBACK")
        yield from self._protocol.read_ok_packet()

    @asyncio.coroutine
    def select_db(self, db):
        """Set current db"""
        yield from self._protocol.execute_command(COMMAND.COM_INIT_DB, db)
        yield from self._protocol.read_ok_packet()

    @asyncio.coroutine
    def show_warnings(self):
        """SHOW WARNINGS"""
        yield from self._protocol.execute_command(
            COMMAND.COM_QUERY, "SHOW WARNINGS")
        result = MySQLResult(self._protocol)
        yield from result.read()
        return result.rows

    def escape(self, obj):
        """ Escape whatever value you pass to it"""
        if isinstance(obj, str):
            return "'" + self.escape_string(obj) + "'"
        return escape_item(obj, self._charset)

    def literal(self, obj):
        """Alias for escape()"""
        return self.escape(obj)

    def escape_string(self, s):
        if (self._protocol.server_status &
                SERVER_STATUS.SERVER_STATUS_NO_BACKSLASH_ESCAPES):
            return s.replace("'", "''")
        return escape_string(s)

    def cursor(self, cursor=None):
        """Instantiates and returns a cursor

        By default, :class:`Cursor` is returned. It is possible to also give a
        custom cursor through the cursor_class parameter, but it needs to
        be a subclass  of :class:`Cursor`

        :param cursor: custom cursor class.
        :returns: instance of cursor, by default :class:`Cursor`
        :raises TypeError: cursor_class is not a subclass of Cursor.
        """
        self._protocol.ensure_alive()
        self._last_usage = self._loop.time()
        if cursor is not None and not issubclass(cursor, Cursor):
            raise TypeError('Custom cursor must be subclass of Cursor')

        if cursor:
            cur = cursor(self, self._echo)
        else:
            cur = self.cursorclass(self, self._echo)
        fut = create_future(self._loop)
        fut.set_result(cur)
        return _ContextManager(fut)

    # The following methods are INTERNAL USE ONLY (called from Cursor)
    @asyncio.coroutine
    def query(self, sql, unbuffered=False):
        yield from self._protocol.query(sql, unbuffered)
        return self.affected_rows

    def affected_rows(self):
        return self._protocol.affected_rows

    @asyncio.coroutine
    def kill(self, thread_id):
        arg = struct.pack('<I', thread_id)
        yield from self._protocol.execute_command(
            COMMAND.COM_PROCESS_KILL,
            arg)
        yield from self._protocol.read_ok_packet()

    @asyncio.coroutine
    def ping(self, reconnect=True):
        """Check if the server is alive"""
        if self._protocol is None:
            if reconnect:
                yield from self._connect()
                reconnect = False
            else:
                raise Error("Already closed")
        try:
            yield from self._protocol.execute_command(COMMAND.COM_PING, "")
            yield from self._protocol.read_ok_packet()
        except Exception:
            if reconnect:
                yield from self._connect()
                yield from self.ping(False)
            else:
                raise

    @asyncio.coroutine
    def set_charset(self, charset):
        """Sets the character set for the current connection"""
        # Make sure charset is supported.
        encoding = charset_by_name(charset).encoding
        yield from self._protocol.execute_command(COMMAND.COM_QUERY,
                                                  "SET NAMES %s" %
                                                  self.escape(charset))
        yield from self._protocol._read_packet()
        self._protocol._charset = charset
        self._protocol._encoding = encoding

    @asyncio.coroutine
    def _connect(self):
        # TODO: Set close callback
        # raise OperationalError(2006,
        # "MySQL server has gone away (%r)" % (e,))
        try:
            def proto_lambda():
                return MySQLProtocol(
                    host=self._host, port=self._port, user=self._user,
                    password=self._password,
                    charset=self._charset, auth_plugin=self._auth_plugin,
                    use_unicode=self.use_unicode, conv=decoders,
                    client_flag=self.client_flag, db=self.db,
                    ssl=self._ssl_context, loop=self.loop
                )

            if self._unix_socket and self._host in ('localhost', '127.0.0.1'):
                self._socket, self._protocol = yield from \
                    self.loop.create_unix_connection(proto_lambda,
                                                     self._unix_socket)
                self.host_info = "Localhost via UNIX socket: " +\
                                 self._unix_socket
            else:
                self._socket, self._protocol = yield from \
                    self.loop.create_connection(proto_lambda,
                                                self._host,
                                                self._port)
                self.host_info = "socket %s:%d" %\
                                 (self._host, self._port)

                self._set_keep_alive()

            if self._no_delay and not self._unix_socket:
                self._set_nodelay(True)

            yield from self._protocol.get_server_information()
            yield from self._protocol.request_authentication()

            self.connected_time = self._loop.time()

            if self.sql_mode is not None:
                yield from self.query("SET sql_mode=%s" % (self.sql_mode,))

            if self.init_command is not None:
                yield from self.query(self.init_command)
                yield from self.commit()

            if self.autocommit_mode is not None:
                yield from self.autocommit(self.autocommit_mode)
        except Exception as e:
            # TODO handle error when tls version not supported
            if self._protocol:
                self._protocol.transport.close()
                self._protocol = None
            raise OperationalError(2003,
                                   "Can't connect to MySQL server on %r" %
                                   self._host) from e

    def _set_keep_alive(self):
        transport = self._protocol.transport
        transport.pause_reading()
        raw_sock = transport.get_extra_info('socket', default=None)
        if raw_sock is None:
            raise RuntimeError("Transport does not expose socket instance")
        raw_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        transport.resume_reading()

    def _set_nodelay(self, value):
        flag = int(bool(value))
        transport = self._protocol.transport
        transport.pause_reading()
        raw_sock = transport.get_extra_info('socket', default=None)
        if raw_sock is None:
            raise RuntimeError("Transport does not expose socket instance")
        raw_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, flag)
        transport.resume_reading()

    def insert_id(self):
        if self._protocol.result:
            return self._protocol.result.insert_id
        else:
            return 0

    if PY_35:  # pragma: no branch
        @asyncio.coroutine
        def __aenter__(self):
            return self

        @asyncio.coroutine
        def __aexit__(self, exc_type, exc_val, exc_tb):
            if exc_type:
                self.close()
            else:
                yield from self.ensure_closed()
            return

    def thread_id(self):
        return self._protocol.server_thread_id[0]

    def character_set_name(self):
        return self._protocol._charset

    def get_host_info(self):
        return self.host_info

    def get_proto_info(self):
        return self._protocol.protocol_version

    def get_transaction_status(self):
        return bool(self._protocol.server_status &
                    SERVER_STATUS.SERVER_STATUS_IN_TRANS)

    def get_server_info(self):
        return self._protocol.server_version

    # Just to always have consistent errors 2 helpers

    def _close_on_cancel(self):
        self.close()
        self._close_reason = "Cancelled during execution"

    if PY_341:  # pragma: no branch
        def __del__(self):
            if self._protocol:
                warnings.warn("Unclosed connection {!r}".format(self),
                              ResourceWarning)
                self.close()
    Warning = Warning
    Error = Error
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError


class MySQLProtocol(asyncio.StreamReaderProtocol):
    def __init__(self, host, port, user, password, client_flag=0,
                 charset='', conv=decoders, auth_plugin='',
                 use_unicode=None, db=None, ssl=None, loop=None):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._db = db
        self.client_flag = client_flag
        self.loop = loop if loop else asyncio.get_event_loop()
        if charset:
            self._charset = charset
            self.use_unicode = True
        else:
            self._charset = DEFAULT_CHARSET
            self.use_unicode = False

        if use_unicode is not None:
            self.use_unicode = use_unicode

        self._auth_plugin = auth_plugin

        self.decoders = conv
        self.encoding = charset_by_name(self._charset).encoding

        super().__init__(
            asyncio.StreamReader(loop=self.loop),
            client_connected_cb=self._client_connected_cb,
            loop=self.loop
        )

        self._original_transport = None
        self._tls_context = ssl
        self._tls_protocol = None
        self.transport = None
        self._tls_ok = False

        self._next_seq_id = 0

        self._server_requested_auth_plugin = None
        self.server_status = None
        self.server_charset = None
        self.server_language = None
        self.server_capabilities = None
        self.salt = None
        self.server_thread_id = None
        self.server_version = None
        self.protocol_version = None

        self.result = None
        self.affected_rows = 0

    def _client_connected_cb(self, reader, writer):
        # This is redundant since we subclass StreamReaderProtocol, but I like
        # the shorter names.
        self._reader = reader
        self._writer = writer

    def connection_made(self, transport):

        if self.transport is not None and self._original_transport is not None:
            # StartTLS over normal connection
            self._reader._transport = transport
            self._writer._transport = transport
            self.transport = transport
            self._tls_ok = True
        else:
            super().connection_made(transport)
            self.transport = transport

    def eof_received(self):
        self._reader.feed_eof()
        if self.transport is not None and self._original_transport is not None:
            # Prevent a warning in SSLProtocol.eof_received:
            # "returning true from eof_received()
            # has no effect when using ssl"
            return False
        return True

    async def query(self, sql, unbuffered=False):
        if isinstance(sql, str):
            sql = sql.encode(self.encoding, 'surrogateescape')
        await self.execute_command(COMMAND.COM_QUERY, sql)
        await self._read_query_result(unbuffered=unbuffered)

    @property
    def closed(self):
        if hasattr(self.transport, '_closed'):
            return self.transport._closed
        elif hasattr(self.transport, '_closing'):
            return self.transport._closing
        elif hasattr(self.transport, '_protocol_connected'):
            return not self.transport._protocol_connected
        return False

    def ensure_alive(self):
        if self.closed:
            if self._close_reason is None:
                raise InterfaceError("(0, 'Not connected')")
            else:
                raise InterfaceError(self._close_reason)

    @asyncio.coroutine
    def execute_command(self, command, sql):
        self.ensure_alive()

        # If the last query was unbuffered, make sure it finishes before
        # sending new commands
        if self.result is not None:
            if self.result.unbuffered_active:
                warnings.warn("Previous unbuffered result was left incomplete")
                self.result._finish_unbuffered_query()
            while self.result.has_next:
                yield from self.next_result()
            self.result = None

        if isinstance(sql, str):
            sql = sql.encode(self.encoding)

        chunk_size = min(MAX_PACKET_LEN, len(sql) + 1)  # +1 is for command

        prelude = struct.pack('<iB', chunk_size, command)
        yield from self.write(prelude + sql[:chunk_size - 1])
        # logger.debug(dump_packet(prelude + sql))
        self._next_seq_id = 1

        if chunk_size < MAX_PACKET_LEN:
            return

        sql = sql[chunk_size - 1:]
        while True:
            chunk_size = min(MAX_PACKET_LEN, len(sql))
            self.write_packet(sql[:chunk_size])
            sql = sql[chunk_size:]
            if not sql and chunk_size < MAX_PACKET_LEN:
                break

    @asyncio.coroutine
    def next_result(self):
        yield from self._read_query_result()
        return self.affected_rows

    @asyncio.coroutine
    def _read_query_result(self, unbuffered=False):
        if unbuffered:
            try:
                result = MySQLResult(self)
                yield from result.init_unbuffered_query()
            except:  # noqa: E722
                result.unbuffered_active = False
                result.connection = None
                raise
        else:
            result = MySQLResult(self)
            yield from result.read()
        self.result = result
        self.affected_rows = result.affected_rows
        if result.server_status is not None:
            self.server_status = result.server_status

    @asyncio.coroutine
    def read_ok_packet(self):
        pkt = yield from self._read_packet()
        if not pkt.is_ok_packet():
            raise OperationalError(2014, "Command Out of Sync")
        ok = OKPacketWrapper(pkt)
        self.server_status = ok.server_status
        return True

    async def get_server_information(self):
        i = 0
        packet = await self._read_packet()
        data = packet.get_all_data()
        # logger.debug(dump_packet(data))
        self.protocol_version = byte2int(data[i:i + 1])
        i += 1

        server_end = data.find(b'\0', i)
        self.server_version = data[i:server_end].decode('latin1')
        i = server_end + 1

        self.server_thread_id = struct.unpack('<I', data[i:i + 4])
        i += 4

        self.salt = data[i:i + 8]
        i += 9  # 8 + 1(filler)

        self.server_capabilities = struct.unpack('<H', data[i:i + 2])[0]
        i += 2

        if len(data) >= i + 6:
            lang, stat, cap_h, salt_len = struct.unpack('<BHHB', data[i:i + 6])
            i += 6
            self.server_language = lang
            self.server_charset = charset_by_id(lang).name

            self.server_status = stat
            # logger.debug("server_status: %s" % _convert_to_str(stat))
            self.server_capabilities |= cap_h << 16
            # logger.debug("salt_len: %s" % _convert_to_str(salt_len))
            salt_len = max(12, salt_len - 9)

        # reserved
        i += 10

        if len(data) >= i + salt_len:
            # salt_len includes auth_plugin_data_part_1 and filler
            self.salt += data[i:i + salt_len]
            i += salt_len

        i += 1

        # AUTH PLUGIN NAME may appear here.
        if self.server_capabilities & CLIENT.PLUGIN_AUTH and len(data) >= i:
            # Due to Bug#59453 the auth-plugin-name is missing the terminating
            # NUL-char in versions prior to 5.5.10 and 5.6.2.
            # ref: https://dev.mysql.com/doc/internals/en/
            # connection-phase-packets.html#packet-Protocol::Handshake
            # didn't use version checks as mariadb is corrected and reports
            # earlier than those two.
            server_end = data.find(b'\0', i)
            if server_end < 0:  # pragma: no cover - very specific upstream bug
                # not found \0 and last field so take it all
                server_auth = data[i:].decode('latin1')
            else:
                server_auth = data[i:server_end].decode('latin1')

            self._server_requested_auth_plugin = server_auth

    async def request_authentication(self):
        # https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse
        if int(self.server_version.split('.', 1)[0]) >= 5:
            self.client_flag |= CLIENT.MULTI_RESULTS

        if self._user is None:
            raise ValueError("Did not specify a username")

        # SSL
        if self._tls_context:
            # capablities, max packet, charset
            data = struct.pack('<IIB', self.client_flag, 16777216, 33)
            data += b'\x00' * (32 - len(data))

            await self.write_packet(data, no_ack=True)

            self._tls_protocol = sslproto.SSLProtocol(self.loop,
                                                      self,
                                                      self._tls_context,
                                                      None,
                                                      server_side=False)
            self._original_transport = self.transport
            self._original_transport._protocol = self._tls_protocol
            self.transport = self._tls_protocol._app_transport
            self._tls_protocol.connection_made(self._original_transport)

            # Wait for handshake, #ProperHacky
            for _ in range(0, 20):
                if self._tls_ok:
                    break
                await asyncio.sleep(0.05, loop=self.loop)
            else:
                raise RuntimeError('TLS Exception')

        charset_id = charset_by_name(self._charset).id

        if isinstance(self._user, str):
            _user = self._user.encode(self.encoding)
        else:
            _user = self._user

        # Auth packet
        data = struct.pack('<IIB', self.client_flag, 16777216, charset_id)
        data += b'\x00' * (32 - len(data))

        data += _user + b'\0'

        authresp = b''

        auth_plugin = self._auth_plugin
        if self._auth_plugin == '':
            auth_plugin = self._server_requested_auth_plugin

        if auth_plugin in ('', 'mysql_native_password'):
            authresp = _scramble(self._password.encode('latin1'), self.salt)
        elif auth_plugin == 'mysql_clear_password':
            authresp = self._password.encode('latin1') + b'\0'

        if self.server_capabilities & CLIENT.PLUGIN_AUTH_LENENC_CLIENT_DATA:
            data += lenenc_int(len(authresp)) + authresp
        elif self.server_capabilities & CLIENT.SECURE_CONNECTION:
            data += struct.pack('B', len(authresp)) + authresp
        else:  # pragma: no cover
            # not testing against servers without secure auth (>=5.0)
            data += authresp + b'\0'

        # Append DB
        if self._db and self.server_capabilities & CLIENT.CONNECT_WITH_DB:
            if isinstance(self._db, str):
                db = self._db.encode('latin1')
            else:
                db = self._db
            data += db + b'\0'

        if self.server_capabilities & CLIENT.PLUGIN_AUTH:
            name = auth_plugin
            if isinstance(name, str):
                name = name.encode('ascii')
            data += name + b'\0'

        await self.write_packet(data, no_ack=True)
        auth_packet = await self._read_packet()

        #
        # # if authentication method isn't accepted the first byte
        # # will have the octet 254
        if auth_packet.is_auth_switch_request():
            # https://dev.mysql.com/doc/internals/en/
            # connection-phase-packets.html#packet-Protocol::AuthSwitchRequest
            auth_packet.read_uint8()  # 0xfe packet identifier
            plugin_name = auth_packet.read_string()
            if (self.server_capabilities & CLIENT.PLUGIN_AUTH and
                    plugin_name is not None):
                auth_packet = self._process_auth(plugin_name, auth_packet)
            else:
                # send legacy handshake
                data = _scramble_323(self._password.encode('latin1'),
                                     self.salt) + b'\0'
                self.write_packet(data)
                auth_packet = await self._read_packet()

    async def write_packet(self, payload, no_ack=False):
        """Writes an entire "mysql packet" in its entirety to the network
        addings its length and sequence number.
        """
        # Internal note: when you build packet manually and calls
        # _write_bytes() directly, you should set self._next_seq_id properly.
        data = pack_int24(len(payload)) + int2byte(self._next_seq_id) + payload
        await self.write(data)

        if no_ack:
            self._next_seq_id = (self._next_seq_id + 1) % 256

    async def write(self, data):
        self._writer.write(data)
        try:
            await self._writer.drain()
        except ConnectionResetError:
            pass

    async def _read_bytes(self, num_bytes):
        try:
            data = await self._reader.readexactly(num_bytes)
        except asyncio.streams.IncompleteReadError as e:
            msg = "Lost connection to MySQL server during query"
            raise OperationalError(2013, msg) from e
        except (IOError, OSError) as e:
            msg = "Lost connection to MySQL server during query (%s)" % (e,)
            raise OperationalError(2013, msg) from e
        return data

    async def _read_packet(self, packet_type=MysqlPacket):
        """Read an entire "mysql packet" in its entirety from the network
        and return a MysqlPacket type that represents the results.
        """
        buff = b''
        while True:
            try:
                packet_header = await self._read_bytes(4)
            except asyncio.CancelledError:
                self._close_on_cancel()
                raise

            btrl, btrh, packet_number = struct.unpack(
                '<HBB', packet_header)
            bytes_to_read = btrl + (btrh << 16)

            # Outbound and inbound packets are numbered sequentialy, so
            # we increment in both write_packet and read_packet. The count
            # is reset at new COMMAND PHASE.
            if packet_number != self._next_seq_id:
                raise InternalError(
                    "Packet sequence number wrong - got %d expected %d" %
                    (packet_number, self._next_seq_id))
            self._next_seq_id = (self._next_seq_id + 1) % 256

            try:
                recv_data = await self._read_bytes(bytes_to_read)
            except asyncio.CancelledError:
                self._close_on_cancel()
                raise

            buff += recv_data
            # https://dev.mysql.com/doc/internals/en/sending-more-than-16mbyte.html
            if bytes_to_read == 0xffffff:
                continue
            if bytes_to_read < MAX_PACKET_LEN:
                break

        packet = packet_type(buff, self.encoding)
        packet.check_error()
        return packet

    def _close_on_cancel(self):
        self.transport.close()
        self._close_reason = "Cancelled during execution"


# TODO: move OK and EOF packet parsing/logic into a proper subclass
# of MysqlPacket like has been done with FieldDescriptorPacket.
class MySQLResult:

    def __init__(self, protocol):
        self.protocol = protocol
        self.affected_rows = None
        self.insert_id = None
        self.server_status = None
        self.warning_count = 0
        self.message = None
        self.field_count = 0
        self.description = None
        self.rows = None
        self.has_next = None
        self.unbuffered_active = False

    @asyncio.coroutine
    def read(self):
        try:
            first_packet = yield from self.protocol._read_packet()

            # TODO: use classes for different packet types?
            if first_packet.is_ok_packet():
                self._read_ok_packet(first_packet)
            elif first_packet.is_load_local_packet():
                yield from self._read_load_local_packet(first_packet)
            else:
                yield from self._read_result_packet(first_packet)
        except OperationalError as err:
            raise err
        finally:
            self.protocol = None

    @asyncio.coroutine
    def init_unbuffered_query(self):
        self.unbuffered_active = True
        first_packet = yield from self.protocol._read_packet()

        if first_packet.is_ok_packet():
            self._read_ok_packet(first_packet)
            self.unbuffered_active = False
            self.protocol = None
        elif first_packet.is_load_local_packet():
            yield from self._read_load_local_packet(first_packet)
            self.unbuffered_active = False
            self.protocol = None
        else:
            self.field_count = first_packet.read_length_encoded_integer()
            yield from self._get_descriptions()

            # Apparently, MySQLdb picks this number because it's the maximum
            # value of a 64bit unsigned integer. Since we're emulating MySQLdb,
            # we set it to this instead of None, which would be preferred.
            self.affected_rows = 18446744073709551615

    def _read_ok_packet(self, first_packet):
        ok_packet = OKPacketWrapper(first_packet)
        self.affected_rows = ok_packet.affected_rows
        self.insert_id = ok_packet.insert_id
        self.server_status = ok_packet.server_status
        self.warning_count = ok_packet.warning_count
        self.message = ok_packet.message
        self.has_next = ok_packet.has_next

    @asyncio.coroutine
    def _read_load_local_packet(self, first_packet):
        load_packet = LoadLocalPacketWrapper(first_packet)
        sender = LoadLocalFile(load_packet.filename, self.protocol)
        try:
            yield from sender.send_data()
        except Exception:
            # Skip ok packet
            yield from self.protocol._read_packet()
            raise

        ok_packet = yield from self.protocol._read_packet()
        if not ok_packet.is_ok_packet():
            raise OperationalError(2014, "Commands Out of Sync")
        self._read_ok_packet(ok_packet)

    def _check_packet_is_eof(self, packet):
        if packet.is_eof_packet():
            eof_packet = EOFPacketWrapper(packet)
            self.warning_count = eof_packet.warning_count
            self.has_next = eof_packet.has_next
            return True
        return False

    @asyncio.coroutine
    def _read_result_packet(self, first_packet):
        self.field_count = first_packet.read_length_encoded_integer()
        yield from self._get_descriptions()
        yield from self._read_rowdata_packet()

    @asyncio.coroutine
    def _read_rowdata_packet_unbuffered(self):
        # Check if in an active query
        if not self.unbuffered_active:
            return

        packet = yield from self.protocol._read_packet()
        if self._check_packet_is_eof(packet):
            self.unbuffered_active = False
            self.connection = None
            self.rows = None
            return

        row = self._read_row_from_packet(packet)
        self.affected_rows = 1
        # rows should tuple of row for MySQL-python compatibility.
        self.rows = (row,)
        return row

    @asyncio.coroutine
    def _finish_unbuffered_query(self):
        # After much reading on the MySQL protocol, it appears that there is,
        # in fact, no way to stop MySQL from sending all the data after
        # executing a query, so we just spin, and wait for an EOF packet.
        while self.unbuffered_active:
            packet = yield from self.protocol._read_packet()
            if self._check_packet_is_eof(packet):
                self.unbuffered_active = False
                # release reference to kill cyclic reference.
                self.connection = None

    @asyncio.coroutine
    def _read_rowdata_packet(self):
        """Read a rowdata packet for each data row in the result set."""
        rows = []
        while True:
            packet = yield from self.protocol._read_packet()
            if self._check_packet_is_eof(packet):
                # release reference to kill cyclic reference.
                self.connection = None
                break
            rows.append(self._read_row_from_packet(packet))

        self.affected_rows = len(rows)
        self.rows = tuple(rows)

    def _read_row_from_packet(self, packet):
        row = []
        for encoding, converter in self.converters:
            try:
                data = packet.read_length_coded_string()
            except IndexError:
                # No more columns in this row
                # See https://github.com/PyMySQL/PyMySQL/pull/434
                break
            if data is not None:
                if encoding is not None:
                    data = data.decode(encoding)
                if converter is not None:
                    data = converter(data)
            row.append(data)
        return tuple(row)

    @asyncio.coroutine
    def _get_descriptions(self):
        """Read a column descriptor packet for each column in the result."""
        self.fields = []
        self.converters = []
        use_unicode = self.protocol.use_unicode
        conn_encoding = self.protocol.encoding
        description = []
        for i in range(self.field_count):
            field = yield from self.protocol._read_packet(
                FieldDescriptorPacket)
            self.fields.append(field)
            description.append(field.description())
            field_type = field.type_code
            if use_unicode:
                if field_type == FIELD_TYPE.JSON:
                    # When SELECT from JSON column: charset = binary
                    # When SELECT CAST(... AS JSON): charset = connection
                    # encoding
                    # This behavior is different from TEXT / BLOB.
                    # We should decode result by connection encoding
                    # regardless charsetnr.
                    # See https://github.com/PyMySQL/PyMySQL/issues/488
                    encoding = conn_encoding  # SELECT CAST(... AS JSON)
                elif field_type in TEXT_TYPES:
                    if field.charsetnr == 63:  # binary
                        # TEXTs with charset=binary means BINARY types.
                        encoding = None
                    else:
                        encoding = conn_encoding
                else:
                    # Integers, Dates and Times, and other basic data
                    # is encoded in ascii
                    encoding = 'ascii'
            else:
                encoding = None
            converter = self.protocol.decoders.get(field_type)
            if converter is through:
                converter = None
            self.converters.append((encoding, converter))

        eof_packet = yield from self.protocol._read_packet()
        assert eof_packet.is_eof_packet(), 'Protocol error, expecting EOF'
        self.description = tuple(description)


class LoadLocalFile(object):
    def __init__(self, filename, protocol):
        self.filename = filename
        self.protocol = protocol
        self._loop = protocol.loop
        self._file_object = None
        self._executor = None  # means use default executor

    def _open_file(self):

        def opener(filename):
            try:
                self._file_object = open(filename, 'rb')
            except (IOError, FileNotFoundError) as e:
                msg = "Can't find file '{0}'".format(filename)
                raise OperationalError(1017, msg) from e

        fut = self._loop.run_in_executor(self._executor, opener, self.filename)
        return fut

    def _file_read(self, chunk_size):

        def freader(chunk_size):
            try:
                chunk = self._file_object.read(chunk_size)

                if not chunk:
                    self._file_object.close()
                    self._file_object = None

            except Exception as e:
                self._file_object.close()
                self._file_object = None
                msg = "Error reading file {}".format(self.filename)
                raise OperationalError(1024, msg) from e
            return chunk

        fut = self._loop.run_in_executor(self._executor, freader, chunk_size)
        return fut

    @asyncio.coroutine
    def send_data(self):
        """Send data packets from the local file to the server"""
        self.protocol.ensure_alive()

        try:
            yield from self._open_file()
            with self._file_object:
                chunk_size = MAX_PACKET_LEN
                while True:
                    chunk = yield from self._file_read(chunk_size)
                    if not chunk:
                        break
                    # TODO: consider drain data
                    yield from self.protocol.write_packet(chunk, no_ack=True)
        except asyncio.CancelledError:
            self.protocol._close_on_cancel()
            raise
        finally:
            # send the empty packet to signify we are done sending data
            yield from self.protocol.write_packet(b"", no_ack=True)
