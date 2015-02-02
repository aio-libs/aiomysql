# Python implementation of the MySQL client-server protocol
# http://dev.mysql.com/doc/internals/en/client-server-protocol.html

import asyncio
import os
import socket
import hashlib
import struct
import sys
import configparser
import getpass
from functools import partial

from pymysql.charset import charset_by_name, charset_by_id
from pymysql.constants import SERVER_STATUS
from pymysql.constants.CLIENT import *  # noqa
from pymysql.constants.COMMAND import *  # noqa
from pymysql.util import byte2int, int2byte
from pymysql.converters import escape_item, encoders, decoders, escape_string
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


# from aiomysql.utils import _convert_to_str
from .cursors import Cursor
# from .log import logger

DEFAULT_USER = getpass.getuser()
sha_new = partial(hashlib.new, 'sha1')


@asyncio.coroutine
def connect(host="localhost", user=None, password="",
            db=None, port=3306, unix_socket=None,
            charset='', sql_mode=None,
            read_default_file=None, conv=decoders, use_unicode=None,
            client_flag=0, cursorclass=Cursor, init_command=None,
            connect_timeout=None, read_default_group=None,
            no_delay=False, autocommit=False, echo=False, loop=None):
    """See connections.Connection.__init__() for information about
    defaults."""

    conn = Connection(host=host, user=user, password=password,
                      db=db, port=port, unix_socket=unix_socket,
                      charset=charset, sql_mode=sql_mode,
                      read_default_file=read_default_file, conv=conv,
                      use_unicode=use_unicode, client_flag=client_flag,
                      cursorclass=cursorclass, init_command=init_command,
                      connect_timeout=connect_timeout,
                      read_default_group=read_default_group, no_delay=no_delay,
                      autocommit=autocommit, echo=echo, loop=loop)

    yield from conn.connect()
    return conn


class Connection:
    """
    Representation of a socket with a mysql server.

    The proper way to get an instance of this class is to call
    connect().
    """

    def __init__(self, host="localhost", user=None, password="",
                 db=None, port=3306, unix_socket=None,
                 charset='', sql_mode=None,
                 read_default_file=None, conv=decoders, use_unicode=None,
                 client_flag=0, cursorclass=Cursor, init_command=None,
                 connect_timeout=None, read_default_group=None,
                 no_delay=False, autocommit=False, echo=False, loop=None):
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

        self._host = host
        self._port = port
        self._user = user or DEFAULT_USER
        self._password = password or ""
        self._db = db
        self._no_delay = no_delay
        self._echo = echo

        self._unix_socket = unix_socket
        if charset:
            self._charset = charset
            self.use_unicode = True
        else:
            self._charset = DEFAULT_CHARSET
            self.use_unicode = False

        if use_unicode is not None:
            self.use_unicode = use_unicode

        self._encoding = charset_by_name(self._charset).encoding

        client_flag |= CAPABILITIES
        client_flag |= MULTI_STATEMENTS
        if self._db:
            client_flag |= CONNECT_WITH_DB
        self.client_flag = client_flag

        self.cursorclass = cursorclass
        self.connect_timeout = connect_timeout

        self._result = None
        self._affected_rows = 0
        self.host_info = "Not connected"

        #: specified autocommit mode. None means use server default.
        self.autocommit_mode = autocommit

        self.encoders = encoders  # Need for MySQLdb compatibility.
        self.decoders = conv
        self.sql_mode = sql_mode
        self.init_command = init_command

        # asyncio StreamReader, StreamWriter
        self._reader = None
        self._writer = None

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
    def loop(self):
        return self._loop

    @property
    def closed(self):
        return self._writer is None

    @property
    def encoding(self):
        return self._encoding

    @property
    def charset(self):
        """Returns the character set for current connection

        This property returns the character set name of the current connection.
        The server is queried when the connection is active. If not connected,
        the configured character set name is returned
        """
        return self._charset

    def close(self):
        """Send the quit message and close the socket"""
        if self._writer:
            self._writer.transport.close()
        self._writer = None
        self._reader = None

    @asyncio.coroutine
    def wait_closed(self):
        send_data = struct.pack('<i', 1) + int2byte(COM_QUIT)
        self._writer.write(send_data)
        yield from self._writer.drain()
        self.close()

    # def __del__(self):
    #     self.close()

    @asyncio.coroutine
    def autocommit(self, value):
        """autocommit value for current MySQL session

        :param value: ``bool``, toggle autocommit
        """
        self.autocommit_mode = bool(value)
        current = self.get_autocommit()
        if value != current:
            yield from self._send_autocommit_mode()

    def get_autocommit(self):
        status = self.server_status & SERVER_STATUS.SERVER_STATUS_AUTOCOMMIT
        return bool(status)

    @asyncio.coroutine
    def _read_ok_packet(self):
        pkt = yield from self._read_packet()
        if not pkt.is_ok_packet():
            raise OperationalError(2014, "Command Out of Sync")
        ok = OKPacketWrapper(pkt)
        self.server_status = ok.server_status
        return True

    @asyncio.coroutine
    def _send_autocommit_mode(self):
        """Set whether or not to commit after every execute() """
        yield from self._execute_command(
            COM_QUERY,
            "SET AUTOCOMMIT = %s" % self.escape(self.autocommit_mode))
        yield from self._read_ok_packet()

    @asyncio.coroutine
    def begin(self):
        """Begin transaction."""
        yield from self._execute_command(COM_QUERY, "BEGIN")
        yield from self._read_ok_packet()

    @asyncio.coroutine
    def commit(self):
        """ Commit changes to stable storage """
        yield from self._execute_command(COM_QUERY, "COMMIT")
        yield from self._read_ok_packet()

    @asyncio.coroutine
    def rollback(self):
        """ Roll back the current transaction """
        yield from self._execute_command(COM_QUERY, "ROLLBACK")
        yield from self._read_ok_packet()

    @asyncio.coroutine
    def select_db(self, db):
        """Set current db"""
        yield from self._execute_command(COM_INIT_DB, db)
        yield from self._read_ok_packet()

    def escape(self, obj):
        """ Escape whatever value you pass to it  """
        if isinstance(obj, str):
            return "'" + self.escape_string(obj) + "'"
        return escape_item(obj, self._charset)

    def literal(self, obj):
        """Alias for escape()"""
        return self.escape(obj)

    def escape_string(self, s):
        if (self.server_status &
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
        if cursor is not None and not issubclass(cursor, Cursor):
            raise TypeError('Custom cursor must be subclass of Cursor')

        cur = cursor(self, self._echo) if cursor else self.cursorclass(self)
        fut = asyncio.Future(loop=self._loop)
        fut.set_result(cur)
        return fut

    # The following methods are INTERNAL USE ONLY (called from Cursor)
    @asyncio.coroutine
    def query(self, sql, unbuffered=False):
        # logger.debug("DEBUG: sending query: %s", _convert_to_str(sql))
        yield from self._execute_command(COM_QUERY, sql)
        yield from self._read_query_result(unbuffered=unbuffered)
        return self._affected_rows

    @asyncio.coroutine
    def next_result(self):
        yield from self._read_query_result()
        return self._affected_rows

    def affected_rows(self):
        return self._affected_rows

    @asyncio.coroutine
    def kill(self, thread_id):
        arg = struct.pack('<I', thread_id)
        yield from self._execute_command(COM_PROCESS_KILL, arg)
        yield from self._read_ok_packet()

    @asyncio.coroutine
    def ping(self, reconnect=True):
        """Check if the server is alive"""
        if self._writer is None and self._reader is None:
            if reconnect:
                yield from self.connect()
                reconnect = False
            else:
                raise Error("Already closed")
        try:
            yield from self._execute_command(COM_PING, "")
            yield from self._read_ok_packet()
        except Exception:
            if reconnect:
                yield from self.connect()
                yield from self.ping(False)
            else:
                raise

    @asyncio.coroutine
    def set_charset(self, charset):
        """Sets the character set for the current connection"""
        # Make sure charset is supported.
        encoding = charset_by_name(charset).encoding
        yield from self._execute_command(COM_QUERY, "SET NAMES %s"
                                         % self.escape(charset))
        yield from self._read_packet()
        self._charset = charset
        self._encoding = encoding

    @asyncio.coroutine
    def connect(self):
        # TODO: Set close callback
        # raise OperationalError(2006,
        # "MySQL server has gone away (%r)" % (e,))
        try:
            if self._unix_socket and self._host in ('localhost', '127.0.0.1'):
                self._reader, self._writer = yield from \
                    asyncio.open_unix_connection(self._unix_socket,
                                                 loop=self._loop)
                self.host_info = "Localhost via UNIX socket: " + \
                                 self._unix_socket
            else:
                self._reader, self._writer = yield from \
                    asyncio.open_connection(self._host, self._port,
                                            loop=self._loop)
                self.host_info = "socket %s:%d" % (self._host, self._port)

            if self._no_delay:
                self._set_nodelay(True)

            yield from self._get_server_information()
            yield from self._request_authentication()

            self.connected_time = self._loop.time()

            if self.sql_mode is not None:
                yield from self.query("SET sql_mode=%s" % (self.sql_mode,))

            if self.init_command is not None:
                yield from self.query(self.init_command)
                yield from self.commit()

            if self.autocommit_mode is not None:
                yield from self.autocommit(self.autocommit_mode)
        except OSError as e:
            self._reader, self._writer = None, None
            raise OperationalError(
                2003, "Can't connect to MySQL server on %r (%s)"
                % (self._host, e))

    def _set_nodelay(self, value):
        flag = int(bool(value))
        transport = self._writer.transport
        transport.pause_reading()
        raw_sock = transport.get_extra_info('socket', default=None)
        if raw_sock is None:
            raise RuntimeError("Transport does not expose socket instance")
        raw_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, flag)
        transport.resume_reading()

    @asyncio.coroutine
    def _read_packet(self, packet_type=MysqlPacket):
        """Read an entire "mysql packet" in its entirety from the network
        and return a MysqlPacket type that represents the results.
        """
        buff = b''
        try:
            while True:
                packet_header = yield from self._reader.readexactly(4)
                # logger.debug(_convert_to_str(packet_header))
                packet_length_bin = packet_header[:3]

                # TODO: check sequence id
                #  packet_number
                byte2int(packet_header[3])
                # pad little-endian number
                bin_length = packet_length_bin + b'\0'
                bytes_to_read = struct.unpack('<I', bin_length)[0]
                recv_data = yield from self._reader.readexactly(bytes_to_read)
                # logger.debug(dump_packet(recv_data))
                buff += recv_data
                if bytes_to_read < MAX_PACKET_LEN:
                    break
        except (OSError, EOFError) as exc:
            msg = "MySQL server has gone away (%s)"
            raise OperationalError(2006, msg % (exc,)) from exc
        packet = packet_type(buff, self._encoding)
        packet.check_error()
        return packet

    def _write_bytes(self, data):
        return self._writer.write(data)

    @asyncio.coroutine
    def _read_query_result(self, unbuffered=False):
        if unbuffered:
            try:
                result = MySQLResult(self)
                yield from result.init_unbuffered_query()
            except:
                result.unbuffered_active = False
                result.connection = None
                raise
        else:
            result = MySQLResult(self)
            yield from result.read()
        self._result = result
        self._affected_rows = result.affected_rows
        if result.server_status is not None:
            self.server_status = result.server_status

    def insert_id(self):
        if self._result:
            return self._result.insert_id
        else:
            return 0

    @asyncio.coroutine
    def _execute_command(self, command, sql):
        if not self._writer:
            raise InterfaceError("(0, 'Not connected')")

        # If the last query was unbuffered, make sure it finishes before
        # sending new commands
        if self._result is not None and self._result.unbuffered_active:
            yield from self._result._finish_unbuffered_query()

        if isinstance(sql, str):
            sql = sql.encode(self._encoding)

        chunk_size = min(MAX_PACKET_LEN, len(sql) + 1)  # +1 is for command

        prelude = struct.pack('<i', chunk_size) + int2byte(command)
        self._write_bytes(prelude + sql[:chunk_size - 1])
        # logger.debug(dump_packet(prelude + sql))
        if chunk_size < MAX_PACKET_LEN:
            return

        seq_id = 1
        sql = sql[chunk_size - 1:]
        while True:
            chunk_size = min(MAX_PACKET_LEN, len(sql))
            prelude = struct.pack('<i', chunk_size)[:3]
            data = prelude + int2byte(seq_id % 256) + sql[:chunk_size]
            self._write_bytes(data)
            # logger.debug(dump_packet(data))
            sql = sql[chunk_size:]
            if not sql and chunk_size < MAX_PACKET_LEN:
                break
            seq_id += 1

    @asyncio.coroutine
    def _request_authentication(self):
        self.client_flag |= CAPABILITIES
        if self.server_version.startswith('5'):
            self.client_flag |= MULTI_RESULTS

        if self._user is None:
            raise ValueError("Did not specify a username")

        charset_id = charset_by_name(self._charset).id
        user = self._user
        if isinstance(self._user, str):
            user = self._user.encode(self._encoding)

        data_init = (
            struct.pack('<i', self.client_flag) + struct.pack("<I", 1) +
            int2byte(charset_id) + int2byte(0) * 23)

        next_packet = 1

        data = data_init + user + b'\0' + _scramble(
            self._password.encode('latin1'), self.salt)

        if self._db:
            db = self._db
            if isinstance(self._db, str):
                db = self._db.encode(self._encoding)
            data += db + int2byte(0)

        data = pack_int24(len(data)) + int2byte(next_packet) + data
        next_packet += 2
        # logger.debug(dump_packet(data))
        self._write_bytes(data)

        auth_packet = yield from self._read_packet()

        # if old_passwords is enabled the packet will be 1 byte long and
        # have the octet 254

        if auth_packet.is_eof_packet():
            # send legacy handshake
            data = _scramble_323(self._password.encode('latin1'),
                                 self.salt) + b'\0'
            data = pack_int24(len(data)) + int2byte(next_packet) + data
            self._write_bytes(data)
            auth_packet = self._read_packet()

    # _mysql support
    def thread_id(self):
        return self.server_thread_id[0]

    def character_set_name(self):
        return self._charset

    def get_host_info(self):
        return self.host_info

    def get_proto_info(self):
        return self.protocol_version

    @asyncio.coroutine
    def _get_server_information(self):
        i = 0
        packet = yield from self._read_packet()
        data = packet.get_all_data()
        # logger.debug(dump_packet(data))
        self.protocol_version = byte2int(data[i:i + 1])
        i += 1

        server_end = data.find(int2byte(0), i)
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
            # TODO: AUTH PLUGIN NAME may appeare here.

    def get_transaction_status(self):
        return bool(self.server_status & SERVER_STATUS.SERVER_STATUS_IN_TRANS)

    def get_server_info(self):
        return self.server_version

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


# TODO: move OK and EOF packet parsing/logic into a proper subclass
# of MysqlPacket like has been done with FieldDescriptorPacket.
class MySQLResult:

    def __init__(self, connection):
        self.connection = connection
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
            first_packet = yield from self.connection._read_packet()

            # TODO: use classes for different packet types?
            if first_packet.is_ok_packet():
                self._read_ok_packet(first_packet)
            else:
                yield from self._read_result_packet(first_packet)
        finally:
            self.connection = None

    @asyncio.coroutine
    def init_unbuffered_query(self):
        self.unbuffered_active = True
        first_packet = yield from self.connection._read_packet()

        if first_packet.is_ok_packet():
            self._read_ok_packet(first_packet)
            self.unbuffered_active = False
            self.connection = None
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

        packet = yield from self.connection._read_packet()
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
            packet = yield from self.connection._read_packet()
            if self._check_packet_is_eof(packet):
                self.unbuffered_active = False
                # release reference to kill cyclic reference.
                self.connection = None

    @asyncio.coroutine
    def _read_rowdata_packet(self):
        """Read a rowdata packet for each data row in the result set."""
        rows = []
        while True:
            packet = yield from self.connection._read_packet()
            if self._check_packet_is_eof(packet):
                # release reference to kill cyclic reference.
                self.connection = None
                break
            rows.append(self._read_row_from_packet(packet))

        self.affected_rows = len(rows)
        self.rows = tuple(rows)

    def _read_row_from_packet(self, packet):
        use_unicode = self.connection.use_unicode
        row = []
        for field in self.fields:
            data = packet.read_length_coded_string()
            if data is not None:
                field_type = field.type_code
                if use_unicode:
                    if field_type in TEXT_TYPES:
                        charset = charset_by_id(field.charsetnr)
                        if use_unicode and not charset.is_binary:
                            # TEXTs with charset=binary means BINARY types.
                            data = data.decode(charset.encoding)
                    else:
                        data = data.decode()

                converter = self.connection.decoders.get(field_type)

                # logger.debug('DEBUG: field={}, converter={}'.format(
                #     field, converter))
                # logger.debug('DEBUG: DATA = {}'.format(data))

                if converter is not None:
                    data = converter(data)
            row.append(data)
        return tuple(row)

    @asyncio.coroutine
    def _get_descriptions(self):
        """Read a column descriptor packet for each column in the result."""
        self.fields = []
        description = []
        for i in range(self.field_count):
            field = yield from self.connection._read_packet(
                FieldDescriptorPacket)
            self.fields.append(field)
            description.append(field.description())

        eof_packet = yield from self.connection._read_packet()
        assert eof_packet.is_eof_packet(), 'Protocol error, expecting EOF'
        self.description = tuple(description)
