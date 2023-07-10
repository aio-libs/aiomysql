from dataclasses import dataclass
from typing import TYPE_CHECKING

from pymysql import OperationalError
from pymysql.connections import _auth

from .log import logger

if TYPE_CHECKING:
    from aiomysql.connection import Connection


@dataclass
class AuthInfo:
    password: str
    secure: bool
    conn: "Connection"


class AuthPlugin:
    """
    Abstract base class for authentication plugins.
    """

    name = ""

    async def auth(self, auth_info, data):
        """
        Async generator for authentication process.

        Subclasses should extend this method.

        Many authentication plugins require back-and-forth exchanges
        with the server. These client/server IO - including constructing
        the MySQL protocol packets - is handled by the Connection.
        All this generator needs to do is receive and send plugin specific data.

        Example:
        ```
        class EchoPlugin(AuthPlugin):
            async def auth(self, auth_info, data):
                data_from_server = data
                while True:
                    data_to_server = data_from_server
                    data_from_server = yield data_to_server
        ```

        :param auth_info: Various metadata from the current connection,
            including a reference to the connection itself.
        :param data: Arbitrary data sent by the server.
            This can be, for example, a salt, but it's really up to the
            plugin protocol to choose.
        """
        yield b""

    async def start(
        self, auth_info, data
    ):
        state = self.auth(auth_info, data)
        data = await state.__anext__()
        return data, state


class MysqlNativePassword(AuthPlugin):
    name = "mysql_native_password"

    async def auth(self, auth_info, data):
        yield _auth.scramble_native_password(auth_info.password.encode('latin1'), data)


class CachingSha2Password(AuthPlugin):
    name = "caching_sha2_password"

    async def auth(self, auth_info, data):
        salt = data
        if auth_info.password:
            data = yield _auth.scramble_caching_sha2(
                auth_info.password.encode('latin1'), data
            )
        else:
            data = yield b""

        # magic numbers:
        # 2 - request public key
        # 3 - fast auth succeeded
        # 4 - need full auth

        n = data[0]

        if n == 3:
            logger.debug("caching sha2: succeeded by fast path.")
            yield None
            return

        if n != 4:
            raise OperationalError("caching sha2: Unknown "
                                   "result for fast auth: {}".format(n))

        logger.debug("caching sha2: Trying full auth...")

        if auth_info.secure:
            logger.debug("caching sha2: Sending plain "
                         "password via secure connection")
            yield auth_info.password.encode('latin1') + b'\0'
            return

        if not auth_info.conn.server_public_key:
            auth_info.conn.server_public_key = yield b'\x02'
            logger.debug(auth_info.conn.server_public_key.decode('ascii'))

        yield _auth.sha2_rsa_encrypt(
            auth_info.password.encode('latin1'), salt,
            auth_info.conn.server_public_key
        )


class Sha256Password(AuthPlugin):
    name = "sha256_password"

    async def auth(self, auth_info, data):
        if auth_info.secure:
            logger.debug("sha256: Sending plain password")
            yield auth_info.password.encode('latin1') + b'\0'
            return

        salt = data

        if auth_info.password:
            data = yield b'\1'  # request public key
            auth_info.conn.server_public_key = data
            logger.debug(
                "Received public key:\n%s",
                auth_info.conn.server_public_key.decode('ascii')
            )
            yield _auth.sha2_rsa_encrypt(
                auth_info.password.encode('latin1'), salt,
                auth_info.conn.server_public_key.server_public_key
            )

        else:
            yield b'\0'  # empty password


class MysqlClearPassword(AuthPlugin):
    name = "mysql_clear_password"

    async def auth(self, auth_info, data):
        yield auth_info.password.encode('latin1') + b'\0'


class MysqlOldPassword(AuthPlugin):
    name = "mysql_old_password"

    async def auth(self, auth_info, data):
        yield _auth.scramble_old_password(
            auth_info.password.encode('latin1'),
            data,
        ) + b'\0'


def get_plugins():
    return [
        MysqlNativePassword(),
        CachingSha2Password(),
        Sha256Password(),
        MysqlClearPassword(),
        MysqlOldPassword()
    ]
