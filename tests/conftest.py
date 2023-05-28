import asyncio
import gc
import os
import re
import ssl

import aiomysql
import pymysql
import pytest
import uvloop


@pytest.fixture
def disable_gc():
    gc_enabled = gc.isenabled()
    if gc_enabled:
        gc.disable()
        gc.collect()
    yield
    if gc_enabled:
        gc.collect()
        gc.enable()


def pytest_generate_tests(metafunc):
    if 'loop_type' in metafunc.fixturenames:
        loop_type = ['asyncio', 'uvloop'] if uvloop else ['asyncio']
        metafunc.parametrize("loop_type", loop_type)

    if "mysql_address" in metafunc.fixturenames:
        mysql_addresses = []
        ids = []

        opt_mysql_unix_socket = \
            list(metafunc.config.getoption("mysql_unix_socket"))
        for i in range(len(opt_mysql_unix_socket)):
            if "=" in opt_mysql_unix_socket[i]:
                label, path = opt_mysql_unix_socket[i].split("=", 1)
                mysql_addresses.append(path)
                ids.append(label)
            else:
                mysql_addresses.append(opt_mysql_unix_socket[i])
                ids.append(f"unix{i}")

        opt_mysql_address = list(metafunc.config.getoption("mysql_address"))
        for i in range(len(opt_mysql_address)):
            if "=" in opt_mysql_address[i]:
                label, addr = opt_mysql_address[i].split("=", 1)
                ids.append(label)
            else:
                addr = opt_mysql_address[i]
                ids.append(f"tcp{i}")

            if ":" in addr:
                addr = addr.split(":", 1)
                mysql_addresses.append((addr[0], int(addr[1])))
            else:
                mysql_addresses.append((addr, 3306))

        # default to connecting to localhost
        if len(mysql_addresses) == 0:
            mysql_addresses = [("127.0.0.1", 3306)]
            ids = ["tcp-local"]

        assert len(mysql_addresses) == len(set(mysql_addresses)), \
            "mysql targets are not unique"
        assert len(ids) == len(set(ids)), \
            "mysql target names are not unique"

        metafunc.parametrize("mysql_address",
                             mysql_addresses,
                             ids=ids,
                             scope="session",
                             )


@pytest.fixture
def loop(request, loop_type):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(None)

    if uvloop and loop_type == 'uvloop':
        loop = uvloop.new_event_loop()
    else:
        loop = asyncio.new_event_loop()

    yield loop

    if not loop._closed:
        loop.call_soon(loop.stop)
        loop.run_forever()
        loop.close()
    gc.collect()
    asyncio.set_event_loop(None)


@pytest.mark.tryfirst
def pytest_pycollect_makeitem(collector, name, obj):
    if collector.funcnamefilter(name):
        if not callable(obj):
            return
        item = pytest.Function.from_parent(collector, name=name)
        if 'run_loop' in item.keywords:
            return list(collector._genfunctions(name, obj))


@pytest.mark.tryfirst
def pytest_pyfunc_call(pyfuncitem):
    """
    Run asyncio marked test functions in an event loop instead of a normal
    function call.
    """
    if 'run_loop' in pyfuncitem.keywords:
        funcargs = pyfuncitem.funcargs
        loop = funcargs['loop']
        testargs = {arg: funcargs[arg]
                    for arg in pyfuncitem._fixtureinfo.argnames}
        loop.run_until_complete(pyfuncitem.obj(**testargs))
        return True


def pytest_runtest_setup(item):
    if 'run_loop' in item.keywords and 'loop' not in item.fixturenames:
        # inject an event loop fixture for all async tests
        item.fixturenames.append('loop')


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "run_loop"
    )
    config.addinivalue_line(
        "markers",
        "mysql_version(db, version): run only on specific database versions"
    )


def pytest_addoption(parser):
    parser.addoption(
        "--mysql-address",
        action="append",
        default=[],
        help="list of addresses to connect to: [name=]host[:port]",
    )
    parser.addoption(
        "--mysql-unix-socket",
        action="append",
        default=[],
        help="list of unix sockets to connect to: [name=]/path/to/socket",
    )


@pytest.fixture
def mysql_params(mysql_server):
    params = {**mysql_server['conn_params'],
              "db": os.environ.get('MYSQL_DB', 'test_pymysql'),
              "local_infile": True,
              "use_unicode": True,
              }
    return params


# TODO: fix this workaround
async def _cursor_wrapper(conn):
    return await conn.cursor()


@pytest.fixture
def cursor(connection, loop):
    cur = loop.run_until_complete(_cursor_wrapper(connection))
    yield cur
    loop.run_until_complete(cur.close())


@pytest.fixture
def connection(mysql_params, loop):
    coro = aiomysql.connect(loop=loop, **mysql_params)
    conn = loop.run_until_complete(coro)
    yield conn
    loop.run_until_complete(conn.ensure_closed())


@pytest.fixture
def connection_creator(mysql_params, loop):
    connections = []

    async def f(**kw):
        conn_kw = mysql_params.copy()
        conn_kw.update(kw)
        _loop = conn_kw.pop('loop', loop)
        conn = await aiomysql.connect(loop=_loop, **conn_kw)
        connections.append(conn)
        return conn

    yield f

    for conn in connections:
        try:
            loop.run_until_complete(conn.ensure_closed())
        except ConnectionResetError:
            pass


@pytest.fixture
def pool_creator(mysql_params, loop):
    pools = []

    async def f(**kw):
        conn_kw = mysql_params.copy()
        conn_kw.update(kw)
        _loop = conn_kw.pop('loop', loop)
        pool = await aiomysql.create_pool(loop=_loop, **conn_kw)
        pools.append(pool)
        return pool

    yield f

    for pool in pools:
        pool.close()
        loop.run_until_complete(pool.wait_closed())


@pytest.fixture
def table_cleanup(loop, connection):
    table_list = []
    cursor = loop.run_until_complete(_cursor_wrapper(connection))

    def _register_table(table_name):
        table_list.append(table_name)

    yield _register_table
    for t in table_list:
        # TODO: probably this is not safe code
        sql = f"DROP TABLE IF EXISTS {t};"
        loop.run_until_complete(cursor.execute(sql))


@pytest.fixture(scope='session')
def mysql_server(mysql_address):
    unix_socket = type(mysql_address) is str

    if not unix_socket:
        ssl_directory = os.path.join(os.path.dirname(__file__),
                                     'ssl_resources', 'ssl')
        ca_file = os.path.join(ssl_directory, 'ca.pem')

        ctx = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        ctx.check_hostname = False
        ctx.load_verify_locations(cafile=ca_file)
        # ctx.verify_mode = ssl.CERT_NONE

    server_params = {
        'user': 'root',
        'password': os.environ.get("MYSQL_ROOT_PASSWORD"),
    }

    if unix_socket:
        server_params["unix_socket"] = mysql_address
    else:
        server_params["host"] = mysql_address[0]
        server_params["port"] = mysql_address[1]
        server_params["ssl"] = ctx

    try:
        connection = pymysql.connect(
            db='mysql',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor,
            **server_params)

        with connection.cursor() as cursor:
            cursor.execute("SELECT VERSION() AS version")
            server_version = cursor.fetchone()["version"]
            server_version_tuple = tuple(
                (int(dig) if dig is not None else 0)
                for dig in
                re.match(r"^(\d+)\.(\d+)(?:\.(\d+))?", server_version).group(1, 2, 3)
            )
            server_version_tuple_short = (server_version_tuple[0],
                                          server_version_tuple[1])
            if server_version_tuple_short in [(5, 7), (8, 0)]:
                db_type = "mysql"
            elif server_version_tuple[0] == 10:
                db_type = "mariadb"
            else:
                pytest.fail("Unable to determine database type from {!r}"
                            .format(server_version_tuple))

            if not unix_socket:
                cursor.execute("SHOW VARIABLES LIKE '%ssl%';")

                result = cursor.fetchall()
                result = {item['Variable_name']:
                          item['Value'] for item in result}

                assert result['have_ssl'] == "YES", \
                    "SSL Not Enabled on MySQL"

                cursor.execute("SHOW STATUS LIKE 'Ssl_version%'")

                result = cursor.fetchone()
                # As we connected with TLS, it should start with that :D
                assert result['Value'].startswith('TLS'), \
                    "Not connected to the database with TLS"

            # Drop possibly existing old databases
            cursor.execute('DROP DATABASE IF EXISTS test_pymysql;')
            cursor.execute('DROP DATABASE IF EXISTS test_pymysql2;')

            # Create Databases
            cursor.execute('CREATE DATABASE test_pymysql  '
                           'DEFAULT CHARACTER SET utf8 '
                           'DEFAULT COLLATE utf8_general_ci;')
            cursor.execute('CREATE DATABASE test_pymysql2 '
                           'DEFAULT CHARACTER SET utf8 '
                           'DEFAULT COLLATE utf8_general_ci;')

            # Do MySQL8+ Specific Setup
            if db_type == "mysql" and server_version_tuple_short == (8, 0):
                # Drop existing users
                cursor.execute('DROP USER IF EXISTS user_sha256;')
                cursor.execute('DROP USER IF EXISTS nopass_sha256;')
                cursor.execute('DROP USER IF EXISTS user_caching_sha2;')
                cursor.execute('DROP USER IF EXISTS nopass_caching_sha2;')

                # Create Users to test SHA256
                cursor.execute('CREATE USER user_sha256 '
                               'IDENTIFIED WITH "sha256_password" '
                               'BY "pass_sha256"')
                cursor.execute('CREATE USER nopass_sha256 '
                               'IDENTIFIED WITH "sha256_password"')
                cursor.execute('CREATE USER user_caching_sha2   '
                               'IDENTIFIED '
                               'WITH "caching_sha2_password" '
                               'BY "pass_caching_sha2"')
                cursor.execute('CREATE USER nopass_caching_sha2 '
                               'IDENTIFIED '
                               'WITH "caching_sha2_password" '
                               'PASSWORD EXPIRE NEVER')
                cursor.execute('FLUSH PRIVILEGES')
        connection.close()
    except Exception:
        pytest.fail("Cannot initialize MySQL environment")

    return {
        "conn_params": server_params,
        "server_version": server_version,
        "server_version_tuple": server_version_tuple,
        "server_version_tuple_short": server_version_tuple_short,
        "db_type": db_type,
    }
