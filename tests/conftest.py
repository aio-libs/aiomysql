import asyncio
import gc
import os
import ssl
import socket
import sys
import time
import uuid

from distutils.version import StrictVersion
from docker import APIClient

import aiomysql
import pymysql
import pytest


PY_35 = sys.version_info >= (3, 5)
if PY_35:
    import uvloop
else:
    uvloop = None


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


@pytest.fixture(scope='session')
def unused_port():
    def f():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', 0))
            return s.getsockname()[1]
    return f


def pytest_generate_tests(metafunc):
    if 'loop_type' in metafunc.fixturenames:
        loop_type = ['asyncio', 'uvloop'] if uvloop else ['asyncio']
        metafunc.parametrize("loop_type", loop_type)


# This is here unless someone fixes the generate_tests bit
@pytest.yield_fixture(scope='session')
def mysql_tag():
    return os.environ.get('DBTAG', '10.5')


@pytest.yield_fixture(scope='session')
def mysql_image():
    return os.environ.get('DB', 'mariadb')


@pytest.yield_fixture
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
        item = pytest.Function(name, parent=collector)
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


def pytest_ignore_collect(path, config):
    if 'pep492' in str(path):
        if sys.version_info < (3, 5, 0):
            return True


def pytest_addoption(parser):
    parser.addoption("--mysql_tag", action="append", default=[],
                     help=("MySQL server versions. "
                           "May be used several times. "
                           "Available values: 5.6, 5.7, 8.0, all"))
    parser.addoption("--no-pull", action="store_true", default=False,
                     help="Don't perform docker images pulling")


@pytest.fixture
def mysql_params(mysql_server):
    params = {**mysql_server['conn_params'],
              "db": os.environ.get('MYSQL_DB', 'test_pymysql'),
              # "password": os.environ.get('MYSQL_PASSWORD', ''),
              "local_infile": True,
              "use_unicode": True,
              }
    return params


# TODO: fix this workaround
@asyncio.coroutine
def _cursor_wrapper(conn):
    cur = yield from conn.cursor()
    return cur


@pytest.yield_fixture
def cursor(connection, loop):
    cur = loop.run_until_complete(_cursor_wrapper(connection))
    yield cur
    loop.run_until_complete(cur.close())


@pytest.yield_fixture
def connection(mysql_params, loop):
    coro = aiomysql.connect(loop=loop, **mysql_params)
    conn = loop.run_until_complete(coro)
    yield conn
    loop.run_until_complete(conn.ensure_closed())


@pytest.yield_fixture
def connection_creator(mysql_params, loop):
    connections = []

    @asyncio.coroutine
    def f(**kw):
        conn_kw = mysql_params.copy()
        conn_kw.update(kw)
        _loop = conn_kw.pop('loop', loop)
        conn = yield from aiomysql.connect(loop=_loop, **conn_kw)
        connections.append(conn)
        return conn

    yield f

    for conn in connections:
        try:
            loop.run_until_complete(conn.ensure_closed())
        except ConnectionResetError:
            pass


@pytest.yield_fixture
def pool_creator(mysql_params, loop):
    pools = []

    @asyncio.coroutine
    def f(**kw):
        conn_kw = mysql_params.copy()
        conn_kw.update(kw)
        _loop = conn_kw.pop('loop', loop)
        pool = yield from aiomysql.create_pool(loop=_loop, **conn_kw)
        pools.append(pool)
        return pool

    yield f

    for pool in pools:
        pool.close()
        loop.run_until_complete(pool.wait_closed())


@pytest.yield_fixture
def table_cleanup(loop, connection):
    table_list = []
    cursor = loop.run_until_complete(_cursor_wrapper(connection))

    def _register_table(table_name):
        table_list.append(table_name)

    yield _register_table
    for t in table_list:
        # TODO: probably this is not safe code
        sql = "DROP TABLE IF EXISTS {};".format(t)
        loop.run_until_complete(cursor.execute(sql))


@pytest.fixture(scope='session')
def session_id():
    """Unique session identifier, random string."""
    return str(uuid.uuid4())


@pytest.fixture(scope='session')
def docker():
    return APIClient(version='auto')


@pytest.fixture(autouse=True)
def ensure_mysql_verison(request, mysql_tag):
    if StrictVersion(pytest.__version__) >= StrictVersion('4.0.0'):
        mysql_version = request.node.get_closest_marker('mysql_verison')
    else:
        mysql_version = request.node.get_marker('mysql_verison')

    if mysql_version and mysql_version.args[0] != mysql_tag:
        pytest.skip('Not applicable for MySQL version: {0}'.format(mysql_tag))


@pytest.fixture(scope='session')
def mysql_server(unused_port, docker, session_id,
                 mysql_image, mysql_tag, request):
    print('\nSTARTUP CONTAINER - {0}\n'.format(mysql_tag))

    if not request.config.option.no_pull:
        docker.pull('{}:{}'.format(mysql_image, mysql_tag))

    # bound IPs do not work on OSX
    host = "127.0.0.1"
    host_port = unused_port()

    # As TLS is optional, might as well always configure it
    ssl_directory = os.path.join(os.path.dirname(__file__),
                                 'ssl_resources', 'ssl')
    ca_file = os.path.join(ssl_directory, 'ca.pem')
    tls_cnf = os.path.join(os.path.dirname(__file__),
                           'ssl_resources', 'tls.cnf')

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    ctx.check_hostname = False
    ctx.load_verify_locations(cafile=ca_file)
    # ctx.verify_mode = ssl.CERT_NONE

    container_args = dict(
        image='{}:{}'.format(mysql_image, mysql_tag),
        name='aiomysql-test-server-{}-{}'.format(mysql_tag, session_id),
        ports=[3306],
        detach=True,
        host_config=docker.create_host_config(
            port_bindings={3306: (host, host_port)},
            binds={
                ssl_directory: {'bind': '/etc/mysql/ssl', 'mode': 'ro'},
                tls_cnf: {'bind': '/etc/mysql/conf.d/tls.cnf', 'mode': 'ro'},
            }
        ),
        environment={'MYSQL_ROOT_PASSWORD': 'rootpw'}
    )

    container = docker.create_container(**container_args)

    try:
        docker.start(container=container['Id'])

        # MySQL restarts at least 4 times in the container before its ready
        time.sleep(10)

        server_params = {
            'host': host,
            'port': host_port,
            'user': 'root',
            'password': 'rootpw',
            'ssl': ctx
        }
        delay = 0.001
        for _ in range(100):
            try:
                connection = pymysql.connect(
                    db='mysql',
                    charset='utf8mb4',
                    cursorclass=pymysql.cursors.DictCursor,
                    **server_params)

                with connection.cursor() as cursor:
                    cursor.execute("SHOW VARIABLES LIKE '%ssl%';")

                    result = cursor.fetchall()
                    result = {item['Variable_name']:
                              item['Value'] for item in result}

                    assert result['have_ssl'] == "YES", \
                        "SSL Not Enabled on docker'd MySQL"

                    cursor.execute("SHOW STATUS LIKE 'Ssl_version%'")

                    result = cursor.fetchone()
                    # As we connected with TLS, it should start with that :D
                    assert result['Value'].startswith('TLS'), \
                        "Not connected to the database with TLS"

                    # Create Databases
                    cursor.execute('CREATE DATABASE test_pymysql  '
                                   'DEFAULT CHARACTER SET utf8 '
                                   'DEFAULT COLLATE utf8_general_ci;')
                    cursor.execute('CREATE DATABASE test_pymysql2 '
                                   'DEFAULT CHARACTER SET utf8 '
                                   'DEFAULT COLLATE utf8_general_ci;')

                    # Do MySQL8+ Specific Setup
                    if mysql_tag in ('8.0',):
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

                break
            except Exception:
                time.sleep(delay)
                delay *= 2
        else:
            pytest.fail("Cannot start MySQL server")

        container['host'] = host
        container['port'] = host_port
        container['conn_params'] = server_params

        yield container
    finally:
        print('\nTEARDOWN CONTAINER - {0}\n'.format(mysql_tag))
        docker.kill(container=container['Id'])
        docker.remove_container(container['Id'])
