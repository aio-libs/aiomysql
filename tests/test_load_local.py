import builtins
import os
from unittest.mock import patch, MagicMock

import aiomysql
import pytest
from pymysql.constants import CLIENT
from pymysql.err import OperationalError


@pytest.fixture
def table_local_file(connection, loop):

    async def prepare_table(conn):
        c = await conn.cursor()
        await c.execute("DROP TABLE IF EXISTS test_load_local;")
        await c.execute("CREATE TABLE test_load_local "
                        "(a INTEGER, b INTEGER)")
        await c.close()

    async def drop_table(conn):
        c = await conn.cursor()
        await c.execute("DROP TABLE test_load_local")
        await c.close()

    loop.run_until_complete(prepare_table(connection))
    yield
    loop.run_until_complete(drop_table(connection))


@pytest.mark.run_loop
async def test_no_file(cursor, table_local_file):
    # Test load local infile when the file does not exist
    sql = "LOAD DATA LOCAL INFILE 'no_data.txt'" + \
          " INTO TABLE test_load_local fields " + \
          "terminated by ','"
    with pytest.raises(OperationalError):
        await cursor.execute(sql)


@pytest.mark.run_loop
async def test_error_on_file_read(cursor, table_local_file):

    with patch.object(builtins, 'open') as open_mocked:
        m = MagicMock()
        m.read.side_effect = OperationalError(1024, 'Error reading file')
        m.close.return_value = None
        open_mocked.return_value = m

        with pytest.raises(OperationalError):
            await cursor.execute("LOAD DATA LOCAL INFILE 'some.txt'"
                                 " INTO TABLE test_load_local fields "
                                 "terminated by ','")


@pytest.mark.run_loop
async def test_load_file(cursor, table_local_file):
    # Test load local infile with a valid file
    filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            'fixtures',
                            'load_local_data.txt')
    await cursor.execute(
        ("LOAD DATA LOCAL INFILE '{0}' INTO TABLE " +
         "test_load_local FIELDS TERMINATED BY ','").format(filename)
    )
    await cursor.execute("SELECT COUNT(*) FROM test_load_local")
    resp = await cursor.fetchone()
    assert 22749 == resp[0]


@pytest.mark.run_loop
async def test_load_warnings(cursor, table_local_file):
    # Test load local infile produces the appropriate warnings
    import warnings

    # TODO: Move to pathlib
    filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            'fixtures', 'load_local_warn_data.txt')

    sql = ("LOAD DATA LOCAL INFILE '{0}' INTO TABLE " +
           "test_load_local FIELDS TERMINATED BY ','").format(filename)

    with warnings.catch_warnings(record=True) as w:
        await cursor.execute(sql)
    assert "Incorrect integer value" in str(w[-1].message)


@pytest.mark.run_loop
async def test_load_local_disabled(mysql_params, table_local_file):
    # By setting the client flag, the server will be informed that we support
    # loading local files. This validates that the client side check catches
    # the server attempting to read files from us without having this
    # explicitly enabled on the connection. The local_infile parameter sets
    # the client flag, but not the other way round.
    params = mysql_params.copy()
    params["local_infile"] = False
    if "client_flag" in params:
        params["client_flag"] |= CLIENT.LOCAL_FILES
    else:
        params["client_flag"] = CLIENT.LOCAL_FILES

    async with aiomysql.connect(**params) as conn:
        async with conn.cursor() as cursor:
            # Test load local infile with a valid file
            filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                    'fixtures',
                                    'load_local_data.txt')
            with pytest.raises(
                RuntimeError,
                match="Received LOAD_LOCAL packet but local_infile option is false",
            ):
                await cursor.execute(
                    ("LOAD DATA LOCAL INFILE '{0}' INTO TABLE " +
                     "test_load_local FIELDS TERMINATED BY ','").format(filename)
                )
