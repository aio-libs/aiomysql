import asyncio
from unittest.mock import patch, MagicMock
from pymysql.err import OperationalError

import os
import builtins
from tests._testutils import run_until_complete
from tests.base import AIOPyMySQLTestCase


class TestLoadLocal(AIOPyMySQLTestCase):
    def setUp(self):
        super().setUp()
        self.loop.run_until_complete(self._prepare_table())

    def tearDown(self):
        self.loop.run_until_complete(self._drop_table())
        super().setUp()

    @asyncio.coroutine
    def _prepare_table(self):
        conn = self.connections[2]
        c = yield from conn.cursor()
        yield from c.execute("DROP TABLE IF EXISTS test_load_local;")
        yield from c.execute("CREATE TABLE test_load_local "
                             "(a INTEGER, b INTEGER)")
        yield from c.close()

    def _drop_table(self):
        conn = self.connections[2]
        c = yield from conn.cursor()
        yield from c.execute("DROP TABLE test_load_local")
        yield from c.close()

    @run_until_complete
    def test_no_file(self):
        # Test load local infile when the file does not exist
        conn = self.connections[2]
        c = yield from conn.cursor()
        with self.assertRaises(OperationalError):
            yield from c.execute("LOAD DATA LOCAL INFILE 'no_data.txt'"
                                 " INTO TABLE test_load_local fields "
                                 "terminated by ','")
        yield from c.close()

    @run_until_complete
    def test_error_on_file_read(self):
        # Test exception while reading file
        conn = self.connections[2]
        c = yield from conn.cursor()

        with patch.object(builtins, 'open') as open_mocked:
            m = MagicMock()
            m.read.side_effect = OperationalError(1024, 'Error reading file')
            m.close.return_value = None
            open_mocked.return_value = m

            with self.assertRaises(OperationalError):
                yield from c.execute("LOAD DATA LOCAL INFILE 'some.txt'"
                                     " INTO TABLE test_load_local fields "
                                     "terminated by ','")
        yield from c.close()

    @run_until_complete
    def test_load_file(self):
        # Test load local infile with a valid file
        conn = self.connections[2]
        c = yield from conn.cursor()
        filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                'fixtures',
                                'load_local_data.txt')
        yield from c.execute(
            ("LOAD DATA LOCAL INFILE '{0}' INTO TABLE " +
             "test_load_local FIELDS TERMINATED BY ','").format(filename)
        )
        yield from c.execute("SELECT COUNT(*) FROM test_load_local")
        resp = yield from c.fetchone()
        yield from c.close()
        self.assertEqual(22749, resp[0])

    @run_until_complete
    def test_load_warnings(self):
        # Test load local infile produces the appropriate warnings
        import warnings

        conn = self.connections[2]
        c = yield from conn.cursor()
        filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                'fixtures',
                                'load_local_warn_data.txt')

        with warnings.catch_warnings(record=True) as w:
            yield from c.execute(
                ("LOAD DATA LOCAL INFILE '{0}' INTO TABLE " +
                 "test_load_local FIELDS TERMINATED BY ','").format(
                    filename)
            )
            self.assertEqual(True, "Incorrect integer value"
                             in str(w[-1].message))
