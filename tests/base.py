import asyncio
import json

import os

import aiomysql
from tests._testutils import BaseTest


class AIOPyMySQLTestCase(BaseTest):
    # You can specify your test environment creating a file named
    # "databases.json" or editing the `databases` variable below.
    fname = os.path.join(os.path.dirname(__file__), "databases.json")
    if os.path.exists(fname):
        with open(fname) as f:
            databases = json.load(f)
    else:
        databases = [
            {"host": "localhost", "user": "root", "passwd": "",
             "db": "test_pymysql", "use_unicode": True},
            {"host": "localhost", "user": "root", "passwd": "",
             "db": "test_pymysql2"}]

    @asyncio.coroutine
    def _connect_all(self):
        for params in self.databases:
            conn = yield from aiomysql.connect(loop=self.loop, **params)
            self.connections.append(conn)

    def setUp(self):
        super(AIOPyMySQLTestCase, self).setUp()
        self.connections = []
        self.loop.run_until_complete(self._connect_all())

    def tearDown(self):
        for connection in self.connections:
            connection.close()
        super(AIOPyMySQLTestCase, self).tearDown()
