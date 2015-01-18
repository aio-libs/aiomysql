import asyncio
import os
import aiomysql
from tests._testutils import BaseTest


class AIOPyMySQLTestCase(BaseTest):

    @asyncio.coroutine
    def _connect_all(self):
        conn1 = yield from aiomysql.connect(loop=self.loop, host=self.host,
                                            port=self.port, user=self.user,
                                            db=self.db,
                                            password=self.password,
                                            use_unicode=True, echo=True)
        conn2 = yield from aiomysql.connect(loop=self.loop, host=self.host,
                                            port=self.port, user=self.user,
                                            db=self.other_db,
                                            password=self.password,
                                            use_unicode=False, echo=True)
        self.connections = [conn1, conn2]

    def setUp(self):
        super(AIOPyMySQLTestCase, self).setUp()
        self.host = os.environ.get('MYSQL_HOST', 'localhost')
        self.port = os.environ.get('MYSQL_PORT', 3306)
        self.user = os.environ.get('MYSQL_USER', 'root')
        self.db = os.environ.get('MYSQL_DB', 'test_pymysql')
        self.other_db = os.environ.get('OTHER_MYSQL_DB', 'test_pymysql2')
        self.password = ''

        self.connections = []
        self.loop.run_until_complete(self._connect_all())

    def tearDown(self):
        for connection in self.connections:
            self.loop.run_until_complete(connection.wait_closed())
        super(AIOPyMySQLTestCase, self).tearDown()

    @asyncio.coroutine
    def connect(self, host='localhost', user='root', password="",
                db='test_pymysql', use_unicode=True, no_delay=None, **kwargs):
        conn = yield from aiomysql.connect(loop=self.loop, host=host,
                                           user=user, password=password,
                                           db=db, use_unicode=use_unicode,
                                           no_delay=no_delay, **kwargs)
        return conn
