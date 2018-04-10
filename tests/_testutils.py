import asyncio
import unittest
import re

from functools import wraps


def run_until_complete(fun):
    if not asyncio.iscoroutinefunction(fun):
        fun = asyncio.coroutine(fun)

    @wraps(fun)
    def wrapper(test, *args, **kw):
        loop = test.loop
        ret = loop.run_until_complete(
            asyncio.wait_for(fun(test, *args, **kw), 15, loop=loop))
        return ret
    return wrapper


class BaseTest(unittest.TestCase):
    """Base test case for unittests.
    """
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()
        del self.loop


def mysql_server_is(server_version, version_tuple):
    """Return True if the given connection is on the version given or
    greater.
    e.g.::
        if self.mysql_server_is(conn, (5, 6, 4)):
            # do something for MySQL 5.6.4 and above
    """
    server_version_tuple = tuple(
        (int(dig) if dig is not None else 0)
        for dig in
        re.match(r'(\d+)\.(\d+)\.(\d+)', server_version).group(1, 2, 3)
    )
    return server_version_tuple >= version_tuple
