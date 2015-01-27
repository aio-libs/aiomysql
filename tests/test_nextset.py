import unittest

from tests import base
from tests._testutils import run_until_complete


class TestNextset(base.AIOPyMySQLTestCase):
    def setUp(self):
        super(TestNextset, self).setUp()
        self.con = self.connections[0]

    @run_until_complete
    def test_nextset(self):
        cur = yield from self.con.cursor()
        yield from cur.execute("SELECT 1; SELECT 2;")
        r = yield from cur.fetchall()
        self.assertEqual([(1,)], list(r))

        r = yield from cur.nextset()
        self.assertTrue(r)

        r = yield from cur.fetchall()
        self.assertEqual([(2,)], list(r))
        res = yield from cur.nextset()
        self.assertIsNone(res)

    @run_until_complete
    def test_skip_nextset(self):
        cur = yield from self.con.cursor()
        yield from cur.execute("SELECT 1; SELECT 2;")
        r = yield from cur.fetchall()
        self.assertEqual([(1,)], list(r))

        yield from cur.execute("SELECT 42")
        r = yield from cur.fetchall()
        self.assertEqual([(42,)], list(r))

    @run_until_complete
    def test_ok_and_next(self):
        cur = yield from self.con.cursor()
        yield from cur.execute("SELECT 1; commit; SELECT 2;")
        r = yield from cur.fetchall()
        self.assertEqual([(1,)], list(r))
        res = yield from cur.nextset()
        self.assertTrue(res)
        res = yield from cur.nextset()
        self.assertTrue(res)
        r = yield from cur.fetchall()
        self.assertEqual([(2,)], list(r))
        res = yield from cur.nextset()
        self.assertIsNone(res)

    @unittest.expectedFailure
    @run_until_complete
    def test_multi_cursor(self):
        cur1 = yield from self.con.cursor()
        cur2 = yield from self.con.cursor()

        yield from cur1.execute("SELECT 1; SELECT 2;")
        yield from cur2.execute("SELECT 42")

        r1 = yield from cur1.fetchall()
        r2 = yield from cur2.fetchall()

        self.assertEqual([(1,)], list(r1))
        self.assertEqual([(42,)], list(r2))

        res = yield from cur1.nextset()
        self.assertTrue(res)

        self.assertEqual([(2,)], list(r1))
        res = yield from cur1.nextset()
        self.assertIsNone(res)

        # TODO: How about SSCursor and nextset?
        # It's very hard to implement correctly...
