import unittest
import sqlalchemy

from aiomysql.sa.connection import _distill_params


class DistillArgsTest(unittest.TestCase):
    def test_distill_none(self):
        self.assertEqual(
            _distill_params(None, None),
            []
        )

    def test_distill_no_multi_no_param(self):
        self.assertEqual(
            _distill_params((), {}),
            []
        )

    def test_distill_dict_multi_none_param(self):
        self.assertEqual(
            _distill_params(None, {"foo": "bar"}),
            [{"foo": "bar"}]
        )

    def test_distill_dict_multi_empty_param(self):
        self.assertEqual(
            _distill_params((), {"foo": "bar"}),
            [{"foo": "bar"}]
        )

    def test_distill_single_dict(self):
        self.assertEqual(
            _distill_params(({"foo": "bar"},), {}),
            [{"foo": "bar"}]
        )

    def test_distill_single_list_strings(self):
        self.assertEqual(
            _distill_params((["foo", "bar"],), {}),
            [["foo", "bar"]]
        )

    def test_distill_single_list_tuples(self):
        self.assertEqual(
            _distill_params(([("foo", "bar"), ("bat", "hoho")],), {}),
            [('foo', 'bar'), ('bat', 'hoho')]
        )

    def test_distill_single_list_tuple(self):
        self.assertEqual(
            _distill_params(([("foo", "bar")],), {}),
            [('foo', 'bar')]
        )

    def test_distill_multi_list_tuple(self):
        self.assertEqual(
            _distill_params(
                ([("foo", "bar")], [("bar", "bat")]),
                {}
            ),
            ([('foo', 'bar')], [('bar', 'bat')])
        )

    def test_distill_multi_strings(self):
        self.assertEqual(
            _distill_params(("foo", "bar"), {}),
            [('foo', 'bar')]
        )

    def test_distill_single_list_dicts(self):
        self.assertEqual(
            _distill_params(([{"foo": "bar"}, {"foo": "hoho"}],), {}),
            [{'foo': 'bar'}, {'foo': 'hoho'}]
        )

    def test_distill_single_string(self):
        self.assertEqual(
            _distill_params(("arg",), {}),
            [["arg"]]
        )

    def test_distill_multi_string_tuple(self):
        self.assertEqual(
            _distill_params((("arg", "arg"),), {}),
            [("arg", "arg")]
        )


sqlalchemy  # for sake of pyflakes checks
