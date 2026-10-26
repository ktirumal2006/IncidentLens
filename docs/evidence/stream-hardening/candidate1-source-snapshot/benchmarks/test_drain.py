import unittest
from drain import account


class DrainTests(unittest.TestCase):
    def test_accounting_distinguishes_unemitted_and_unacknowledged(self):
        expected = {'a': {'services': ['x', 'y', 'z'], 'emitted': True, 'acked': True},
                    'b': {'services': ['x', 'y', 'z'], 'emitted': True, 'acked': False},
                    'c': {'services': ['x', 'y', 'z'], 'emitted': False, 'acked': False}}
        result = account(expected, [('a', '01', 'x'), ('a', '01', 'x'), ('a', '03', 'wrong'), ('b', '06', 'z'), ('unknown', '01', 'x')])
        self.assertEqual(result, dict(planned_missing=16, emitted_missing=10, acked_missing=5,
                                     expected_stored=2, unknown=2, duplicate_logical_identities=1))
