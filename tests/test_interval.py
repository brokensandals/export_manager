import unittest
from datetime import timedelta
from export_manager.interval import parse_delta


class IntervalTests(unittest.TestCase):
    def test_empty(self):
        self.assertRaises(ValueError, parse_delta, '')

    def test_blank(self):
        self.assertRaises(ValueError, parse_delta, ' \t ')

    def test_invalid_chars(self):
        self.assertRaises(ValueError, parse_delta, '3 minutes + 5 seconds')

    def test_multi(self):
        self.assertEqual(parse_delta('3 minutes 5 seconds'),
                         timedelta(minutes=3, seconds=5))

    def test_comma(self):
        self.assertEqual(parse_delta('3 minutes, 5 seconds'),
                         timedelta(minutes=3, seconds=5))

    def test_all(self):
        self.assertEqual(parse_delta('1 week 2 days 3 hours 4 minutes 5 seconds'),
                         timedelta(weeks=1, days=2, hours=3, minutes=4,
                                   seconds=5))

    def test_short(self):
        self.assertEqual(parse_delta('1w2d3h4m5s'),
                         timedelta(weeks=1, days=2, hours=3, minutes=4,
                                   seconds=5))


if __name__ == '__main__':
    unittest.main()
