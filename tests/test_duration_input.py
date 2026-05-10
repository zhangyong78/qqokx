from __future__ import annotations

import unittest

from okx_quant.duration_input import (
    format_duration_cn_compact,
    parse_nonnegative_duration_seconds,
    try_parse_nonnegative_duration_seconds,
)


class ParseNonnegativeDurationSecondsTests(unittest.TestCase):
    def test_plain_integer_seconds(self) -> None:
        self.assertEqual(parse_nonnegative_duration_seconds("0", field_name="x"), 0)
        self.assertEqual(parse_nonnegative_duration_seconds("", field_name="x"), 0)
        self.assertEqual(parse_nonnegative_duration_seconds("300", field_name="x"), 300)
        self.assertEqual(parse_nonnegative_duration_seconds("  86400  ", field_name="x"), 86400)

    def test_composed_units(self) -> None:
        self.assertEqual(parse_nonnegative_duration_seconds("5m", field_name="x"), 300)
        self.assertEqual(parse_nonnegative_duration_seconds("2h30m", field_name="x"), 9000)
        self.assertEqual(parse_nonnegative_duration_seconds("1天", field_name="x"), 86400)
        self.assertEqual(parse_nonnegative_duration_seconds("1天2小时", field_name="x"), 93600)
        self.assertEqual(parse_nonnegative_duration_seconds("90分", field_name="x"), 5400)
        self.assertEqual(parse_nonnegative_duration_seconds("45秒", field_name="x"), 45)
        self.assertEqual(parse_nonnegative_duration_seconds("1d12h", field_name="x"), 129600)

    def test_whitespace_tolerance(self) -> None:
        self.assertEqual(parse_nonnegative_duration_seconds(" 2 h 15 m ", field_name="x"), 8100)

    def test_invalid_raises(self) -> None:
        with self.assertRaises(ValueError):
            parse_nonnegative_duration_seconds("-1", field_name="f")
        with self.assertRaises(ValueError):
            parse_nonnegative_duration_seconds("1x", field_name="f")
        with self.assertRaises(ValueError):
            parse_nonnegative_duration_seconds("h30m", field_name="f")

    def test_try_parse_returns_none_on_invalid(self) -> None:
        self.assertIsNone(try_parse_nonnegative_duration_seconds("oops"))


class FormatDurationCnCompactTests(unittest.TestCase):
    def test_examples(self) -> None:
        self.assertEqual(format_duration_cn_compact(0), "0秒")
        self.assertEqual(format_duration_cn_compact(45), "45秒")
        self.assertEqual(format_duration_cn_compact(300), "5分")
        self.assertEqual(format_duration_cn_compact(3661), "1小时1分1秒")
        self.assertEqual(format_duration_cn_compact(86400), "1天")


if __name__ == "__main__":
    unittest.main()
