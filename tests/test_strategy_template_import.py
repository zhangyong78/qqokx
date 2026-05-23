from decimal import Decimal
from unittest import TestCase

from okx_quant.ui import QuantApp


class StrategyTemplateImportCompatibilityTest(TestCase):
    def test_optional_nonnegative_order_size_accepts_zero(self) -> None:
        self.assertEqual(QuantApp._parse_optional_nonnegative_decimal_input("0", "固定数量"), Decimal("0"))
        self.assertIsNone(QuantApp._parse_optional_nonnegative_decimal_input("", "固定数量"))
        with self.assertRaisesRegex(ValueError, "固定数量 不能小于 0"):
            QuantApp._parse_optional_nonnegative_decimal_input("-1", "固定数量")

    def test_optional_positive_entry_decimal_hides_zero_values(self) -> None:
        self.assertEqual(QuantApp._format_optional_positive_entry_decimal(Decimal("0")), "")
        self.assertEqual(QuantApp._format_optional_positive_entry_decimal(Decimal("1")), "1")
