from unittest import TestCase

from okx_quant.ui import _format_network_error_message


class UiHelpersTest(TestCase):
    def test_format_network_error_message_read_timeout(self) -> None:
        self.assertEqual(
            _format_network_error_message("The read operation timed out"),
            "网络读取超时，请稍后重试。",
        )

    def test_format_network_error_message_handshake_timeout(self) -> None:
        self.assertEqual(
            _format_network_error_message("_ssl.c:983: The handshake operation timed out"),
            "网络握手超时，请稍后重试。",
        )
