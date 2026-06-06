from __future__ import annotations

from decimal import Decimal
from unittest import TestCase

from okx_quant.engine_retry_policy import EngineRetryPolicy
from okx_quant.models import Credentials, StrategyConfig
from okx_quant.okx_client import OkxOrderStatus


def _make_status(*, state: str, ord_id: str = "ord-1") -> OkxOrderStatus:
    return OkxOrderStatus(
        ord_id=ord_id,
        state=state,
        side="buy",
        ord_type="limit",
        price=Decimal("70000"),
        avg_price=Decimal("70001") if state == "filled" else None,
        size=Decimal("1"),
        filled_size=Decimal("1") if state == "filled" else Decimal("0"),
        raw={},
    )


class _StopStub:
    @staticmethod
    def is_set() -> bool:
        return False

    @staticmethod
    def wait(timeout: float) -> bool:  # noqa: ARG002
        return False


class _EngineStub:
    def __init__(self, client) -> None:  # noqa: ANN001
        self._client = client
        self._stop_event = _StopStub()
        self._logger = lambda message: None


class EngineRetryPolicyOrderWsTest(TestCase):
    def setUp(self) -> None:
        self.credentials = Credentials(api_key="k", secret_key="s", passphrase="p")
        self.config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="1H",
            ema_period=21,
            atr_period=14,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0.01"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="last",
        )

    def test_get_order_uses_terminal_private_ws_snapshot_before_rest(self) -> None:
        class _Client:
            @staticmethod
            def get_cached_private_order_status(credentials, *, environment: str, inst_id: str, ord_id=None, cl_ord_id=None):  # noqa: ANN001,ARG004
                return 3, _make_status(state="filled", ord_id=ord_id or "ord-1")

            @staticmethod
            def wait_private_order_update(*args, **kwargs):  # noqa: ANN002,ANN003
                raise AssertionError("should not wait when cached order state is terminal")

            @staticmethod
            def get_order(*args, **kwargs):  # noqa: ANN002,ANN003
                raise AssertionError("should not call REST order lookup")

        policy = EngineRetryPolicy(_EngineStub(_Client()))  # type: ignore[arg-type]

        status = policy.get_order(self.credentials, self.config, inst_id="BTC-USDT-SWAP", ord_id="ord-1")

        self.assertEqual(status.state, "filled")
        self.assertEqual(status.ord_id, "ord-1")

    def test_get_order_falls_back_to_rest_when_cached_ws_state_is_non_terminal(self) -> None:
        captured_wait: dict[str, object] = {}

        class _Client:
            rest_calls = 0

            @staticmethod
            def get_cached_private_order_status(credentials, *, environment: str, inst_id: str, ord_id=None, cl_ord_id=None):  # noqa: ANN001,ARG004
                return 5, _make_status(state="live", ord_id=ord_id or "ord-2")

            @staticmethod
            def wait_private_order_update(credentials, *, environment: str, inst_id: str, ord_id=None, cl_ord_id=None, after_version: int = 0, timeout: float = 1.0):  # noqa: ANN001,ARG004,E501
                captured_wait["environment"] = environment
                captured_wait["inst_id"] = inst_id
                captured_wait["ord_id"] = ord_id
                captured_wait["cl_ord_id"] = cl_ord_id
                captured_wait["after_version"] = after_version
                captured_wait["timeout"] = timeout
                return None

            @classmethod
            def get_order(cls, credentials, config, *, inst_id: str, ord_id=None, cl_ord_id=None):  # noqa: ANN001,ARG004
                cls.rest_calls += 1
                return _make_status(state="filled", ord_id=ord_id or "ord-2")

        client = _Client()
        policy = EngineRetryPolicy(_EngineStub(client))  # type: ignore[arg-type]

        status = policy.get_order(self.credentials, self.config, inst_id="BTC-USDT-SWAP", ord_id="ord-2")

        self.assertEqual(status.state, "filled")
        self.assertEqual(client.rest_calls, 1)
        self.assertEqual(captured_wait["after_version"], 5)
        self.assertEqual(captured_wait["ord_id"], "ord-2")
        self.assertEqual(captured_wait["inst_id"], "BTC-USDT-SWAP")
        self.assertGreater(float(captured_wait["timeout"]), 0.0)

    def test_get_order_uses_fresh_private_ws_update_before_rest(self) -> None:
        class _Client:
            @staticmethod
            def get_cached_private_order_status(credentials, *, environment: str, inst_id: str, ord_id=None, cl_ord_id=None):  # noqa: ANN001,ARG004
                return None

            @staticmethod
            def wait_private_order_update(credentials, *, environment: str, inst_id: str, ord_id=None, cl_ord_id=None, after_version: int = 0, timeout: float = 1.0):  # noqa: ANN001,ARG004,E501
                return 8, _make_status(state="partially_filled", ord_id=ord_id or "ord-3")

            @staticmethod
            def get_order(*args, **kwargs):  # noqa: ANN002,ANN003
                raise AssertionError("should not call REST order lookup when a fresh WS update is available")

        policy = EngineRetryPolicy(_EngineStub(_Client()))  # type: ignore[arg-type]

        status = policy.get_order(self.credentials, self.config, inst_id="BTC-USDT-SWAP", ord_id="ord-3")

        self.assertEqual(status.state, "partially_filled")
        self.assertEqual(status.ord_id, "ord-3")
