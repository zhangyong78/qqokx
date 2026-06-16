import json
import tempfile
from pathlib import Path
from unittest import TestCase

from okx_quant.persistence import (
    build_profile_switch_password_snapshot,
    credential_profile_has_switch_password,
    load_credentials_profiles_snapshot,
    load_credentials_snapshot,
    load_position_notes_snapshot,
    save_credentials_profiles_snapshot,
    save_credentials_snapshot,
    save_position_notes_snapshot,
    verify_profile_switch_password,
)


class CredentialProfilesPersistenceTest(TestCase):
    def test_load_legacy_single_profile_snapshot_as_api1(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / ".okx_quant_credentials.json"
            path.write_text(
                json.dumps(
                    {
                        "api_key": "legacy-key",
                        "secret_key": "legacy-secret",
                        "passphrase": "legacy-pass",
                    }
                ),
                encoding="utf-8",
            )

            snapshot = load_credentials_profiles_snapshot(path)

            self.assertEqual(snapshot["selected_profile"], "api1")
            self.assertEqual(
                snapshot["profiles"]["api1"],
                {
                    "api_key": "legacy-key",
                    "secret_key": "legacy-secret",
                    "passphrase": "legacy-pass",
                    "environment": "",
                    "spot_maker_fee_rate": "",
                    "spot_taker_fee_rate": "",
                    "futures_maker_fee_rate": "",
                    "futures_taker_fee_rate": "",
                    "option_maker_fee_rate": "",
                    "option_taker_fee_rate": "",
                    "switch_password_hash": "",
                    "switch_password_salt": "",
                    "switch_password_iterations": "",
                },
            )

    def test_save_and_load_multiple_profiles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / ".okx_quant_credentials.json"
            save_credentials_profiles_snapshot(
                selected_profile="api2",
                profiles={
                    "api1": {
                        "api_key": "key-1",
                        "secret_key": "secret-1",
                        "passphrase": "pass-1",
                    },
                    "api2": {
                        "api_key": "key-2",
                        "secret_key": "secret-2",
                        "passphrase": "pass-2",
                    },
                },
                path=path,
            )

            self.assertEqual(load_credentials_snapshot(path, profile_name="api1")["api_key"], "key-1")
            self.assertEqual(load_credentials_snapshot(path, profile_name="api2")["api_key"], "key-2")

    def test_save_credentials_snapshot_updates_selected_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / ".okx_quant_credentials.json"
            save_credentials_snapshot("key-1", "secret-1", "pass-1", path, profile_name="api1")
            save_credentials_snapshot("key-2", "secret-2", "pass-2", path, profile_name="api2")

            snapshot = load_credentials_profiles_snapshot(path)

            self.assertEqual(snapshot["selected_profile"], "api2")
            self.assertEqual(snapshot["profiles"]["api1"]["api_key"], "key-1")
            self.assertEqual(snapshot["profiles"]["api2"]["api_key"], "key-2")

    def test_save_and_load_custom_profile_names(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / ".okx_quant_credentials.json"
            main_profile = "\u4e3b\u8d26\u6237"
            option_profile = "\u671f\u6743\u8d26\u6237"
            save_credentials_profiles_snapshot(
                selected_profile=option_profile,
                profiles={
                    main_profile: {
                        "api_key": "main-key",
                        "secret_key": "main-secret",
                        "passphrase": "main-pass",
                    },
                    option_profile: {
                        "api_key": "option-key",
                        "secret_key": "option-secret",
                        "passphrase": "option-pass",
                    },
                },
                path=path,
            )

            snapshot = load_credentials_profiles_snapshot(path)

            self.assertEqual(snapshot["selected_profile"], option_profile)
            self.assertEqual(snapshot["profiles"][main_profile]["api_key"], "main-key")
            self.assertEqual(snapshot["profiles"][option_profile]["api_key"], "option-key")

    def test_load_profile_environment_from_saved_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / ".okx_quant_credentials.json"
            save_credentials_profiles_snapshot(
                selected_profile="live",
                profiles={
                    "live": {
                        "api_key": "live-key",
                        "secret_key": "live-secret",
                        "passphrase": "live-pass",
                        "environment": "live",
                    }
                },
                path=path,
            )

            snapshot = load_credentials_snapshot(path, profile_name="live")

            self.assertEqual(snapshot["environment"], "live")

    def test_load_profile_environment_from_environment_label_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / ".okx_quant_credentials.json"
            path.write_text(
                json.dumps(
                    {
                        "selected_profile": "demo",
                        "profiles": {
                            "demo": {
                                "api_key": "demo-key",
                                "secret_key": "demo-secret",
                                "passphrase": "demo-pass",
                                "environment_label": "\u6a21\u62df\u76d8 demo",
                            }
                        },
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            snapshot = load_credentials_snapshot(path, profile_name="demo")

            self.assertEqual(snapshot["environment"], "demo")

    def test_save_credentials_profiles_snapshot_encrypts_values_at_rest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / ".okx_quant_credentials.json"
            save_credentials_profiles_snapshot(
                selected_profile="desk",
                profiles={
                    "desk": {
                        "api_key": "plain-api-key",
                        "secret_key": "plain-secret-key",
                        "passphrase": "plain-passphrase",
                        "environment": "demo",
                    }
                },
                path=path,
            )

            raw = path.read_text(encoding="utf-8")
            snapshot = load_credentials_snapshot(path, profile_name="desk")

            self.assertNotIn("plain-api-key", raw)
            self.assertNotIn("plain-secret-key", raw)
            self.assertNotIn("plain-passphrase", raw)
            self.assertEqual(snapshot["api_key"], "plain-api-key")
            self.assertEqual(snapshot["secret_key"], "plain-secret-key")
            self.assertEqual(snapshot["passphrase"], "plain-passphrase")

    def test_profile_switch_password_snapshot_can_roundtrip(self) -> None:
        password_snapshot = build_profile_switch_password_snapshot("desk-123")

        self.assertTrue(credential_profile_has_switch_password(password_snapshot))
        self.assertTrue(verify_profile_switch_password(password_snapshot, "desk-123"))
        self.assertFalse(verify_profile_switch_password(password_snapshot, "desk-456"))


class PositionNotesPersistenceTest(TestCase):
    def test_save_and_load_position_notes_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "position_notes.json"
            save_position_notes_snapshot(
                current_notes=[
                    {
                        "record_key": "moni|demo|BTC-USD-260501-77000-C|short|cross",
                        "profile_name": "moni",
                        "environment": "demo",
                        "inst_id": "BTC-USD-260501-77000-C",
                        "pos_side": "short",
                        "mgn_mode": "cross",
                        "note": "当前持仓备注",
                        "activated_at_ms": 1000,
                        "updated_at_ms": 1000,
                        "missing_success_count": 0,
                        "missing_started_at_ms": None,
                        "linked_history_keys": ["history-1"],
                    }
                ],
                history_notes=[
                    {
                        "record_key": "history-1",
                        "profile_name": "moni",
                        "environment": "demo",
                        "inst_id": "BTC-USD-260501-77000-C",
                        "update_time": 2000,
                        "mgn_mode": "cross",
                        "pos_side": "short",
                        "direction": "short",
                        "close_size": "0.2",
                        "close_avg_price": "0.03",
                        "note": "历史仓位备注",
                        "source_current_key": "moni|demo|BTC-USD-260501-77000-C|short|cross",
                        "updated_at_ms": 2000,
                    }
                ],
                path=path,
            )

            snapshot = load_position_notes_snapshot(path)

            self.assertEqual(snapshot["current_notes"][0]["note"], "当前持仓备注")
            self.assertEqual(snapshot["current_notes"][0]["linked_history_keys"], ["history-1"])
            self.assertEqual(snapshot["history_notes"][0]["note"], "历史仓位备注")
            self.assertEqual(snapshot["history_notes"][0]["update_time"], 2000)

    def test_load_position_notes_snapshot_drops_blank_notes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "position_notes.json"
            path.write_text(
                json.dumps(
                    {
                        "current_notes": [
                            {
                                "record_key": "current-1",
                                "profile_name": "moni",
                                "environment": "demo",
                                "inst_id": "BTC-USD-260501-77000-C",
                                "pos_side": "short",
                                "mgn_mode": "cross",
                                "note": "   ",
                            }
                        ],
                        "history_notes": [
                            {
                                "record_key": "history-1",
                                "profile_name": "moni",
                                "environment": "demo",
                                "inst_id": "BTC-USD-260501-77000-C",
                                "note": "",
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            snapshot = load_position_notes_snapshot(path)

            self.assertEqual(snapshot["current_notes"], [])
            self.assertEqual(snapshot["history_notes"], [])
