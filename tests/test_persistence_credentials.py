import json
import tempfile
from pathlib import Path
from unittest import TestCase

from okx_quant.persistence import (
    load_credentials_profiles_snapshot,
    load_credentials_snapshot,
    save_credentials_profiles_snapshot,
    save_credentials_snapshot,
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
            save_credentials_profiles_snapshot(
                selected_profile="期权账户",
                profiles={
                    "主账户": {
                        "api_key": "main-key",
                        "secret_key": "main-secret",
                        "passphrase": "main-pass",
                    },
                    "期权账户": {
                        "api_key": "option-key",
                        "secret_key": "option-secret",
                        "passphrase": "option-pass",
                    },
                },
                path=path,
            )

            snapshot = load_credentials_profiles_snapshot(path)

            self.assertEqual(snapshot["selected_profile"], "期权账户")
            self.assertEqual(snapshot["profiles"]["主账户"]["api_key"], "main-key")
            self.assertEqual(snapshot["profiles"]["期权账户"]["api_key"], "option-key")
