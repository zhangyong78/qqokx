from __future__ import annotations

from unittest import TestCase
from unittest.mock import patch

from roll_terminal_qt.runtime import load_runtime


class RuntimeSelectionTests(TestCase):
    def test_selected_account_uses_its_own_position_mode(self) -> None:
        snapshot = {
            "selected_profile": "one-way",
            "profiles": {
                "one-way": {"api_key": "a", "secret_key": "b", "passphrase": "c", "environment": "live", "position_mode": "net"},
                "hedge": {"api_key": "d", "secret_key": "e", "passphrase": "f", "environment": "demo", "position_mode": "long_short"},
            },
        }
        with patch("roll_terminal_qt.runtime.load_credentials_profiles_snapshot", return_value=snapshot):
            self.assertEqual(load_runtime("one-way").position_mode, "net")
            self.assertEqual(load_runtime("hedge").position_mode, "long_short")

    def test_explicit_missing_profile_does_not_fall_back_to_another_account(self) -> None:
        snapshot = {
            "selected_profile": "live-account",
            "profiles": {
                "live-account": {"api_key": "key", "secret_key": "secret", "passphrase": "pass", "environment": "live"},
            },
        }
        with patch("roll_terminal_qt.runtime.load_credentials_profiles_snapshot", return_value=snapshot):
            self.assertIsNone(load_runtime("misspelled-demo"))

