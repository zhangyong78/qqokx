import shutil
from pathlib import Path
from unittest import TestCase
from uuid import uuid4

from okx_quant.persistence import (
    credentials_file_path,
    load_credentials_snapshot,
    load_smart_order_favorites_snapshot,
    save_credentials_snapshot,
    save_smart_order_favorites_snapshot,
    smart_order_favorites_file_path,
)


class PersistenceTest(TestCase):
    def _workspace_temp_dir(self) -> Path:
        temp_dir = Path("tests_artifacts") / uuid4().hex
        temp_dir.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))
        return temp_dir

    def test_save_and_load_credentials_snapshot(self) -> None:
        temp_dir = self._workspace_temp_dir()
        temp_path = credentials_file_path(temp_dir)
        save_credentials_snapshot("ak", "sk", "pp", temp_path)
        snapshot = load_credentials_snapshot(temp_path)
        self.assertEqual(snapshot["api_key"], "ak")
        self.assertEqual(snapshot["secret_key"], "sk")
        self.assertEqual(snapshot["passphrase"], "pp")

    def test_load_returns_empty_values_when_file_missing(self) -> None:
        temp_dir = self._workspace_temp_dir()
        temp_path = credentials_file_path(temp_dir)
        snapshot = load_credentials_snapshot(temp_path)
        self.assertEqual(snapshot["api_key"], "")
        self.assertEqual(snapshot["secret_key"], "")
        self.assertEqual(snapshot["passphrase"], "")

    def test_save_and_load_smart_order_favorites_snapshot(self) -> None:
        temp_dir = self._workspace_temp_dir()
        temp_path = smart_order_favorites_file_path(temp_dir)
        save_smart_order_favorites_snapshot(
            [
                {"inst_type": "OPTION", "inst_id": "BTC-USD-260410-66000-P"},
                {"inst_type": "SWAP", "inst_id": "BTC-USDT-SWAP"},
            ],
            temp_path,
        )

        snapshot = load_smart_order_favorites_snapshot(temp_path)

        self.assertEqual(
            snapshot["favorites"],
            [
                {"inst_type": "OPTION", "inst_id": "BTC-USD-260410-66000-P"},
                {"inst_type": "SWAP", "inst_id": "BTC-USDT-SWAP"},
            ],
        )

    def test_smart_order_favorites_snapshot_deduplicates_items(self) -> None:
        temp_dir = self._workspace_temp_dir()
        temp_path = smart_order_favorites_file_path(temp_dir)
        save_smart_order_favorites_snapshot(
            [
                {"inst_type": "OPTION", "inst_id": "btc-usd-260410-66000-p"},
                {"inst_type": "OPTION", "inst_id": "BTC-USD-260410-66000-P"},
            ],
            temp_path,
        )

        snapshot = load_smart_order_favorites_snapshot(temp_path)

        self.assertEqual(
            snapshot["favorites"],
            [{"inst_type": "OPTION", "inst_id": "BTC-USD-260410-66000-P"}],
        )
