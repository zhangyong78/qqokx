import shutil
from pathlib import Path
from unittest import TestCase
from uuid import uuid4

from okx_quant.persistence import credentials_file_path, load_credentials_snapshot, save_credentials_snapshot


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
