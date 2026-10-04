from __future__ import annotations

import shutil
import tempfile
from pathlib import Path
from unittest import TestCase

from scripts.build_server_package import package_ignore


class ServerPackageFilterTest(TestCase):
    def test_excludes_backups_temporary_files_and_credentials(self) -> None:
        excluded = [
            "__pycache__",
            "module.pyc",
            "module.PYO",
            "module.py.bak_20260901",
            "module.py.bak",
            "arbitrage_ui.py.brokenbak",
            "arbitrage_ui.py.corrupted_backup",
            "module.py~",
            ".workspace.json.123.tmp",
            "module.temp",
            "module.swp",
            "module.swo",
            ".env",
            ".env.production",
            ".okx_quant_credentials.json",
            "credentials.json",
            "settings.json",
            "server.pem",
            "private.key",
            "client.p12",
            "client.pfx",
        ]
        included = ["okx_client.py", "persistence.py", "profile_access.py", "app_icon.png", "README.md"]
        self.assertEqual(package_ignore("unused", excluded + included), set(excluded))

    def test_copytree_filters_nested_artifacts_without_touching_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "source"
            target = Path(tmp) / "package"
            nested = source / "nested"
            nested.mkdir(parents=True)
            (source / "module.py").write_text("pass\n", encoding="utf-8")
            (nested / "child.py").write_text("pass\n", encoding="utf-8")
            for name in ("child.py.corrupted_backup", ".env", "credentials.json", "private.pem"):
                (nested / name).write_text("synthetic test content", encoding="utf-8")

            shutil.copytree(source, target, ignore=package_ignore)

            self.assertEqual(
                {path.relative_to(target).as_posix() for path in target.rglob("*") if path.is_file()},
                {"module.py", "nested/child.py"},
            )
            self.assertTrue((nested / "credentials.json").exists())
