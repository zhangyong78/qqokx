from __future__ import annotations

import subprocess
import unittest
from unittest.mock import patch

import run_main_bootstrap as bootstrap


class MainBootstrapTest(unittest.TestCase):
    def test_installed_dependencies_launch_without_pip_and_preserve_arguments(self) -> None:
        with (
            patch.object(bootstrap, "_dependencies_ready", return_value=True),
            patch.object(bootstrap.subprocess, "run") as install,
            patch.object(bootstrap.os, "execv") as launch,
            patch.object(bootstrap.sys, "argv", ["bootstrap", "--data-dir", "C:\\QQOKX data"]),
        ):
            self.assertEqual(bootstrap.main(), 0)
        install.assert_not_called()
        launch.assert_called_once_with(
            bootstrap.sys.executable,
            [bootstrap.sys.executable, str(bootstrap.ENTRYPOINT), "--data-dir", "C:\\QQOKX data"],
        )

    def test_missing_dependencies_install_with_the_launch_interpreter(self) -> None:
        with (
            patch.object(bootstrap, "_dependencies_ready", side_effect=[False, True]),
            patch.object(bootstrap.subprocess, "run", return_value=subprocess.CompletedProcess([], 0)) as install,
            patch.object(bootstrap.os, "execv") as launch,
        ):
            self.assertEqual(bootstrap.main(), 0)
        install.assert_called_once_with(
            [bootstrap.sys.executable, "-m", "pip", "install", "-r", str(bootstrap.REQUIREMENTS)],
            cwd=str(bootstrap.ROOT),
            check=False,
        )
        launch.assert_called_once()

    def test_install_failure_does_not_start_workbench(self) -> None:
        with (
            patch.object(bootstrap, "_dependencies_ready", return_value=False),
            patch.object(bootstrap.subprocess, "run", return_value=subprocess.CompletedProcess([], 2)),
            patch.object(bootstrap.os, "execv") as launch,
        ):
            self.assertEqual(bootstrap.main(), 2)
        launch.assert_not_called()

    def test_dependencies_still_missing_after_install_does_not_start_workbench(self) -> None:
        with (
            patch.object(bootstrap, "_dependencies_ready", return_value=False),
            patch.object(bootstrap.subprocess, "run", return_value=subprocess.CompletedProcess([], 0)),
            patch.object(bootstrap.os, "execv") as launch,
        ):
            self.assertEqual(bootstrap.main(), 1)
        launch.assert_not_called()

    def test_pandas_is_required_even_when_other_dependencies_exist(self) -> None:
        with patch.object(
            bootstrap.importlib.util, "find_spec", side_effect=lambda name: None if name == "pandas" else object()
        ):
            self.assertFalse(bootstrap._dependencies_ready())
