import os
from pathlib import Path
import shutil
import subprocess
import tomllib

import pytest


@pytest.mark.parametrize("suffix,has_bom", [(".toml", False), (".md", True)])
def test_release_text_updater_keeps_toml_parseable_without_changing_doc_encoding(tmp_path, suffix, has_bom):
    shell = shutil.which("pwsh")
    if shell is None:
        pytest.skip("PowerShell 7 is required for release-script validation")
    repo = Path(__file__).resolve().parents[1]
    target = tmp_path / f"encoding-probe{suffix}"
    target.write_text('[project]\nversion = "0.7.86"\n', encoding="utf-8-sig")
    env = dict(os.environ, QQOKX_RELEASE_TEST_FILE=str(target), QQOKX_RELEASE_TEST_SOURCE=str(repo / "scripts/release_one_click.ps1"))
    script = """
$ErrorActionPreference = 'Stop'
$tokens = $null
$parseErrors = $null
$tree = [System.Management.Automation.Language.Parser]::ParseFile($env:QQOKX_RELEASE_TEST_SOURCE, [ref]$tokens, [ref]$parseErrors)
if ($parseErrors.Count -gt 0) { throw ($parseErrors | Out-String) }
$definition = $tree.Find({param($node) $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq 'Update-TextFile'}, $true)
Invoke-Expression $definition.Extent.Text
$utf8Bom = [System.Text.UTF8Encoding]::new($true)
$utf8NoBom = [System.Text.UTF8Encoding]::new($false)
Update-TextFile $env:QQOKX_RELEASE_TEST_FILE { param($text) $text.Replace('0.7.86', '0.7.87') }
"""
    result = subprocess.run([shell, "-NoProfile", "-NonInteractive", "-Command", script], env=env, capture_output=True, timeout=20)
    assert result.returncode == 0, result.stderr.decode("utf-8", errors="replace")
    assert target.read_bytes().startswith(b"\xef\xbb\xbf") == has_bom
    if suffix == ".toml":
        with target.open("rb") as stream:
            assert tomllib.load(stream)["project"]["version"] == "0.7.87"
