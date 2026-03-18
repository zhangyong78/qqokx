from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


CREDENTIALS_FILE_NAME = ".okx_quant_credentials.json"


def credentials_file_path(base_dir: Path | None = None) -> Path:
    if base_dir is None:
        base_dir = Path(__file__).resolve().parent.parent
    return Path(base_dir) / CREDENTIALS_FILE_NAME


def load_credentials_snapshot(path: Path | None = None) -> dict[str, str]:
    target = path or credentials_file_path()
    if not target.exists():
        return {
            "api_key": "",
            "secret_key": "",
            "passphrase": "",
        }

    payload = json.loads(target.read_text(encoding="utf-8"))
    return {
        "api_key": str(payload.get("api_key", "")),
        "secret_key": str(payload.get("secret_key", "")),
        "passphrase": str(payload.get("passphrase", "")),
    }


def save_credentials_snapshot(
    api_key: str,
    secret_key: str,
    passphrase: str,
    path: Path | None = None,
) -> Path:
    target = path or credentials_file_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "api_key": api_key.strip(),
        "secret_key": secret_key.strip(),
        "passphrase": passphrase.strip(),
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
    temp_path = target.with_suffix(target.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(target)
    return target
