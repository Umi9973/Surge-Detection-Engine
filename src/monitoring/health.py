from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def write_health(
    path: Optional[Path],
    *,
    status: str,
    component: str = "hn_ingestion",
    error_type: Optional[str] = None,
    error_msg: Optional[str] = None,
) -> None:
    """Write health JSON atomically. Best-effort: never raises, logs to stderr."""
    if path is None:
        return
    try:
        now  = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        prev = _read_existing(path)
        payload = {
            "status":                    status,
            "component":                 component,
            "last_heartbeat_utc":        now,
            "last_successful_fetch_utc": now if status == "ok" else prev.get("last_successful_fetch_utc"),
            "last_error_utc":            now if status != "ok" else None,
            "last_error_type":           error_type,
            "last_error_message":        error_msg,
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        tmp.replace(path)
    except Exception as exc:
        print(f"[health] write failed: {exc}", file=sys.stderr)


def _read_existing(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
