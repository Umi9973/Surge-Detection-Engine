"""
Bluesky Shadow Runner — standalone process, independent of main.py / live_hn().

Run (from project root):
    PYTHONPATH=. python3 src/shadow/bluesky_shadow.py

Outputs:
  data/debug/bluesky/health.json                          — refreshed every 30 s
  data/debug/bluesky/routed_samples/latest_routed_pretty.json  — rolling 100-post sample
  data/debug/bluesky/dropped/bluesky_dropped_*.jsonl      — written by BlueskyIngestor
  data/debug/bluesky/runs/run_summary_*.json              — written by BlueskyIngestor

GCS uploads (every 5 min):
  gs://hn-surge-dashboard-01/shadow/bluesky/health.json
  gs://hn-surge-dashboard-01/shadow/bluesky/routed_samples/latest_routed_pretty.json
  gs://hn-surge-dashboard-01/shadow/bluesky/runs/run_summary_*.json
  gs://hn-surge-dashboard-01/shadow/bluesky/dropped/bluesky_dropped_*.jsonl

Startup:
  - Deletes local debug files older than 2 days.
  - BlueskyIngestor opens its own WebSocket in a background thread.
"""
from __future__ import annotations

import json
import os
import sys
import time
from collections import Counter, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Deque, Dict

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_GCS_BUCKET         = os.environ.get("GCS_BUCKET",  "hn-surge-dashboard-01")
_GCS_PROJECT        = os.environ.get("GCS_PROJECT", "project-8299dfb6-57e5-4dcf-bc0")
_GCS_SHADOW_PREFIX  = "shadow/bluesky"

_HEALTH_INTERVAL_S  = 30     # rewrite health.json and routed sample
_UPLOAD_INTERVAL_S  = 300    # 5 minutes — upload everything to GCS
_MAX_ROUTED_SAMPLES = 100    # rolling buffer size for kept posts
_MAX_LOCAL_AGE_DAYS = 2      # delete local debug files older than this on startup

# ---------------------------------------------------------------------------
# Paths  (same _DEBUG_BASE as BlueskyIngestor)
# ---------------------------------------------------------------------------

_PROJECT_ROOT   = Path(__file__).resolve().parent.parent.parent
_DEBUG_BASE     = _PROJECT_ROOT / "data" / "debug" / "bluesky"
_DEBUG_ROUTED   = _DEBUG_BASE / "routed_samples"
_HEALTH_PATH    = _DEBUG_BASE / "health.json"


# ---------------------------------------------------------------------------
# GCS helpers
# ---------------------------------------------------------------------------

def _upload_file(local_path: Path) -> None:
    rel       = local_path.relative_to(_DEBUG_BASE)
    blob_name = f"{_GCS_SHADOW_PREFIX}/{rel.as_posix()}"
    try:
        from google.cloud import storage
        client = storage.Client(project=_GCS_PROJECT)
        client.bucket(_GCS_BUCKET).blob(blob_name).upload_from_filename(str(local_path))
        print(f"[shadow] → gs://{_GCS_BUCKET}/{blob_name}", flush=True)
    except Exception as exc:
        print(f"[shadow] GCS upload failed ({local_path.name}): {exc}", file=sys.stderr, flush=True)


def _upload_all() -> None:
    for path in sorted(_DEBUG_BASE.rglob("*")):
        if path.is_file() and path.suffix in (".json", ".jsonl"):
            _upload_file(path)


# ---------------------------------------------------------------------------
# Startup cleanup
# ---------------------------------------------------------------------------

def _cleanup_old_files() -> None:
    cutoff  = time.time() - _MAX_LOCAL_AGE_DAYS * 86_400
    deleted = 0
    for path in _DEBUG_BASE.rglob("*"):
        if path.is_file() and path.stat().st_mtime < cutoff:
            try:
                path.unlink()
                deleted += 1
            except OSError:
                pass
    if deleted:
        print(f"[shadow] removed {deleted} local debug file(s) older than {_MAX_LOCAL_AGE_DAYS}d",
              flush=True)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    _DEBUG_ROUTED.mkdir(parents=True, exist_ok=True)
    _cleanup_old_files()

    # Import here so startup errors are obvious
    from src.ingestion.bluesky import BlueskyIngestor

    ingestor = BlueskyIngestor()
    stream   = ingestor.stream()   # starts WebSocket in background thread

    routed_sample:  Deque[Dict] = deque(maxlen=_MAX_ROUTED_SAMPLES)
    channel_counts: Counter     = Counter()
    total_kept  = 0
    start_mono  = time.monotonic()
    wall_start  = datetime.now(timezone.utc).isoformat()
    last_health = start_mono
    last_upload = start_mono
    routed_path = _DEBUG_ROUTED / "latest_routed_pretty.json"

    print(f"[shadow] started at {wall_start}", flush=True)
    print(f"[shadow] debug → {_DEBUG_BASE}", flush=True)
    print(f"[shadow] GCS   → gs://{_GCS_BUCKET}/{_GCS_SHADOW_PREFIX}/", flush=True)

    for item in stream:
        total_kept += 1
        ch = item.get("subreddit", "general")
        channel_counts[ch] += 1

        routed_sample.append({
            "id":       item["id"],
            "body":     item["body"][:200],
            "channel":  ch,
            "timestamp": item["timestamp"],
            "hashtags": item["hashtags"],
            "domain":   item.get("external_domain", ""),
            "routing":  item["routing"],
        })

        now     = time.monotonic()
        elapsed = max(now - start_mono, 1.0)

        # ── Health + routed sample ────────────────────────────────────────────
        if now - last_health >= _HEALTH_INTERVAL_S:
            health = {
                "started_at":     wall_start,
                "uptime_s":       int(elapsed),
                "total_kept":     total_kept,
                "kept_per_min":   round(total_kept / elapsed * 60, 1),
                "channel_counts": dict(channel_counts.most_common()),
                "updated_at":     datetime.now(timezone.utc).isoformat(),
            }
            try:
                _HEALTH_PATH.write_text(
                    json.dumps(health, indent=2, ensure_ascii=False), encoding="utf-8"
                )
            except OSError as exc:
                print(f"[shadow] health write failed: {exc}", file=sys.stderr, flush=True)

            try:
                routed_path.write_text(
                    json.dumps(list(routed_sample), indent=2, ensure_ascii=False),
                    encoding="utf-8",
                )
            except OSError as exc:
                print(f"[shadow] routed sample write failed: {exc}", file=sys.stderr, flush=True)

            last_health = now

        # ── GCS upload ────────────────────────────────────────────────────────
        if now - last_upload >= _UPLOAD_INTERVAL_S:
            print(f"[shadow] uploading snapshot to GCS …", flush=True)
            _upload_all()
            last_upload = now


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[shadow] interrupted.", flush=True)
        sys.exit(0)
