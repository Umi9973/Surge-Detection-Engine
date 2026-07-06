"""
Bluesky Anomaly Shadow Runner — standalone process, independent of main.py / live_hn().

Wires BlueskyIngestor.stream() into SlidingWindowTripwire + AlertGate on Redis DB 1.
No webhooks. No HN changes. Shadow output only.

Run (from project root):
    PYTHONPATH=. python3 src/shadow/bluesky_anomaly_shadow.py

Env vars (all optional):
    BSKY_MIN_HISTORY      default 6    (hours of baseline before anomalies can fire)
    BSKY_Z_THRESHOLD      default 3.0
    BSKY_WINDOW_TTL       default 3600 (1-hour sliding window)
    BSKY_EVAL_INTERVAL    default 300  (5-minute evaluation ticks)
    BSKY_BASELINE_INTERVAL default 3600 (hourly baseline ticks)
    GCS_BUCKET            default hn-surge-dashboard-01
    GCS_PROJECT           default project-8299dfb6-57e5-4dcf-bc0

Before a clean experiment:
    redis-cli -n 1 FLUSHDB

Outputs:
    data/debug/bluesky/anomaly/anomaly_summary_latest.json   — overwritten every 5 min
    data/debug/bluesky/anomaly/anomaly_events_{run_ts}.jsonl — appended as anomalies fire

GCS uploads (every 5 min):
    shadow/bluesky/anomaly/anomaly_summary_latest.json
    shadow/bluesky/anomaly/anomaly_events_{run_ts}.jsonl

NOTE: Stats here will not exactly match bluesky_shadow.py because this process opens
its own independent Jetstream WebSocket connection (two separate samples).
"""
from __future__ import annotations

import json
import os
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import redis

# ---------------------------------------------------------------------------
# Config from env vars
# ---------------------------------------------------------------------------

_MIN_HISTORY       = int(os.environ.get("BSKY_MIN_HISTORY",       "6"))
_Z_THRESHOLD       = float(os.environ.get("BSKY_Z_THRESHOLD",     "3.0"))
_WINDOW_TTL        = int(os.environ.get("BSKY_WINDOW_TTL",        "3600"))
_EVAL_INTERVAL     = int(os.environ.get("BSKY_EVAL_INTERVAL",     "300"))
_BASELINE_INTERVAL = int(os.environ.get("BSKY_BASELINE_INTERVAL", "3600"))

_GCS_BUCKET        = os.environ.get("GCS_BUCKET",  "hn-surge-dashboard-01")
_GCS_PROJECT       = os.environ.get("GCS_PROJECT", "project-8299dfb6-57e5-4dcf-bc0")
_GCS_PREFIX        = "shadow/bluesky/anomaly"

_SUMMARY_INTERVAL  = 300   # write/upload summary every 5 min
_MAX_ANOMALY_LOG   = 50    # keep last N anomaly events in the summary

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_DEBUG_BASE   = _PROJECT_ROOT / "data" / "debug" / "bluesky"
_ANOMALY_DIR  = _DEBUG_BASE / "anomaly"
_SUMMARY_PATH = _ANOMALY_DIR / "anomaly_summary_latest.json"

# ---------------------------------------------------------------------------
# Bluesky channels
# ---------------------------------------------------------------------------

BSKY_CHANNELS = [
    "cybersecurity", "ai_tech", "war_diplomacy", "us_politics",
    "activism_rights", "climate_weather", "health_medicine", "science_space",
    "money_markets", "social_platforms", "sports", "entertainment_fandom",
]

# ---------------------------------------------------------------------------
# _to_comment — copied from main.py to avoid importing a script module
# ---------------------------------------------------------------------------

from src.models import Comment

def _to_comment(raw: Dict) -> Comment:
    return Comment(
        id          = raw.get("id", ""),
        subreddit   = raw.get("subreddit", ""),
        body        = raw.get("body", ""),
        timestamp   = int(raw.get("timestamp", 0)),
        author      = raw.get("author", ""),
        score       = int(raw.get("score", 0)),
        story_id    = int(raw.get("story_id", 0)),
        story_title = raw.get("story_title", ""),
        domain      = raw.get("domain", ""),
        item_type   = raw.get("item_type", ""),
        item_id     = int(raw.get("item_id", 0)),
        created_at  = int(raw.get("created_at", 0)),
    )

# ---------------------------------------------------------------------------
# GCS upload
# ---------------------------------------------------------------------------

def _upload_file(local_path: Path) -> None:
    rel       = local_path.relative_to(_ANOMALY_DIR)
    blob_name = f"{_GCS_PREFIX}/{rel.as_posix()}"
    try:
        from google.cloud import storage
        client = storage.Client(project=_GCS_PROJECT)
        client.bucket(_GCS_BUCKET).blob(blob_name).upload_from_filename(str(local_path))
        print(f"[anomaly] → gs://{_GCS_BUCKET}/{blob_name}", flush=True)
    except Exception as exc:
        print(f"[anomaly] GCS upload failed ({local_path.name}): {exc}",
              file=sys.stderr, flush=True)

def _upload_all() -> None:
    for path in sorted(_ANOMALY_DIR.rglob("*")):
        if path.is_file() and path.suffix in (".json", ".jsonl"):
            _upload_file(path)

# ---------------------------------------------------------------------------
# Tick firing (reconnect-safe: processes ALL overdue ticks in a while loop)
# ---------------------------------------------------------------------------

def _fire_ticks(
    now:       int,
    tripwire,
    gate,
    ticks:     Dict,
    kept_by_channel:  Counter,
    fires_by_channel: Counter,
    max_z_by_channel: dict,
    anomaly_log:      List,
    anomaly_events_path: Path,
    eval_count:       list,   # [int] — mutable counter
    base_count:       list,
    anomaly_count:    list,
) -> None:
    while True:
        e, b = ticks["eval"], ticks["base"]
        if min(e, b) > now:
            break

        if e <= b:
            raw_events = tripwire.evaluation_tick(e)

            # Split by event phase before gating.
            starts   = [ev for ev in raw_events if ev.event_type == "start"]
            updates  = [ev for ev in raw_events if ev.event_type == "update"]
            releases = [ev for ev in raw_events if ev.event_type == "release"]

            # Only start events go through AlertGate (cooldown for notifications).
            gated_starts = gate.process(starts)

            # ── start events: new distinct surge begins ────────────────────────
            for ev in gated_starts:
                fired_at     = datetime.now(timezone.utc).isoformat()
                sample_texts = [it.get("text", "")[:150] for it in (ev.items or [])[:3]]
                entry = {
                    "source":       "bluesky",
                    "event_type":   "start",
                    "channel":      ev.subreddit,
                    "fired_at":     fired_at,
                    "event_start":  ev.event_start,
                    "window_start": ev.window_start,
                    "window_end":   ev.window_end,
                    "count":        ev.count,
                    "z_score":      ev.z_score,
                    "mean":         ev.mean,
                    "std":          ev.std,
                    "sample_texts": sample_texts,
                }
                fires_by_channel[ev.subreddit] += 1
                if ev.z_score > max_z_by_channel.get(ev.subreddit, float("-inf")):
                    max_z_by_channel[ev.subreddit] = ev.z_score
                try:
                    with open(anomaly_events_path, "a", encoding="utf-8") as f:
                        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
                except OSError as exc:
                    print(f"[anomaly] event write failed: {exc}",
                          file=sys.stderr, flush=True)
                anomaly_log.append(entry)
                if len(anomaly_log) > _MAX_ANOMALY_LOG:
                    anomaly_log.pop(0)
                anomaly_count[0] += 1
                print(
                    f"[anomaly] SURGE START  {ev.subreddit}  z={ev.z_score}  "
                    f"count={ev.count}  mean={ev.mean}",
                    flush=True,
                )

            # ── update events: track peak_z only, no new counter ──────────────
            for ev in updates:
                if ev.z_score > max_z_by_channel.get(ev.subreddit, float("-inf")):
                    max_z_by_channel[ev.subreddit] = ev.z_score

            # ── release events: log duration, no counter increment ─────────────
            for ev in releases:
                duration_s = ev.window_end - ev.event_start
                entry = {
                    "source":       "bluesky",
                    "event_type":   "release",
                    "channel":      ev.subreddit,
                    "fired_at":     datetime.now(timezone.utc).isoformat(),
                    "event_start":  ev.event_start,
                    "window_end":   ev.window_end,
                    "duration_s":   duration_s,
                    "z_score":      ev.z_score,
                    "peak_z":       max_z_by_channel.get(ev.subreddit),
                    "mean":         ev.mean,
                    "std":          ev.std,
                }
                try:
                    with open(anomaly_events_path, "a", encoding="utf-8") as f:
                        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
                except OSError as exc:
                    print(f"[anomaly] event write failed: {exc}",
                          file=sys.stderr, flush=True)
                print(
                    f"[anomaly] SURGE END    {ev.subreddit}  duration={duration_s//60}min  "
                    f"peak_z={max_z_by_channel.get(ev.subreddit)}",
                    flush=True,
                )

            eval_count[0] += 1
            ticks["eval"] = e + _EVAL_INTERVAL

        else:
            tripwire.baseline_tick(b)
            base_count[0] += 1
            ticks["base"] = b + _BASELINE_INTERVAL

# ---------------------------------------------------------------------------
# Summary write
# ---------------------------------------------------------------------------

def _write_summary(
    path:             Path,
    wall_start:       str,
    start_mono:       float,
    kept_by_channel:  Counter,
    fires_by_channel: Counter,
    max_z_by_channel: dict,
    tripwire,
    state,
    eval_count:       list,
    base_count:       list,
    anomaly_count:    list,
    anomaly_log:      List,
) -> None:
    now = int(time.time())
    last_eval = tripwire.channel_stats()

    # Build per-channel summary merging z-stats, fire counts, and window counts
    channel_summary = {}
    for ch in BSKY_CHANNELS:
        try:
            wc = state.get_window_count(ch, now)
        except Exception:
            wc = -1
        ev = last_eval.get(ch, {})
        channel_summary[ch] = {
            "window_count": wc,
            "fires":        fires_by_channel.get(ch, 0),
            "max_z":        max_z_by_channel.get(ch, None),
            "last_z":       ev.get("z_score"),
            "last_mean":    ev.get("mean"),
            "last_std":     ev.get("std"),
            "elevated":     ev.get("elevated", False),
        }

    summary = {
        "source":               "bluesky",
        "started_at":           wall_start,
        "uptime_s":             int(time.monotonic() - start_mono),
        "eval_ticks_fired":     eval_count[0],
        "baseline_ticks_fired": base_count[0],
        "anomalies_fired":      anomaly_count[0],
        "kept_by_channel":      dict(kept_by_channel),
        "channel_stats":        channel_summary,
        "updated_at":           datetime.now(timezone.utc).isoformat(),
        "anomaly_events":       anomaly_log[-_MAX_ANOMALY_LOG:],
    }
    tmp = path.with_suffix(".json.tmp")
    try:
        tmp.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(path)
    except OSError as exc:
        print(f"[anomaly] summary write failed: {exc}", file=sys.stderr, flush=True)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    _ANOMALY_DIR.mkdir(parents=True, exist_ok=True)

    # Imports here so startup errors are obvious
    from src.ingestion.bluesky import BlueskyIngestor
    from src.pipeline.alert_gate import AlertGate
    from src.pipeline.sliding_tripwire import SlidingWindowTripwire
    from src.storage.state_manager import RedisStateManager

    # Redis DB 1 — isolated from HN production (DB 0)
    r = redis.Redis(host="localhost", port=6379, db=1, decode_responses=True)
    try:
        r.ping()
    except redis.ConnectionError as exc:
        sys.exit(f"[anomaly] Redis not reachable: {exc}")

    state    = RedisStateManager(r, window_ttl=_WINDOW_TTL)
    tripwire = SlidingWindowTripwire(
        state, BSKY_CHANNELS,
        min_history=_MIN_HISTORY,
        z_threshold=_Z_THRESHOLD,
        window_ttl=_WINDOW_TTL,
    )
    # AlertGate: in-memory only (r=None) — no Redis, no webhooks, no dispatcher
    gate = AlertGate()

    ingestor = BlueskyIngestor()
    stream   = ingestor.stream()

    ts          = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    wall_start  = datetime.now(timezone.utc).isoformat()
    start_mono  = time.monotonic()
    anomaly_events_path = _ANOMALY_DIR / f"anomaly_events_{ts}.jsonl"

    kept_by_channel:  Counter      = Counter()
    fires_by_channel: Counter      = Counter()
    max_z_by_channel: Dict[str, float] = {}
    anomaly_log:      List         = []
    eval_count    = [0]
    base_count    = [0]
    anomaly_count = [0]
    ticks: Dict   = {"eval": None, "base": None}
    last_summary = time.monotonic()
    last_upload  = time.monotonic()

    print(f"[anomaly] started at {wall_start}", flush=True)
    print(f"[anomaly] Redis DB 1 | MIN_HISTORY={_MIN_HISTORY} | "
          f"Z_THRESHOLD={_Z_THRESHOLD} | WINDOW_TTL={_WINDOW_TTL}s", flush=True)
    print(f"[anomaly] Output → {_ANOMALY_DIR}", flush=True)
    print(f"[anomaly] GCS    → gs://{_GCS_BUCKET}/{_GCS_PREFIX}/", flush=True)

    for item in stream:
        now = int(time.time())

        # Initialise tick timestamps from wall clock on first item
        if ticks["eval"] is None:
            ticks["eval"] = now + _EVAL_INTERVAL
            ticks["base"] = now + _BASELINE_INTERVAL

        kept_by_channel[item["subreddit"]] += 1
        tripwire.ingest(_to_comment(item))

        # Fire all overdue ticks (while loop handles reconnect-caused gaps)
        _fire_ticks(
            now, tripwire, gate, ticks,
            kept_by_channel, fires_by_channel, max_z_by_channel,
            anomaly_log, anomaly_events_path,
            eval_count, base_count, anomaly_count,
        )

        mono = time.monotonic()

        if mono - last_summary >= _SUMMARY_INTERVAL:
            _write_summary(
                _SUMMARY_PATH, wall_start, start_mono,
                kept_by_channel, fires_by_channel, max_z_by_channel,
                tripwire, state,
                eval_count, base_count, anomaly_count, anomaly_log,
            )
            last_summary = mono

        if mono - last_upload >= _SUMMARY_INTERVAL:
            print("[anomaly] uploading to GCS …", flush=True)
            _upload_all()
            last_upload = mono


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[anomaly] interrupted.", flush=True)
        sys.exit(0)
