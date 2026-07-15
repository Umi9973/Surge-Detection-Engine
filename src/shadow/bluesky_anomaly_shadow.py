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
    data/debug/bluesky/anomaly/anomaly_events_{run_ts}.jsonl — one record per completed event

GCS uploads (every 5 min):
    shadow/bluesky/anomaly/anomaly_summary_latest.json
    shadow/bluesky/anomaly/anomaly_events_{run_ts}.jsonl

Event grouping model:
    start  → opens an _active_events record; fires_by_channel / anomaly_count increment
    update → accumulates peak_z, peak_count, update_count into the active record; no JSONL write
    release→ finalises the active record and writes ONE rich JSONL line with full lifecycle data

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
_MAX_ANOMALY_LOG   = 50    # keep last N completed events in the summary

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
        id               = raw.get("id", ""),
        subreddit        = raw.get("subreddit", ""),
        body             = raw.get("body", ""),
        timestamp        = int(raw.get("timestamp", 0)),
        author           = raw.get("author_did", "") or raw.get("author", ""),
        score            = int(raw.get("score", 0)),
        story_id         = int(raw.get("story_id", 0)),
        story_title      = raw.get("story_title", ""),
        domain           = raw.get("domain", ""),
        item_type        = raw.get("item_type", ""),
        item_id          = int(raw.get("item_id", 0)),
        created_at       = int(raw.get("created_at", 0)),
        platform_uri     = raw.get("platform_item_uri", ""),
        root_uri         = raw.get("root_uri", ""),
        hashtags         = raw.get("hashtags", []),
        matched_keywords = raw.get("routing", {}).get("matched_keywords", []),
    )

# ---------------------------------------------------------------------------
# Enrichment helper
# ---------------------------------------------------------------------------

def _enrich_items(items: List[Dict]) -> Dict:
    kw_counts   = Counter(kw for it in items for kw in it.get("matched_keywords", []))
    ht_counts   = Counter(ht for it in items for ht in it.get("hashtags", []))
    dom_counts  = Counter(it.get("domain", "") for it in items if it.get("domain"))
    auth_counts = Counter(it.get("author", "") for it in items if it.get("author"))
    posts = [
        {
            "text":         it.get("text", "")[:200],
            "platform_uri": it.get("platform_uri", ""),
            "root_uri":     it.get("root_uri", ""),
            "author":       it.get("author", ""),
            "hashtags":     it.get("hashtags", []),
            "keywords":     it.get("matched_keywords", []),
            "domain":       it.get("domain", ""),
        }
        for it in items[:5]
    ]
    return {
        "posts":    posts,
        "keywords": kw_counts.most_common(10),
        "hashtags": ht_counts.most_common(10),
        "domains":  dom_counts.most_common(5),
        "authors":  auth_counts.most_common(5),
    }


def _is_concentration_burst(opening: Dict, items: List[Dict]) -> tuple[bool, list]:
    """Return (True, reasons) when BOTH a single author AND a single domain
    dominate the sampled window — strong signal for a scheduled feed bot."""
    total = len(items)
    if not total:
        return False, []
    author_hit = bool(opening["authors"] and opening["authors"][0][1] / total > 0.4)
    domain_hit = bool(opening["domains"] and opening["domains"][0][1] / total > 0.6)
    if author_hit and domain_hit:
        return True, [
            f"author:{opening['authors'][0][0]}",
            f"domain:{opening['domains'][0][0]}",
        ]
    return False, []


def _find_recurring(channel: str, event_start: int, anomaly_log: List, skip_id: str) -> str | None:
    """Return the event_id of a prior same-channel event that started within
    ±45 min of the same UTC time-of-day, or None if none found."""
    tod = event_start % 86400
    for prior in reversed(anomaly_log):
        if prior.get("channel") == channel and prior.get("event_id") != skip_id:
            if abs(prior["event_start"] % 86400 - tod) <= 2700:
                return prior["event_id"]
    return None


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
    active_events:    Dict,       # keyed by channel; accumulates state across start→release
    anomaly_log:      List,
    anomaly_events_path: Path,
    eval_count:          list,    # [int] — mutable counter
    base_count:          list,
    threshold_count:     list,    # [int] — all start events (every threshold crossing)
    feed_suspect_count:  list,    # [int] — confirmed feed bursts (both windows)
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

            # ── start: open an in-memory event record ─────────────────────────
            for ev in gated_starts:
                opening      = _enrich_items(ev.items or [])
                sample_texts = [p["text"][:150] for p in opening["posts"][:3]]
                is_conc, conc_reasons = _is_concentration_burst(opening, ev.items or [])
                rec = {
                    "event_id":     f"bluesky-{ev.subreddit}-{ev.event_start}",
                    "source":       "bluesky",
                    "channel":      ev.subreddit,
                    "event_start":  ev.event_start,
                    "first_seen":   e,
                    "last_seen":    e,
                    "opening_z":    ev.z_score,
                    "peak_z":       ev.z_score,
                    "peak_count":   ev.count,
                    "mean":         ev.mean,
                    "std":          ev.std,
                    "update_count": 0,
                    "opening":      opening,
                    "sample_texts": sample_texts,
                }
                if is_conc:
                    rec["opening_concentration_suspect"] = True
                    rec["concentration_reasons"]         = conc_reasons
                active_events[ev.subreddit] = rec
                fires_by_channel[ev.subreddit] += 1
                if ev.z_score > max_z_by_channel.get(ev.subreddit, float("-inf")):
                    max_z_by_channel[ev.subreddit] = ev.z_score
                threshold_count[0] += 1
                label = "CONCENTRATION SUSPECT" if is_conc else "SURGE START"
                print(
                    f"[anomaly] {label}  {ev.subreddit}  z={ev.z_score}  "
                    f"count={ev.count}  mean={ev.mean}",
                    flush=True,
                )

            # ── update: accumulate peak state into existing record ─────────────
            for ev in updates:
                if ev.z_score > max_z_by_channel.get(ev.subreddit, float("-inf")):
                    max_z_by_channel[ev.subreddit] = ev.z_score
                rec = active_events.get(ev.subreddit)
                if rec is not None:
                    rec["last_seen"]    = e
                    rec["update_count"] += 1
                    new_peak_z     = ev.z_score > rec["peak_z"]
                    new_peak_count = ev.count   > rec["peak_count"]
                    if new_peak_z:
                        rec["peak_z"] = ev.z_score
                    if new_peak_count:
                        rec["peak_count"] = ev.count
                    if new_peak_z or new_peak_count:
                        peak_items  = tripwire.state.get_window_items(ev.subreddit, e)
                        rec["peak"] = _enrich_items(peak_items)
                        peak_conc, _ = _is_concentration_burst(rec["peak"], peak_items)
                        if peak_conc:
                            rec["peak_concentration_suspect"] = True

            # ── release: finalise and write ONE rich record to JSONL ───────────
            for ev in releases:
                rec = active_events.pop(ev.subreddit, None)
                if rec is None:
                    # Process restarted mid-surge — no start record in memory.
                    rec = {
                        "event_id":     f"bluesky-{ev.subreddit}-{ev.event_start}",
                        "source":       "bluesky",
                        "channel":      ev.subreddit,
                        "event_start":  ev.event_start,
                        "first_seen":   ev.event_start,
                        "last_seen":    e,
                        "opening_z":    None,
                        "peak_z":       ev.z_score,
                        "peak_count":   ev.count,
                        "mean":         ev.mean,
                        "std":          ev.std,
                        "update_count": 0,
                        "sample_texts": [],
                    }
                # Merge final z/count in case release tick itself is the peak.
                if ev.z_score > rec["peak_z"]:
                    rec["peak_z"] = ev.z_score
                if ev.count > rec["peak_count"]:
                    rec["peak_count"] = ev.count

                # Confirm feed burst: both opening AND peak windows concentrated.
                if rec.get("opening_concentration_suspect") and rec.get("peak_concentration_suspect"):
                    rec["feed_burst_suspect"] = True
                    feed_suspect_count[0] += 1

                # Recurrence: same channel, same UTC hour in prior completed events.
                prior_id = _find_recurring(ev.subreddit, rec["event_start"], anomaly_log, rec["event_id"])
                if prior_id:
                    rec["recurring_prior"] = prior_id
                    if rec.get("feed_burst_suspect"):
                        rec["recurring_feed_suspect"] = True

                duration_s = e - rec["event_start"]
                entry = {
                    **rec,
                    "released_at": datetime.now(timezone.utc).isoformat(),
                    "window_end":  e,
                    "duration_s":  duration_s,
                    "closing_z":   ev.z_score,
                }
                try:
                    with open(anomaly_events_path, "a", encoding="utf-8") as f:
                        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
                except OSError as exc:
                    print(f"[anomaly] event write failed: {exc}",
                          file=sys.stderr, flush=True)
                anomaly_log.append(entry)
                if len(anomaly_log) > _MAX_ANOMALY_LOG:
                    anomaly_log.pop(0)
                print(
                    f"[anomaly] SURGE END    {ev.subreddit}  "
                    f"duration={duration_s // 60}min  "
                    f"peak_z={rec['peak_z']}  updates={rec['update_count']}",
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
    active_events:    Dict,
    tripwire,
    state,
    eval_count:         list,
    base_count:         list,
    threshold_count:    list,
    feed_suspect_count: list,
    anomaly_log:        List,
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

    # Snapshot of in-progress surges (not yet released)
    active_snapshot = {
        ch: {
            "event_id":     rec["event_id"],
            "event_start":  rec["event_start"],
            "peak_z":       rec["peak_z"],
            "peak_count":   rec["peak_count"],
            "update_count": rec["update_count"],
        }
        for ch, rec in active_events.items()
    }

    summary = {
        "source":               "bluesky",
        "started_at":           wall_start,
        "uptime_s":             int(time.monotonic() - start_mono),
        "eval_ticks_fired":     eval_count[0],
        "baseline_ticks_fired": base_count[0],
        "threshold_crossings":  threshold_count[0],
        "feed_burst_suspects":  feed_suspect_count[0],
        "reportable_anomalies": threshold_count[0] - feed_suspect_count[0],
        "kept_by_channel":      dict(kept_by_channel),
        "channel_stats":        channel_summary,
        "active_surges":        active_snapshot,
        "updated_at":           datetime.now(timezone.utc).isoformat(),
        "completed_events":     anomaly_log[-_MAX_ANOMALY_LOG:],
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
    # AlertGate: in-memory only — no Redis, no webhooks, no dispatcher
    gate = AlertGate()

    ingestor = BlueskyIngestor()
    stream   = ingestor.stream()

    ts          = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    wall_start  = datetime.now(timezone.utc).isoformat()
    start_mono  = time.monotonic()
    anomaly_events_path = _ANOMALY_DIR / f"anomaly_events_{ts}.jsonl"

    kept_by_channel:  Counter           = Counter()
    fires_by_channel: Counter           = Counter()
    max_z_by_channel: Dict[str, float]  = {}
    active_events:    Dict[str, dict]   = {}   # currently-elevated channels
    anomaly_log:      List              = []   # completed event records (for summary)
    eval_count         = [0]
    base_count         = [0]
    threshold_count    = [0]
    feed_suspect_count = [0]
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
            active_events, anomaly_log, anomaly_events_path,
            eval_count, base_count, threshold_count, feed_suspect_count,
        )

        mono = time.monotonic()

        if mono - last_summary >= _SUMMARY_INTERVAL:
            _write_summary(
                _SUMMARY_PATH, wall_start, start_mono,
                kept_by_channel, fires_by_channel, max_z_by_channel,
                active_events, tripwire, state,
                eval_count, base_count, threshold_count, feed_suspect_count, anomaly_log,
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
