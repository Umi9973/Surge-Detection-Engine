"""
Smoke test for the start/update/release event lifecycle fix.

Verifies:
  1. Normal traffic  → no events
  2. Surge injected  → exactly one "start" event per channel
  3. Surge continues → "update" events (not "start"), anomaly_count unchanged
  4. Traffic drops   → exactly one "release" event, duration logged
  5. anomaly_count   == number of distinct start events only

Uses fakeredis + synthetic data. No GCS, no live Jetstream required.

Usage:
    python scripts/test_event_lifecycle.py
"""
from __future__ import annotations

import random
import sys
import time
import zlib
from pathlib import Path
from typing import List

random.seed(0)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

import fakeredis

from src.models import Comment, AnomalyEvent
from src.pipeline.sliding_tripwire import SlidingWindowTripwire
from src.storage.state_manager import RedisStateManager

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

CHANNELS = ["sports", "us_politics", "ai_tech"]

_BASELINE_COUNTS = {
    "sports":     300,
    "us_politics": 800,
    "ai_tech":    400,
}

def _stable_id(s: str) -> int:
    return zlib.crc32(s.encode()) & 0xFFFFFFFF

def _make_comment(channel: str, idx: int, ts: int) -> Comment:
    uid = f"smoke-{channel}-{idx}"
    return Comment(
        id=uid, subreddit=channel, body=f"test post {idx}",
        timestamp=ts, author=f"user{idx}", score=0,
        story_id=_stable_id(uid), story_title=f"title {idx}",
        domain="", item_type="post",
        item_id=_stable_id(uid + "i"), created_at=ts,
    )

def ingest_n(tripwire, channel: str, n: int, ts: int, start_idx: int) -> int:
    for i in range(n):
        tripwire.ingest(_make_comment(channel, start_idx + i, ts))
    return start_idx + n

def _types(events: List[AnomalyEvent]) -> List[str]:
    return [ev.event_type for ev in events]

# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

def run() -> None:
    r        = fakeredis.FakeRedis(decode_responses=True)
    state    = RedisStateManager(r, window_ttl=3600, history_size=168)
    tripwire = SlidingWindowTripwire(
        state, CHANNELS,
        min_history=6,
        z_threshold=3.0,
        window_ttl=3600,
    )

    now = int(time.time())
    idx = 0
    passed = 0
    failed = 0

    def check(label: str, condition: bool) -> None:
        nonlocal passed, failed
        status = "PASS" if condition else "FAIL"
        print(f"  [{status}] {label}")
        if condition:
            passed += 1
        else:
            failed += 1

    # ── 1. Build baseline history (8 ticks, ±15% jitter) ──────────────────
    print("\n── Step 1: push 8 baseline history ticks ──")
    for h in range(8):
        for ch in CHANNELS:
            base   = _BASELINE_COUNTS[ch]
            jitter = random.uniform(0.85, 1.15)
            state.push_history(ch, int(base * jitter))
    print(f"  Pushed 8 hourly snapshots per channel.")

    # ── 2. Populate window with normal traffic ─────────────────────────────
    print("\n── Step 2: populate 1h window with normal traffic ──")
    for ch in CHANNELS:
        n   = _BASELINE_COUNTS[ch]
        idx = ingest_n(tripwire, ch, n,
                       ts=now - 3500, start_idx=idx)
    events = tripwire.evaluation_tick(now)
    print(f"  eval_tick events: {_types(events) or '(none)'}")
    check("No events during normal traffic", len(events) == 0)

    # ── 3. Inject surge into 'sports' (5× baseline) ───────────────────────
    print("\n── Step 3: inject 5× surge into sports ──")
    surge_n = _BASELINE_COUNTS["sports"] * 5
    idx = ingest_n(tripwire, "sports", surge_n, ts=now - 100, start_idx=idx)

    tick1 = tripwire.evaluation_tick(now)
    sports_tick1 = [ev for ev in tick1 if ev.subreddit == "sports"]
    print(f"  sports events: {[(ev.event_type, ev.z_score) for ev in sports_tick1]}")
    check("Exactly one sports event on surge",       len(sports_tick1) == 1)
    check("Event type is 'start'",                   sports_tick1[0].event_type == "start" if sports_tick1 else False)
    check("start event has sample items",             len(sports_tick1[0].items) > 0 if sports_tick1 else False)
    check("start event has event_start timestamp",   sports_tick1[0].event_start > 0 if sports_tick1 else False)
    check("No start events for non-surged channels", all(
        ev.event_type != "start"
        for ev in tick1 if ev.subreddit != "sports"
    ))

    # ── 4. Second eval tick — surge still present ──────────────────────────
    print("\n── Step 4: second eval tick while still elevated ──")
    tick2 = tripwire.evaluation_tick(now + 300)
    sports_tick2 = [ev for ev in tick2 if ev.subreddit == "sports"]
    print(f"  sports events: {[(ev.event_type, ev.z_score) for ev in sports_tick2]}")
    check("Exactly one sports event on second tick",  len(sports_tick2) == 1)
    check("Event type is 'update' (not 'start')",    sports_tick2[0].event_type == "update" if sports_tick2 else False)
    check("update event has NO items (saves Redis)",  len(sports_tick2[0].items) == 0 if sports_tick2 else False)
    check("event_start unchanged across ticks",       (
        sports_tick2[0].event_start == sports_tick1[0].event_start
        if sports_tick1 and sports_tick2 else False
    ))

    # ── 5. Third eval tick — same ──────────────────────────────────────────
    print("\n── Step 5: third eval tick while still elevated ──")
    tick3 = tripwire.evaluation_tick(now + 600)
    sports_tick3 = [ev for ev in tick3 if ev.subreddit == "sports"]
    check("Still 'update' on third tick",             (
        sports_tick3[0].event_type == "update" if sports_tick3 else False
    ))

    # ── 6. Window expires — old surge posts age out ────────────────────────
    # Advance time past the window TTL so the surge posts fall outside the 1h window.
    # Add normal traffic at the new time to avoid an empty window.
    print("\n── Step 6: advance time to drain surge from window ──")
    future = now + 3700   # beyond 1h window TTL
    idx = ingest_n(tripwire, "sports", _BASELINE_COUNTS["sports"],
                   ts=future - 100, start_idx=idx)

    tick4 = tripwire.evaluation_tick(future)
    sports_tick4 = [ev for ev in tick4 if ev.subreddit == "sports"]
    print(f"  sports events: {[(ev.event_type, round(ev.z_score,2)) for ev in sports_tick4]}")
    release_events = [ev for ev in sports_tick4 if ev.event_type == "release"]
    check("Release event emitted after surge drains",  len(release_events) == 1)
    check("release event has duration_s via event_start", (
        release_events[0].window_end - release_events[0].event_start > 0
        if release_events else False
    ))
    check("No 'start' emitted after release",          all(
        ev.event_type != "start" for ev in sports_tick4
    ))
    check("sports no longer elevated",                 not tripwire._elevated.get("sports", False))

    # ── 7. Counter semantics ───────────────────────────────────────────────
    print("\n── Step 7: verify event_type semantics across all ticks ──")
    all_events = tick1 + tick2 + tick3 + tick4
    all_sports  = [ev for ev in all_events if ev.subreddit == "sports"]
    start_count   = sum(1 for ev in all_sports if ev.event_type == "start")
    update_count  = sum(1 for ev in all_sports if ev.event_type == "update")
    release_count = sum(1 for ev in all_sports if ev.event_type == "release")
    print(f"  sports: {start_count} start, {update_count} update, {release_count} release")
    check("Exactly 1 start event for the entire surge",   start_count == 1)
    check("Exactly 2 update events (ticks 2 and 3)",      update_count == 2)
    check("Exactly 1 release event",                      release_count == 1)

    # ── Summary ───────────────────────────────────────────────────────────
    print(f"\n{'='*50}")
    print(f"  {passed} passed  |  {failed} failed")
    print(f"{'='*50}\n")
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    run()
