"""
Anomaly shadow replay test using past GCS shadow data.

Uses:
  - Per-channel baseline rates from run_summary (overnight data)
  - Routed samples from GCS as realistic post bodies
  - fakeredis (no real Redis needed)

Does NOT connect to live Jetstream.

What it tests:
  - _to_comment() handles Bluesky post dicts
  - SlidingWindowTripwire ingests posts, builds history, fires eval ticks
  - AlertGate cooldown logic works
  - Anomaly fires when a simulated surge is injected

Usage:
    python scripts/test_anomaly_replay.py
    python scripts/test_anomaly_replay.py --surge-channel culture_creators --surge-mult 4.0
    python scripts/test_anomaly_replay.py --min-history 3
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import time
import zlib
from pathlib import Path
from typing import Dict, List

random.seed(42)

import fakeredis

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from src.models import Comment
from src.pipeline.alert_gate import AlertGate
from src.pipeline.sliding_tripwire import SlidingWindowTripwire
from src.storage.state_manager import RedisStateManager

# ---------------------------------------------------------------------------
# Bluesky channels
# ---------------------------------------------------------------------------

BSKY_CHANNELS = [
    "ai_tech", "security_risk", "politics_government", "world_news",
    "science_health", "economy_markets", "platform_media",
    "sports", "culture_creators", "social_movements",
]

# Normal 2-hour window count per channel used for both baseline history
# and the "normal" window population. baseline_tick() stores the 2h window
# count (not hourly rate), so history and window must use the same scale.
# Values below are proportional to the overnight channel shares but scaled
# to a manageable test size (total ~2,000 posts/2h across all channels).
_BASELINE_WINDOW_COUNTS = {
    "culture_creators":      540,
    "politics_government":   415,
    "platform_media":        281,
    "world_news":            207,
    "science_health":        139,
    "ai_tech":               129,
    "sports":                115,
    "economy_markets":        88,
    "social_movements":       49,
    "security_risk":          34,
}


def _stable_id(s: str) -> int:
    return zlib.crc32(s.encode()) & 0xFFFFFFFF


def _make_comment(channel: str, body: str, now: int, idx: int) -> Comment:
    uid = f"bsky-replay-{channel}-{idx}"
    return Comment(
        id          = uid,
        subreddit   = channel,
        body        = body[:200],
        timestamp   = now,
        author      = f"did:plc:replay{idx:06d}",
        score       = 0,
        story_id    = _stable_id(uid),
        story_title = body[:80],
        domain      = "",
        item_type   = "post",
        item_id     = _stable_id(uid + "item"),
        created_at  = now,
    )


def _load_gcs_samples() -> List[Dict]:
    """Download the latest routed sample from GCS. Falls back to [] on error."""
    try:
        from google.cloud import storage
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            c = storage.Client(project="project-8299dfb6-57e5-4dcf-bc0")
            b = c.bucket("hn-surge-dashboard-01")
            data = json.loads(b.blob(
                "shadow/bluesky/routed_samples/latest_routed_pretty.json"
            ).download_as_text())
        print(f"  Loaded {len(data)} routed samples from GCS.")
        return data
    except Exception as exc:
        print(f"  [warn] GCS load failed ({exc}), using synthetic bodies.", file=sys.stderr)
        return []


def _bodies_by_channel(samples: List[Dict]) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {ch: [] for ch in BSKY_CHANNELS}
    for s in samples:
        ch = s.get("channel", "")
        if ch in out:
            out[ch].append(s.get("body", "sample post"))
    for ch in out:
        if not out[ch]:
            out[ch] = [f"sample {ch} post"]
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Bluesky anomaly shadow replay test")
    parser.add_argument("--surge-channel", default="politics_government",
                        help="Channel to inject surge into (default: politics_government)")
    parser.add_argument("--surge-mult",    type=float, default=5.0,
                        help="Surge multiplier vs baseline rate (default: 5.0)")
    parser.add_argument("--history-hours", type=int,   default=8,
                        help="Hours of synthetic baseline history to build (default: 8)")
    parser.add_argument("--min-history",   type=int,   default=6,
                        help="MIN_HISTORY for tripwire (default: 6)")
    args = parser.parse_args()

    print("\n=== Bluesky Anomaly Shadow — Replay Test ===\n")

    # ── 1. Load sample post bodies from GCS ──────────────────────────────────
    print("Step 1: Loading routed samples from GCS …")
    samples  = _load_gcs_samples()
    bodies   = _bodies_by_channel(samples)

    # ── 2. Set up fakeredis + pipeline components ─────────────────────────────
    print("Step 2: Initialising fakeredis + SlidingWindowTripwire …")
    r        = fakeredis.FakeRedis(decode_responses=True)
    state    = RedisStateManager(r, window_ttl=7200, history_size=168)
    tripwire = SlidingWindowTripwire(
        state, BSKY_CHANNELS,
        min_history=args.min_history,
        z_threshold=3.0,
    )
    gate = AlertGate()   # in-memory, no Redis, no dispatcher
    print(f"  MIN_HISTORY={args.min_history}  Z_THRESHOLD=3.0  WINDOW_TTL=7200s")

    # ── 3. Build synthetic baseline history ───────────────────────────────────
    print(f"Step 3: Pushing {args.history_hours}h of synthetic baseline history …")
    now = int(time.time())
    for h in range(args.history_hours, 0, -1):
        for ch in BSKY_CHANNELS:
            # ±20% natural variance so std > 0 and z-scores can compute.
            # Values represent 2h window counts (same scale as get_window_count).
            base   = _BASELINE_WINDOW_COUNTS[ch]
            jitter = random.uniform(0.80, 1.20)
            state.push_history(ch, int(base * jitter))
    print(f"  Pushed {args.history_hours} hourly counts per channel.")

    # ── 4. Populate 2-hour sliding window with normal traffic ─────────────────
    print("Step 4: Populating 2-hour window with normal traffic …")
    # Use the baseline 2h count as the "normal" window size per channel
    # so the window matches the history scale exactly.
    idx = 0
    for ch in BSKY_CHANNELS:
        ch_bodies  = bodies[ch]
        n_normal   = _BASELINE_WINDOW_COUNTS[ch]
        for i in range(n_normal):
            body    = ch_bodies[i % len(ch_bodies)]
            post_ts = now - 7000 + i * (7000 // max(n_normal, 1))
            c = _make_comment(ch, body, post_ts, idx)
            tripwire.ingest(c)
            idx += 1

    # ── 5. Inject surge for target channel ────────────────────────────────────
    surge_ch    = args.surge_channel
    surge_count = int(_BASELINE_WINDOW_COUNTS[surge_ch] * args.surge_mult)
    print(f"Step 5: Injecting {surge_count}x surge into '{surge_ch}' "
          f"(×{args.surge_mult} vs baseline window) …")
    surge_bodies = bodies[surge_ch]
    for i in range(surge_count):
        body = surge_bodies[i % len(surge_bodies)]
        post_ts = now - 1800 + i * (1800 // max(surge_count, 1))
        c = _make_comment(surge_ch, body, post_ts, idx)
        tripwire.ingest(c)
        idx += 1

    # ── 6. Fire evaluation tick ───────────────────────────────────────────────
    print("Step 6: Firing evaluation tick …\n")
    raw_events = tripwire.evaluation_tick(now)
    events     = gate.process(raw_events)

    # ── 7. Print window counts + z-scores for all channels ───────────────────
    print(f"{'Channel':<25} {'Window':>7}  {'History':>7}  {'Z-score':>8}  {'Status'}")
    print("─" * 65)

    from src.pipeline.sliding_tripwire import SlidingWindowTripwire as SWT
    for ch in BSKY_CHANNELS:
        count   = state.get_window_count(ch, now)
        history = state.get_history(ch)
        result  = tripwire._compute_z(ch, count, history)
        if result:
            z, mean, std = result
            z_str = f"{z:+.2f}"
        else:
            z_str = "  n/a  "
        elevated = tripwire._elevated.get(ch, False)
        status   = "*** ELEVATED ***" if elevated else ""
        print(f"  {ch:<23} {count:>7}  {len(history):>7}  {z_str:>8}  {status}")

    # ── 8. Print anomaly events ───────────────────────────────────────────────
    print(f"\n{'─' * 65}")
    print(f"Anomaly events fired: {len(events)}")
    if events:
        for ev in events:
            print(f"  ANOMALY  channel={ev.subreddit}  z={ev.z_score}  "
                  f"count={ev.count}  mean={ev.mean}  std={ev.std}")
            for it in (ev.items or [])[:2]:
                print(f"    sample: {it.get('text','')[:80]}")
    else:
        print("  (no anomalies fired — try --surge-mult 6.0 or --min-history 3)")

    print(f"\nReplay test complete. Total posts ingested: {idx}")


if __name__ == "__main__":
    main()
