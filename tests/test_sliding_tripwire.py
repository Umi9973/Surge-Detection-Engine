from __future__ import annotations

import random
import sys
import time
from pathlib import Path
from typing import List

import fakeredis

# Ensure project root is on the path when run as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.models import AnomalyEvent, Comment
from src.storage.state_manager import RedisStateManager
from src.pipeline.sliding_tripwire import SlidingWindowTripwire
from src.pipeline.alert_gate import AlertGate

TARGET_SUBREDDITS: List[str] = [
    "gaming", "Games", "pcgaming", "PS5", "XboxSeriesX", "NintendoSwitch",
    "movies", "television", "entertainment", "popculturechat", "Music",
    "news", "worldnews",
]


def _make_comments(
    subreddit: str,
    n: int,
    hour_start: int,
    hour_end: int,
    template: str = "Comment {i} about {sub}",
) -> List[Comment]:
    return [
        Comment(
            id=f"{subreddit}-{hour_start}-{i}",
            subreddit=subreddit,
            body=template.format(i=i, sub=subreddit),
            timestamp=random.randint(hour_start, hour_end - 1),
        )
        for i in range(n)
    ]


def run_load_test() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    print("=" * 70)
    print("  Redis Sliding Window — Load Test")
    print("=" * 70)

    r        = fakeredis.FakeRedis(decode_responses=True)
    state    = RedisStateManager(r)
    tripwire = SlidingWindowTripwire(state, TARGET_SUBREDDITS)
    gate     = AlertGate()

    BASE_TS = 1_700_000_000   # arbitrary fixed epoch
    HOUR    = 3600

    total_comments  = 0
    total_anomalies = 0
    anomaly_log: List[AnomalyEvent] = []

    # 4 hours: 2 warmup (builds 2 clean baseline entries) + 1 spike + 1 cooldown verify
    for hour_idx in range(1, 5):
        hour_start = BASE_TS + (hour_idx - 1) * HOUR
        hour_end   = hour_start + HOUR
        hour_label = f"HOUR {hour_idx}"

        # --- Inject comments for this hour ---
        comments: List[Comment] = []
        for sub in TARGET_SUBREDDITS:
            n = 800 if (sub == "gaming" and hour_idx == 3) else random.randint(80, 120)
            comments.extend(_make_comments(sub, n, hour_start, hour_end))

        random.shuffle(comments)
        t0 = time.perf_counter()
        for c in comments:
            tripwire.ingest(c)
        inject_ms = (time.perf_counter() - t0) * 1000
        total_comments += len(comments)

        print(f"\n[{hour_label}] Injected {len(comments)} comments ({inject_ms:.1f}ms)")

        # --- Evaluation ticks FIRST (every 5 min) ---
        # Must evaluate before baseline_tick so the current hour's count isn't
        # in the history yet — otherwise the spike inflates its own baseline.
        hour_anomalies = 0
        for tick in range(12):
            now     = hour_start + (tick + 1) * SlidingWindowTripwire.EVAL_INTERVAL
            t0         = time.perf_counter()
            raw_events = tripwire.evaluation_tick(now)
            tick_ms    = (time.perf_counter() - t0) * 1000
            events     = gate.process(raw_events)

            if events:
                for ev in events:
                    hour_anomalies += 1
                    total_anomalies += 1
                    anomaly_log.append(ev)
                    print(
                        f"  *** ANOMALY tick {tick+1:02d} | "
                        f"r/{ev.subreddit} | count={ev.count} | "
                        f"z={ev.z_score} | texts={len(ev.texts)} | "
                        f"query={tick_ms:.2f}ms ***"
                    )
            else:
                if tick == 0:
                    print(f"  tick {tick+1:02d} — no anomalies (query={tick_ms:.2f}ms)")

        if hour_anomalies == 0:
            print(f"  All 12 ticks: no anomalies")

        # --- Baseline tick AFTER evaluation ---
        tripwire.baseline_tick(hour_end)
        print(f"  baseline_tick @ {hour_end}")

    # --- Assertions ---
    print("\n" + "=" * 70)
    print("  Assertions")
    print("=" * 70)

    passed = True
    gaming_alerts = [ev for ev in anomaly_log if ev.subreddit == "gaming"]

    # 1. r/gaming must have been alerted at least once
    if gaming_alerts:
        print(f"  [PASS] r/gaming alerted {len(gaming_alerts)} time(s) (z={gaming_alerts[0].z_score})")
    else:
        print(f"  [FAIL] r/gaming never alerted")
        passed = False

    # 2. No two r/gaming alerts within the cooldown window (gate working correctly)
    cooldown_ok = True
    for i in range(1, len(gaming_alerts)):
        gap = gaming_alerts[i].window_end - gaming_alerts[i - 1].window_end
        if gap <= AlertGate.COOLDOWN:
            print(f"  [FAIL] Two r/gaming alerts only {gap}s apart (cooldown={AlertGate.COOLDOWN}s)")
            cooldown_ok = False
            passed = False
    if cooldown_ok and gaming_alerts:
        print(f"  [PASS] AlertGate cooldown respected across all r/gaming alerts")

    # 3. Texts are capped at TEXT_CAP=500; size equals min(window_count, 500)
    if gaming_alerts:
        texts = gaming_alerts[0].texts
        expected_texts = min(gaming_alerts[0].count, 500)
        if len(texts) == expected_texts:
            print(f"  [PASS] Sample size correct ({len(texts)} texts, cap=500)")
        else:
            print(f"  [FAIL] Expected {expected_texts} texts, got {len(texts)}")
            passed = False

    # 4. Ask at BASE_TS + 3*HOUR + 7200 (= 2 hours after the spike hour ended).
    #    Window = [BASE_TS+3*HOUR, BASE_TS+3*HOUR+7200] — only hour-4 data lands here;
    #    hours 1-3 are all older than 7200s relative to this timestamp.
    prune_now    = BASE_TS + 3 * HOUR + 7200
    gaming_count = state.get_window_count("gaming", prune_now)
    expected_range = (80, 120)
    if expected_range[0] <= gaming_count <= expected_range[1]:
        print(f"  [PASS] Post-prune r/gaming count={gaming_count} (only hour-4 data survives)")
    else:
        print(f"  [FAIL] Post-prune r/gaming count={gaming_count}, expected {expected_range}")
        passed = False

    print("\n" + "=" * 70)
    print(f"  Total comments injected : {total_comments}")
    print(f"  Total anomalies fired   : {total_anomalies}")
    status = "LOAD TEST PASSED" if passed else "LOAD TEST FAILED"
    print(f"  {status}")
    print("=" * 70)

    state.flush()
    gate.flush()


if __name__ == "__main__":
    run_load_test()
