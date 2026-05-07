from __future__ import annotations

import random
import sys
import time
from typing import List, Optional, Set

import fakeredis
import numpy as np

from ..models import AnomalyEvent, Comment
from .state_manager import RedisStateManager, StateManager

TARGET_SUBREDDITS: List[str] = [
    "gaming", "Games", "pcgaming", "PS5", "XboxSeriesX", "NintendoSwitch",
    "movies", "television", "entertainment", "popculturechat", "Music",
    "news", "worldnews",
]


class SlidingWindowTripwire:
    """Redis-backed sliding window anomaly detector.

    Completely decoupled from storage: communicates only through the
    StateManager interface, with no knowledge of Redis internals.

    Tick contract:
        evaluation_tick(now)  — called every 5 min. Fires AnomalyEvents.
        baseline_tick(now)    — called every 1 hour. Commits count to history.
        ingest(comment)       — called per incoming comment.
    """

    EVAL_INTERVAL     = 300    # 5 minutes
    BASELINE_INTERVAL = 3600   # 1 hour
    Z_THRESHOLD       = 3.0
    MIN_HISTORY       = 2
    VOLATILE_SUBS: Set[str] = {"news", "worldnews"}

    def __init__(self, state: StateManager, subreddits: List[str]) -> None:
        self.state      = state
        self.subreddits = subreddits

    def ingest(self, comment: Comment) -> None:
        self.state.ingest(comment)

    def baseline_tick(self, now: int) -> None:
        """Record the current 2-hour window count into rolling history."""
        for sub in self.subreddits:
            count = self.state.get_window_count(sub, now)
            self.state.push_history(sub, count)

    def evaluation_tick(self, now: int) -> List[AnomalyEvent]:
        """Evaluate every subreddit for anomalies. Returns fired events."""
        events: List[AnomalyEvent] = []

        for sub in self.subreddits:
            if self.state.get_last_alert(sub) is not None:
                continue  # cooldown active

            count   = self.state.get_window_count(sub, now)
            history = self.state.get_history(sub)

            if len(history) < self.MIN_HISTORY:
                continue  # cold start — not enough baseline

            arr = np.array(history, dtype=float)

            if sub in self.VOLATILE_SUBS:
                median  = float(np.median(arr))
                mad     = float(np.median(np.abs(arr - median)))
                if mad == 0:
                    continue  # flat baseline — any jitter looks infinite
                z_score = (count - median) / (mad + 1e-6)
                mean_val, std_val = median, mad
            else:
                mean_val = float(arr.mean())
                std_val  = float(arr.std())
                if std_val == 0:
                    continue
                z_score = (count - mean_val) / std_val

            if z_score >= self.Z_THRESHOLD:
                self.state.set_last_alert(sub, now)
                texts = self.state.get_window_texts(sub, now)
                events.append(AnomalyEvent(
                    subreddit=sub,
                    window_start=now - 7200,
                    window_end=now,
                    count=count,
                    z_score=round(z_score, 2),
                    texts=texts,
                    mean=round(mean_val, 2),
                    std=round(std_val, 2),
                ))

        return events


# ---------------------------------------------------------------------------
# Load test
# ---------------------------------------------------------------------------

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

    r       = fakeredis.FakeRedis(decode_responses=True)
    state   = RedisStateManager(r)
    tripwire = SlidingWindowTripwire(state, TARGET_SUBREDDITS)

    BASE_TS   = 1_700_000_000   # arbitrary fixed epoch
    HOUR      = 3600

    total_comments   = 0
    total_anomalies  = 0
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
            now    = hour_start + (tick + 1) * SlidingWindowTripwire.EVAL_INTERVAL
            t0     = time.perf_counter()
            events = tripwire.evaluation_tick(now)
            tick_ms = (time.perf_counter() - t0) * 1000

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

    # 1. Exactly 1 anomaly must have fired
    if total_anomalies == 1:
        print(f"  [PASS] Exactly 1 anomaly fired")
    else:
        print(f"  [FAIL] Expected 1 anomaly, got {total_anomalies}")
        passed = False

    # 2. The anomaly must be r/gaming
    if anomaly_log and anomaly_log[0].subreddit == "gaming":
        print(f"  [PASS] Anomaly is r/gaming (z={anomaly_log[0].z_score})")
    else:
        sub = anomaly_log[0].subreddit if anomaly_log else "none"
        print(f"  [FAIL] Expected r/gaming, got r/{sub}")
        passed = False

    # 3. Texts are capped at TEXT_CAP=500; size equals min(window_count, 500)
    if anomaly_log:
        texts = anomaly_log[0].texts
        expected_texts = min(anomaly_log[0].count, 500)
        if len(texts) == expected_texts:
            print(f"  [PASS] Sample size correct ({len(texts)} texts, cap=500)")
        else:
            print(f"  [FAIL] Expected {expected_texts} texts, got {len(texts)}")
            passed = False

    # 4. Ask at BASE_TS + 3*HOUR + 7200 (= 2 hours after the spike hour ended).
    #    Window = [BASE_TS+3*HOUR, BASE_TS+3*HOUR+7200] — only hour-4 data lands here;
    #    hours 1-3 are all older than 7200s relative to this timestamp.
    prune_now = BASE_TS + 3 * HOUR + 7200
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


if __name__ == "__main__":
    run_load_test()
