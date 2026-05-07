from __future__ import annotations

from typing import List, Set

import numpy as np

from ..models import AnomalyEvent, Comment
from ..storage.state_manager import StateManager


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
