from __future__ import annotations

from typing import Dict, List, Optional, Set, Tuple

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

    Detection model:
        Trigger threshold  Z ≥ 3.0  — subreddit enters ELEVATED state, event begins.
        Release threshold  Z ≤ 2.0  — subreddit exits ELEVATED state, event ends.
        Hysteresis zone    2.0 < Z < 3.0  — once ELEVATED, keeps emitting events.
        Baseline freeze    while ELEVATED, baseline_tick pushes last clean count
                           instead of the anomalous one, keeping the mean anchored.
    """

    EVAL_INTERVAL     = 300    # 5 minutes
    BASELINE_INTERVAL = 3600   # 1 hour
    Z_THRESHOLD       = 3.0    # trigger: event begins
    RELEASE_THRESHOLD = 2.0    # release: event ends
    MIN_HISTORY       = 2
    VOLATILE_SUBS: Set[str] = {"news", "worldnews"}

    def __init__(self, state: StateManager, subreddits: List[str]) -> None:
        self.state      = state
        self.subreddits = subreddits
        self._elevated: Dict[str, bool] = {}   # per-subreddit Schmitt trigger state

    def ingest(self, comment: Comment) -> None:
        self.state.ingest(comment)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _compute_z(
        self, sub: str, count: int, history: List[float]
    ) -> Optional[Tuple[float, float, float]]:
        """Return (z_score, mean_val, std_val) or None if conditions not met."""
        if len(history) < self.MIN_HISTORY:
            return None
        arr = np.array(history, dtype=float)
        if sub in self.VOLATILE_SUBS:
            median = float(np.median(arr))
            mad    = float(np.median(np.abs(arr - median)))
            if mad == 0:
                return None   # flat baseline — any jitter looks infinite
            return (count - median) / (mad + 1e-6), median, mad
        else:
            mean_val = float(arr.mean())
            std_val  = float(arr.std())
            if std_val == 0:
                return None
            return (count - mean_val) / std_val, mean_val, std_val

    # ------------------------------------------------------------------
    # Tick methods
    # ------------------------------------------------------------------

    def baseline_tick(self, now: int) -> None:
        """Record hourly count into rolling history.

        Baseline Freeze: if the subreddit is currently ELEVATED, push the
        last clean count instead of the anomalous current count. This keeps
        the mathematical baseline anchored at pre-event ambient noise so the
        Z-score doesn't decay to zero during a sustained surge.
        """
        for sub in self.subreddits:
            count   = self.state.get_window_count(sub, now)
            history = self.state.get_history(sub)
            if self._elevated.get(sub, False) and history:
                self.state.push_history(sub, history[-1])
            else:
                self.state.push_history(sub, count)

    def evaluation_tick(self, now: int) -> List[AnomalyEvent]:
        """Evaluate every subreddit for anomalies. Returns all scored events.

        Implements a Schmitt Trigger (hysteresis) state machine per subreddit:
          - Enters ELEVATED on Z ≥ Z_THRESHOLD (3.0)
          - Stays ELEVATED while Z > RELEASE_THRESHOLD (2.0)
          - Exits ELEVATED when Z ≤ RELEASE_THRESHOLD (2.0)

        No cooldown logic here — every event while ELEVATED is returned.
        Cooldown for user notifications is applied downstream by AlertGate.
        """
        events: List[AnomalyEvent] = []

        for sub in self.subreddits:
            count   = self.state.get_window_count(sub, now)
            history = self.state.get_history(sub)
            result  = self._compute_z(sub, count, history)
            if result is None:
                continue

            z_score, mean_val, std_val = result
            elevated = self._elevated.get(sub, False)

            if not elevated and z_score >= self.Z_THRESHOLD:
                self._elevated[sub] = True
                elevated = True
            elif elevated and z_score <= self.RELEASE_THRESHOLD:
                self._elevated[sub] = False
                elevated = False

            if elevated:
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
