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
    MIN_HISTORY       = 24   # require 24 hourly baseline samples before alerting
    MIN_COUNT         = 10   # ignore windows with fewer than 10 items (low-volume noise)
    VOLATILE_SUBS: Set[str] = {"news", "worldnews"}

    def __init__(
        self,
        state:       StateManager,
        subreddits:  List[str],
        *,
        min_history: Optional[int]   = None,
        z_threshold: Optional[float] = None,
        window_ttl:  Optional[int]   = None,
    ) -> None:
        self.state      = state
        self.subreddits = subreddits
        self._elevated:       Dict[str, bool] = {}
        self._last_eval:      Dict[str, dict] = {}
        self._elevation_start: Dict[str, int]  = {}  # sub → unix ts when ELEVATED began
        # Per-instance overrides — use is-not-None so 0 / 0.0 are valid values.
        # Class constants remain unchanged so HN production is unaffected.
        if min_history is not None:
            self.MIN_HISTORY = min_history
        if z_threshold is not None:
            self.Z_THRESHOLD = z_threshold
        self.window_ttl = window_ttl if window_ttl is not None else 7200

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

        Event lifecycle per channel:
          event_type="start"   — channel just crossed Z_THRESHOLD (new distinct surge)
          event_type="update"  — channel still ELEVATED (heartbeat, no counter increment)
          event_type="release" — channel dropped below RELEASE_THRESHOLD (surge ended)
        Only "start" events should be counted as new anomalies downstream.
        """
        events: List[AnomalyEvent] = []

        for sub in self.subreddits:
            count    = self.state.get_window_count(sub, now)
            elevated = self._elevated.get(sub, False)

            # Block low-volume windows from triggering ELEVATED; if already
            # elevated, still run so the Schmitt trigger can release cleanly.
            if count < self.MIN_COUNT and not elevated:
                continue

            history = self.state.get_history(sub)
            result  = self._compute_z(sub, count, history)
            if result is None:
                continue

            z_score, mean_val, std_val = result

            just_entered  = False
            just_released = False

            if not elevated and z_score >= self.Z_THRESHOLD:
                self._elevated[sub] = True
                self._elevation_start[sub] = now
                elevated     = True
                just_entered = True
            elif elevated and z_score <= self.RELEASE_THRESHOLD:
                self._elevated[sub] = False
                elevated      = False
                just_released = True

            self._last_eval[sub] = {
                "count":    count,
                "z_score":  round(z_score, 2),
                "mean":     round(mean_val, 2),
                "std":      round(std_val, 2),
                "elevated": elevated,
            }

            if elevated:
                # Fetch sample items only on entry — saves Redis calls on updates.
                items      = self.state.get_window_items(sub, now) if just_entered else []
                event_type = "start" if just_entered else "update"
                events.append(AnomalyEvent(
                    subreddit=sub,
                    window_start=now - self.window_ttl,
                    window_end=now,
                    count=count,
                    z_score=round(z_score, 2),
                    items=items,
                    mean=round(mean_val, 2),
                    std=round(std_val, 2),
                    event_type=event_type,
                    event_start=self._elevation_start.get(sub, now),
                ))
            elif just_released:
                events.append(AnomalyEvent(
                    subreddit=sub,
                    window_start=now - self.window_ttl,
                    window_end=now,
                    count=count,
                    z_score=round(z_score, 2),
                    items=[],
                    mean=round(mean_val, 2),
                    std=round(std_val, 2),
                    event_type="release",
                    event_start=self._elevation_start.pop(sub, now),
                ))

        return events

    def channel_stats(self) -> Dict[str, dict]:
        """Return last-evaluation stats (z, mean, std, count, elevated) per channel."""
        return dict(self._last_eval)
