from __future__ import annotations

from typing import Dict, List, Optional

from ..models import AnomalyEvent

_COOLDOWN = 1800  # 30 minutes


class AlertGate:
    """Cooldown gate between the detection engine and the notification layer.

    Every anomaly the tripwire scores reaches here; only those that pass
    the per-subreddit cooldown are forwarded as actionable alerts.

    Two backends:
        In-memory (default / backtest): compares simulated event timestamps.
            Correct when simulated time runs faster than wall-clock.
        Redis (production): uses SET ... EX for TTL-based expiry.
            Survives process restarts; consistent across worker processes.

    Production swap (one line):
        gate = AlertGate(r=redis.Redis(...))   # TTL-based, multi-process safe
        gate = AlertGate()                     # in-memory, backtest / single process
    """

    COOLDOWN = _COOLDOWN

    def __init__(
        self,
        cooldown: int = _COOLDOWN,
        r: Optional[object] = None,
    ) -> None:
        self._cooldown = cooldown
        self._r = r
        self._memory: Dict[str, int] = {}

    def process(self, events: List[AnomalyEvent]) -> List[AnomalyEvent]:
        """Return only events that pass the cooldown and record them."""
        passed = []
        for ev in events:
            if self._check_and_set(ev.subreddit, ev.window_end):
                passed.append(ev)
        return passed

    def _check_and_set(self, subreddit: str, event_ts: int) -> bool:
        if self._r is not None:
            key = f"alert:{subreddit}"
            if self._r.get(key) is None:
                self._r.set(key, event_ts, ex=self._cooldown)
                return True
            return False
        last = self._memory.get(subreddit)
        if last is None or event_ts - last > self._cooldown:
            self._memory[subreddit] = event_ts
            return True
        return False

    def flush(self) -> None:
        """Reset all cooldown state — used between test runs."""
        self._memory.clear()
        if self._r is not None:
            for key in self._r.scan_iter("alert:*"):
                self._r.delete(key)
