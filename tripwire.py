from __future__ import annotations

import random
from collections import defaultdict, deque
from typing import Dict, Iterator, List, Optional, Set

import numpy as np


class TumblingWindowTripwire:
    def __init__(
        self,
        window_seconds: int = 3600,
        history_size: int = 24,
        z_threshold: float = 3.0,
        min_history: int = 2,
        text_buffer_cap: int = 500,
        volatile_subreddits: Optional[Set[str]] = None,
    ) -> None:
        self.window_seconds = window_seconds
        self.z_threshold = z_threshold
        self.min_history = min_history
        self.text_buffer_cap = text_buffer_cap
        self.volatile_subreddits = volatile_subreddits or set()
        self._history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=history_size))
        self._current_counts: Dict[str, int] = defaultdict(int)
        self._current_texts: Dict[str, List[str]] = defaultdict(list)
        self._reservoir_counts: Dict[str, int] = defaultdict(int)
        self._current_window_start: Optional[int] = None

    def _reservoir_sample(self, subreddit: str, text: str) -> None:
        """Add text to the reservoir, maintaining a uniform random sample of up to text_buffer_cap items."""
        n = self._reservoir_counts[subreddit]
        self._reservoir_counts[subreddit] += 1
        if n < self.text_buffer_cap:
            self._current_texts[subreddit].append(text)
        else:
            j = random.randint(0, n)
            if j < self.text_buffer_cap:
                self._current_texts[subreddit][j] = text

    def _tumble(self) -> Iterator[Dict]:
        for subreddit, count in self._current_counts.items():
            history = self._history[subreddit]
            texts = self._current_texts.get(subreddit, [])

            if len(history) >= self.min_history:
                arr = np.array(history, dtype=float)

                if subreddit in self.volatile_subreddits:
                    median = np.median(arr)
                    mad = np.median(np.abs(arr - median))
                    epsilon = 1e-6
                    z_score = (count - median) / (mad + epsilon)
                    if z_score >= self.z_threshold:
                        yield {
                            "subreddit":    subreddit,
                            "window_start": self._current_window_start,
                            "window_end":   self._current_window_start + self.window_seconds,
                            "count":        count,
                            "median":       round(float(median), 2),
                            "mad":          round(float(mad), 2),
                            "z_score":      round(float(z_score), 2),
                            "texts":        texts,
                        }
                else:
                    mean = np.mean(arr)
                    std = np.std(arr)
                    if std > 0:
                        z_score = (count - mean) / std
                        if z_score >= self.z_threshold:
                            yield {
                                "subreddit":    subreddit,
                                "window_start": self._current_window_start,
                                "window_end":   self._current_window_start + self.window_seconds,
                                "count":        count,
                                "mean":         round(float(mean), 2),
                                "std":          round(float(std), 2),
                                "z_score":      round(float(z_score), 2),
                                "texts":        texts,
                            }

            history.append(count)

        self._current_counts.clear()
        self._current_texts.clear()
        self._reservoir_counts.clear()

    def stream(self, source: Iterator[Dict]) -> Iterator[Dict]:
        for comment in source:
            ts = int(comment["timestamp"])
            window_start = (ts // self.window_seconds) * self.window_seconds

            if self._current_window_start is None:
                self._current_window_start = window_start

            if window_start > self._current_window_start:
                yield from self._tumble()
                self._current_window_start = window_start

            subreddit = comment["subreddit"]
            self._current_counts[subreddit] += 1
            body = comment.get("body", "").strip()
            if body and body != "[deleted]" and body != "[removed]":
                self._reservoir_sample(subreddit, body)

        if self._current_counts:
            yield from self._tumble()


if __name__ == "__main__":
    import os
    import sys
    from datetime import datetime, timezone

    sys.stdout.reconfigure(encoding="utf-8")

    from ingestion import ZstFileIngestor
    from filter import SubredditFilter

    TARGET_SUBREDDITS = [
        "gaming", "Games", "pcgaming", "PS5", "XboxSeriesX", "NintendoSwitch",
        "movies", "television", "entertainment", "popculturechat", "Music",
        "news", "worldnews",
    ]

    DATA_FILE = os.path.join(os.path.dirname(__file__), "RC_2023-12.zst")
    HOURS_TO_PROCESS = 48
    CUTOFF_SECONDS = HOURS_TO_PROCESS * 3600

    ingestor = ZstFileIngestor(DATA_FILE)
    filter_ = SubredditFilter(TARGET_SUBREDDITS)
    tripwire = TumblingWindowTripwire()

    start_ts: Optional[int] = None

    def capped_stream() -> Iterator[Dict]:
        global start_ts
        for comment in filter_.stream(ingestor.stream()):
            ts = int(comment["timestamp"])
            if start_ts is None:
                start_ts = ts
            if ts - start_ts > CUTOFF_SECONDS:
                return
            yield comment

    anomaly_count = 0
    for anomaly in tripwire.stream(capped_stream()):
        anomaly_count += 1
        window_start_dt = datetime.fromtimestamp(anomaly["window_start"], tz=timezone.utc)
        print(
            f"[ANOMALY #{anomaly_count}] "
            f"r/{anomaly['subreddit']} | "
            f"{window_start_dt.strftime('%Y-%m-%d %H:00 UTC')} | "
            f"count={anomaly['count']} | z={anomaly['z_score']} | "
            f"texts_sampled={len(anomaly['texts'])}"
        )

    print(f"\nDone. {anomaly_count} anomalies detected across 48 hours.")
