"""
HashtagStatsCollector — bounded per-hashtag statistics for Bluesky discovery.

Attached to BlueskyIngestor as an observer. Records every post (matched +
unmatched) after language filtering. Produces a snapshot JSON read by
scripts/hashtag_report.py to generate discovery reports.

Memory design:
  - Cheap counters (total/matched/unmatched) for all hashtags up to max_unique cap.
  - Rich stats (co-hashtags, domains, samples, authors) only for heavy hitters
    that cross _RICH_THRESHOLD occurrences AND fit in the top rich_top_n slots.
  - Author tracking is capped per hashtag; reported as unique_authors_capped.
"""
from __future__ import annotations

import json
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Dict, List, Set

_RICH_THRESHOLD  = 5    # min total_count before rich stats are collected
_MAX_COHT_TRACK  = 50   # max unique co-hashtags tracked per hashtag (report top 20)
_MAX_DOM_TRACK   = 50   # max unique domains tracked per hashtag (report top 20)


class HashtagStatsCollector:
    """
    Collect per-hashtag statistics during a live ingestion run.

    Call record() for every post that passes the language filter, regardless
    of whether it is kept or dropped. Write a cumulative snapshot JSON with
    write_snapshot() periodically (recommended: every 5 minutes).
    """

    def __init__(
        self,
        max_unique:  int = 50_000,
        rich_top_n:  int = 500,
        max_samples: int = 3,
        max_authors: int = 100,
    ) -> None:
        self._max_unique  = max_unique
        self._rich_top_n  = rich_top_n
        self._max_samples = max_samples
        self._max_authors = max_authors

        # Cheap counters — tracked for all hashtags up to the cap
        self._total:     Counter = Counter()
        self._matched:   Counter = Counter()
        self._unmatched: Counter = Counter()
        self._overflow   = 0     # hashtags silently dropped after cap

        # Rich stats — only for heavy hitters, created lazily
        self._channels:   Dict[str, Counter]       = {}
        self._cohashtags: Dict[str, Counter]       = {}
        self._domains:    Dict[str, Counter]       = {}
        self._samples:    Dict[str, List[str]]     = {}
        self._authors:    Dict[str, Set[str]]      = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def record(
        self,
        hashtags:    List[str],
        channel:     str,
        drop_reason: str,
        did:         str,
        domain:      str,
        body:        str,
    ) -> None:
        """
        Record one post.

        hashtags    — raw tags extracted from AT Protocol facets
        channel     — winning channel from router ("general" if unmatched)
        drop_reason — empty string if kept; "unmatched_topic", "adult_spam", etc. if dropped
        did         — author DID (for capped unique-author approximation)
        domain      — external link domain or ""
        body        — post text (for sample capture)
        """
        if not hashtags:
            return

        ht_lower   = [ht.lower() for ht in hashtags]
        is_matched = (not drop_reason) and (channel != "general")

        for ht in ht_lower:
            # Enforce unique-hashtag cap
            if ht not in self._total:
                if len(self._total) >= self._max_unique:
                    self._overflow += 1
                    continue

            # Cheap counters
            self._total[ht] += 1
            if is_matched:
                self._matched[ht] += 1
            else:
                self._unmatched[ht] += 1

            # Promote to rich set when threshold crossed and capacity available
            if (ht not in self._channels
                    and self._total[ht] >= _RICH_THRESHOLD
                    and len(self._channels) < self._rich_top_n):
                self._channels[ht]   = Counter()
                self._cohashtags[ht] = Counter()
                self._domains[ht]    = Counter()
                self._samples[ht]    = []
                self._authors[ht]    = set()

            # Update rich stats
            if ht not in self._channels:
                continue

            if channel and channel != "general":
                self._channels[ht][channel] += 1

            for other in ht_lower:
                if other != ht:
                    if len(self._cohashtags[ht]) < _MAX_COHT_TRACK or other in self._cohashtags[ht]:
                        self._cohashtags[ht][other] += 1

            if domain:
                if len(self._domains[ht]) < _MAX_DOM_TRACK or domain in self._domains[ht]:
                    self._domains[ht][domain] += 1

            if len(self._samples[ht]) < self._max_samples:
                self._samples[ht].append(body[:150])

            if len(self._authors[ht]) < self._max_authors:
                self._authors[ht].add(did)

    def snapshot(self) -> dict:
        """Return serialisable snapshot of all collected stats."""
        hashtags: dict = {}
        for ht, total in self._total.items():
            entry: dict = {
                "total":     total,
                "matched":   self._matched.get(ht, 0),
                "unmatched": self._unmatched.get(ht, 0),
            }
            if ht in self._channels:
                entry["channels"]              = dict(self._channels[ht].most_common(10))
                entry["cohashtags"]            = dict(self._cohashtags[ht].most_common(20))
                entry["domains"]               = dict(self._domains[ht].most_common(20))
                entry["samples"]               = list(self._samples[ht])
                entry["unique_authors_capped"] = len(self._authors[ht])
            hashtags[ht] = entry

        return {
            "generated_at":    time.time(),
            "unique_hashtags":  len(self._total),
            "overflow_count":   self._overflow,
            "hashtags":         hashtags,
        }

    def write_snapshot(self, path: Path) -> None:
        """Atomically write snapshot JSON (tmp → rename)."""
        tmp = path.with_suffix(".json.tmp")
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.snapshot(), f, ensure_ascii=False)
            tmp.replace(path)
        except OSError as exc:
            print(f"[hashtag_stats] write failed: {exc}", file=sys.stderr, flush=True)
