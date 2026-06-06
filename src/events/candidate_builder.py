from __future__ import annotations

from typing import List, Optional

from .classifier import classify
from .models import EventCandidate

# Lowercase substrings that identify recurring HN structural threads.
# These are not spontaneous events — force to topic_surge so they never
# promote to event_candidate regardless of unique_story_count.
_RECURRING_THREAD_MARKERS = frozenset({
    "who is hiring",
    "who wants to be hired",
    "ask hn: who",
})


class CandidateBuilder:
    def __init__(
        self,
        source: str = "hacker_news",
        dbscan_eps: float = 0.5,
        dbscan_min_samples: int = 5,
    ) -> None:
        self._source             = source
        self._dbscan_eps         = dbscan_eps
        self._dbscan_min_samples = dbscan_min_samples

    def from_enriched_row(
        self,
        row: dict,
        dbscan_eps: Optional[float] = None,
    ) -> List[EventCandidate]:
        """Convert one enriched Parquet row into a list of EventCandidates.

        Clusters classified as "unknown" (no attributed items) are silently dropped.
        Pass dbscan_eps to override the builder default — used when channel-specific
        eps was applied during enrichment so the actual value is stamped on candidates.
        """
        channel      = row.get("channel", "")
        window_end   = row.get("window_end", 0)
        z_score      = float(row.get("z_score", 0.0))
        window_count = int(row.get("count", 0))
        effective_eps = dbscan_eps if dbscan_eps is not None else self._dbscan_eps

        candidates: List[EventCandidate] = []
        for cluster in (row.get("clusters") or []):
            candidate = self._build(cluster, channel, window_end, z_score, window_count, effective_eps)
            if candidate is not None:
                candidates.append(candidate)
        return candidates

    def _build(
        self,
        cluster:      dict,
        channel:      str,
        window_end:   int,
        z_score:      float,
        window_count: int,
        dbscan_eps:   Optional[float] = None,
    ) -> Optional[EventCandidate]:
        title = (cluster.get("top_story_title") or "").lower()
        if any(m in title for m in _RECURRING_THREAD_MARKERS):
            kind, event_score = "topic_surge", 0.0
        else:
            kind, event_score = classify(cluster, z_score, channel=channel)
        if kind == "unknown":
            return None

        cluster_id = cluster.get("cluster_id", 0)
        return EventCandidate(
            candidate_id              = f"{self._source}:{channel}:{window_end}:{cluster_id}",
            source                    = self._source,
            channel                   = channel,
            window_end                = window_end,
            cluster_id                = cluster_id,
            kind                      = kind,
            event_score               = event_score,
            size                      = cluster.get("size", 0),
            unique_conversation_count = cluster.get("unique_story_count", 0),
            top_conversation_pct      = cluster.get("top_story_pct", 0.0),
            top_story_id              = cluster.get("top_story_id", 0),
            top_story_title           = cluster.get("top_story_title", ""),
            top_domains               = cluster.get("top_domains") or [],
            keywords                  = cluster.get("keywords") or [],
            z_score                   = z_score,
            window_count              = window_count,
            dbscan_eps                = dbscan_eps if dbscan_eps is not None else self._dbscan_eps,
            dbscan_min_samples        = self._dbscan_min_samples,
        )
