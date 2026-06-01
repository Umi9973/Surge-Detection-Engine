from __future__ import annotations

from typing import Tuple


def classify(
    cluster: dict,
    z_score: float,
) -> Tuple[str, float]:
    """Classify one enriched cluster and return (kind, event_score).

    Returns ("unknown", 0.0) when the cluster has no attributed items —
    caller should drop these rather than emit a candidate.

    kind values: "unknown" | "viral_post" | "topic_surge" | "event_candidate"
    event_score: additive 0.0–1.0, computed for all non-unknown kinds.
    """
    unique = cluster.get("unique_story_count", 0)
    top_pct = cluster.get("top_story_pct", 0.0)
    size = cluster.get("size", 0)
    top_domains = cluster.get("top_domains") or []

    # Guard: no attributed items — cannot classify meaningfully
    if unique == 0:
        return ("unknown", 0.0)

    score = _score(unique, top_pct, top_domains, size, z_score)

    if top_pct >= 0.80 or unique <= 1:
        kind = "viral_post"
    elif unique >= 3 and len(top_domains) >= 2 and size >= 10:
        kind = "event_candidate"
    else:
        kind = "topic_surge"

    return (kind, round(score, 4))


def _score(
    unique: int,
    top_pct: float,
    top_domains: list,
    size: int,
    z_score: float,
) -> float:
    return (
        0.30 * min(unique / 5,          1.0) +
        0.20 * (1.0 - top_pct)              +
        0.20 * min(len(top_domains) / 3, 1.0) +
        0.15 * min(size / 100,          1.0) +
        0.15 * min(z_score / 10.0,      1.0)
    )
