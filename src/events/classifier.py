from __future__ import annotations

from typing import List, Tuple

# Structural fields in HN hiring thread responses — not topic signal.
# If ≥ 3 of the top-4 cluster keywords are template markers, the cluster
# is a hiring template cluster, not an event.
_TEMPLATE_MARKERS = frozenset({
    "remote", "location", "yes", "full", "engineer",
    "python", "willing", "relocate", "senior", "stack",
    "hybrid", "onsite", "visa", "salary",
})


def classify(
    cluster: dict,
    z_score: float,
    channel: str = "",
) -> Tuple[str, float]:
    """Classify one enriched cluster and return (kind, event_score).

    Returns ("unknown", 0.0) when the cluster has no attributed items —
    caller should drop these rather than emit a candidate.

    kind values: "unknown" | "viral_post" | "topic_surge" | "event_candidate"
    event_score: additive 0.0–1.0, computed for all non-unknown kinds.
    """
    unique     = cluster.get("unique_story_count", 0)
    top_pct    = cluster.get("top_story_pct", 0.0)
    size       = cluster.get("size", 0)
    top_domains = cluster.get("top_domains") or []
    keywords   = cluster.get("keywords") or []

    if unique == 0:
        return ("unknown", 0.0)

    score = _score(unique, top_pct, top_domains, size, z_score, keywords, channel)

    # Template cluster: ≥ 3 of top-4 keywords are hiring-thread structural fields
    top4 = (keywords or [])[:4]
    if sum(1 for kw in top4 if kw in _TEMPLATE_MARKERS) >= 3:
        return ("topic_surge", round(score, 4))

    if top_pct >= 0.80 or unique <= 1:
        kind = "viral_post"
    elif len(keywords) < 2:
        kind = "topic_surge"
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
    keywords: List[str],
    channel: str = "",
) -> float:
    keyword_quality = min(len(keywords) / 5, 1.0)
    unique_weight   = min(unique / 5, 1.0)
    if channel == "general":
        # general channel: conversation diversity only counts when keywords are coherent.
        # High unique + weak keywords = noise, not an event.
        unique_weight *= keyword_quality
    return (
        0.25 * unique_weight                   +
        0.20 * (1.0 - top_pct)                 +
        0.20 * min(len(top_domains) / 3, 1.0)  +
        0.15 * min(size / 100,           1.0)  +
        0.10 * min(z_score / 10.0,       1.0)  +
        0.10 * keyword_quality
    )
