from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class EventCandidate:
    candidate_id:              str
    source:                    str
    channel:                   str
    window_start:              int
    window_end:                int
    cluster_id:                int
    kind:                      str   # "viral_post" | "topic_surge" | "event_candidate"
    event_score:               float
    size:                      int
    unique_conversation_count: int
    top_conversation_pct:      float
    top_story_id:              int
    top_story_title:           str
    conversation_ids:          List[int] = field(default_factory=list)
    top_domains:               List[str] = field(default_factory=list)
    keywords:                  List[str] = field(default_factory=list)
    z_score:                   float = 0.0
    window_count:              int   = 0
    dbscan_eps:                float = 0.5
    dbscan_min_samples:        int   = 5


@dataclass
class TrackedEvent:
    event_id:             str
    sources:              List[str]
    source_counts:        Dict[str, int]
    channels:             List[str]
    primary_channel:      str
    first_seen:           int              # unix timestamp (window_start of first candidate)
    last_seen:            int              # unix timestamp (window_end of latest candidate)
    duration_minutes:     float
    candidate_ids:        List[str]
    candidate_count:      int
    representative_title: str
    top_titles:           List[str]
    keywords:             List[str]
    conversation_ids:     List[int]
    top_conversation_id:  int
    domains:              List[str]
    communities:          List[str]
    total_item_count:     int
    peak_score:           float
    avg_score:            float
    peak_z_score:         float
    event_kind:           str = "event_candidate"
    duration_kind:        str = "unknown"  # isolated | flash | developing | sustained
    status:               str = "active"   # active | closed
    merge_trace:          List[dict] = field(default_factory=list)
    # Small, stable set of story IDs used as matching identity.
    # Populated from seed + merges with meaningful topical evidence.
    # conversation_ids is full provenance (audit only), not a strong match anchor.
    anchor_conversation_ids: List[int] = field(default_factory=list)
