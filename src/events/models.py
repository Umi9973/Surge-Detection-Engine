from __future__ import annotations

from dataclasses import dataclass, field
from typing import List


@dataclass
class EventCandidate:
    candidate_id:              str
    source:                    str
    channel:                   str
    window_end:                int
    cluster_id:                int
    kind:                      str   # "viral_post" | "topic_surge" | "event_candidate"
    event_score:               float
    size:                      int
    unique_conversation_count: int
    top_conversation_pct:      float
    top_story_id:              int
    top_story_title:           str
    top_domains:               List[str] = field(default_factory=list)
    keywords:                  List[str] = field(default_factory=list)
    z_score:                   float = 0.0
    window_count:              int   = 0
