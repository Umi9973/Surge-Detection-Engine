from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from .models import EventCandidate

# Canonical short source name used internally by the consolidator.
_SOURCE_ALIASES = {
    "hacker_news": "hn",
}


@dataclass
class EventEvidence:
    candidate_id:         str
    source:               str        # canonical short name: "hn", "reddit", etc.
    channel:              str
    window_start:         int
    window_end:           int
    candidate_kind:       str
    event_score:          float
    keywords:             List[str]
    conversation_ids:     List[int]
    top_conversation_id:  int
    top_conversation_title: str
    domains:              List[str]
    communities:          List[str]
    item_count:           int
    z_score:              float

    @classmethod
    def from_candidate(cls, c: EventCandidate) -> "EventEvidence":
        return cls(
            candidate_id          = c.candidate_id,
            source                = _SOURCE_ALIASES.get(c.source, c.source),
            channel               = c.channel,
            window_start          = c.window_start,
            window_end            = c.window_end,
            candidate_kind        = c.kind,
            event_score           = c.event_score,
            keywords              = list(c.keywords),
            conversation_ids      = list(c.conversation_ids),
            top_conversation_id   = c.top_story_id,
            top_conversation_title = c.top_story_title,
            domains               = list(c.top_domains),
            communities           = [],   # HN has no sub-communities in v1
            item_count            = c.unique_conversation_count,
            z_score               = c.z_score,
        )
