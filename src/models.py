from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class Comment:
    id:          str
    subreddit:   str
    body:        str
    timestamp:   int
    author:      str = ""
    score:       int = 0
    story_id:    int = 0
    story_title: str = ""
    domain:      str = ""
    item_type:   str = ""
    item_id:     int = 0
    created_at:  int = 0


@dataclass
class AnomalyEvent:
    subreddit:    str
    window_start: int
    window_end:   int
    count:        int
    z_score:      float
    items:        List[Dict] = field(default_factory=list)  # {item_id, text, story_id, story_title, domain, item_type, created_at}
    mean:         float = 0.0
    std:          float = 0.0
    event_type:   str   = "start"   # "start" | "update" | "release"
    event_start:  int   = 0         # unix ts when this elevation period began
