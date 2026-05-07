from __future__ import annotations

from dataclasses import dataclass, field
from typing import List


@dataclass
class Comment:
    id:        str
    subreddit: str
    body:      str
    timestamp: int
    author:    str = ""
    score:     int = 0


@dataclass
class AnomalyEvent:
    subreddit:    str
    window_start: int
    window_end:   int
    count:        int
    z_score:      float
    texts:        List[str] = field(default_factory=list)
    mean:         float = 0.0
    std:          float = 0.0
