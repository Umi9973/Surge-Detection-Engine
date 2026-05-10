from __future__ import annotations

import random
from abc import ABC, abstractmethod
from typing import List, Optional
from uuid import uuid4

import redis

from ..models import Comment

_WINDOW_TTL   = 7200   # 2-hour sliding window (seconds)
_HISTORY_SIZE = 168    # 7 days × 24 hours — stable against multi-hour surges
_TEXT_CAP     = 500    # max comment texts passed to NLP


class StateManager(ABC):
    """Abstract contract between the math engine and the storage backend.

    The tripwire calls only these methods — it has no knowledge of Redis,
    SQLite, or any other storage technology.
    """

    @abstractmethod
    def ingest(self, comment: Comment) -> None:
        """Store an incoming comment in the sliding window."""

    @abstractmethod
    def get_window_count(self, subreddit: str, now: int) -> int:
        """Return the number of comments in the 2-hour window ending at `now`."""

    @abstractmethod
    def get_window_texts(self, subreddit: str, now: int) -> List[str]:
        """Return a random sample of comment bodies from the current window."""

    @abstractmethod
    def get_history(self, subreddit: str) -> List[float]:
        """Return the list of past hourly counts (up to HISTORY_SIZE)."""

    @abstractmethod
    def push_history(self, subreddit: str, count: int) -> None:
        """Append the current hourly count to the rolling history."""

    @abstractmethod
    def flush(self) -> None:
        """Delete all keys — used to reset state between load-test runs."""


class RedisStateManager(StateManager):
    """Redis ZSET implementation of the 2-hour sliding window.

    Redis key schema:
        window:{sub}   ZSET    score=Unix timestamp, member="{uuid}:{text[:120]}"
        history:{sub}  LIST    last _HISTORY_SIZE hourly counts (floats)
        alert:{sub}    STRING  last alert Unix timestamp; TTL = _ALERT_COOLDOWN

    To swap to production Redis, pass a real redis.Redis instance instead of
    fakeredis.FakeRedis — all ZSET/LIST/STRING calls are identical.
    """

    def __init__(
        self,
        r: redis.Redis,
        window_ttl:   int = _WINDOW_TTL,
        history_size: int = _HISTORY_SIZE,
        text_cap:     int = _TEXT_CAP,
    ) -> None:
        self.r = r
        self._window_ttl   = window_ttl
        self._history_size = history_size
        self._text_cap     = text_cap

    # --- Ingestion ---

    def ingest(self, comment: Comment) -> None:
        key    = f"window:{comment.subreddit}"
        member = f"{uuid4().hex}:{comment.body[:120]}"
        self.r.zadd(key, {member: comment.timestamp})

    # --- Window ---

    def _prune(self, subreddit: str, now: int) -> None:
        self.r.zremrangebyscore(
            f"window:{subreddit}", "-inf", now - self._window_ttl
        )

    def get_window_count(self, subreddit: str, now: int) -> int:
        self._prune(subreddit, now)
        return self.r.zcount(f"window:{subreddit}", now - self._window_ttl, now)

    def get_window_texts(self, subreddit: str, now: int) -> List[str]:
        self._prune(subreddit, now)
        members = self.r.zrangebyscore(
            f"window:{subreddit}", now - self._window_ttl, now
        )
        sample = random.sample(members, min(len(members), self._text_cap))
        return [m.split(":", 1)[1] for m in sample]

    # --- History ---

    def get_history(self, subreddit: str) -> List[float]:
        raw = self.r.lrange(f"history:{subreddit}", 0, -1)
        return [float(x) for x in raw]

    def push_history(self, subreddit: str, count: int) -> None:
        key = f"history:{subreddit}"
        self.r.rpush(key, count)
        self.r.ltrim(key, -self._history_size, -1)

    # --- Utility ---

    def flush(self) -> None:
        self.r.flushdb()
