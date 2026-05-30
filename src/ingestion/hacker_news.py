from __future__ import annotations

import asyncio
import csv
import html as html_mod
import queue
import re
import threading
from html.parser import HTMLParser
from typing import Dict, Iterator, List, Optional, Tuple
from urllib.parse import urlparse

import aiohttp

from .base import DataIngestor

# ---------------------------------------------------------------------------
# Topic router
# ---------------------------------------------------------------------------

# Each entry is (keyword, weight). Tiers:
#   3 — exact brand/model names (unambiguous, high specificity)
#   2 — domain jargon / multi-word phrases (strong signal, low FP risk)
#   1 — generic single words (weak signal, never win a tie alone)
TOPIC_CHANNELS: Dict[str, List[Tuple[str, float]]] = {
    "ai": [
        ("gpt", 3), ("llm", 3), ("openai", 3), ("chatgpt", 3), ("anthropic", 3),
        ("mistral", 3), ("llama", 3), ("gemini", 3), ("claude", 3),
        ("machine learning", 2), ("deep learning", 2), ("neural network", 2),
        ("neural", 1), ("ai", 1),
    ],
    "security": [
        ("ransomware", 3), ("malware", 3), ("cve", 3), ("zero-day", 3),
        ("cybersecurity", 2), ("vulnerability", 2), ("phishing", 2), ("exploit", 2),
        ("breach", 1), ("hack", 1), ("security", 1),
    ],
    "startup": [
        ("series a", 3), ("series b", 3), ("series c", 3), ("ipo", 3),
        ("acquisition", 2), ("acquired", 2), ("venture capital", 2), ("valuation", 2),
        ("funding", 1), ("startup", 1), ("seed", 1),
    ],
    "crypto": [
        ("bitcoin", 3), ("ethereum", 3), ("solana", 3), ("binance", 3), ("coinbase", 3),
        ("blockchain", 2), ("defi", 2), ("web3", 2), ("nft", 2),
        ("crypto", 1),
    ],
    "science": [
        ("arxiv", 3), ("crispr", 3), ("genomics", 3),
        ("nasa", 3), ("blue origin", 3), ("new glenn", 3), ("starship", 3),
        ("rocket", 2), ("orbital", 2), ("aerospace", 2), ("booster", 2),
        ("spacex", 1),
        ("physics", 2), ("biology", 2), ("chemistry", 2), ("climate", 2), ("journal", 2),
        ("discovery", 1), ("research", 1), ("study", 1), ("paper", 1),
        ("space", 1), ("launch", 1),
    ],
    "tech": [
        ("nvidia", 3), ("amd", 3), ("apple", 3), ("google", 3), ("microsoft", 3),
        ("amazon", 3), ("meta", 3),
        ("home assistant", 2), ("sqlite", 2), ("postgres", 2), ("oauth", 2),
        ("gpu", 2), ("chip", 2), ("hardware", 2),
        ("database", 1), ("software", 1),
    ],
    "policy": [
        ("gdpr", 3), ("antitrust", 3), ("sec", 3), ("congress", 3), ("senate", 3),
        ("regulation", 2), ("court", 2), ("government", 2), ("election", 2),
        ("law", 1), ("policy", 1), ("ban", 1),
    ],
}

# Domain → (channel, boost_weight). Exact netloc match after stripping www.
DOMAIN_BOOSTS: Dict[str, Tuple[str, float]] = {
    "github.com":          ("tech",     2),
    "arxiv.org":           ("science",  3),
    "techcrunch.com":      ("startup",  2),
    "sec.gov":             ("policy",   3),
    "reuters.com":         ("policy",   1),
    "bloomberg.com":       ("startup",  1),
    "wired.com":           ("tech",     1),
    "arstechnica.com":     ("tech",     1),
    "theregister.com":     ("security", 2),
    "krebsonsecurity.com": ("security", 3),
    "coindesk.com":        ("crypto",   3),
    "nature.com":          ("science",  3),
    "science.org":         ("science",  3),
    "nasa.gov":            ("science",  3),
    "space.com":           ("science",  2),
    "spacenews.com":       ("science",  2),
}

# Channel priority order for tie-breaking. "general" is last — it's the catch-all,
# never a winner when any topical channel has scored.
_PRIORITY: List[str] = [
    "ai", "security", "crypto", "startup", "policy", "science", "tech", "general",
]

# Keywords where we want left-boundary only so inflected forms match:
# hack → hacker/hacked/hacking, breach → breached/breaching, exploit → exploited/exploiting
_PREFIX_ROOTS: frozenset = frozenset({"hack", "breach", "exploit"})


def _build_pattern(kw: str) -> re.Pattern:
    escaped = re.escape(kw)
    if kw in _PREFIX_ROOTS:
        return re.compile(r"\b" + escaped, re.IGNORECASE)
    return re.compile(r"\b" + escaped + r"\b", re.IGNORECASE)


def _extract_domain(url: str) -> str:
    host = urlparse(url).netloc.lower()
    return host[4:] if host.startswith("www.") else host


class HNTopicRouter:
    """
    Classify an HN story title (+ optional URL) into a virtual channel.

    Scoring: sum of weights of matched keywords (regex word-boundary safe).
    Domain boosts are added on top. Returns 'general' when score is zero.
    """

    def __init__(self) -> None:
        self._patterns: Dict[str, List[Tuple[re.Pattern, float]]] = {
            channel: [(_build_pattern(kw), weight) for kw, weight in entries]
            for channel, entries in TOPIC_CHANNELS.items()
        }

    def classify(self, title: str, url: Optional[str] = None) -> str:
        """Return channel with highest weighted score, or 'general' if zero."""
        scores: Dict[str, float] = {}

        for channel, pat_weights in self._patterns.items():
            total = sum(w for pat, w in pat_weights if pat.search(title))
            if total > 0:
                scores[channel] = total

        if url:
            domain = _extract_domain(url)
            if domain in DOMAIN_BOOSTS:
                ch, boost = DOMAIN_BOOSTS[domain]
                scores[ch] = scores.get(ch, 0) + boost

        if not scores:
            return "general"

        best_score = max(scores.values())
        for channel in _PRIORITY:
            if scores.get(channel, 0) == best_score:
                return channel
        return max(scores, key=scores.__getitem__)


# ---------------------------------------------------------------------------
# HTML stripper (stdlib only)
# ---------------------------------------------------------------------------

class _HTMLStripper(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._parts: List[str] = []

    def handle_data(self, data: str) -> None:
        self._parts.append(data)

    def get_text(self) -> str:
        return re.sub(r"\s+", " ", " ".join(self._parts)).strip()


# ---------------------------------------------------------------------------
# Shared processing mixin
# ---------------------------------------------------------------------------

class _HNItemProcessor:
    """
    Shared classification and normalization logic for all HN ingestors.

    Expects items in Firebase-compatible schema:
        id       int         item ID
        type     str         "story" | "comment"
        title    str         story title (empty for comments)
        text     str         comment body HTML (empty for stories)
        url      str         story URL (empty for comments)
        time     int         Unix timestamp
        by       str         author username
        score    int         upvotes (0 for comments)
        parent   int|None    immediate parent ID (None for top-level stories)
        deleted  bool        moderation flag
        dead     bool        moderation flag

    Both HackerNewsIngestor (live Firebase) and HNCsvIngestor (CSV backtest)
    normalize their source data to this schema before calling _process().
    """

    ITEM_TYPES  = frozenset({"story", "comment"})
    _EVICT_AT   = 50_000
    _EVICT_DROP = 10_000

    def __init__(self) -> None:
        self._item_topics: Dict[int, str] = {}
        self._item_story:  Dict[int, int] = {}   # item_id → root story_id
        self._story_meta:  Dict[int, Dict] = {}  # story_id → {title, domain}
        self._router = HNTopicRouter()

    def _process(self, items: List[Optional[Dict]]) -> Iterator[Dict]:
        for item in items:
            if not item or item.get("type") not in self.ITEM_TYPES:
                continue
            if item.get("deleted") or item.get("dead"):
                continue

            item_id = item.get("id")
            if item_id is None:
                continue

            if item["type"] == "story":
                title   = item.get("title", "")
                url     = item.get("url", "")
                channel = self._router.classify(title, url=url)
                self._item_topics[item_id] = channel
                self._item_story[item_id]  = item_id
                self._story_meta[item_id]  = {
                    "title":  title,
                    "domain": _extract_domain(url),
                }

            else:  # comment
                parent_id = item.get("parent")
                if parent_id is None:
                    continue
                channel = self._item_topics.get(parent_id)
                if channel is None:
                    continue
                self._item_topics[item_id] = channel
                # If parent was evicted from _item_story, use 0 — parent_id
                # may be a comment ID, not a story ID, so it must not be used
                # as a fallback story lookup key.
                self._item_story[item_id] = self._item_story.get(parent_id, 0)

            if len(self._item_topics) > self._EVICT_AT:
                keys_to_drop = list(self._item_topics.keys())[: self._EVICT_DROP]
                for k in keys_to_drop:
                    del self._item_topics[k]
                    self._item_story.pop(k, None)

            # Only evict story metadata not referenced by any live item.
            if len(self._story_meta) > 10_000:
                referenced = set(self._item_story.values()) - {0}
                to_drop = [sid for sid in list(self._story_meta)[:2_000] if sid not in referenced]
                for k in to_drop:
                    del self._story_meta[k]

            body = self._html_to_text(item.get("text") or item.get("title") or "")
            if not body:
                continue

            yield self._to_dict(item, channel, body)

    @staticmethod
    def _html_to_text(raw: str) -> str:
        stripper = _HTMLStripper()
        stripper.feed(raw)
        return html_mod.unescape(stripper.get_text())

    def _to_dict(self, item: Dict, channel: str, body: str) -> Dict:
        story_id = self._item_story.get(item["id"], 0)
        meta     = self._story_meta.get(story_id, {})
        return {
            "id":          str(item["id"]),
            "subreddit":   channel,
            "body":        body,
            "timestamp":   item.get("time", 0),
            "author":      item.get("by", ""),
            "score":       item.get("score", 0),
            "story_id":    story_id,
            "story_title": meta.get("title", ""),
            "domain":      meta.get("domain", ""),
            "item_type":   item.get("type", ""),
        }


# ---------------------------------------------------------------------------
# Live ingestor (Firebase REST API)
# ---------------------------------------------------------------------------

class HackerNewsIngestor(DataIngestor, _HNItemProcessor):
    """
    Streams new HN items in real-time using the Firebase REST API.

    Architecture: sync stream() drains a queue.Queue fed by a background
    thread that runs a single persistent aiohttp.ClientSession for the
    lifetime of the process. This avoids the asyncio.run()-per-cycle trap
    (which destroys and re-creates the event loop and TCP connection pool on
    every poll cycle).
    """

    BASE_URL = "https://hacker-news.firebaseio.com/v0"

    def __init__(self, poll_interval: float = 5.0, concurrency: int = 20) -> None:
        _HNItemProcessor.__init__(self)
        self._poll_interval = poll_interval
        self._concurrency   = concurrency
        self._last_id: int  = 0

    def stream(self) -> Iterator[Dict]:
        q: queue.Queue = queue.Queue(maxsize=1000)
        t = threading.Thread(target=self._run_background, args=(q,), daemon=True)
        t.start()
        while True:
            yield q.get()

    def _run_background(self, q: queue.Queue) -> None:
        asyncio.run(self._async_main(q))

    async def _async_main(self, q: queue.Queue) -> None:
        async with aiohttp.ClientSession() as session:
            await self._bootstrap(session)
            while True:
                items = await self._fetch_new(session)
                for d in self._process(items):
                    q.put(d)
                await asyncio.sleep(self._poll_interval)

    async def _bootstrap(self, session: aiohttp.ClientSession) -> None:
        async with session.get(f"{self.BASE_URL}/maxitem.json") as resp:
            self._last_id = int(await resp.json())

    async def _fetch_new(self, session: aiohttp.ClientSession) -> List[Dict]:
        async with session.get(f"{self.BASE_URL}/maxitem.json") as resp:
            max_id = int(await resp.json())

        if max_id <= self._last_id:
            return []

        new_ids = list(range(self._last_id + 1, max_id + 1))
        self._last_id = max_id

        sem = asyncio.Semaphore(self._concurrency)
        tasks = [self._fetch_item(session, sem, item_id) for item_id in new_ids]
        results = await asyncio.gather(*tasks)
        return [r for r in results if r is not None]

    async def _fetch_item(
        self, session: aiohttp.ClientSession, sem: asyncio.Semaphore, item_id: int
    ) -> Optional[Dict]:
        async with sem:
            try:
                async with session.get(f"{self.BASE_URL}/item/{item_id}.json") as resp:
                    return await resp.json()
            except Exception:
                return None


# ---------------------------------------------------------------------------
# CSV backtest ingestor
# ---------------------------------------------------------------------------

class HNCsvIngestor(DataIngestor, _HNItemProcessor):
    """
    Replays a HN CSV dump through the same pipeline as the live ingestor.

    Expected CSV columns: id, type, author, timestamp, body, title, parent

    Rows are sorted by timestamp before processing so the _item_topics cache
    sees stories before their comments, matching real-time arrival order.
    CSV field names are normalized to the Firebase-compatible schema that
    _HNItemProcessor._process() expects before being passed through.
    """

    def __init__(self, file_path: str) -> None:
        _HNItemProcessor.__init__(self)
        self._file_path = file_path

    def stream(self) -> Iterator[Dict]:
        with open(self._file_path, newline="", encoding="utf-8") as f:
            rows = sorted(csv.DictReader(f), key=lambda r: int(r["timestamp"]))

        for row in rows:
            normalized = self._normalize(row)
            yield from self._process([normalized])

    @staticmethod
    def _normalize(row: Dict) -> Dict:
        """Map CSV column names to the Firebase-compatible schema."""
        parent_raw = row.get("parent", "")
        return {
            "id":      int(row["id"]),
            "type":    row["type"],
            "by":      row.get("author", ""),
            "time":    int(row["timestamp"]),
            "text":    row.get("body", ""),
            "title":   row.get("title", ""),
            "url":     "",
            "score":   0,
            "parent":  int(parent_raw) if parent_raw else None,
            "deleted": False,
            "dead":    False,
        }
