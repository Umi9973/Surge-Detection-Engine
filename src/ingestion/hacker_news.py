from __future__ import annotations

import asyncio
import html as html_mod
import queue
import threading
from html.parser import HTMLParser
from typing import Dict, Iterator, List, Optional

import aiohttp

from .ingestion import DataIngestor

# ---------------------------------------------------------------------------
# Topic router
# ---------------------------------------------------------------------------

TOPIC_CHANNELS: Dict[str, List[str]] = {
    "ai": [
        "ai", "llm", "gpt", "openai", "anthropic", "claude", "gemini",
        "machine learning", "deep learning", "neural", "chatgpt", "mistral", "llama",
    ],
    "security": [
        "security", "hack", "breach", "vulnerability", "malware", "ransomware",
        "exploit", "zero-day", "cve", "phishing", "cybersecurity",
    ],
    "startup": [
        "startup", "vc", "funding", "series a", "series b", "acquired",
        "acquisition", "ipo", "valuation", "seed", "venture",
    ],
    "crypto": [
        "crypto", "bitcoin", "ethereum", "blockchain", "web3", "nft",
        "defi", "solana", "binance", "coinbase",
    ],
    "science": [
        "research", "study", "paper", "journal", "discovery",
        "physics", "biology", "chemistry", "climate",
    ],
    "tech": [
        "google", "apple", "microsoft", "amazon", "meta",
        "software", "hardware", "chip", "gpu", "nvidia", "amd",
    ],
    "policy": [
        "regulation", "law", "court", "congress", "senate", "election",
        "government", "policy", "ban", "gdpr", "antitrust",
    ],
}

# Channel priority order for tie-breaking (most specific first)
_PRIORITY: List[str] = ["ai", "security", "crypto", "startup", "policy", "science", "tech"]


class HNTopicRouter:
    """Classify an HN story title into a virtual channel by keyword density."""

    def __init__(self) -> None:
        self._channels = list(TOPIC_CHANNELS.items())

    def classify(self, title: str) -> Optional[str]:
        """Return channel with the most keyword hits, or None if no match."""
        lower = title.lower()
        scores: Dict[str, int] = {}
        for channel, keywords in self._channels:
            hits = sum(1 for kw in keywords if kw in lower)
            if hits:
                scores[channel] = hits

        if not scores:
            return None

        best_score = max(scores.values())
        # Among channels tied at best_score, pick by priority order
        for channel in _PRIORITY:
            if scores.get(channel) == best_score:
                return channel
        # Fallback: return any channel at best_score (shouldn't be reached)
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
        import re
        return re.sub(r"\s+", " ", " ".join(self._parts)).strip()


# ---------------------------------------------------------------------------
# Ingestor
# ---------------------------------------------------------------------------

class HackerNewsIngestor(DataIngestor):
    """
    Streams new HN items in real-time using the Firebase REST API.

    Architecture: sync `stream()` drains a `queue.Queue` fed by a background
    thread that runs a single persistent `aiohttp.ClientSession` for the
    lifetime of the process. This avoids the asyncio.run()-per-cycle trap
    (which destroys and re-creates the event loop and TCP connection pool on
    every poll cycle).

    Comment tree inheritance: `_item_topics` is keyed by *any* item ID (story
    or comment). When a story is classified, its ID → channel is cached. When a
    comment arrives, its channel is looked up via `parent` (the immediate
    parent's ID), and then the comment's own ID is also stored so its replies
    can inherit. Items whose parent is not in cache are silently skipped.
    """

    BASE_URL   = "https://hacker-news.firebaseio.com/v0"
    ITEM_TYPES = frozenset({"story", "comment"})

    _EVICT_AT   = 50_000
    _EVICT_DROP = 10_000

    def __init__(self, poll_interval: float = 5.0, concurrency: int = 20) -> None:
        self._poll_interval = poll_interval
        self._concurrency   = concurrency
        self._last_id: int  = 0
        self._item_topics: Dict[int, str] = {}
        self._router = HNTopicRouter()

    # ------------------------------------------------------------------
    # Public sync interface
    # ------------------------------------------------------------------

    def stream(self) -> Iterator[Dict]:
        """Sync generator. Blocks until items are available from the live feed."""
        q: queue.Queue = queue.Queue(maxsize=1000)
        t = threading.Thread(target=self._run_background, args=(q,), daemon=True)
        t.start()
        while True:
            yield q.get()

    # ------------------------------------------------------------------
    # Background thread entry point
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Async helpers
    # ------------------------------------------------------------------

    async def _bootstrap(self, session: aiohttp.ClientSession) -> None:
        """Seed _last_id to current maxitem so we skip all historical items."""
        url = f"{self.BASE_URL}/maxitem.json"
        async with session.get(url) as resp:
            self._last_id = int(await resp.json())

    async def _fetch_new(self, session: aiohttp.ClientSession) -> List[Dict]:
        """Fetch all item IDs since _last_id concurrently."""
        url = f"{self.BASE_URL}/maxitem.json"
        async with session.get(url) as resp:
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
            url = f"{self.BASE_URL}/item/{item_id}.json"
            try:
                async with session.get(url) as resp:
                    return await resp.json()
            except Exception:
                return None

    # ------------------------------------------------------------------
    # Classification + normalization
    # ------------------------------------------------------------------

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
                title = item.get("title", "")
                channel = self._router.classify(title)
                if channel is None:
                    continue
                self._item_topics[item_id] = channel

            else:  # comment
                parent_id = item.get("parent")
                if parent_id is None:
                    continue
                channel = self._item_topics.get(parent_id)
                if channel is None:
                    continue
                self._item_topics[item_id] = channel

            # Evict oldest entries to cap memory
            if len(self._item_topics) > self._EVICT_AT:
                keys_to_drop = list(self._item_topics.keys())[: self._EVICT_DROP]
                for k in keys_to_drop:
                    del self._item_topics[k]

            body = self._html_to_text(item.get("text") or item.get("title") or "")
            if not body:
                continue

            yield self._to_dict(item, channel, body)

    @staticmethod
    def _html_to_text(raw: str) -> str:
        """Strip HTML tags and decode entities using stdlib html.parser."""
        stripper = _HTMLStripper()
        stripper.feed(raw)
        text = stripper.get_text()
        return html_mod.unescape(text)

    @staticmethod
    def _to_dict(item: Dict, channel: str, body: str) -> Dict:
        return {
            "id":        str(item["id"]),
            "subreddit": channel,
            "body":      body,
            "timestamp": item.get("time", 0),
            "author":    item.get("by", ""),
            "score":     item.get("score", 0),
        }
