from __future__ import annotations

import asyncio
import json
import queue
import re
import sys
import threading
import zlib
from typing import Dict, Iterator, List, Optional, Tuple
from urllib.parse import urlparse

import websockets

from .base import DataIngestor

# ---------------------------------------------------------------------------
# Jetstream endpoint
# ---------------------------------------------------------------------------

_JETSTREAM_URL = (
    "wss://jetstream2.us-east.bsky.network/subscribe"
    "?wantedCollections=app.bsky.feed.post"
)

# ---------------------------------------------------------------------------
# Topic router — Option B channels
# ---------------------------------------------------------------------------

# Each entry is (keyword, weight). Tiers:
#   3 — exact brand / entity names (unambiguous, high specificity)
#   2 — domain jargon / multi-word phrases (strong signal, low FP risk)
#   1 — generic single words (weak signal, never win a tie alone)
_TOPIC_CHANNELS: Dict[str, List[Tuple[str, float]]] = {
    "ai_tech": [
        ("gpt", 3), ("llm", 3), ("openai", 3), ("chatgpt", 3), ("anthropic", 3),
        ("claude", 3), ("gemini", 3), ("llama", 3), ("mistral", 3), ("nvidia", 3),
        ("amd", 3), ("apple", 3), ("google", 3), ("microsoft", 3), ("amazon", 3),
        ("intel", 3), ("qualcomm", 3),
        ("machine learning", 2), ("deep learning", 2), ("neural network", 2),
        ("artificial intelligence", 2), ("open source", 2), ("software engineer", 2),
        ("data science", 2), ("semiconductor", 2), ("large language model", 2),
        ("ai", 1), ("tech", 1), ("code", 1), ("coding", 1), ("python", 1),
        ("javascript", 1), ("rust", 1), ("linux", 1), ("software", 1), ("hardware", 1),
    ],
    "security_risk": [
        ("ransomware", 3), ("malware", 3), ("cve", 3), ("zero-day", 3),
        ("nsa", 3), ("cia", 3), ("fbi", 3),
        ("cybersecurity", 2), ("vulnerability", 2), ("data breach", 2),
        ("encryption", 2), ("surveillance", 2), ("phishing", 2), ("exploit", 2),
        ("identity theft", 2), ("two-factor", 2),
        ("hack", 1), ("security", 1), ("breach", 1), ("scam", 1), ("leak", 1), ("privacy", 1),
    ],
    "politics_government": [
        ("congress", 3), ("senate", 3), ("supreme court", 3), ("white house", 3),
        ("trump", 3), ("biden", 3), ("democrat", 3), ("republican", 3), ("gop", 3),
        ("doj", 3), ("dhs", 3), ("fec", 3),
        ("election", 2), ("legislation", 2), ("regulation", 2), ("campaign", 2),
        ("voting rights", 2), ("executive order", 2), ("filibuster", 2), ("impeach", 2),
        ("politics", 1), ("policy", 1), ("vote", 1), ("law", 1), ("government", 1), ("president", 1),
    ],
    "world_news": [
        ("ukraine", 3), ("russia", 3), ("israel", 3), ("gaza", 3), ("china", 3),
        ("taiwan", 3), ("nato", 3), ("united nations", 3), ("idf", 3), ("hamas", 3),
        ("iran", 3), ("north korea", 3),
        ("war", 2), ("conflict", 2), ("ceasefire", 2), ("sanctions", 2),
        ("diplomacy", 2), ("humanitarian", 2), ("refugee", 2),
        ("breaking", 1), ("global", 1), ("international", 1), ("world", 1),
    ],
    "science_health": [
        ("nasa", 3), ("crispr", 3), ("fda", 3), ("cdc", 3), ("nih", 3),
        ("spacex", 3), ("arxiv", 3),
        ("climate change", 2), ("mental health", 2), ("vaccine", 2), ("pandemic", 2),
        ("clinical trial", 2), ("space exploration", 2), ("astronomy", 2), ("genomics", 2),
        ("science", 1), ("health", 1), ("medicine", 1), ("study", 1),
        ("discovery", 1), ("space", 1), ("climate", 1), ("biology", 1),
    ],
    "economy_markets": [
        ("bitcoin", 3), ("ethereum", 3), ("federal reserve", 3), ("wall street", 3),
        ("nasdaq", 3), ("s&p 500", 3), ("imf", 3), ("sec", 3),
        ("stock market", 2), ("interest rate", 2), ("cryptocurrency", 2),
        ("inflation", 2), ("recession", 2), ("gdp", 2), ("tariff", 2),
        ("venture capital", 2), ("ipo", 2), ("defi", 2), ("hedge fund", 2),
        ("economy", 1), ("market", 1), ("crypto", 1), ("finance", 1),
        ("money", 1), ("investment", 1), ("stocks", 1),
    ],
    "platform_media": [
        ("bluesky", 3), ("twitter", 3), ("tiktok", 3), ("instagram", 3),
        ("youtube", 3), ("facebook", 3), ("threads", 3), ("mastodon", 3),
        ("substack", 3), ("reddit", 3), ("linkedin", 3),
        ("social media", 2), ("content moderation", 2), ("algorithm", 2),
        ("disinformation", 2), ("misinformation", 2), ("journalism", 2),
        ("newsletter", 2), ("podcast", 2),
        ("platform", 1), ("media", 1), ("viral", 1),
    ],
    "culture_creators": [
        ("netflix", 3), ("disney", 3), ("marvel", 3), ("spotify", 3),
        ("twitch", 3), ("steam", 3), ("playstation", 3), ("xbox", 3),
        ("nintendo", 3), ("patreon", 3),
        ("gaming", 2), ("streaming", 2), ("anime", 2), ("fan fiction", 2),
        ("cosplay", 2), ("creator economy", 2), ("influencer", 2),
        ("new album", 2), ("music video", 2), ("music festival", 2),
        ("game", 1), ("movie", 1), ("music", 1), ("film", 1), ("show", 1),
        ("art", 1), ("book", 1), ("entertainment", 1), ("song", 1), ("album", 1),
        ("artist", 1), ("band", 1), ("trailer", 1), ("concert", 1), ("manga", 1),
    ],
    "social_movements": [
        ("lgbtq", 3), ("blm", 3), ("metoo", 3), ("aclu", 3),
        ("civil rights", 2), ("climate justice", 2), ("feminism", 2),
        ("racism", 2), ("discrimination", 2), ("immigration", 2),
        ("abortion", 2), ("inequality", 2), ("protest", 2), ("activism", 2),
        ("movement", 1), ("justice", 1), ("rights", 1), ("solidarity", 1),
        ("diversity", 1), ("inclusion", 1),
    ],
}

# Domain → (channel, boost_weight). Exact netloc match after stripping www.
_DOMAIN_BOOSTS: Dict[str, Tuple[str, float]] = {
    "apnews.com":               ("world_news",           3),
    "bbc.com":                  ("world_news",           2),
    "reuters.com":              ("world_news",           2),
    "theguardian.com":          ("world_news",           2),
    "nytimes.com":              ("world_news",           2),
    "aljazeera.com":            ("world_news",           2),
    "washingtonpost.com":       ("politics_government",  2),
    "politico.com":             ("politics_government",  3),
    "thehill.com":              ("politics_government",  2),
    "krebsonsecurity.com":      ("security_risk",        3),
    "theregister.com":          ("security_risk",        2),
    "wired.com":                ("security_risk",        1),
    "arxiv.org":                ("science_health",       3),
    "nature.com":               ("science_health",       3),
    "science.org":              ("science_health",       3),
    "pubmed.ncbi.nlm.nih.gov":  ("science_health",       3),
    "nasa.gov":                 ("science_health",       3),
    "coindesk.com":             ("economy_markets",      3),
    "bloomberg.com":            ("economy_markets",      2),
    "wsj.com":                  ("economy_markets",      2),
    "ft.com":                   ("economy_markets",      2),
    "github.com":               ("ai_tech",              2),
    "techcrunch.com":           ("ai_tech",              2),
    "arstechnica.com":          ("ai_tech",              2),
    "youtu.be":                 ("culture_creators",     2),
    "youtube.com":              ("culture_creators",     2),
    "open.spotify.com":         ("culture_creators",     2),
    "ign.com":                  ("culture_creators",     3),
    "variety.com":              ("culture_creators",     2),
    "rollingstone.com":         ("culture_creators",     2),
    "niemanlab.org":            ("platform_media",       2),
    "poynter.org":              ("platform_media",       2),
}

# Priority order for tie-breaking. "general" is the catch-all, always last.
_PRIORITY: List[str] = [
    "ai_tech", "security_risk", "politics_government", "world_news",
    "science_health", "economy_markets", "platform_media",
    "culture_creators", "social_movements", "general",
]

# Keywords where left-boundary match only so inflected forms match:
# hack → hacked/hacking, breach → breached, exploit → exploiting, etc.
_PREFIX_ROOTS: frozenset = frozenset({"hack", "breach", "exploit", "leak", "protest"})

# Hashtag keyword scoring multiplier — hashtags are intentional signals,
# slightly higher weight than incidental body text matches.
_HASHTAG_WEIGHT_MULT = 1.5

# Splits CamelCase hashtags into space-separated words before keyword matching.
# e.g. MusicChallenge → Music Challenge, AINews → AI News
_CAMEL_SPLIT_RE = re.compile(r"(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")

# Regex to extract bare URLs from body text (fallback when no embed external link).
# Catches https?:// URLs and common bare short-domain patterns (youtu.be, etc.).
_BODY_URL_RE = re.compile(
    r'https?://[^\s<>"\')]+|'
    r'(?<!\w)(?:youtu\.be|bit\.ly|t\.co|tinyurl\.com|open\.spotify\.com)/\S+',
    re.IGNORECASE,
)


def _build_pattern(kw: str) -> re.Pattern:
    escaped = re.escape(kw)
    if kw in _PREFIX_ROOTS:
        return re.compile(r"\b" + escaped, re.IGNORECASE)
    return re.compile(r"\b" + escaped + r"\b", re.IGNORECASE)


def _extract_domain(url: str) -> str:
    if "://" not in url:
        url = "https://" + url
    host = urlparse(url).netloc.lower()
    return host[4:] if host.startswith("www.") else host


class BlueskyTopicRouter:
    """
    Classify a Bluesky post body (+ optional external URL + hashtags) into an
    Option B channel.

    Scoring has three layers:
      1. Keyword scan on body text (weighted sum, word-boundary regex)
      2. Hashtag scan — same patterns, scores multiplied by _HASHTAG_WEIGHT_MULT
         (hashtags are intentional signals, higher confidence than incidental text)
      3. Domain boost on external URL (from embed or body text fallback)

    Returns 'general' when no channel scores above zero.
    """

    def __init__(self) -> None:
        self._patterns: Dict[str, List[Tuple[re.Pattern, str, float]]] = {
            ch: [(_build_pattern(kw), kw, w) for kw, w in entries]
            for ch, entries in _TOPIC_CHANNELS.items()
        }

    def classify(self, text: str, url: Optional[str] = None,
                 hashtags: Optional[List[str]] = None) -> str:
        return self.classify_with_audit(text, url, hashtags)["channel"]

    def classify_with_audit(self, text: str, url: Optional[str] = None,
                            hashtags: Optional[List[str]] = None) -> Dict:
        """Return full routing decision including scores, matched keywords, domain boost."""
        channel_scores:   Dict[str, float]     = {}
        channel_keywords: Dict[str, List[str]] = {}

        # Pass 1 — body text
        for ch, pat_kw_weights in self._patterns.items():
            matched, total = [], 0.0
            for pat, kw, weight in pat_kw_weights:
                if pat.search(text):
                    total  += weight
                    matched.append(kw)
            if total > 0:
                channel_scores[ch]   = channel_scores.get(ch, 0) + total
                channel_keywords[ch] = channel_keywords.get(ch, []) + matched

        # Pass 2 — hashtags (CamelCase split then joined, weighted x1.5)
        # Split e.g. MusicChallenge → Music Challenge so \bmusic\b matches.
        if hashtags:
            hashtag_text = " ".join(_CAMEL_SPLIT_RE.sub(" ", tag) for tag in hashtags)
            for ch, pat_kw_weights in self._patterns.items():
                matched, total = [], 0.0
                for pat, kw, weight in pat_kw_weights:
                    if pat.search(hashtag_text):
                        boosted = weight * _HASHTAG_WEIGHT_MULT
                        total  += boosted
                        matched.append(kw)
                if total > 0:
                    channel_scores[ch]    = channel_scores.get(ch, 0) + total
                    channel_keywords[ch]  = channel_keywords.get(ch, []) + matched

        # Pass 3 — domain boost from external URL
        domain_boost_str = ""
        if url:
            domain = _extract_domain(url)
            if domain in _DOMAIN_BOOSTS:
                ch, boost = _DOMAIN_BOOSTS[domain]
                channel_scores[ch] = channel_scores.get(ch, 0) + boost
                domain_boost_str   = f"{domain} → {ch}+{int(boost)}"

        if not channel_scores:
            return {
                "channel": "general", "top_score": 0.0,
                "second_channel": "", "second_score": 0.0, "score_margin": 0.0,
                "matched_keywords": [], "domain_boost_used": "",
            }

        best_score = max(channel_scores.values())
        winner = next(
            (ch for ch in _PRIORITY if channel_scores.get(ch, 0) == best_score),
            max(channel_scores, key=channel_scores.__getitem__),
        )
        others = sorted(
            [(ch, s) for ch, s in channel_scores.items() if ch != winner],
            key=lambda x: -x[1],
        )
        second_ch, second_score = others[0] if others else ("", 0.0)

        return {
            "channel":           winner,
            "top_score":         round(best_score, 3),
            "second_channel":    second_ch,
            "second_score":      round(second_score, 3),
            "score_margin":      round(best_score - second_score, 3),
            "matched_keywords":  list(dict.fromkeys(channel_keywords.get(winner, []))),
            "domain_boost_used": domain_boost_str,
        }


# ---------------------------------------------------------------------------
# Post normalization helpers
# ---------------------------------------------------------------------------

def _stable_id(s: str) -> int:
    """Deterministic 32-bit unsigned int. zlib.crc32 is stable across processes."""
    return zlib.crc32(s.encode()) & 0xFFFFFFFF


def _parse_hashtags(facets: Optional[list]) -> List[str]:
    """Extract hashtag strings from AT Protocol richtext facets."""
    tags: List[str] = []
    for facet in (facets or []):
        for feature in (facet.get("features") or []):
            if feature.get("$type") == "app.bsky.richtext.facet#tag":
                tag = feature.get("tag", "")
                if tag:
                    tags.append(tag)
    return tags


def _parse_facet_links(facets: Optional[list]) -> List[str]:
    """Extract link URIs from AT Protocol richtext facets (app.bsky.richtext.facet#link)."""
    links: List[str] = []
    for facet in (facets or []):
        for feature in (facet.get("features") or []):
            if feature.get("$type") == "app.bsky.richtext.facet#link":
                uri = feature.get("uri", "")
                if uri:
                    links.append(uri)
    return links


def _parse_embed(record: dict) -> dict:
    """Extract media flags and external link fields from a post record's embed."""
    embed = record.get("embed") or {}
    etype = embed.get("$type", "")

    has_image       = False
    image_count     = 0
    has_video       = False
    has_external    = False
    external_uri    = ""
    external_domain = ""
    has_quote       = False
    alt_texts: List[str] = []

    if etype == "app.bsky.embed.images":
        images      = embed.get("images") or []
        has_image   = bool(images)
        image_count = len(images)
        alt_texts   = [img.get("alt", "") for img in images if img.get("alt")]

    elif etype == "app.bsky.embed.video":
        has_video = True

    elif etype == "app.bsky.embed.external":
        ext             = embed.get("external") or {}
        external_uri    = ext.get("uri", "")
        external_domain = _extract_domain(external_uri) if external_uri else ""
        has_external    = bool(external_uri)

    elif etype == "app.bsky.embed.record":
        has_quote = True

    elif etype == "app.bsky.embed.recordWithMedia":
        has_quote = True
        media     = embed.get("media") or {}
        mtype     = media.get("$type", "")
        if mtype == "app.bsky.embed.images":
            images      = media.get("images") or []
            has_image   = bool(images)
            image_count = len(images)
            alt_texts   = [img.get("alt", "") for img in images if img.get("alt")]
        elif mtype == "app.bsky.embed.video":
            has_video = True
        elif mtype == "app.bsky.embed.external":
            ext             = media.get("external") or {}
            external_uri    = ext.get("uri", "")
            external_domain = _extract_domain(external_uri) if external_uri else ""
            has_external    = bool(external_uri)

    return {
        "embed_type":        etype,
        "has_image":         has_image,
        "image_count":       image_count,
        "has_video":         has_video,
        "has_external_link": has_external,
        "external_uri":      external_uri,
        "external_domain":   external_domain,
        "has_quote_post":    has_quote,
        "alt_texts":         alt_texts,
    }


def _normalize_post(msg: dict, router: BlueskyTopicRouter) -> Optional[Dict]:
    """
    Map a raw Jetstream commit message to the pipeline's normalized item dict.
    Returns None if the post has no usable text.
    """
    commit  = msg.get("commit", {})
    record  = commit.get("record", {})
    did     = msg.get("did", "")
    time_us = msg.get("time_us", 0)
    rkey    = commit.get("rkey", "")
    cid     = commit.get("cid", "")

    text = (record.get("text") or "").strip()
    if not text:
        return None

    post_uri   = f"at://{did}/app.bsky.feed.post/{rkey}"
    reply_ref  = record.get("reply")
    is_reply   = reply_ref is not None
    root_uri   = reply_ref["root"]["uri"]   if is_reply else post_uri
    parent_uri = reply_ref["parent"]["uri"] if is_reply else ""

    facets   = record.get("facets")
    hashtags = _parse_hashtags(facets)

    # Embed parsing (fix 4: recordWithMedia external now handled)
    embed_info = _parse_embed(record)

    # URL fallback: facet links first, then body text regex, when embed has no external link
    if not embed_info["has_external_link"]:
        facet_links = _parse_facet_links(facets)
        raw_url = facet_links[0] if facet_links else None
        if not raw_url:
            m = _BODY_URL_RE.search(text)
            raw_url = m.group(0) if m else None
        if raw_url:
            domain = _extract_domain(raw_url)
            if domain:
                embed_info = dict(embed_info)
                embed_info["has_external_link"] = True
                embed_info["external_uri"]      = raw_url
                embed_info["external_domain"]   = domain

    external_url = embed_info["external_uri"] or None
    routing      = router.classify_with_audit(text, url=external_url, hashtags=hashtags)
    channel      = routing["channel"]

    timestamp = time_us // 1_000_000

    return {
        # Legacy compatibility fields
        "id":          post_uri,
        "subreddit":   channel,
        "body":        text,
        "timestamp":   timestamp,
        "author":      did,
        "score":       0,
        "story_id":    _stable_id(root_uri),
        "story_title": text[:100],
        "domain":      embed_info["external_domain"],
        "item_type":   "comment" if is_reply else "story",
        "item_id":     _stable_id(post_uri),
        "created_at":  timestamp,
        "routing":     routing,
        # Bluesky-native metadata
        "source":             "bluesky",
        "platform_item_uri":  post_uri,
        "platform_cid":       cid,
        "author_did":         did,
        "author_handle":      "",
        "root_uri":           root_uri,
        "parent_uri":         parent_uri,
        "is_reply":           is_reply,
        "platform_item_type": "reply" if is_reply else "post",
        "langs":              record.get("langs") or [],
        "hashtags":           hashtags,
        **embed_info,
    }


# ---------------------------------------------------------------------------
# Live ingestor
# ---------------------------------------------------------------------------

class BlueskyIngestor(DataIngestor):
    """
    Streams Bluesky posts in real-time via the Jetstream WebSocket firehose.

    Architecture mirrors HackerNewsIngestor: sync stream() drains a queue.Queue
    fed by a background thread running a persistent asyncio event loop and a
    single persistent WebSocket connection.

    Posts routed to 'general' (no keyword match) are dropped and not emitted
    downstream — they are counted in _dropped_general for monitoring only.

    Args:
        lang_filter: only yield posts whose langs list contains this code.
                     Defaults to "en". Pass None to receive all languages.
        queue_size:  internal queue depth (default 2000).
    """

    def __init__(
        self,
        lang_filter: Optional[str] = "en",
        queue_size:  int = 2000,
    ) -> None:
        self._lang_filter      = lang_filter
        self._queue_size       = queue_size
        self._router           = BlueskyTopicRouter()
        self._processed        = 0
        self._dropped_general  = 0

    def stream(self) -> Iterator[Dict]:
        q: queue.Queue = queue.Queue(maxsize=self._queue_size)
        t = threading.Thread(target=self._run_background, args=(q,), daemon=True)
        t.start()
        while True:
            yield q.get()

    def _run_background(self, q: queue.Queue) -> None:
        asyncio.run(self._async_main(q))

    async def _async_main(self, q: queue.Queue) -> None:
        delay = 5
        while True:
            try:
                async with websockets.connect(_JETSTREAM_URL) as ws:
                    delay = 5
                    async for raw in ws:
                        msg = json.loads(raw)

                        if msg.get("kind") != "commit":
                            continue
                        commit = msg.get("commit", {})
                        if commit.get("collection") != "app.bsky.feed.post":
                            continue
                        if commit.get("operation") != "create":
                            continue

                        if self._lang_filter:
                            record = commit.get("record", {})
                            if self._lang_filter not in (record.get("langs") or []):
                                continue

                        item = _normalize_post(msg, self._router)
                        if item is None:
                            continue

                        self._processed += 1

                        # Drop unmatched chatter — general is a routing failure,
                        # not a real signal channel
                        if item["subreddit"] == "general":
                            self._dropped_general += 1
                            if self._dropped_general % 500 == 0:
                                pct = 100 * self._dropped_general // max(self._processed, 1)
                                print(
                                    f"[bluesky] processed={self._processed} "
                                    f"dropped_general={self._dropped_general} ({pct}%)",
                                    file=sys.stderr,
                                )
                            continue

                        q.put(item)

            except Exception as exc:
                print(
                    f"[bluesky] websocket error: {type(exc).__name__}: {exc}",
                    file=sys.stderr,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)
