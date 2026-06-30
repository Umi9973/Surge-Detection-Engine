from __future__ import annotations

import asyncio
import json
import queue
import re
import sys
import threading
import time
import zlib
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
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
        ("maga", 2), ("liberal", 2), ("conservative", 2), ("parliament", 2),
        ("premier", 2), ("minister", 2), ("monarchy", 2), ("senate bill", 2),
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
        ("electric vehicle", 2), ("heat wave", 2), ("wet bulb", 2),
        ("carbon emissions", 2), ("renewable energy", 2), ("climate crisis", 2),
        ("zev", 2),
        ("science", 1), ("health", 1), ("medicine", 1), ("study", 1),
        ("discovery", 1), ("space", 1), ("climate", 1), ("biology", 1),
        ("weather", 1), ("forecast", 1),
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
        ("fediverse", 2), ("activitypub", 2), ("self-hosted", 2),
        ("platform", 1), ("media", 1), ("viral", 1), ("rss", 1), ("blog", 1),
    ],
    "culture_creators": [
        ("netflix", 3), ("disney", 3), ("marvel", 3), ("spotify", 3),
        ("twitch", 3), ("steam", 3), ("playstation", 3), ("xbox", 3),
        ("nintendo", 3), ("patreon", 3),
        ("pokemon", 3), ("ffxiv", 3), ("evangelion", 3), ("criticalrole", 3),
        ("genshin", 3), ("valorant", 3), ("elden ring", 3), ("hogwarts", 3),
        # Compound fandom names: both concatenated (for hashtag exact match) and
        # spaced (for CamelCase-split hashtag and body text)
        ("starwars", 3), ("star wars", 3),
        ("dragonball", 3), ("dragon ball", 3),
        ("onepiece", 3), ("one piece", 3),
        ("jujutsu", 3),
        ("gaming", 2), ("streaming", 2), ("anime", 2), ("fan fiction", 2),
        ("cosplay", 2), ("creator economy", 2), ("influencer", 2),
        ("new album", 2), ("music video", 2), ("music festival", 2),
        ("fanart", 2), ("fandom", 2), ("internet radio", 2),
        ("artfight", 3), ("deltarune", 3),
        ("photography", 2), ("digital art", 2), ("digitalart", 2),
        ("illustration", 2), ("furry art", 2), ("furryart", 2),
        ("game", 1), ("movie", 1), ("music", 1), ("film", 1), ("show", 1),
        ("art", 1), ("book", 1), ("entertainment", 1), ("song", 1), ("album", 1),
        ("artist", 1), ("band", 1), ("trailer", 1), ("concert", 1), ("manga", 1),
        ("radio", 1),
    ],
    "social_movements": [
        ("lgbtq", 3), ("blm", 3), ("metoo", 3), ("aclu", 3),
        ("civil rights", 2), ("climate justice", 2), ("feminism", 2),
        ("racism", 2), ("discrimination", 2), ("immigration", 2),
        ("abortion", 2), ("inequality", 2), ("protest", 2), ("activism", 2),
        ("movement", 1), ("justice", 1), ("rights", 1), ("solidarity", 1),
        ("diversity", 1), ("inclusion", 1),
    ],
    "sports": [
        ("nfl", 3), ("nba", 3), ("nhl", 3), ("mlb", 3), ("nascar", 3),
        ("fifa", 3), ("uefa", 3), ("wimbledon", 3),
        ("premier league", 3), ("champions league", 3), ("bundesliga", 3),
        ("la liga", 3), ("serie a", 3),
        ("super bowl", 2), ("world cup", 2), ("transfer window", 2),
        ("match day", 2), ("fantasy football", 2), ("fantasy sports", 2),
        ("playoffs", 2), ("championship", 2),
        ("formula 1", 2), ("motorsport", 2), ("racing", 2),
        ("football", 1), ("soccer", 1), ("tennis", 1), ("basketball", 1),
        ("baseball", 1), ("cricket", 1), ("rugby", 1), ("golf", 1),
        ("athletics", 1), ("cycling", 1),
    ],
}

# Domain → (channel, boost_weight). Exact netloc match after stripping www.
_DOMAIN_BOOSTS: Dict[str, Tuple[str, float]] = {
    "apnews.com":                ("world_news",           3),
    "bbc.com":                   ("world_news",           2),
    "reuters.com":               ("world_news",           2),
    "theguardian.com":           ("world_news",           2),
    "nytimes.com":               ("world_news",           2),
    "aljazeera.com":             ("world_news",           2),
    "smh.com.au":                ("world_news",           2),
    "abc.net.au":                ("world_news",           2),
    "abc7.com":                  ("world_news",           1),   # local US — weak signal only
    "nation.cymru":              ("world_news",           2),
    "washingtonpost.com":        ("politics_government",  2),
    "politico.com":              ("politics_government",  3),
    "thehill.com":               ("politics_government",  2),
    "salon.com":                 ("politics_government",  2),
    "laprogressive.com":         ("politics_government",  2),
    "krebsonsecurity.com":       ("security_risk",        3),
    "theregister.com":           ("security_risk",        2),
    "wired.com":                 ("security_risk",        1),
    "arxiv.org":                 ("science_health",       3),
    "nature.com":                ("science_health",       3),
    "science.org":               ("science_health",       3),
    "pubmed.ncbi.nlm.nih.gov":   ("science_health",       3),
    "nasa.gov":                  ("science_health",       3),
    "mesonet.agron.iastate.edu": ("science_health",       2),
    "thedrive.com":              ("science_health",       1),
    "coindesk.com":              ("economy_markets",      3),
    "bloomberg.com":             ("economy_markets",      2),
    "wsj.com":                   ("economy_markets",      2),
    "ft.com":                    ("economy_markets",      2),
    "github.com":                ("ai_tech",              2),
    "techcrunch.com":            ("ai_tech",              2),
    "arstechnica.com":           ("ai_tech",              2),
    "youtu.be":                  ("culture_creators",     2),
    "youtube.com":               ("culture_creators",     2),
    "open.spotify.com":          ("culture_creators",     2),
    "ign.com":                   ("culture_creators",     3),
    "variety.com":               ("culture_creators",     2),
    "rollingstone.com":          ("culture_creators",     2),
    "berlinartlink.com":         ("culture_creators",     2),
    "niemanlab.org":             ("platform_media",       2),
    "poynter.org":               ("platform_media",       2),
    "bbc.co.uk":                 ("world_news",           2),
    "spc.noaa.gov":              ("science_health",       2),
    "weather.gov":               ("science_health",       2),
    "europesays.com":            ("world_news",           1),
    "ko-fi.com":                 ("culture_creators",     2),
    "transfermarkt.com":         ("sports",               3),
    "artfight.net":              ("culture_creators",     3),
}

# Priority order for tie-breaking. "general" is the catch-all, always last.
_PRIORITY: List[str] = [
    "ai_tech", "security_risk", "politics_government", "world_news",
    "science_health", "economy_markets", "platform_media",
    "sports", "culture_creators", "social_movements", "general",
]

# Keywords where left-boundary match only so inflected forms match:
# hack → hacked/hacking, breach → breached, exploit → exploiting, etc.
_PREFIX_ROOTS: frozenset = frozenset({"hack", "breach", "exploit", "leak", "protest", "artfight"})

# Hashtag keyword scoring multiplier — hashtags are intentional signals,
# slightly higher weight than incidental body text matches.
_HASHTAG_WEIGHT_MULT = 1.5

# Splits CamelCase hashtags into space-separated words before keyword matching.
# e.g. MusicChallenge → Music Challenge, AINews → AI News
_CAMEL_SPLIT_RE = re.compile(r"(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")

# Hashtag alias expansions (layer 2).
# Short or ambiguous hashtags that are too noisy to add as raw body-text keywords
# get expanded into synthetic tokens that flow through normal _TOPIC_CHANNELS scoring.
# Keys are lowercase; values are space-separated keyword strings.
_HASHTAG_ALIASES: Dict[str, str] = {
    "wx":          "weather noaa forecast",
    "f1":          "formula 1 motorsport racing",
    "oc":          "original character art creator",
    "nowplaying":  "music song album artist",
    "booksky":     "book reading author",
}

# Regex to extract bare URLs from body text (fallback when no embed external link).
_BODY_URL_RE = re.compile(
    r'https?://[^\s<>"\')]+|'
    r'(?<!\w)(?:youtu\.be|bit\.ly|t\.co|tinyurl\.com|open\.spotify\.com)/\S+',
    re.IGNORECASE,
)

# Adult/NSFW hashtag blocklist — dropped before routing to keep stats clean.
_ADULT_HASHTAGS: frozenset = frozenset({
    "nsfw", "porn", "nude", "naked", "xxx", "gayporn", "onlyfans",
    "fansly", "18+", "adult", "horny", "nudes", "explicit",
})

# Wordle/Connections grid: 4+ coloured square emoji in sequence.
# Only triggers routine_template when COMBINED with a game word or routine hashtag.
_ROUTINE_EMOJI_RE = re.compile(r"[⬜🟨🟩⬛🟥🟦]{4,}", re.UNICODE)

# Known recurring game-share keywords in body text.
_ROUTINE_WORD_RE = re.compile(
    r"\b(wordle|connections|quordle|worldle|daily puzzle|nyt games)\b",
    re.IGNORECASE,
)

# Hashtags that mark recurring game-share templates.
_ROUTINE_HASHTAGS: frozenset = frozenset({
    "wordle", "connections", "nytgames", "worldle", "quordle", "starbattle",
})

# Language mismatch: strip URLs, @mentions, #hashtags before ratio check so
# they don't dilute the Latin-character count of otherwise English posts.
_LANG_STRIP_RE = re.compile(r"https?://\S+|@\w+|#\w+", re.IGNORECASE)

# ---------------------------------------------------------------------------
# Debug output paths and limits
# ---------------------------------------------------------------------------

_DEBUG_BASE          = Path(__file__).resolve().parent.parent.parent / "data" / "debug" / "bluesky"
_SUMMARY_INTERVAL_S  = 30    # rewrite run_summary JSON every N seconds
_MAX_DROPPED_SAMPLES = 200   # max rows written to dropped JSONL per drop reason per session


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
    Classify a Bluesky post body (+ optional embed text + external URL + hashtags)
    into an Option B channel.

    Scoring has four layers:
      1a. Keyword scan on body text only            → body_score per channel
      1b. Keyword scan on embed title+description   → embed_score per channel
      2.  Hashtag scan (CamelCase split, ×1.5)      → added to body_score
      3.  Domain boost from external URL

    Audit dict includes body_score and embed_score for the winning channel so
    callers can tell whether a short post was routed by its own text or the
    link preview metadata.
    """

    def __init__(self) -> None:
        self._patterns: Dict[str, List[Tuple[re.Pattern, str, float]]] = {
            ch: [(_build_pattern(kw), kw, w) for kw, w in entries]
            for ch, entries in _TOPIC_CHANNELS.items()
        }

    def classify(self, body: str, url: Optional[str] = None,
                 hashtags: Optional[List[str]] = None,
                 embed_text: str = "") -> str:
        return self.classify_with_audit(body, url, hashtags, embed_text)["channel"]

    def classify_with_audit(self, body: str, url: Optional[str] = None,
                            hashtags: Optional[List[str]] = None,
                            embed_text: str = "") -> Dict:
        """Return full routing decision with per-source score breakdown."""
        body_scores:    Dict[str, float]     = {}
        embed_scores:   Dict[str, float]     = {}
        channel_keywords: Dict[str, List[str]] = {}

        # Pass 1a — body text
        for ch, pat_kw_weights in self._patterns.items():
            matched, total = [], 0.0
            for pat, kw, weight in pat_kw_weights:
                if pat.search(body):
                    total  += weight
                    matched.append(kw)
            if total > 0:
                body_scores[ch]      = total
                channel_keywords[ch] = channel_keywords.get(ch, []) + matched

        # Pass 1b — embed title + description (separate score bucket)
        if embed_text:
            for ch, pat_kw_weights in self._patterns.items():
                matched, total = [], 0.0
                for pat, kw, weight in pat_kw_weights:
                    if pat.search(embed_text):
                        total  += weight
                        matched.append(kw)
                if total > 0:
                    embed_scores[ch]     = total
                    channel_keywords[ch] = channel_keywords.get(ch, []) + matched

        # Pass 2 — hashtags (alias expand or CamelCase split, ×1.5, counted in body_scores)
        if hashtags:
            hashtag_text = " ".join(
                _HASHTAG_ALIASES.get(tag.lower()) or _CAMEL_SPLIT_RE.sub(" ", tag)
                for tag in hashtags
            )
            for ch, pat_kw_weights in self._patterns.items():
                matched, total = [], 0.0
                for pat, kw, weight in pat_kw_weights:
                    if pat.search(hashtag_text):
                        boosted = weight * _HASHTAG_WEIGHT_MULT
                        total  += boosted
                        matched.append(kw)
                if total > 0:
                    body_scores[ch]      = body_scores.get(ch, 0) + total
                    channel_keywords[ch] = channel_keywords.get(ch, []) + matched

        # Combined scores
        all_channels = set(body_scores) | set(embed_scores)
        channel_scores = {
            ch: body_scores.get(ch, 0) + embed_scores.get(ch, 0)
            for ch in all_channels
        }

        # Pass 3 — domain boost
        domain_boost_str = ""
        if url:
            domain = _extract_domain(url)
            if domain in _DOMAIN_BOOSTS:
                ch, boost = _DOMAIN_BOOSTS[domain]
                channel_scores[ch]   = channel_scores.get(ch, 0) + boost
                domain_boost_str     = f"{domain} → {ch}+{int(boost)}"

        if not channel_scores:
            return {
                "channel": "general", "top_score": 0.0,
                "second_channel": "", "second_score": 0.0, "score_margin": 0.0,
                "matched_keywords": [], "domain_boost_used": "",
                "body_score": 0.0, "embed_score": 0.0,
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
            "body_score":        round(body_scores.get(winner, 0), 3),
            "embed_score":       round(embed_scores.get(winner, 0), 3),
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
    """Extract link URIs from AT Protocol richtext facets."""
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


def _embed_routing_text(record: dict) -> str:
    """Return embed external title + description for routing (not stored on item)."""
    embed = record.get("embed") or {}
    etype = embed.get("$type", "")
    ext: dict = {}
    if etype == "app.bsky.embed.external":
        ext = embed.get("external") or {}
    elif etype == "app.bsky.embed.recordWithMedia":
        media = embed.get("media") or {}
        if media.get("$type") == "app.bsky.embed.external":
            ext = media.get("external") or {}
    parts = [ext.get("title", ""), ext.get("description", "")]
    return " ".join(p for p in parts if p)


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

    embed_info = _parse_embed(record)

    # URL fallback: facet links first, then body text regex
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

    # Embed title/description passed separately so router can track body_score vs embed_score
    embed_text   = _embed_routing_text(record)
    external_url = embed_info["external_uri"] or None
    routing      = router.classify_with_audit(text, url=external_url,
                                              hashtags=hashtags, embed_text=embed_text)
    channel      = routing["channel"]
    timestamp    = time_us // 1_000_000

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
# Debug helpers
# ---------------------------------------------------------------------------

def _drop_row(reason: str, msg: dict, item: Optional[Dict] = None) -> dict:
    """Build a compact row for the dropped-posts JSONL diagnostic log."""
    if item is not None:
        routing = item["routing"]
        return {
            "id":              item["id"],
            "body":            item["body"][:200],
            "timestamp":       item["timestamp"],
            "langs":           item["langs"],
            "hashtags":        item["hashtags"],
            "external_domain": item["external_domain"],
            "embed_type":      item["embed_type"],
            "drop_reason":     reason,
            "top_score":       routing["top_score"],
            "body_score":      routing.get("body_score", 0.0),
            "embed_score":     routing.get("embed_score", 0.0),
            "second_channel":  routing["second_channel"],
            "second_score":    routing["second_score"],
            "routing":         routing,
        }
    # Pre-normalization drop — extract from raw message
    commit = msg.get("commit", {})
    record = commit.get("record", {})
    did    = msg.get("did", "")
    rkey   = commit.get("rkey", "")
    return {
        "id":              f"at://{did}/app.bsky.feed.post/{rkey}",
        "body":            (record.get("text") or "")[:200],
        "timestamp":       msg.get("time_us", 0) // 1_000_000,
        "langs":           record.get("langs") or [],
        "hashtags":        _parse_hashtags(record.get("facets")),
        "external_domain": "",
        "embed_type":      (record.get("embed") or {}).get("$type", ""),
        "drop_reason":     reason,
        "top_score":       0.0,
        "body_score":      0.0,
        "embed_score":     0.0,
        "second_channel":  "",
        "second_score":    0.0,
        "routing":         {},
    }


# ---------------------------------------------------------------------------
# Live ingestor
# ---------------------------------------------------------------------------

class BlueskyIngestor(DataIngestor):
    """
    Streams Bluesky posts in real-time via the Jetstream WebSocket firehose.

    Three outputs per run:
      1. Kept stream  — topical posts only → queue → downstream pipeline
      2. Dropped log  — data/debug/bluesky/dropped/bluesky_dropped_YYYYMMDD_HHMM.jsonl
      3. Run summary  — data/debug/bluesky/runs/run_summary_YYYYMMDD_HHMM.json

    Drop reasons (in pipeline order):
      empty_text        — no text after strip
      non_english       — langs filter miss
      language_mismatch — langs=en but body is non-Latin alphabetic script
      adult_spam        — NSFW hashtags detected
      routine_template  — Wordle/Connections: game hashtag OR (grid emoji + game word)
      unmatched_topic   — score 0 after full router
    """

    def __init__(
        self,
        lang_filter: Optional[str] = "en",
        queue_size:  int = 2000,
    ) -> None:
        self._lang_filter = lang_filter
        self._queue_size  = queue_size
        self._router      = BlueskyTopicRouter()

    def stream(self) -> Iterator[Dict]:
        q: queue.Queue = queue.Queue(maxsize=self._queue_size)
        t = threading.Thread(target=self._run_background, args=(q,), daemon=True)
        t.start()
        while True:
            yield q.get()

    def _run_background(self, q: queue.Queue) -> None:
        asyncio.run(self._async_main(q))

    @staticmethod
    def _write_summary(path: Path, data: dict) -> None:
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except OSError:
            pass

    async def _async_main(self, q: queue.Queue) -> None:
        ts          = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
        dropped_dir = _DEBUG_BASE / "dropped"
        runs_dir    = _DEBUG_BASE / "runs"
        dropped_dir.mkdir(parents=True, exist_ok=True)
        runs_dir.mkdir(parents=True, exist_ok=True)
        dropped_path = dropped_dir / f"bluesky_dropped_{ts}.jsonl"
        summary_path = runs_dir    / f"run_summary_{ts}.json"

        total_seen          = 0
        english_seen        = 0
        kept_count          = 0
        dropped_by_reason:   Counter = Counter()
        kept_by_channel:     Counter = Counter()
        matched_keywords:    Counter = Counter()
        unmatched_hashtags:  Counter = Counter()
        unmatched_domains:   Counter = Counter()
        samples_per_reason:  Counter = Counter()
        reconnect_count     = 0
        last_summary        = time.monotonic()
        delay               = 5

        self._write_summary(summary_path, {
            "total_seen": 0, "english_seen": 0, "kept_count": 0,
            "dropped_count": 0, "kept_rate": 0.0, "dropped_rate": 0.0,
            "dropped_by_reason": {}, "kept_by_channel": {},
            "top_matched_keywords": {}, "top_unmatched_hashtags": {},
            "top_unmatched_domains": {}, "sample_dropped_count": 0,
            "reconnect_count": 0,
        })

        with open(dropped_path, "w", encoding="utf-8") as drop_f:
            while True:
                try:
                    async with websockets.connect(_JETSTREAM_URL) as ws:
                        delay = 5
                        async for raw in ws:
                            now = time.monotonic()

                            if now - last_summary >= _SUMMARY_INTERVAL_S:
                                dropped_count = sum(dropped_by_reason.values())
                                self._write_summary(summary_path, {
                                    "total_seen":             total_seen,
                                    "english_seen":           english_seen,
                                    "kept_count":             kept_count,
                                    "dropped_count":          dropped_count,
                                    "kept_rate":              round(kept_count / max(total_seen, 1), 4),
                                    "dropped_rate":           round(dropped_count / max(total_seen, 1), 4),
                                    "dropped_by_reason":      dict(dropped_by_reason),
                                    "kept_by_channel":        dict(kept_by_channel),
                                    "top_matched_keywords":   dict(matched_keywords.most_common(20)),
                                    "top_unmatched_hashtags": dict(unmatched_hashtags.most_common(30)),
                                    "top_unmatched_domains":  dict(unmatched_domains.most_common(20)),
                                    "sample_dropped_count":   sum(samples_per_reason.values()),
                                    "reconnect_count":        reconnect_count,
                                })
                                drop_f.flush()
                                last_summary = now

                            msg    = json.loads(raw)

                            if msg.get("kind") != "commit":
                                continue
                            commit = msg.get("commit", {})
                            if commit.get("collection") != "app.bsky.feed.post":
                                continue
                            if commit.get("operation") != "create":
                                continue

                            record = commit.get("record", {})
                            text   = (record.get("text") or "").strip()
                            total_seen += 1

                            # ── Drop: empty text ──────────────────────────────
                            if not text:
                                reason = "empty_text"
                                dropped_by_reason[reason] += 1
                                if samples_per_reason[reason] < _MAX_DROPPED_SAMPLES:
                                    samples_per_reason[reason] += 1
                                    drop_f.write(json.dumps(_drop_row(reason, msg), ensure_ascii=False) + "\n")
                                continue

                            # ── Drop: non-English ─────────────────────────────
                            if self._lang_filter and self._lang_filter not in (record.get("langs") or []):
                                reason = "non_english"
                                dropped_by_reason[reason] += 1
                                if samples_per_reason[reason] < _MAX_DROPPED_SAMPLES:
                                    samples_per_reason[reason] += 1
                                    drop_f.write(json.dumps(_drop_row(reason, msg), ensure_ascii=False) + "\n")
                                continue

                            english_seen += 1

                            # ── Drop: language mismatch (langs=en false positive) ──
                            # Strip URLs/@mentions/#hashtags before ratio — they dilute Latin count.
                            # Only fire when text is long enough (>40 chars) and mostly non-Latin alpha.
                            clean = _LANG_STRIP_RE.sub("", text).strip()
                            if len(clean) > 40:
                                alpha      = sum(1 for c in clean if c.isalpha())
                                latin_alpha = sum(1 for c in clean if c.isalpha() and ord(c) <= 0x024F)
                                if alpha > 0 and latin_alpha < alpha * 0.5:
                                    reason = "language_mismatch"
                                    dropped_by_reason[reason] += 1
                                    if samples_per_reason[reason] < _MAX_DROPPED_SAMPLES:
                                        samples_per_reason[reason] += 1
                                        drop_f.write(json.dumps(_drop_row(reason, msg), ensure_ascii=False) + "\n")
                                    continue

                            # Normalize + route
                            item = _normalize_post(msg, self._router)
                            if item is None:
                                dropped_by_reason["empty_text"] += 1
                                continue

                            # ── Drop: adult spam ──────────────────────────────
                            if any(ht.lower() in _ADULT_HASHTAGS for ht in item["hashtags"]):
                                reason = "adult_spam"
                                dropped_by_reason[reason] += 1
                                if samples_per_reason[reason] < _MAX_DROPPED_SAMPLES:
                                    samples_per_reason[reason] += 1
                                    drop_f.write(json.dumps(_drop_row(reason, msg, item), ensure_ascii=False) + "\n")
                                continue

                            # ── Drop: routine template ────────────────────────
                            # Emoji grid alone is too broad; require game hashtag OR game word.
                            has_routine_tag  = any(ht.lower() in _ROUTINE_HASHTAGS for ht in item["hashtags"])
                            has_game_word    = bool(_ROUTINE_WORD_RE.search(item["body"]))
                            has_grid_emoji   = bool(_ROUTINE_EMOJI_RE.search(item["body"]))
                            if has_routine_tag or (has_grid_emoji and has_game_word):
                                reason = "routine_template"
                                dropped_by_reason[reason] += 1
                                if samples_per_reason[reason] < _MAX_DROPPED_SAMPLES:
                                    samples_per_reason[reason] += 1
                                    drop_f.write(json.dumps(_drop_row(reason, msg, item), ensure_ascii=False) + "\n")
                                continue

                            channel = item["subreddit"]

                            # ── Drop: unmatched topic ─────────────────────────
                            if channel == "general":
                                reason = "unmatched_topic"
                                dropped_by_reason[reason] += 1
                                if samples_per_reason[reason] < _MAX_DROPPED_SAMPLES:
                                    samples_per_reason[reason] += 1
                                    drop_f.write(json.dumps(_drop_row(reason, msg, item), ensure_ascii=False) + "\n")
                                for ht in item["hashtags"]:
                                    unmatched_hashtags[ht] += 1
                                if item["external_domain"]:
                                    unmatched_domains[item["external_domain"]] += 1
                                continue

                            # ── Keep ──────────────────────────────────────────
                            kept_count += 1
                            kept_by_channel[channel] += 1
                            for kw in item["routing"].get("matched_keywords", []):
                                matched_keywords[kw] += 1
                            q.put(item)

                except Exception as exc:
                    reconnect_count += 1
                    print(
                        f"[bluesky] websocket error: {type(exc).__name__}: {exc}",
                        file=sys.stderr,
                    )
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 60)
