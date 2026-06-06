"""
Regression tests for HNTopicRouter.classify().

Each case captures a real routing decision (or discovered miss) so keyword
changes can be verified without re-running the live pipeline.
"""
from __future__ import annotations

import pytest

from src.ingestion.hacker_news import HNTopicRouter

@pytest.fixture(scope="module")
def router() -> HNTopicRouter:
    return HNTopicRouter()


# ---------------------------------------------------------------------------
# Core channel routing
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("title, url, expected", [
    # AI — tier-3 brand names
    ("OpenAI releases GPT-5", "", "ai"),
    ("Anthropic publishes Claude safety report", "", "ai"),
    ("Meta releases Llama 3.1", "", "ai"),
    # AI — llm prefix root: must match plural and compound forms
    ("Ask HN: Do you know any company that's making money with LLMs", "", "ai"),
    ("Building an LLM-based code review tool", "", "ai"),
    ("LLM-powered search is replacing traditional IR", "", "ai"),
    # AI — new tier-2/3 keywords
    ("RLHF is surprisingly fragile at scale", "", "ai"),
    ("Fine-tuning Mistral on domain-specific data", "", "ai"),
    ("Foundation models are becoming commodity infrastructure", "", "ai"),
    # Security
    ("New ransomware campaign targets hospital networks", "", "security"),
    ("CVE-2024-1234: critical RCE in OpenSSH", "", "security"),
    ("Phishing kit bypasses two-factor authentication", "", "security"),
    # Startup
    ("Stripe raises Series B at $50B valuation", "", "startup"),
    ("Y Combinator-backed startup acquired by Google", "", "startup"),
    # Crypto
    ("Bitcoin ETF approved by SEC", "", "crypto"),
    ("Ethereum merge reduces energy use by 99%", "", "crypto"),
    # Science — domain boost
    ("A new approach to protein folding", "https://arxiv.org/abs/2406.1234", "science"),
    ("CRISPR used to cure sickle cell disease", "", "science"),
    ("NASA Artemis mission update", "", "science"),
    # Tech — brands
    ("Apple announces M4 MacBook Pro", "", "tech"),
    ("Nvidia H100 supply constraints ease", "", "tech"),
    # Tech — new keywords
    ("Laptop makers are sacrificing ports and repairability for thin designs", "", "tech"),
    # Policy
    ("EU antitrust regulators fine Google €4B", "", "policy"),
    ("Senate passes new AI regulation bill", "", "policy"),
])
def test_channel_routing(router, title, url, expected):
    assert router.classify(title, url or None) == expected


# ---------------------------------------------------------------------------
# Audit fields
# ---------------------------------------------------------------------------

def test_audit_llm_plural(router):
    d = router.classify_with_audit("Companies making money with LLMs in 2024")
    assert d["channel"] == "ai"
    assert "llm" in d["matched_keywords"]


def test_audit_domain_boost_recorded(router):
    d = router.classify_with_audit(
        "New protein folding paper",
        url="https://arxiv.org/abs/2406.9999",
    )
    assert d["channel"] == "science"
    assert "arxiv.org" in d["domain_boost_used"]


def test_audit_margin_on_tie(router):
    # "gemini" scores ai=3; "google" scores tech=3 — priority tie-break, margin=0
    d = router.classify_with_audit("Google releases Gemini 2.0 Flash")
    assert d["channel"] == "ai"
    assert d["score_margin"] == 0.0
    assert d["second_channel"] == "tech"


def test_audit_general_has_zero_score(router):
    d = router.classify_with_audit("How the Internet Invented Bread Clip Science")
    assert d["channel"] == "general"
    assert d["top_score"] == 0.0


# ---------------------------------------------------------------------------
# Legitimately general — must NOT be pulled into a topic channel
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("title", [
    "Lucky Bracelet Guide: Symbols, Energy and Everyday Wear",
    "How to Easily Create Your Own Blender Theme",
    "Lee Kuan Yew's Singapore Story",
    "Fighting Entropy",
    "Old New York Stories – Jacques Barzun Interview",
])
def test_stays_general(router, title):
    assert router.classify(title) == "general"
