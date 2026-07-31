"""
Tests for the Kafka envelope: construction, round-trip serialization, and
validation of both valid and invalid messages.
"""
from __future__ import annotations

import copy
import json

import pytest

from src.streaming.envelope import (
    EnvelopeValidationError,
    build_envelope,
    deserialize_envelope,
    serialize_envelope,
    validate_envelope,
)


@pytest.fixture
def sample_item() -> dict:
    """A realistic normalized item dict, matching the shape
    src/ingestion/bluesky.py's _normalize_post() yields for a science_space post."""
    return {
        "id":          "at://did:plc:zvdk7qjbc7hemapgs3iddjd3/app.bsky.feed.post/3mrbxdcoq6q2a",
        "subreddit":   "science_space",
        "body":        "Conformal QED in AdS as a BCFT\nhttps://arxiv.org/pdf/2607.19464",
        "timestamp":   1784779568,
        "author":      "did:plc:zvdk7qjbc7hemapgs3iddjd3",
        "score":       0,
        "story_id":    123456789,
        "story_title": "Conformal QED in AdS as a BCFT",
        "domain":      "arxiv.org",
        "item_type":   "story",
        "item_id":     987654321,
        "created_at":  1784779568,
        "routing": {
            "channel": "science_space", "top_score": 9.0,
            "second_channel": "", "second_score": 0.0, "score_margin": 9.0,
            "matched_keywords": ["arxiv"], "domain_boost_used": "arxiv.org → science_space+3",
            "body_score": 9.0, "embed_score": 0.0,
        },
        "source":             "bluesky",
        "platform_item_uri":  "at://did:plc:zvdk7qjbc7hemapgs3iddjd3/app.bsky.feed.post/3mrbxdcoq6q2a",
        "platform_cid":       "bafyrei...",
        "author_did":         "did:plc:zvdk7qjbc7hemapgs3iddjd3",
        "author_handle":      "",
        "root_uri":           "at://did:plc:zvdk7qjbc7hemapgs3iddjd3/app.bsky.feed.post/3mrbxdcoq6q2a",
        "parent_uri":         "",
        "is_reply":           False,
        "platform_item_type": "post",
        "langs":              ["en"],
        "hashtags":           [],
        "embed_type":         "app.bsky.embed.external",
        "has_image":          False,
        "image_count":        0,
        "has_video":          False,
        "has_external_link":  True,
        "external_uri":       "https://arxiv.org/pdf/2607.19464",
        "external_domain":    "arxiv.org",
        "has_quote_post":     False,
        "alt_texts":          [],
    }


# ---------------------------------------------------------------------------
# Build + round-trip
# ---------------------------------------------------------------------------

def test_build_and_round_trip(sample_item):
    envelope = build_envelope(sample_item)
    assert envelope.schema_version == 1
    assert envelope.source == "bluesky"
    assert envelope.key == sample_item["platform_item_uri"]
    assert envelope.payload == sample_item

    raw = serialize_envelope(envelope)
    assert isinstance(raw, bytes)

    restored = deserialize_envelope(raw)
    assert restored.schema_version == envelope.schema_version
    assert restored.source == envelope.source
    assert restored.produced_at == envelope.produced_at
    assert restored.key == envelope.key
    assert restored.payload == sample_item

    validate_envelope(json.loads(raw))  # must not raise


def test_valid_envelope_passes(sample_item):
    envelope = build_envelope(sample_item)
    data = json.loads(serialize_envelope(envelope))
    validate_envelope(data)  # must not raise


# ---------------------------------------------------------------------------
# Invalid envelopes
# ---------------------------------------------------------------------------

def test_missing_payload_rejected(sample_item):
    envelope = build_envelope(sample_item)
    data = json.loads(serialize_envelope(envelope))
    del data["payload"]
    with pytest.raises(EnvelopeValidationError, match="payload"):
        validate_envelope(data)


@pytest.mark.parametrize("missing_field", ["id", "subreddit", "routing", "domain", "created_at"])
def test_payload_missing_required_field_rejected(sample_item, missing_field):
    envelope = build_envelope(sample_item)
    data = json.loads(serialize_envelope(envelope))
    del data["payload"][missing_field]
    with pytest.raises(EnvelopeValidationError, match=missing_field):
        validate_envelope(data)


def test_wrong_type_schema_version_rejected(sample_item):
    envelope = build_envelope(sample_item)
    data = json.loads(serialize_envelope(envelope))
    data["schema_version"] = "1"  # string instead of int
    with pytest.raises(EnvelopeValidationError, match="schema_version"):
        validate_envelope(data)


def test_payload_not_a_dict_rejected(sample_item):
    envelope = build_envelope(sample_item)
    data = json.loads(serialize_envelope(envelope))
    data["payload"] = "not-a-dict"
    with pytest.raises(EnvelopeValidationError, match="payload"):
        validate_envelope(data)


def test_envelope_not_a_dict_rejected():
    with pytest.raises(EnvelopeValidationError, match="dict"):
        validate_envelope(["not", "a", "dict"])


def test_deserialize_malformed_json():
    with pytest.raises(json.JSONDecodeError):
        deserialize_envelope(b"{not valid json")


def test_deserialize_missing_key_raises(sample_item):
    envelope = build_envelope(sample_item)
    data = json.loads(serialize_envelope(envelope))
    del data["key"]
    with pytest.raises(KeyError):
        deserialize_envelope(json.dumps(data).encode("utf-8"))


def test_build_envelope_does_not_mutate_input(sample_item):
    original = copy.deepcopy(sample_item)
    build_envelope(sample_item)
    assert sample_item == original
