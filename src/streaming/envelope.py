"""
Kafka message envelope for Bluesky — construction, (de)serialization, validation.

The envelope wraps the normalized item dict BlueskyIngestor.stream() already
yields (see src/ingestion/bluesky.py:_normalize_post and the "Shared item dict
schema" table in STRUCTURE.md) without altering its shape. Downstream code
(consumer side, Phase D) unwraps `payload` and feeds it into the exact same
_to_comment() path used by the direct-Jetstream shadow runner today — no
schema changes ripple past this file.
"""
from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from typing import Dict

# Required keys in `payload`, mirroring STRUCTURE.md's documented shared item
# dict schema (src/ingestion/base.py section). Single source of truth so
# Phase D's consumer-side validation reuses this exact list.
REQUIRED_PAYLOAD_FIELDS = (
    "id", "subreddit", "body", "timestamp", "author", "score",
    "story_id", "story_title", "domain", "item_type", "item_id",
    "created_at", "routing",
)

_REQUIRED_ENVELOPE_FIELDS = ("schema_version", "source", "produced_at", "key", "payload")


class EnvelopeValidationError(ValueError):
    """Raised when an envelope or its payload fails validation."""


@dataclass
class KafkaEnvelope:
    schema_version: int
    source:         str
    produced_at:    int
    key:            str
    payload:        Dict   # exact shape from bluesky.py's _normalize_post()


def build_envelope(item: Dict) -> KafkaEnvelope:
    """Wrap a normalized Bluesky item dict in a Kafka envelope. Does not
    mutate or reshape `item` — it is stored verbatim as `payload`."""
    return KafkaEnvelope(
        schema_version=1,
        source="bluesky",
        produced_at=int(time.time()),
        key=item.get("platform_item_uri", ""),
        payload=item,
    )


def serialize_envelope(envelope: KafkaEnvelope) -> bytes:
    return json.dumps(asdict(envelope), ensure_ascii=False).encode("utf-8")


def deserialize_envelope(raw: bytes) -> KafkaEnvelope:
    data = json.loads(raw)
    return KafkaEnvelope(
        schema_version=data["schema_version"],
        source=data["source"],
        produced_at=data["produced_at"],
        key=data["key"],
        payload=data["payload"],
    )


def validate_envelope(data: dict) -> None:
    """Raise EnvelopeValidationError on the first problem found. Returns None
    when the envelope and its payload are well-formed."""
    if not isinstance(data, dict):
        raise EnvelopeValidationError(f"envelope must be a dict, got {type(data).__name__}")

    for key in _REQUIRED_ENVELOPE_FIELDS:
        if key not in data:
            raise EnvelopeValidationError(f"envelope missing required field: {key}")

    if not isinstance(data["schema_version"], int):
        raise EnvelopeValidationError(
            f"schema_version must be int, got {type(data['schema_version']).__name__}"
        )
    if not isinstance(data["source"], str):
        raise EnvelopeValidationError(f"source must be str, got {type(data['source']).__name__}")
    if not isinstance(data["produced_at"], int):
        raise EnvelopeValidationError(
            f"produced_at must be int, got {type(data['produced_at']).__name__}"
        )
    if not isinstance(data["key"], str):
        raise EnvelopeValidationError(f"key must be str, got {type(data['key']).__name__}")

    payload = data["payload"]
    if not isinstance(payload, dict):
        raise EnvelopeValidationError(f"payload must be a dict, got {type(payload).__name__}")

    for field_name in REQUIRED_PAYLOAD_FIELDS:
        if field_name not in payload:
            raise EnvelopeValidationError(f"payload missing required field: {field_name}")
