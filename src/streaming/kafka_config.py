"""
Bluesky Kafka configuration — env var loading and fail-fast validation.

No I/O. No Kafka client. This module never imports confluent_kafka, never
connects to anything, and never touches Redis or src/ingestion/. It only
answers "what are the settings, and are they sufficient for the requested
input mode?"

Env vars (all optional, all read at BlueskyKafkaConfig.from_env() call time):
    BSKY_INPUT_MODE              default "jetstream"  ("jetstream" | "kafka")
    KAFKA_BOOTSTRAP_SERVERS      default ""            (required when input_mode="kafka")
    KAFKA_TOPIC_BLUESKY_ROUTED   default "surge.bluesky.routed.v1"
    KAFKA_BSKY_ANOMALY_GROUP_ID  default "surge-bluesky-anomaly-v1"
    KAFKA_SECURITY_PROTOCOL      default "PLAINTEXT"   ("PLAINTEXT" | "SASL_PLAINTEXT" | "SASL_SSL")
    KAFKA_SASL_MECHANISM         default "PLAIN"        (enforced only when protocol requires SASL)
    KAFKA_SASL_USERNAME          default ""
    KAFKA_SASL_PASSWORD          default ""             (never included in repr())

Topic naming: "surge.bluesky.routed.v1" — the Kafka boundary sits after
BlueskyIngestor's filter/route/normalize pipeline, so this topic carries
accepted, channel-routed records, not raw firehose posts. A future raw-post
topic would be "surge.bluesky.raw.v1" with no naming collision.

Consumer group naming: "surge-bluesky-anomaly-v1" is dedicated to the anomaly
shadow consumer. Kafka consumer groups sharing a group ID divide a topic's
messages between members; groups with different IDs each get the full stream
independently. A future passive-stats consumer must use its own group
(e.g. "surge-bluesky-stats-v1"), never this one, or it would silently steal
half the anomaly detector's messages instead of getting its own full copy.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


class KafkaConfigError(ValueError):
    """Raised when Kafka configuration is insufficient for the requested input mode."""


@dataclass(frozen=True)
class BlueskyKafkaConfig:
    input_mode:        str            # "jetstream" | "kafka"
    bootstrap_servers: str
    topic:             str
    consumer_group:    str
    security_protocol: str
    sasl_mechanism:    str
    sasl_username:     str
    sasl_password:     str = field(default="", repr=False)

    @classmethod
    def from_env(cls) -> "BlueskyKafkaConfig":
        return cls(
            input_mode=os.environ.get("BSKY_INPUT_MODE", "jetstream"),
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", ""),
            topic=os.environ.get("KAFKA_TOPIC_BLUESKY_ROUTED", "surge.bluesky.routed.v1"),
            consumer_group=os.environ.get("KAFKA_BSKY_ANOMALY_GROUP_ID", "surge-bluesky-anomaly-v1"),
            security_protocol=os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
            sasl_mechanism=os.environ.get("KAFKA_SASL_MECHANISM", "PLAIN"),
            sasl_username=os.environ.get("KAFKA_SASL_USERNAME", ""),
            sasl_password=os.environ.get("KAFKA_SASL_PASSWORD", ""),
        )

    def validate(self) -> None:
        """Raise KafkaConfigError if config is insufficient for input_mode.

        No I/O — pure string checks. Does not attempt to connect.
        """
        if self.input_mode != "kafka":
            return  # jetstream mode: an unconfigured Kafka section is harmless

        if not self.bootstrap_servers:
            raise KafkaConfigError(
                "KAFKA_BOOTSTRAP_SERVERS is required when BSKY_INPUT_MODE=kafka"
            )

        if self.security_protocol in ("SASL_PLAINTEXT", "SASL_SSL"):
            missing = [
                name for name, val in (
                    ("KAFKA_SASL_MECHANISM", self.sasl_mechanism),
                    ("KAFKA_SASL_USERNAME",  self.sasl_username),
                    ("KAFKA_SASL_PASSWORD",  self.sasl_password),
                )
                if not val
            ]
            if missing:
                raise KafkaConfigError(
                    f"{self.security_protocol} requires: {', '.join(missing)}"
                )
