"""
Tests for BlueskyKafkaConfig — env var loading and fail-fast validation.

No Kafka broker required; these are pure config/validation checks.
"""
from __future__ import annotations

import dataclasses

import pytest

from src.streaming.kafka_config import BlueskyKafkaConfig, KafkaConfigError

_KAFKA_ENV_VARS = (
    "BSKY_INPUT_MODE",
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_TOPIC_BLUESKY_ROUTED",
    "KAFKA_BSKY_ANOMALY_GROUP_ID",
    "KAFKA_SECURITY_PROTOCOL",
    "KAFKA_SASL_MECHANISM",
    "KAFKA_SASL_USERNAME",
    "KAFKA_SASL_PASSWORD",
)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    """Ensure no stray Kafka env vars leak in from the real environment."""
    for name in _KAFKA_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


# ---------------------------------------------------------------------------
# Defaults and jetstream mode
# ---------------------------------------------------------------------------

def test_defaults_load_without_error():
    config = BlueskyKafkaConfig.from_env()
    assert config.input_mode == "jetstream"
    assert config.bootstrap_servers == ""
    assert config.topic == "surge.bluesky.routed.v1"
    assert config.consumer_group == "surge-bluesky-anomaly-v1"
    assert config.security_protocol == "PLAINTEXT"
    assert config.sasl_mechanism == "PLAIN"
    config.validate()  # must not raise


def test_jetstream_mode_ignores_empty_kafka_config(monkeypatch):
    monkeypatch.setenv("BSKY_INPUT_MODE", "jetstream")
    config = BlueskyKafkaConfig.from_env()
    config.validate()  # still safe even with no bootstrap servers


# ---------------------------------------------------------------------------
# Kafka mode — fail-fast validation
# ---------------------------------------------------------------------------

def test_kafka_mode_requires_bootstrap_servers(monkeypatch):
    monkeypatch.setenv("BSKY_INPUT_MODE", "kafka")
    config = BlueskyKafkaConfig.from_env()
    with pytest.raises(KafkaConfigError) as exc:
        config.validate()
    assert str(exc.value) == "KAFKA_BOOTSTRAP_SERVERS is required when BSKY_INPUT_MODE=kafka"


def test_kafka_mode_with_bootstrap_and_plaintext_ok(monkeypatch):
    monkeypatch.setenv("BSKY_INPUT_MODE", "kafka")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    config = BlueskyKafkaConfig.from_env()
    config.validate()  # PLAINTEXT default — SASL fields correctly not required


@pytest.mark.parametrize("protocol", ["SASL_PLAINTEXT", "SASL_SSL"])
def test_sasl_requires_credentials(monkeypatch, protocol):
    # KAFKA_SASL_MECHANISM defaults to "PLAIN" (non-empty), so only the two
    # genuinely-empty-by-default fields (username, password) should be flagged.
    monkeypatch.setenv("BSKY_INPUT_MODE", "kafka")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", protocol)
    config = BlueskyKafkaConfig.from_env()
    with pytest.raises(KafkaConfigError) as exc:
        config.validate()
    msg = str(exc.value)
    assert "KAFKA_SASL_MECHANISM" not in msg
    assert "KAFKA_SASL_USERNAME" in msg
    assert "KAFKA_SASL_PASSWORD" in msg


def test_sasl_requires_mechanism_when_explicitly_blank(monkeypatch):
    monkeypatch.setenv("BSKY_INPUT_MODE", "kafka")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "user")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "secret")
    config = BlueskyKafkaConfig.from_env()
    with pytest.raises(KafkaConfigError) as exc:
        config.validate()
    msg = str(exc.value)
    assert "KAFKA_SASL_MECHANISM" in msg
    assert "KAFKA_SASL_USERNAME" not in msg
    assert "KAFKA_SASL_PASSWORD" not in msg


def test_sasl_ssl_with_full_credentials_ok(monkeypatch):
    monkeypatch.setenv("BSKY_INPUT_MODE", "kafka")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "PLAIN")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "user")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "secret")
    config = BlueskyKafkaConfig.from_env()
    config.validate()  # must not raise


# ---------------------------------------------------------------------------
# Safety: secrets never leak into repr; config is immutable
# ---------------------------------------------------------------------------

def test_password_excluded_from_repr(monkeypatch):
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "super-secret-token-xyz")
    config = BlueskyKafkaConfig.from_env()
    assert "super-secret-token-xyz" not in repr(config)
    assert "super-secret-token-xyz" not in str(config)


def test_config_is_frozen():
    config = BlueskyKafkaConfig.from_env()
    with pytest.raises(dataclasses.FrozenInstanceError):
        config.bootstrap_servers = "tampered:9092"
