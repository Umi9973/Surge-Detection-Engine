# Kafka Implementation Plan — Bluesky Ingestion

## Purpose

Replace `BlueskyIngestor`'s direct Jetstream WebSocket connection with a Kafka-backed
transport, without changing anything downstream of the normalized item dict. This
document is the phase plan. No implementation has started; nothing in this document
has been built yet.

## Design invariant

The seam identified by reading the current pipeline end-to-end is
`BlueskyIngestor.stream() -> Iterator[Dict]`. Everything downstream — `_to_comment()`,
`SlidingWindowTripwire`, `RedisStateManager`, the concentration-burst / coordination /
recurrence logic in `bluesky_anomaly_shadow.py` — consumes that dict shape and has no
knowledge of where it came from. **Kafka only replaces what produces items for that
iterator.** If the dict shape at the consumer's exit point matches what
`_normalize_post()` currently yields (`src/ingestion/bluesky.py`), no other file needs
to change. Every phase below is designed to protect that invariant.

A second invariant, made explicit because Kafka introduces at-least-once delivery
where Jetstream (as currently consumed) effectively didn't: **Redis ingestion must be
idempotent before any Kafka consumer is allowed to write into a detector Redis
uses for real evaluation.** This is why Phase C is sequenced before Phase D.

## Status legend

`☐` not started · `◐` in progress · `☑` done

---

## Phase A — Contract and configuration ☑

No live producer or consumer. Pure code + config + tests — safe to merge without any
running infrastructure.

### Deliverables (as built)

- **Kafka configuration loading** — `src/streaming/kafka_config.py`. A frozen
  `BlueskyKafkaConfig` dataclass (deliberately Bluesky-scoped, not a generic
  `KafkaConfig` — see rationale in the class's docstring) with a `from_env()`
  classmethod, reading env vars with the same `os.environ.get(NAME, default)`
  pattern already used in `bluesky_anomaly_shadow.py`. A `validate()` method
  fail-fasts with a clear `KafkaConfigError` when `BSKY_INPUT_MODE=kafka` but
  `KAFKA_BOOTSTRAP_SERVERS` is unset, or when a SASL security protocol is selected
  without credentials — added during plan review so a misconfigured Kafka-mode
  process refuses to start instead of failing repeatedly and opaquely once it tries
  to connect. `sasl_password` is excluded from `repr()`/`str()` so it can never leak
  via an accidental `print(config)`/log line.
- **Message-envelope construction** — `src/streaming/envelope.py`. A thin JSON
  wrapper around the existing normalized item dict, not a new payload shape:

  ```json
  {
    "schema_version": 1,
    "source": "bluesky",
    "produced_at": 1784812268,
    "key": "at://did:plc:xxx/app.bsky.feed.post/yyy",
    "payload": { "...": "the exact dict _normalize_post() already yields" }
  }
  ```

  `key` doubles as the Kafka partition/message key (see Discussion — partitioning).
  `payload` is deliberately the untouched normalized dict so Phase D's consumer can
  unwrap the envelope and feed `payload` into the same `_to_comment()` unchanged.
- **Serialization** — JSON, matching every other wire/storage format already in this
  codebase (Redis ZSET payloads, JSONL event logs, GCS summaries).
- **Validation** — `envelope.validate_envelope(dict) -> None`, raising
  `EnvelopeValidationError` on the first problem found: envelope-level keys missing
  or wrong-typed, or `payload` missing any key from the shared item dict schema
  (`REQUIRED_PAYLOAD_FIELDS` — a module-level constant mirroring `STRUCTURE.md`'s
  documented shape). Reject-and-log philosophy, matching `bluesky.py`'s drop-reason
  handling; callers (Phase D's consumer) use try/except rather than letting this
  crash a main loop.
- **Unit tests** — `tests/test_kafka_config.py` (24 total cases across both files)
  covering config defaults, fail-fast validation for every mode/protocol
  combination, secret-repr safety, and dataclass immutability; and
  `tests/test_kafka_envelope.py` covering valid round-trips and a parametrized
  matrix of invalid envelopes (missing fields, wrong types, malformed JSON).
- **`confluent-kafka` dependency** — added to `requirements.txt`. Installed cleanly
  via a prebuilt wheel on Windows dev (`confluent_kafka-2.15.0-cp313-win_amd64`);
  still worth a smoke-install on the actual e2-micro Ubuntu VM before Phase B, since
  a native-build failure there would be an infra blocker rather than a code one.

### Explicitly out of scope for Phase A

No running broker required. No producer process. No consumer process. No changes to
`bluesky.py`, `bluesky_anomaly_shadow.py`, `state_manager.py`, or `sliding_tripwire.py`.
No commit-strategy/offset-reset/acks/idempotence settings (those belong to the
Phase B/D adapters, not to config). No TLS certificate-path config (deferred until
broker hosting is decided).

---

## Phase B — Producer only ☐

### Deliverables

- New process, e.g. `src/streaming/bluesky_producer.py`: wraps `BlueskyIngestor().stream()`
  (unmodified — reuses the existing filter/route/normalize pipeline exactly as-is),
  wraps each yielded dict in the Phase A envelope, and publishes to
  `KAFKA_TOPIC_BLUESKY_ROUTED` (default `surge.bluesky.routed.v1`) using
  `confluent-kafka`'s `Producer`, configured via `BlueskyKafkaConfig.from_env()`.
- **Delivery failure handling** — publish failures must not crash the process or
  silently vanish. Reuse the existing debug-output convention
  (`data/debug/bluesky/` — see `dropped/*.jsonl` and `runs/run_summary_*.json` in
  `bluesky.py`) for a `kafka_produce_errors_*.jsonl` log, so operators have the same
  visibility they already have for routing drops.
- **New systemd unit** — `deploy/bluesky-kafka-producer.service`, following the shape
  of the existing three units in `deploy/` (`Type=simple`, `Restart=on-failure`,
  journal logging).
- **Manual verification** — confirm end-to-end delivery with the stock Kafka console
  consumer, e.g.:
  ```bash
  kafka-console-consumer.sh --bootstrap-server <host:port> \
    --topic surge.bluesky.routed.v1 --from-beginning | head -20
  ```
  Confirm envelope shape matches Phase A's schema and `payload` fields match what
  `bluesky.py`'s `_normalize_post()` currently produces.

### Explicitly unchanged in Phase B

`bluesky_anomaly_shadow.py` keeps its own independent `BlueskyIngestor().stream()`
call and direct Jetstream connection, exactly as today. Two processes, two separate
WebSocket connections to Jetstream, zero coupling — the producer is purely additive
and cannot affect the existing shadow detector even if it misbehaves.

---

## Phase C — Idempotent Redis behavior ☐

### The problem

`RedisStateManager.ingest()` (`src/storage/state_manager.py:78-81`) currently builds
every ZSET member with a fresh `uuid4().hex` prefix:

```python
member = f"{uuid4().hex}:{json.dumps({...})}"
self.r.zadd(key, {member: comment.timestamp})
```

Every call creates a brand-new, guaranteed-unique member — so re-ingesting the same
logical post twice (Kafka at-least-once redelivery after a consumer restart or
rebalance) currently produces **two** ZSET entries, inflating `get_window_count()`
and corrupting the z-score math the whole detector depends on. This must be fixed
before any Kafka consumer writes into a detector that's actually being evaluated.

### Deliverables

- Extend `RedisStateManager.ingest()` (or add a sibling method) to build the ZSET
  member key from a **stable identity** when one is available, instead of always
  using a random UUID. `Comment.platform_uri` already exists for this purpose
  (`src/models.py:22` — populated for Bluesky via `_to_comment()`'s
  `platform_item_uri` mapping, always `""` for HN comments today). Proposed rule:
  member key = `comment.platform_uri` if non-empty, else `uuid4().hex` (preserves
  current random-key behavior for every existing HN caller with zero code changes on
  the HN side).
- Since `ZADD key {member: score}` on an **existing** member updates its score rather
  than duplicating the entry, a stable member key makes re-ingestion of the same post
  naturally idempotent — no separate dedup table needed.
- Make this opt-in/backward-compatible: this is the "optional stable identity" from
  the task description — behavior only changes when `platform_uri` is populated,
  which today is Bluesky-only.

### Tests

- **Same URI twice → one Redis member.** Ingest the same `Comment` (same
  `platform_uri`) twice; assert `ZCARD window:{channel}` == 1 after both calls, and
  the stored score reflects the second call's timestamp.
- **Different URIs → two Redis members.** Ingest two `Comment`s with distinct
  `platform_uri` values; assert `ZCARD` == 2.
- **HN/default mode → old behavior preserved.** Ingest a `Comment` with
  `platform_uri=""` (HN shape) twice; assert `ZCARD` == 2 (still creates two separate
  entries, matching current production behavior — HN has no delivery-duplication risk
  today, so this must not silently change).

Use `fakeredis` for these tests, matching the existing pattern in
`tests/test_sliding_tripwire.py` and `scripts/test_event_lifecycle.py`.

---

## Phase D — Consumer and isolated detector test ☐

### Deliverables

- Add Kafka input as an alternate source for the anomaly runner. Recommended
  approach: a small adapter that consumes envelopes, validates them (Phase A's
  `validate_envelope`), unwraps `payload`, and yields it — giving an
  `Iterator[Dict]` with the exact same shape `BlueskyIngestor.stream()` yields, so
  `bluesky_anomaly_shadow.py`'s `for item in stream:` loop and everything after it
  (`_to_comment()`, `tripwire.ingest()`) needs **zero changes**.
- **Temporary consumer group** — a throwaway group id (e.g.
  `surge-bluesky-anomaly-v1-test`), distinct from `KAFKA_BSKY_ANOMALY_GROUP_ID`'s
  production default (`surge-bluesky-anomaly-v1`, see Phase A), so offsets can be
  reset freely during testing without any risk of colliding with the later real
  deployment.
- **Temporary Redis DB 2** — distinct from DB 0 (HN production) and DB 1 (existing
  Bluesky Jetstream-direct shadow, per the docstring in `bluesky_anomaly_shadow.py`).
  This keeps the entire experiment isolated: if something goes wrong, `redis-cli -n 2
  FLUSHDB` and re-run, with zero effect on the DB 1 shadow that's still running
  Jetstream-direct the whole time.
- `BSKY_INPUT_MODE` (default `jetstream`, alternate `kafka`) was already introduced
  in Phase A since `BlueskyKafkaConfig.validate()`'s fail-fast check depends on it —
  Phase D is where it's first actually exercised to switch the anomaly runner's
  input source.

### Verification

- Confirm a message published by the Phase B producer becomes a valid `Comment`
  object (compare field-by-field against what `_to_comment()` would produce from the
  same raw item via the direct Jetstream path) and reaches
  `SlidingWindowTripwire.ingest()` without error.
- Confirm the isolated DB 2 tripwire can complete a full evaluation tick
  (`evaluation_tick()`) against Kafka-sourced data without touching DB 0 or DB 1.
- Confirm malformed/invalid envelopes (per Phase A's validator) are logged and
  skipped, not crashing the consumer loop.

---

## Phase E — Failure and restart testing ☐

Run against the Phase D isolated setup (temp group, DB 2) — never against DB 1 or a
production path.

### Required tests (from the task)

- Consumer restart — offsets resume correctly, no gap, no crash on startup.
- Duplicate message (simulate redelivery) — Phase C's idempotent Redis write means
  no double-count; assert `ZCARD` unaffected by a manually replayed message.
- Redis unavailable — consumer's behavior when `RedisStateManager` can't reach Redis:
  should log and either retry-with-backoff or stop cleanly, not crash-loop silently
  or drop messages without a trace.
- Kafka unavailable — producer and consumer both need defined behavior (retry with
  backoff, don't crash, surface the outage in whatever monitoring exists).
- Malformed message — already covered by Phase A/D validation, re-verify under this
  phase's broader failure-injection pass.
- Quiet topic (no messages for an extended period) — confirm this looks identical to
  "genuinely quiet channel" in the summary output, not indistinguishable from "broken."
- Consumer lag accumulation and recovery — inject a burst, let lag build, confirm the
  consumer catches back up and that late-arriving messages still land in the correct
  Redis ZSET score bucket (window scoring uses `comment.timestamp`, i.e. event time,
  not consume-time — verify this holds true when catching up from lag, not just in
  the steady state).

### Additional tests worth adding

- **Partition rebalance mid-processing** — distinct from a clean restart: trigger a
  rebalance while messages are in flight (e.g. scale consumer group members up/down)
  and confirm no message is processed twice in a way that breaks idempotency, and no
  message is silently dropped during the handoff.
- **Poison-pill message** — a message that deterministically fails deserialization on
  every retry. Confirm the consumer skips-and-logs it and keeps advancing, rather than
  getting stuck retrying the same offset forever (a plain "malformed message" test may
  not catch a retry loop; this one specifically does).
- **Offset commit strategy** — explicitly test that offsets are committed *after*
  successful Redis write (at-least-once, not at-most-once). This is the guarantee
  Phase C's idempotent write exists to make safe; worth a dedicated test that kills
  the consumer between "Redis write succeeded" and "offset committed" and confirms
  the replayed message on restart is a harmless duplicate, not silently lost data.
- **Health/monitoring visibility during outages** — extend or reuse
  `src/monitoring/health.py`'s stall-detection pattern so a Kafka-mode consumer
  reports "stalled" distinctly from "quiet," matching the ask in the "Quiet topic"
  test above.

---

## Phase F — Bluesky cutover ☐

Only after every Phase E test passes against the isolated DB 2 setup.

### Steps (from the task)

1. Stop the direct-Jetstream anomaly runner (`bluesky_anomaly_shadow.py` in its
   current `jetstream` mode).
2. Retain **Redis DB 1** — this is the key risk-reducer: production Bluesky detection
   state doesn't move to a fresh, unwarmed database. The Kafka-mode consumer picks up
   the same DB 1 history/baseline the Jetstream-direct process was building.
3. Set `BSKY_INPUT_MODE=kafka`.
4. Start the Phase B producer (now pointed at real, non-test infrastructure).
5. Start the Kafka-mode anomaly consumer, writing into DB 1.

### Recommended additions

- **Parallel-run before hard cutover.** Rather than an instant switch, run the Kafka
  producer + consumer against a scratch DB (reuse DB 2, or a new DB 3) *in parallel*
  with the still-running Jetstream-direct DB 1 process for a defined window (e.g.
  24–48h), and diff `channel_stats`/`kept_by_channel` counts between the two. Only
  flip DB 1 over to Kafka-mode once parity is confirmed. This turns Phase F from a
  single risky switch into a comparison + a low-risk switch.
- **Explicit rollback trigger criteria**, decided in advance rather than improvised
  under pressure — e.g. consumer lag exceeding N minutes sustained, missing-event
  rate vs. the Jetstream-direct baseline exceeding some threshold, or any Redis
  window-count discrepancy discovered post-cutover. `BSKY_INPUT_MODE=jetstream`
  remains the rollback path — confirm the flag is a true single-line revert (kill
  Kafka-mode consumer, restart the old direct process pointed at the same DB 1) with
  no other manual steps required.
- **Documentation pass** — update `deploy/` with the final unit files actually used,
  and update `README.md`/`STRUCTURE.md` to describe the Kafka-mode path once it's the
  live one, same as was done for the original Bluesky shadow-pipeline integration.
- **Cutover "done" criteria** — define explicitly (e.g. N consecutive days at
  near-zero lag, zero rollback triggers, event counts within X% of the historical
  Jetstream-direct baseline) so "cutover complete" isn't a subjective call.

Keep `jetstream` mode available and tested indefinitely as the rollback path, per the
task's instruction — don't delete `BlueskyIngestor`'s direct-connection code path.

---

## Configuration reference (as implemented in Phase A)

`BlueskyKafkaConfig` in `src/streaming/kafka_config.py`:

| Variable | Introduced | Default | Purpose |
|---|---|---|---|
| `BSKY_INPUT_MODE` | Phase A | `jetstream` | `jetstream` \| `kafka` — the Phase F cutover switch; `validate()`'s fail-fast check depends on it |
| `KAFKA_BOOTSTRAP_SERVERS` | Phase A | `""` | Broker address(es) — empty is only safe when `input_mode="jetstream"` |
| `KAFKA_TOPIC_BLUESKY_ROUTED` | Phase A | `surge.bluesky.routed.v1` | Topic name — "routed" because the Kafka boundary sits after filter/route/normalize, not raw firehose |
| `KAFKA_BSKY_ANOMALY_GROUP_ID` | Phase A | `surge-bluesky-anomaly-v1` | Dedicated to the anomaly consumer; a future stats consumer gets its own `KAFKA_BSKY_STATS_GROUP_ID`, never shares this |
| `KAFKA_SECURITY_PROTOCOL` | Phase A | `PLAINTEXT` | `SASL_PLAINTEXT`/`SASL_SSL` for authenticated brokers |
| `KAFKA_SASL_MECHANISM` | Phase A | `PLAIN` | Only enforced by `validate()` when protocol requires SASL |
| `KAFKA_SASL_USERNAME` | Phase A | `""` | |
| `KAFKA_SASL_PASSWORD` | Phase A | `""` | Excluded from `repr()`/`str()` — never logged |
| `BSKY_KAFKA_REDIS_DB` | Phase D (not yet implemented) | `2` | Isolated test DB; unused once Phase F retains DB 1 |

## Redis DB usage across phases

| DB | Owner | Phase introduced | Notes |
|---|---|---|---|
| 0 | HN production (`main.py`) | pre-existing | Untouched by this entire plan |
| 1 | Bluesky shadow (`bluesky_anomaly_shadow.py`, Jetstream-direct) | pre-existing | Becomes the Kafka-mode consumer's target in Phase F |
| 2 | Kafka consumer isolation test | Phase D | Scratch, freely flushable; also usable for Phase F's parallel-run comparison |

## New files introduced by this plan

| File | Phase | Status |
|---|---|---|
| `src/streaming/__init__.py` | A | ☑ |
| `src/streaming/kafka_config.py` | A | ☑ |
| `src/streaming/envelope.py` | A | ☑ |
| `tests/test_kafka_config.py` | A | ☑ |
| `tests/test_kafka_envelope.py` | A | ☑ |
| `src/streaming/bluesky_producer.py` | B | ☐ |
| `deploy/bluesky-kafka-producer.service` | B | ☐ |
| Redis idempotency changes in `src/storage/state_manager.py` | C | ☐ |
| Kafka-mode input adapter in `src/shadow/bluesky_anomaly_shadow.py` (or a sibling module) | D | ☐ |
| `deploy/bluesky-kafka-consumer.service` (or updated `bluesky-anomaly-shadow.service`) | F | ☐ |

---

## Open questions for discussion

1. **Broker choice** — self-hosted single-node Kafka (e.g. Docker on the existing VM
   or a separate small instance) vs. a managed service (Confluent Cloud, AWS MSK,
   etc.)? This affects `KAFKA_SECURITY_PROTOCOL`/SASL config in Phase A and cost.
2. **Partitioning** — the proposed envelope uses the post URI as key, which spreads
   load evenly but gives no per-channel ordering guarantee. Given the tripwire is
   pull-based (`evaluation_tick()` reads the current Redis ZSET state, not a stream)
   and Redis scores by event `timestamp` not arrival order, strict ordering doesn't
   appear to matter for correctness — worth confirming that reasoning before settling
   on a partition key, since channel-keyed partitioning would be the alternative if
   ordering ever does matter (e.g. for a future feature).
3. **Topic retention / replay value** — is there a desire to use Kafka retention for
   backfill/replay of Bluesky data (an existing gap — `bluesky.py` has no offline CSV
   ingestor equivalent to `HNCsvIngestor`)? If yes, that shapes retention config and
   might be worth calling out as a nice side-benefit of this migration in the plan.
4. **Who runs the producer relative to `BlueskyIngestor`'s existing filter/route
   logic** — Phase A's envelope and topic naming (`surge.bluesky.routed.v1`, "routed"
   not "raw") already commit to filtering/routing happening in the producer, before
   Kafka. Flagging this as effectively decided by the Phase A implementation, not
   fully open anymore — revisit only if a raw-firehose use case appears later
   (`surge.bluesky.raw.v1` was named specifically to leave room for that without
   collision).
5. **`author_handle`** is defined in the normalized dict schema but never populated
   today (`bluesky.py`, always `""`). Worth deciding whether the Kafka envelope should
   carry it anyway for forward-compatibility, or intentionally omit it.
