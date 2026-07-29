# Surge Detection Engine — Source Structure

Real-time event detection across social discussion platforms. The pipeline spots when the internet starts talking about something before it becomes mainstream news, by monitoring topic channels, detecting statistical surges, and consolidating repeated signals into typed TrackedEvents.

**Current platforms:** Hacker News (live, production) and Bluesky (live, shadow-mode — ingesting and detecting in parallel, not yet wired into production alerting). Reddit ingestion is pending API approval. The architecture is deliberately split so all platform-specific code lives in one place — everything downstream is platform-neutral.

---

## How a new platform plugs in

Two touch-points only:

1. **`src/ingestion/`** — implement `DataIngestor.stream()` (the ABC in `base.py`). The method must yield normalized item dicts in the shared schema (see `_HNItemProcessor._to_dict()` in `hacker_news.py` for the field contract). No other file changes required for ingestion.

2. **`src/events/models.py`** — `EventCandidate.source` is already a free-text string (`"hacker_news"`, `"reddit"`, `"bluesky"`, …). The consolidator groups candidates by source via `SameChannelPolicy`, which gates on `evidence.source in event.sources`. Multi-source events are supported; no schema changes needed.

Everything else — the Z-score detector, NLP enricher, classifier, consolidator, matching policy, and all reporting — is platform-neutral and requires no changes.

---

## Directory map

```
src/
  main.py
  models.py
  backfill_clusters.py
  backtest_driver.py
  hn_backtest_driver.py

  ingestion/          ← platform boundary (only place that knows about HN / Bluesky)
    base.py
    hacker_news.py
    reddit.py
    bluesky.py                 Jetstream WebSocket adapter + 12-channel topic router
    bluesky_hashtag_stats.py    Passive hashtag-discovery stats collector

  pipeline/           ← anomaly detection + NLP enrichment
    sliding_tripwire.py
    tripwire.py
    context.py
    alert_gate.py
    filter.py

  storage/            ← Redis state + Parquet writers
    state_manager.py
    parquet_archiver.py
    candidate_archiver.py

  events/             ← typed event layer (platform-neutral)
    models.py
    classifier.py
    candidate_builder.py
    evidence.py
    matching_policy.py
    consolidator.py
    store.py

  alerting/
    webhooks.py

  monitoring/
    health.py

  shadow/             ← standalone Bluesky shadow runners (not wired into main.py)
    bluesky_shadow.py           Stats-only ingestion audit
    bluesky_anomaly_shadow.py   Full anomaly-detection shadow (Redis DB 1, isolated)

  reporting/          ← offline tools and BI export
    batch_enricher.py
    dashboard_generator.py
    export_powerbi.py
    inspect_candidates.py
    inspect_events.py

tests/                ← regression suite (mostly standalone scripts, one true pytest file)
  test_router.py
  test_recurring_thread.py
  test_sliding_tripwire.py
  test_jetstream.py
  regression_merge_baseline.py

scripts/              ← operational/diagnostic tools run against live or GCS-backed data
  hashtag_report.py
  test_anomaly_replay.py
  test_event_lifecycle.py
  test_jetstream.py            ← different file from tests/test_jetstream.py, see below

deploy/               ← systemd unit templates for the three production processes
  hn-ingestor.service
  bluesky-shadow.service
  bluesky-anomaly-shadow.service
```

---

## `src/` — top level

### `main.py`
Entry points for the live pipeline. `live_hn()` wires together the HN ingestor, Redis state manager, sliding-window tripwire, Parquet archiver, and webhook dispatcher, then loops forever. Also contains a Reddit CSV backtest entry point (offline).

### `models.py`
Shared primitive dataclasses used across the pipeline: `AnomalyEvent` (a fired surge detection) and `Comment` (a single platform item before channel assignment). These are the pipeline's internal currency — not platform-specific.

### `backfill_clusters.py`
One-shot script that re-runs NLP enrichment on already-archived raw Parquet files. Used to retroactively classify anomalies that were detected before the enricher existed.

### `backtest_driver.py`
Drives a full offline backtest against a Reddit CSV dump: ingestor → tumbling-window tripwire → Parquet archiver. Used to validate the detection pipeline without a live API connection.

### `hn_backtest_driver.py`
Same as `backtest_driver.py` but uses `HNCsvIngestor` and the sliding-window tripwire, matching the live HN pipeline exactly. Used to replay historical HN data through the current detection logic.

---

## `src/ingestion/` — platform boundary

This is the **only** package that is platform-specific. Adding Bluesky means adding one file here and nowhere else.

### `base.py`
**`DataIngestor` (ABC)** — the single extension point for all platforms.

```python
class DataIngestor(ABC):
    @abstractmethod
    def stream(self) -> Iterator[Dict]: ...
```

A Bluesky ingestor must subclass `DataIngestor` and implement `stream()`, yielding normalized item dicts in the shared schema. The rest of the pipeline consumes these dicts without knowing which platform produced them.

**Shared item dict schema** (contract between ingestor and pipeline):

| Key | Type | Description |
|-----|------|-------------|
| `id` | str | Platform item ID |
| `subreddit` | str | Assigned topic channel (e.g. `"ai"`, `"security"`) |
| `body` | str | Cleaned text (HTML stripped) |
| `timestamp` | int | Unix timestamp |
| `author` | str | Username |
| `score` | int | Upvotes / like count |
| `story_id` | int | Root post ID (0 if unknown) |
| `story_title` | str | Root post title |
| `domain` | str | Linked domain (empty for replies) |
| `item_type` | str | `"story"` or `"comment"` |
| `item_id` | int | Same as `id` as int |
| `created_at` | int | Same as `timestamp` |
| `routing` | dict\|None | Channel routing audit (None if evicted) |

### `hacker_news.py`
Three classes for HN ingestion:

- **`HNTopicRouter`** — classifies a story title + URL into one of 8 topic channels (`ai`, `security`, `startup`, `crypto`, `science`, `tech`, `policy`, `general`) using weighted keyword scoring and domain boosts. Bluesky posts would use the same router, or a Bluesky-specific router that maps AT Protocol post metadata.

- **`_HNItemProcessor`** — shared mixin holding the item cache, story-to-channel lookup, and `_process() / _to_dict()` normalization logic. Both live and CSV ingestors inherit this. A Bluesky ingestor would write its own equivalent normalization, then output the same dict schema.

- **`HackerNewsIngestor`** — live Firebase REST API ingestor. Polls the HN `/maxitem` endpoint every 5 s, fetches new items concurrently (asyncio + aiohttp, 20-way semaphore), and feeds a `queue.Queue` consumed by the synchronous `stream()`. The sync/async bridge (background thread + queue) is the pattern to replicate for a Bluesky Firehose ingestor.

- **`HNCsvIngestor`** — replays a HN CSV dump in timestamp order through the same `_process()` path as the live ingestor. Used for backtests. A Bluesky offline ingestor (e.g. reading a JSONL Firehose dump) would follow the same pattern.

### `reddit.py`
**`ZstFileIngestor`** — reads a `.zst`-compressed Reddit comment dump and yields normalized dicts. Backtest-only (Reddit live API is pending). Demonstrates the minimal ingestor pattern: implement `stream()`, normalize each row to the shared schema, yield.

### `bluesky.py`
Live Bluesky ingestion, the first non-HN platform actually wired in. Two classes plus normalization helpers:

- **`BlueskyTopicRouter`** — classifies a post into one of **12** topic channels (`cybersecurity`, `ai_tech`, `war_diplomacy`, `us_politics`, `activism_rights`, `climate_weather`, `health_medicine`, `science_space`, `money_markets`, `social_platforms`, `sports`, `entertainment_fandom`) or `"general"`. Scoring is a 4-layer pipeline: body text scan → embed (link-preview) text scan → hashtag scan (aliased/CamelCase-split, weight × `_HASHTAG_WEIGHT_MULT` = 1.5) → domain boost via `_DOMAIN_BOOSTS` (~40 known domains). After combining scores, a **context gate** (`_CONTEXT_GATE`) discounts `cybersecurity`/`science_space`/`health_medicine` by ×0.25 when the only matched keywords are "weak" (ambiguous) and no "strong" co-signal term appears anywhere in the text — this is what stops a gaming post mentioning "exploit" from routing to `cybersecurity`. A per-channel score floor (`_MIN_SCORE`, currently only `social_platforms` ≥ 2.0) excludes bare platform-name mentions. `classify_with_audit()` returns the full scoring trail (winner, runner-up, margin, matched keywords, domain boost used); `classify()` is a thin wrapper returning just the channel string.

- **`BlueskyIngestor(DataIngestor)`** — connects to the Jetstream WebSocket firehose using the same sync/async bridge pattern as `HackerNewsIngestor` (`stream()` spawns a background thread running an asyncio event loop, yields items off a bounded queue). Each incoming post passes through a 9-stage filter (non-commit event, empty text, non-English, language-tag mismatch, adult-hashtag spam via `_ADULT_HASHTAGS`, recurring game-share templates, unmatched topic) before being routed and queued; every dropped post is logged with its reason to `data/debug/bluesky/dropped/*.jsonl` for auditing. Reconnects with exponential backoff (5s → 60s cap) on any WebSocket error.

**Connection:** imports only `.base` (`DataIngestor`) and sibling module `.bluesky_hashtag_stats` (`HashtagStatsCollector`) — no `src/models.py`, `src/pipeline/`, or `src/storage/` — preserving the "ingestion is the only platform-aware package" rule stated above. Imported by `src/shadow/bluesky_shadow.py`, `src/shadow/bluesky_anomaly_shadow.py`, and `tests/test_jetstream.py`.

### `bluesky_hashtag_stats.py`
**`HashtagStatsCollector`** — bounded, tiered hashtag-discovery collector run alongside `BlueskyIngestor`. Every hashtag gets a cheap total/matched/unmatched count (capped at 50,000 unique hashtags); once a hashtag crosses 5 occurrences it's "promoted" to rich tracking (co-occurring channels, co-hashtags, external domains, a few body-text samples, capped author count), capped at 500 simultaneously-promoted hashtags. `write_snapshot()` atomically writes the current state to JSON, feeding `scripts/hashtag_report.py`'s keyword-discovery reports — used to decide what to add to `BlueskyTopicRouter`'s keyword lists.

**Connection:** fully self-contained (stdlib only) — imports nothing from the rest of `src/`. Imported only by `bluesky.py` (`from .bluesky_hashtag_stats import HashtagStatsCollector`); `scripts/hashtag_report.py` consumes its JSON output file directly rather than importing the class, keeping the report generator decoupled from the collector's implementation.

---

## `src/pipeline/` — anomaly detection and NLP

### `sliding_tripwire.py`
**`SlidingWindowTripwire`** — the core detector. Maintains a 2-hour sliding window per channel in Redis, computes a Z-score against 7 days of hourly baseline counts, and fires `AnomalyEvent` objects when Z ≥ 3.0 (Schmitt trigger with Z ≤ 2.0 release). Completely decoupled from storage — talks only to the `StateManager` interface. The same detector drives both HN live and any future Bluesky live pipeline without modification.

### `tripwire.py`
**`TumblingWindowTripwire`** — simpler tumbling (non-sliding) window detector. Reddit backtest only. Not used in the live pipeline.

### `context.py`
**`DBSCANContextEngine`** — the NLP enrichment step. Takes raw texts from an `AnomalyEvent`, runs `SentenceTransformer → UMAP → DBSCAN → c-TF-IDF` to produce dense clusters with keywords. Platform-neutral: only cares about the text content of items, not their source.

### `alert_gate.py`
**`AlertGate`** — per-channel 30-minute cooldown gate. Suppresses rapid re-alerting when a channel stays ELEVATED for an extended period. Platform-neutral.

### `filter.py`
**`SubredditFilter`** — originally written for Reddit. Routes items to channels and filters bot authors. The HN pipeline uses `HNTopicRouter` instead; a Bluesky pipeline would use its own router. This file is Reddit-specific and would not need changes for Bluesky.

---

## `src/storage/` — persistence

### `state_manager.py`
**`StateManager` (abstract interface) + `RedisStateManager`** — maintains the sliding-window ZSET and hourly baseline history per channel in Redis. `SlidingWindowTripwire` depends only on the abstract interface, so a Bluesky pipeline running on a different Redis instance (or an alternative backend) requires no changes to the detector.

### `parquet_archiver.py`
**`ParquetArchiver`** — atomically writes raw `AnomalyEvent` Parquet files to GCS. One file per fired anomaly. Platform-neutral: the Parquet schema includes a `source` field that already distinguishes `hacker_news` from future platforms.

### `candidate_archiver.py`
**`CandidateArchiver`** — atomically writes `EventCandidate` Parquet files to local disk (date-partitioned). Used by the offline enricher after NLP classification. Platform-neutral.

---

## `src/events/` — typed event layer

All files in this package are **fully platform-neutral**. No changes required for Bluesky.

### `models.py`
Two core dataclasses:

- **`EventCandidate`** — one NLP cluster from one anomaly window, classified and scored. The `source` field (`"hacker_news"`, `"bluesky"`, …) is how the consolidator tracks platform provenance. `kind` is `"viral_post"`, `"topic_surge"`, `"event_candidate"`, or `"recurring_thread"`.

- **`TrackedEvent`** — a persistent, multi-candidate event spanning minutes to hours. Aggregates candidates via the consolidator. `sources` is a list so a single TrackedEvent can span multiple platforms (e.g. an event discussed on both HN and Bluesky simultaneously).

Also contains `make_display_keywords()` — strips generic noise words from raw keywords for display and export (raw `keywords` field is never modified; `display_keywords` is the cleaned copy).

### `classifier.py`
**`classify(cluster, z_score, channel)`** — pure function. Maps one enriched cluster dict to `(kind, event_score)`. No I/O, no state, fully platform-neutral. The 0–1 `event_score` formula weights conversation diversity, domain spread, volume, anomaly strength, and keyword coherence.

### `candidate_builder.py`
**`CandidateBuilder`** — constructs `EventCandidate` objects from enriched cluster dicts. Applies `_RECURRING_THREAD_MARKERS` to force known HN megathreads (e.g. "who is hiring") to `recurring_thread` kind so they never promote to `event_candidate`. A Bluesky equivalent would add Bluesky-specific recurring post markers here without affecting HN logic.

### `evidence.py`
**`EventEvidence`** — lightweight adapter that extracts the fields `SameChannelPolicy.score()` needs from an `EventCandidate`. Decouples the policy from the full candidate schema.

### `matching_policy.py`
**`MatchingPolicy` (ABC) + `SameChannelPolicy`** — the merge decision layer.

```python
class MatchingPolicy(ABC):
    @abstractmethod
    def score(self, evidence: EventEvidence, event: TrackedEvent) -> Tuple[float, dict]: ...

    @property
    @abstractmethod
    def merge_threshold(self) -> float: ...
```

`SameChannelPolicy` is the V1 implementation: same source + same channel + 90-minute gap gate, scored on anchor conversation match (0.25), reverse anchor (0.10), conversation Jaccard overlap (0.30), keyword Simpson overlap (0.15), and domain overlap (0.05). Hard gates block merges when keyword evidence is absent. A future `CrossSourcePolicy` could implement the ABC to allow HN + Bluesky candidates about the same topic to merge into one `TrackedEvent`.

### `consolidator.py`
**`EventConsolidator`** — reads `EventCandidate` Parquet files for a date range, sorts by time, and runs each candidate through `MatchingPolicy.score()` to either merge it into an existing `TrackedEvent` or seed a new one. Writes `TrackedEvent` JSONL to `data/events/`. Platform-neutral: handles any mix of sources present in the candidates directory.

### `store.py`
**`EventStore`** — reads `TrackedEvent` JSONL files from `data/events/`. Used by all reporting tools to load events without knowing how they were serialized.

---

## `src/alerting/`

### `webhooks.py`
**`WebhookDispatcher`** — fire-and-forget HTTP POST to Discord or Slack. Called by the live pipeline when an `AnomalyEvent` fires. Platform-neutral payload format.

---

## `src/monitoring/`

### `health.py`
**`write_health()`** — writes a JSON health file to disk on each poll cycle. Consumed by external monitoring to detect ingestor stalls (both live HN and any future Bluesky live ingestor would call this).

---

## `src/shadow/` — standalone Bluesky shadow runners

Both files here are **standalone scripts**, never imported by any other Python module (only invoked via the `deploy/bluesky-*.service` systemd units). They deliberately do not touch `main.py` — Bluesky was integrated as a parallel, isolated shadow pipeline rather than by extending the production HN entry point, so it could be observed and tuned without any risk to live HN alerting.

### `bluesky_shadow.py`
Stats-only ingestion audit runner — no detection logic at all. Streams `BlueskyIngestor`, keeps a per-channel `Counter` and a rolling 100-post sample (`deque`), rewrites `data/debug/bluesky/health.json` and `routed_samples/latest_routed_pretty.json` every 30s, and uploads the whole debug tree to GCS every 300s. Answers exactly one question: "is ingestion healthy and what is it routing right now?"

**Connection:** imports `BlueskyIngestor` from `src/ingestion/bluesky.py` (lazily, inside `main()`, so import errors surface immediately at startup) and `google.cloud.storage` (lazily, only when an upload is attempted). Not imported by anything else; run only via `deploy/bluesky-shadow.service`.

### `bluesky_anomaly_shadow.py`
The full anomaly-detection shadow — wires the real Bluesky stream into the same `SlidingWindowTripwire` + `AlertGate` used by the HN pipeline, but pointed at an isolated **Redis DB 1** (production HN uses DB 0) with no webhook dispatch, so operators can validate Bluesky detection quality before it's ever wired into live alerting. Key pieces:

- **Event lifecycle** — same start/update/release Schmitt-trigger hysteresis as HN (enter ELEVATED at Z ≥ 3.0, release at Z ≤ 2.0), but every threshold is env-overridable (`BSKY_Z_THRESHOLD`, `BSKY_WINDOW_TTL`=3600s i.e. a 1-hour window vs. HN's 2-hour, `BSKY_MIN_HISTORY`=6h, `BSKY_EVAL_INTERVAL`, `BSKY_BASELINE_INTERVAL`).
- **`_enrich_items()`** — builds `opening`/`peak` window snapshots per event: keyword/hashtag/domain/author frequency counts, the first 5 posts (with full routing-audit fields), and two dedup/coordination signals — `top_text_pct` (share of items with the same 80-char text prefix) and `text_unique_ratio` (distinct prefixes / total items).
- **`_is_concentration_burst()`** — two independent feed-detection rules: (1) known-feed-domain — `_KNOWN_FEED_DOMAINS = {"science_space": {"arxiv.org", "biorxiv.org", "medrxiv.org"}}`, fires when that domain is ≥60% of a ≥100-item window; (2) single-source aggregator — fires when one author covers >40% AND one domain covers >35% of a ≥50-item window, for any channel. `feed_burst_suspect` is only confirmed if **both** the opening and peak windows are concentrated.
- **`_find_recurring()`** — flags likely scheduled/bot bursts by checking if any of the last 50 completed events in the same channel started within ±45 minutes of the same UTC time-of-day (`recurring_prior` / `recurring_feed_suspect`).
- **`coordinated_suspect`** — separately flagged (independent of feed-burst) when `top_text_pct` exceeds 0.25 at either window and the event isn't already a feed burst — catches copy-paste template spam (e.g. activism hashtag chains) that a single bot-domain rule wouldn't.
- No context-gate logic lives here — the only gate is `AlertGate`, and it's instantiated in-memory only (no Redis backend), so its cooldown state resets on every restart.
- **Output** — `anomaly_summary_latest.json` (rewritten every 300s: channel stats, active surges, last 50 completed events) and `anomaly_events_{run_ts}.jsonl` (one appended line per released event, containing the full opening/peak enrichment and all forensic flags above), both mirrored to GCS every 300s.

**Connection:** imports `BlueskyIngestor` (`src/ingestion/bluesky.py`), `Comment` (`src/models.py`), `SlidingWindowTripwire` and `AlertGate` (`src/pipeline/`), and `RedisStateManager` (`src/storage/state_manager.py`) — all lazily, inside `main()`. Deliberately duplicates `main.py`'s comment-conversion logic locally (`_to_comment()`) rather than importing the script module. Not imported by anything else; run only via `deploy/bluesky-anomaly-shadow.service`.

| | `bluesky_shadow.py` | `bluesky_anomaly_shadow.py` |
|---|---|---|
| Detection engine | None | `SlidingWindowTripwire` |
| Redis | None | DB 1 (isolated) |
| Event concept | Rolling counts only | Full start/update/release lifecycle |
| Output cadence | 30s (health) / 300s (GCS) | 300s (summary + GCS) |
| Env-configurable | No | Yes (5 `BSKY_*` vars) |

---

## `src/reporting/` — offline tools and BI export

### `batch_enricher.py`
**Offline NLP pipeline.** Downloads raw anomaly Parquet from GCS, runs `DBSCANContextEngine`, attributes items back to source stories, passes clusters to `CandidateBuilder`, and writes `EventCandidate` Parquet locally. Designed to run on a laptop where SentenceTransformer fits in memory. Platform-neutral: processes any Parquet files regardless of source.

### `dashboard_generator.py`
Generates a Plotly 4×2 channel activity dashboard and uploads it to GCS as `index.html`. Shows hourly anomaly counts per channel. Platform-neutral display layer.

### `export_powerbi.py`
Exports six flat CSV tables to `data/bi_export/` for Power BI consumption: `events.csv`, `event_keywords.csv`, `event_domains.csv`, `event_candidates.csv`, `channel_daily_summary.csv`, `evaluation_summary.csv`. Applies additional display-keyword noise filtering (`NOISE_DISPLAY_KEYWORDS`) beyond what `models.py` strips, and runs the regression baseline inline to populate `pairwise_pass_rate` and `e2e_pass_rate`. Adding Bluesky as a source does not require changes here — `source` is already exported as a column.

### `inspect_candidates.py`
CLI viewer for `EventCandidate` Parquet files. Supports filtering by date, channel, kind, and min-score. Recurring threads are hidden by default (`--include-recurring` to show). Useful for diagnosing what the enricher produced for a given day.

### `inspect_events.py`
CLI viewer for `TrackedEvent` JSONL files. Three modes: individual event blocks (default), `--summary` for aggregate diagnostics, and `--audit` for full candidate timelines with merge scores. Supports `--output PATH` to write the report to a file instead of stdout.

---

## `tests/` — regression suite

Only one file here is a true pytest suite; the rest are standalone scripts (runnable directly, some also pytest-discoverable) kept in `tests/` because they check correctness rather than run against live/production-shaped data.

### `test_router.py`
The only true pytest suite in the repo (`@pytest.fixture` + `@pytest.mark.parametrize`). Regression-pins `HNTopicRouter`'s keyword scoring, tier-3 brand names, domain boosts, and tie-break margin logic so keyword-list edits can be checked without running the live pipeline.

**Connection:** imports only `src.ingestion.hacker_news.HNTopicRouter`.

### `test_recurring_thread.py`
Verifies HN megathread detection (e.g. "Who is hiring?") is classified `kind == "recurring_thread"` and that superficially similar titles are not misclassified. Written as plain `test_*` functions, runnable both via pytest and directly.

**Connection:** imports `src.events.candidate_builder.CandidateBuilder`.

### `test_sliding_tripwire.py`
Standalone load-test (not pytest-discoverable despite the filename) using `fakeredis`: simulates 4 hours of synthetic traffic across 13 channels, injects an 800-comment spike, and asserts the anomaly fires, `AlertGate`'s cooldown suppresses duplicates, sampled texts are capped at 500, and old window data is pruned after TTL.

**Connection:** imports `src.models.{AnomalyEvent,Comment}`, `src.storage.state_manager.RedisStateManager`, `src.pipeline.sliding_tripwire.SlidingWindowTripwire`, `src.pipeline.alert_gate.AlertGate`.

### `test_jetstream.py`
Live connectivity/diagnostic tool for the Bluesky Jetstream firehose — modes: default (print N posts), `--volume` (throughput), `--channel-stats` (routes live posts through the real router and reports per-channel rates), `--raw`. Requires live internet access; not CI-suitable. **Different file from `scripts/test_jetstream.py`** — this one imports and exercises the router, the `scripts/` version is a bare connectivity check with no `src/` dependency at all.

**Connection:** imports `src.ingestion.bluesky.{BlueskyTopicRouter,_normalize_post}` (only in `--channel-stats` mode).

### `regression_merge_baseline.py`
The most important regression gate in the repo. Replays a hand-curated golden dataset (`data/audit/golden_merge_cases.json`) through `SameChannelPolicy.score()` and a full `EventConsolidator` pass, hard-failing (exit 1) on any `must_merge`/`must_not_merge` violation; `scenario_e2e` (full grouping scenarios) failures are reported as warnings only, not hard failures.

**Connection:** imports `src.events.consolidator.{EventConsolidator,_row_to_candidate}`, `src.events.evidence.EventEvidence`, `src.events.matching_policy.SameChannelPolicy`, `src.events.models.{EventCandidate,TrackedEvent}`; reads `data/audit/golden_merge_cases.json` and `data/event_candidates/**/*.parquet` directly.

---

## `scripts/` — operational and diagnostic tools

Distinct from `tests/`: these run against live or GCS-backed production-shaped data rather than isolated fixtures.

### `hashtag_report.py`
Reads the latest `HashtagStatsCollector` snapshot and writes three discovery reports (`top_unmatched_hashtags.csv`, `top_routed_hashtags.csv`, `hashtag_suggestions.json`) suggesting router keyword additions. Run periodically while a Bluesky shadow service is live.

**Connection:** no `src/` import — reads the JSON file `src/ingestion/bluesky_hashtag_stats.py`'s `HashtagStatsCollector.write_snapshot()` produces, keeping the report generator decoupled from the collector's implementation.

### `test_anomaly_replay.py`
Offline replay of the Bluesky anomaly path using `fakeredis`: downloads real routed sample post bodies from GCS (falls back to synthetic text), builds 8 hours of synthetic per-channel baseline, injects a configurable surge multiplier into one channel, and prints the resulting per-channel z-scores and any fired anomalies. Used to sanity-check tripwire/gate changes without a live Jetstream connection.

**Connection:** imports `src.models.Comment`, `src.pipeline.alert_gate.AlertGate`, `src.pipeline.sliding_tripwire.SlidingWindowTripwire`, `src.storage.state_manager.RedisStateManager`.

### `test_event_lifecycle.py`
Deterministic (`random.seed(0)`), fully synthetic smoke test of the start/update/release event-lifecycle state machine: injects a 5x surge, verifies exactly one `start`, subsequent `update`s (not repeated starts), and exactly one `release` on drain. CI-friendly exit codes (~15 internal assertions).

**Connection:** imports `src.models.{Comment,AnomalyEvent}`, `src.pipeline.sliding_tripwire.SlidingWindowTripwire`, `src.storage.state_manager.RedisStateManager`.

### `test_jetstream.py`
Bare-bones Jetstream connectivity check — connects, prints the first N posts, exits. No router, no channel stats, no `src/` dependency at all. **Different file from `tests/test_jetstream.py`** (the fuller diagnostic tool with router integration) — same filename, two genuinely different scripts; do not confuse them.

**Connection:** none — fully self-contained aside from the `websockets` library.

---

## `deploy/` — systemd unit templates

Three production processes, all sharing the same shape (`Type=simple`, `Restart=on-failure`, journal logging).

| Unit | Runs | Notes |
|------|------|-------|
| `hn-ingestor.service` | `src/main.py` (`live_hn()`) | The production HN pipeline. No tunable env vars — thresholds are hardcoded in `sliding_tripwire.py`. |
| `bluesky-shadow.service` | `src/shadow/bluesky_shadow.py` | Stats-only audit. No tunable env vars. |
| `bluesky-anomaly-shadow.service` | `src/shadow/bluesky_anomaly_shadow.py` | Full anomaly shadow. **Only unit with tunable `Environment=` vars** (`BSKY_MIN_HISTORY`, `BSKY_Z_THRESHOLD`, `BSKY_WINDOW_TTL`, `BSKY_EVAL_INTERVAL`, `BSKY_BASELINE_INTERVAL`) and the only one declaring `After=redis.service` (it's the only shadow process that touches Redis). |

**Connection:** each unit's `ExecStart` maps 1:1 to the entry-point files documented above (`src/main.py`, `src/shadow/bluesky_shadow.py`, `src/shadow/bluesky_anomaly_shadow.py`) — see those sections for what each process imports.

---

## How Bluesky actually integrated

The original plan (below, kept for history) was to extend `main.py` with a `live_bluesky()` entry point sharing the HN pipeline's `ParquetArchiver`/`WebhookDispatcher`. That's not what happened. Instead, Bluesky was integrated as a **fully parallel, isolated shadow pipeline** (`src/shadow/`) — separate Redis DB, no webhook dispatch, own JSONL/GCS output — so ingestion quality and detection tuning could be validated live without any risk to production HN alerting. As of this writing Bluesky has not been wired into `main.py` or production alerting; see `src/shadow/` above for the actual current architecture.

### Original plan (superseded)

| Step | File | What to do |
|------|------|-----------|
| 1 | `src/ingestion/bluesky.py` | Subclass `DataIngestor`, implement `stream()`. Connect to the AT Protocol Firehose (or polling endpoint). Normalize each post/reply to the shared item dict schema. Route to a topic channel using `HNTopicRouter` or a Bluesky-specific router. |
| 2 | `src/main.py` | Add a `live_bluesky()` entry point wiring the new ingestor into the existing pipeline (same `SlidingWindowTripwire` + `ParquetArchiver` + `WebhookDispatcher`). |
| 3 | `src/events/candidate_builder.py` | Optionally add Bluesky-specific `_RECURRING_POST_MARKERS` (e.g. weekly digest posts) alongside the existing HN markers. |
| 4 | Nothing else | The NLP enricher, classifier, consolidator, matching policy, consolidator, store, and all reporting tools are platform-neutral and require no changes. |
