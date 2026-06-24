# Surge Detection Engine — Source Structure

Real-time event detection across social discussion platforms. The pipeline spots when the internet starts talking about something before it becomes mainstream news, by monitoring topic channels, detecting statistical surges, and consolidating repeated signals into typed TrackedEvents.

**Current platform:** Hacker News. Bluesky (and Reddit) are the next planned sources. The architecture is deliberately split so all platform-specific code lives in one place — everything downstream is platform-neutral.

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

  reporting/          ← offline tools and BI export
    batch_enricher.py
    dashboard_generator.py
    export_powerbi.py
    inspect_candidates.py
    inspect_events.py
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

## Adding Bluesky — summary checklist

| Step | File | What to do |
|------|------|-----------|
| 1 | `src/ingestion/bluesky.py` | Subclass `DataIngestor`, implement `stream()`. Connect to the AT Protocol Firehose (or polling endpoint). Normalize each post/reply to the shared item dict schema. Route to a topic channel using `HNTopicRouter` or a Bluesky-specific router. |
| 2 | `src/main.py` | Add a `live_bluesky()` entry point wiring the new ingestor into the existing pipeline (same `SlidingWindowTripwire` + `ParquetArchiver` + `WebhookDispatcher`). |
| 3 | `src/events/candidate_builder.py` | Optionally add Bluesky-specific `_RECURRING_POST_MARKERS` (e.g. weekly digest posts) alongside the existing HN markers. |
| 4 | Nothing else | The NLP enricher, classifier, consolidator, matching policy, consolidator, store, and all reporting tools are platform-neutral and require no changes. |
