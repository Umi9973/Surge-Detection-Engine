# Phase 2 Implementation: Live Stream & Architecture Upgrades

## 1. Completed: Phase 1 (Historical Validation)
- [x] Fixed "PS5 Bug" by implementing a two-pointer sliding window aggregation.
- [x] Solved "Keyword Drift" via NLP Lemmatization/Stemming (`SnowballStemmer`).
- [x] Solved "Frankenstein Union" bug by migrating from Jaccard to Overlap Coefficient.
- [x] Established Split Thresholds (FLASH_OVERLAP = 0.3, SUSTAINED_OVERLAP = 0.5).
- [x] Fixed Pipeline Routing: Engine prioritizes FLASH over SUSTAINED signal claims.
- [x] Upstream Stopword Optimization: Filtered generic nouns and sentiment noise *before* c-TF-IDF.
- [x] Full Month Validation: Detected Game Awards, GTA VI leak; suppressed Thanksgiving noise.
- [x] Dynamic Baseline TF-IDF: 7-day frequency baseline penalizes seasonal vocabulary shifts.
- [x] Anchor Clustering: Multi-Track Anchor prevents Concept Drift.

---

## 2. Completed: Architecture Modernization
- [x] **Repository Restructure:** Migrated from a flat script directory to a modular `src/` tree (`ingestion`, `pipeline`, `state`, `alerting`).
- [x] **Dependency Injection:** Created Protocol interfaces (e.g., `NLPContextEngine`) to decouple math logic from state management.
- [x] **Data Transfer Objects:** Defined strict `dataclasses` (`AnomalyEvent`, `Comment`) to prevent dictionary tangling across the pipeline.

---

## 3. Completed: State Management (Redis Sliding Window)
- [x] `RedisStateManager` using `fakeredis`: ZADD/ZREMRANGEBYSCORE sliding window, 2-hour TTL prune.
- [x] 168-hour (7-day) rolling baseline history via Redis LIST + `LTRIM`.
- [x] **Baseline Freeze:** push last calm count instead of anomalous count while `ELEVATED`.
- [x] **Schmitt Trigger Hysteresis:** enter `ELEVATED` at Z≥3.0, release at Z≤2.0.
- [x] Reservoir sampling (up to 500 texts) passed to `DBSCANContextEngine`.

---

## 4. Completed: The Hacker News Firehose
- [x] `HNTopicRouter`: 8 virtual channels (+ `general` catch-all), weighted keywords (3-tier), pre-compiled regex word boundaries, domain boosts, priority tie-breaking.
- [x] `HackerNewsIngestor`: threaded producer/consumer with persistent `aiohttp.ClientSession`; sync `stream()` drains `queue.Queue`.
- [x] `_item_topics` propagation cache: deep comment inheritance via immediate-parent chain.
- [x] Filter updated with HN bot accounts (`dang`, `pg`) and flagging boilerplate.
- [x] `live_hn()` entry point in `src/main.py` wires full pipeline end-to-end.
- [x] `HNCsvIngestor` + `_HNItemProcessor` mixin: DRY shared classification/normalization for live and backtest ingestors.
- [x] `hn_backtest_driver.py`: replays `HN_2023-11.csv` through full pipeline; detected Sam Altman firing at Nov 17 18:45 UTC (peak z=5733 by Nov 22).

---

## 5. Completed: Backtest & Aggregator Upgrades
- [x] `backtest_driver.py` streams `RC_2023-11.zst` into `RedisSlidingWindow` (fakeredis).
- [x] `Phase1Aggregator` frozen; `Phase2Aggregator` handles 5-minute anomaly rows via `_hourly_reps()`.
- [x] `AggregatorConfig` dataclass for threshold injection.
- [x] AGGREGATOR_STOPWORDS expanded (modbot artifacts + image metadata).
- [x] Benchmark result: **Grade A** — 88% F1, 0% Frankenstein rate, Thanksgiving suppressed.

---

## 6. Final Sprint: MLOps & Delivery

### A. The Webhook Dispatcher (`src/alerting/webhooks.py`)
- [x] **Action:** Format solidified FLASH/SUSTAINED events into a JSON payload.
- [x] **Logic:** POST alert, Z-score, subreddits, and keyword summary to a private Discord/Slack Webhook.
- [x] `WebhookDispatcher`: fire-and-forget daemon thread, auto-detects Discord/Slack/plain, named exception handling (429, URLError, TimeoutError, unexpected), no-op when `WEBHOOK_URL` unset.
- [x] Verified live: Sam Altman firing embed (ai, Nov 17 22:00 UTC, z=1672.96) delivered to Discord correctly.

### B. Parquet Cold Storage (`src/storage/parquet_archiver.py`)
- [x] **Action:** Implement an archival script to run before Redis TTL expires data.
- [x] **Logic:** Write expired anomalies to compressed `.parquet` files for cheap S3 storage and Phase 3 ML training.
- [x] `ParquetArchiver`: lazy `ParquetWriter`, snappy compression, date-partitioned paths (`data/parquet/YYYY/MM/DD/`), context manager, `list<string>` columns for texts and keywords.
- [x] Wired into `live_hn()` (inline NLP + try/finally close) and `hn_backtest_driver.py` (post-NLP enrichment loop).
- [ ] **Phase 3 TODO:** Validate Parquet output end-to-end against a full backtest run (schema, row counts, keyword content). Add S3 upload hook once cloud infra is provisioned.

---

### C. Event Consolidator — Flash/Sustained Classification for Live Pipeline (`src/pipeline/event_consolidator.py`)
- [ ] **Context:** The aggregator in `src/analysis/aggregator.py` already implements FLASH/SUSTAINED/ISOLATED classification with Union-Find, Multi-Track Anchor keyword locking, and Szymkiewicz–Simpson overlap scoring. It was built for the Reddit historical backtest (batch, reads from SQLite). The live HN pipeline has no equivalent — every raw `AnomalyEvent` is treated identically regardless of whether it is a 13-hour sustained surge or a one-window blip. See **Issue #18** in `docs/ISSUES.md`.
- [ ] **Action:** Port aggregator logic into a streaming `EventConsolidator` component that buffers `AnomalyEvent` objects and emits labelled consolidated events with a short look-ahead delay (2 eval ticks = 10 min).
- [ ] **Core algorithms to port (minimal changes needed):**
  - FLASH: Union-Find linking channels with ≥30% cluster overlap within 2-hour window, ≥2 shared keywords
  - SUSTAINED: Multi-Track Anchor locking dominant DBSCAN cluster after Hour 2, ≥4 consecutive windows at ≥50% overlap
  - ISOLATED: catch-all for unclaimed events
  - Claim order: FLASH → SUSTAINED → ISOLATED (prevents double-counting)
- [ ] **Interface change:** Add `event_type: str` (`"FLASH"` / `"SUSTAINED"` / `"ISOLATED"`) and `event_duration_s: int` to consolidated event output.
- [ ] **Wire-up:** Insert `EventConsolidator` between `AlertGate` and `archiver/dispatcher` in `live_hn()`.
- [ ] **Key trade-off:** Look-ahead buffer adds notification latency. 2-tick (10 min) buffer gives accurate classification for most flash events; sustained events are confirmed only after ≥4 consecutive windows (20 min).

---

## 7. BLOCKED: Reddit Live Ingestion
- [ ] **Status:** Waiting on Reddit Developer Portal manual approval.
- [ ] **Action:** Once approved, build `LiveRedditIngestor` using `AsyncPRAW`, adhering strictly to the 100 QPM limit.