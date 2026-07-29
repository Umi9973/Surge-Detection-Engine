# Phase Summary — May–July 2026

## Overview

76 commits across ~3 months. Three major arcs: HN pipeline foundation, EventCandidate classification layer, and Bluesky platform integration + router refinement.

---

## Arc 1: HN Pipeline & Infrastructure (May 2026)

**Core pipeline built and validated end-to-end.**

- Added live HN firehose via Firebase REST API — `src/ingestion/hacker_news.py`
- Added `HNCsvIngestor` for offline backtest (Nov 2023 Sam Altman saga as ground truth)
- Added `ParquetArchiver` → GCS cold storage (one Parquet file per anomaly event)
- Added `WebhookDispatcher` for Discord/Slack fire-and-forget alerts
- Added Plotly 4×2 channel dashboard with GCS upload (`src/reporting/dashboard_generator.py`)
- Moved NLP enrichment off VM hot path → offline `batch_enricher.py` runs on laptop (SentenceTransformer exceeds VM RAM)
- Added auditable routing to `HNTopicRouter` — routing decisions persisted to SQLite for inspection
- Fixed cold-start flood (Issue #8) and low-count false positives (Issue #9)
- Added `WeightedKeywordRouter` with tier 1/2/3 scoring, regex word boundaries, domain boosts

**Benchmark:** November 2023 Reddit backtest — Grade A (stemmed stopwords fix eliminated 5 gaming false-positive FLASHes)

---

## Arc 2: EventCandidate Classification Layer (Late May–June 2026)

**Added structured event representation on top of raw anomaly detection.**

- Added `EventCandidate` layer: classify NLP clusters into `viral_post` / `topic_surge` / `event_candidate`
- Added `EventConsolidator`: merge repeated candidates across consecutive ticks into unified `TrackedEvent` records
- Added `SameChannelPolicy` with reverse anchor match (Issue #21 fix)
- Added golden merge regression baseline — 6 `must_merge`, 6 `must_not_merge`, 4 E2E cases (`data/audit/golden_merge_cases.json`)
- Added `HN ingestion robustness + health monitoring` (`src/monitoring/health.py`)
- Added Power BI export pipeline (`src/reporting/export_powerbi.py`)
- Added `recurring_thread` event kind, `display_keywords`, CLI candidate inspector
- Project renamed: **Reddit Surge Detection → Surge Detection Engine**
- Added MIT License

---

## Arc 3: Bluesky Integration + Router Refinement (June–July 2026)

### Platform Adapter (June 24)

- Added Bluesky Jetstream WebSocket adapter — `src/ingestion/bluesky.py`
- `BlueskyTopicRouter` with 12 topic channels (extended from 8 HN channels)
- Post-level metadata: `platform_uri`, `root_uri`, `hashtags`, `matched_keywords`
- Added `HashtagStatsCollector` for passive hashtag discovery (`src/ingestion/bluesky_hashtag_stats.py`)

### Router Refinement (4 rounds, June 27–July 1)

- Round 1: audit visibility, filter precision fixes
- Round 2: hashtag aliases, adult content blocklist, domain boosts (YouTube→entertainment, arxiv→science_space)
- Round 3: sports channel addition, art keywords, domain boost expansion
- Round 4: adult blocklist extension, routing edge case cleanup
- Channel redesign: 10 → **12 slugs**; split `science_health` → `science_space` + `health_medicine`
- `science_space` keyword cleanup: removed general-science false-positive triggers

### Shadow Runner Architecture (July 1)

- `bluesky_shadow.py` — passive stats runner (kept-by-channel, top hashtags)
- `bluesky_anomaly_shadow.py` — full anomaly lifecycle runner with JSONL output and GCS upload
  - Event grouping: `start` → `update` → `release` phases per elevation period
  - In-memory active event accumulation; one JSONL record written at release

### Anomaly Event Enrichment (July 13)

- Per-post routing audit fields: `route_score`, `route_runner`, `route_margin`, `route_boost`
- Per-post enrichment: `platform_uri`, `root_uri`, `hashtags`, `matched_keywords` stored in Redis ZSET payload
- Opening and peak window snapshots per event: `posts[5]`, keyword/hashtag/domain/author aggregations

### Feed Signal Quality (July 15–22)

**Feed burst detection** (`_is_concentration_burst`):

- Rule 1 — Known feed domain: `arxiv.org / biorxiv.org / medrxiv.org` ≥ 60% of window, total ≥ 100
- Rule 2 — Single-source aggregator: top author > 40% AND top domain > 35%, total ≥ 50
- Lifecycle flags: `opening_concentration_suspect` → `peak_concentration_suspect` → `feed_burst_suspect` (requires both windows)
- Recurrence: `_find_recurring()` checks same channel within ±45 min same UTC hour → `recurring_feed_suspect`
- Three-counter summary: `threshold_crossings`, `feed_burst_suspects`, `reportable_anomalies`

**Context gate** (`_CONTEXT_GATE`): weak channel keywords (`security`, `health`, `science`, `space`, `exploit`, `medicine`) scored ×0.25 unless a strong co-signal keyword is present — prevents low-confidence misrouting

**Text coordination detection**:

- `top_text_pct`: fraction of window items sharing the same 80-char text prefix
- `text_unique_ratio`: unique prefix count / total items
- `coordinated_suspect` flag fires when `max(opening_top, peak_top) > 0.25` and no `feed_burst_suspect`

---

## Audit Results (July 5–25 2026 Runs)

Two complete runs totalling ~7 weeks of data. Key findings from July 22–25 (55-hour run, 8 events):

| Event | Channel | Peak Z | Flags |
|-------|---------|--------|-------|
| YouTube/TikTok/Instagram surge | social_platforms | 3.06 | — (first-ever fire) |
| AI discourse surge | ai_tech | 3.58 | — |
| arxiv batch (ramp-up) | science_space | 7.34 | `peak_concentration_suspect` only |
| arxiv batch | science_space | 19.92 | `feed_burst_suspect` `recurring_feed_suspect` |
| ENISA CVE feed | cybersecurity | 3.23 | — (slipped through) |
| arxiv batch | science_space | 8.90 | `feed_burst_suspect` `recurring_feed_suspect` |
| arxiv batch (peak z=32.2) | science_space | 32.2 | `feed_burst_suspect` `recurring_feed_suspect` |
| "#August1st" template spam | activism_rights | 3.49 | `coordinated_suspect` (top_text_pct=0.34) |

---

## Open Issues / Next Steps

| Issue | Description | Priority |
|-------|-------------|----------|
| #5 | Subchannel detection — us_politics (586k posts, 0 fires) and entertainment_fandom (787k, 0 fires) need hashtag/entity-level subcounters | High |
| #7 | Time-of-day baseline — flat 168-sample history inflates std for diurnal channels; needs same-hour bucketing (requires 3+ weeks warm-up) | Medium |
| — | Add `euvd.enisa.europa.eu` to `_KNOWN_FEED_DOMAINS["cybersecurity"]` (ENISA CVE bot slipped through) | Low |
| — | `feed_burst_suspect` counter off by 1 in summary JSON vs JSONL ground truth (minor, cosmetic) | Low |
| #8/#9 | Recurring elevated states and semantic granularity — not yet started | Deferred |
