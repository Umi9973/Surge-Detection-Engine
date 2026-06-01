# ARCHITECTURE.md — HN Surge Detection Engine (Phase 2)

## 1. System Overview

A real-time data engineering pipeline that detects statistically anomalous
spikes in Hacker News comment volume, clusters the spike texts into semantic
themes using DBSCAN, and attributes each cluster to its dominant source story.

**Current Phase:** Phase 2 — Live HN stream on GCP e2-micro VM.  
**Live data source:** Hacker News Firebase REST API (not Reddit — Reddit live
API approval is still pending).  
**Reddit data source:** Historical `.zst` pushshift dumps for backtest only.

---

## 2. Core Architectural Principles

- **Separation of concerns.** Every component communicates through an abstract
  interface. The math engine has no knowledge of Redis internals;
  the ingestor does zero math.
- **In-memory sliding window.** Items arrive, age, and expire inside a Redis
  ZSET with a 2-hour TTL. No disk I/O in the hot path.
- **Frugal MLOps.** No Kafka, no GPU, no JVM. Redis ZSETs for the window,
  `sentence-transformers/all-MiniLM-L6-v2` for CPU embeddings, Snappy-
  compressed Parquet for cold storage. Infrastructure cost < $15/month.
- **Offline enrichment.** DBSCAN (CPU-intensive) runs on a laptop, not the VM.
  The VM archives raw Parquet to GCS; the laptop downloads, enriches, and
  writes enriched files locally.

---

## 3. Data Models (`src/models.py`)

Two dataclasses define the contract between every component in the pipeline.

### `Comment`
The normalised unit of ingestion. Every ingestor (HN live, HN CSV, Reddit zst)
produces this shape before anything else in the pipeline sees the data.

```
id           str    — source item ID (HN Firebase ID or Reddit comment ID)
subreddit    str    — virtual channel (HN) or subreddit name (Reddit)
body         str    — comment/story text body
timestamp    int    — Unix timestamp (item creation time)
author       str    — username
score        int    — upvotes (0 for HN comments)
story_id     int    — root HN story ID (0 if unknown or Reddit)
story_title  str    — root story headline
domain       str    — netloc of story URL (e.g. "techcrunch.com")
item_type    str    — "story" | "comment"
item_id      int    — HN Firebase item ID (same as id cast to int)
created_at   int    — Unix timestamp (alias of timestamp; explicit in items)
```

### `AnomalyEvent`
The output of the detection engine. Passed to the gate, archiver, and dispatcher.

```
subreddit    str         — channel that fired
window_start int         — Unix timestamp (now − 7200)
window_end   int         — Unix timestamp (now)
count        int         — items in the 2-hour window
z_score      float       — raw z-score (uncapped)
items        List[Dict]  — sampled window items (up to 500); see Parquet schema
mean         float       — baseline mean (or median for volatile channels)
std          float       — baseline std (or MAD for volatile channels)
```

---

## 4. Pipeline Architecture

### Full live flow

```
HN Firebase API
      │
      ▼
HackerNewsIngestor          ← threaded async producer, persistent aiohttp session
      │  stream() → Dict
      ▼
SubredditFilter             ← drops bots, boilerplate; routes by virtual channel
      │  stream() → Dict
      ▼
_to_comment()               ← normalises raw dict → Comment dataclass
      │
      ▼
SlidingWindowTripwire
  ├── ingest(comment)       ← RedisStateManager.ingest() → ZADD window:{ch}
  ├── baseline_tick(now)    ← every 60 min; push hourly count to history:{ch}
  └── evaluation_tick(now)  ← every 5 min; compute Z, Schmitt trigger
      │  → List[AnomalyEvent]
      ▼
AlertGate.process()         ← 30-min cooldown; passes first alert of each burst
      │  → List[AnomalyEvent]
      ├──▶ ParquetArchiver.archive()   → GCS (raw, clusters=[])
      ├──▶ SQLite _save_live_anomaly() → anomalies_hn_live.db
      └──▶ WebhookDispatcher.dispatch() → Discord/Slack (fire-and-forget thread)

[Offline, on laptop]
      ▼
batch_enricher.run()
  ├── download raw Parquet from GCS
  ├── DBSCANContextEngine.summarize_anomaly()
  │     → cluster keywords + text_indices
  ├── compute top_story_id / story_ids / top_domains per cluster
  └── write enriched Parquet to data/parquet_enriched/
```

---

## 5. Component Reference

### 5.1 Ingestion Layer

#### `src/ingestion/base.py` — `DataIngestor` (ABC)
Single abstract method: `stream() → Iterator[Dict]`. Every ingestor
implements this contract. The rest of the pipeline never imports a concrete
ingestor class directly.

---

#### `src/ingestion/hacker_news.py`

**`HNTopicRouter`**  
Classifies an HN story title (+ optional URL domain) into one of 8 virtual
channels: `ai`, `security`, `startup`, `crypto`, `science`, `tech`, `policy`,
`general`.

- `TOPIC_CHANNELS`: weighted keyword lists per channel. Three tiers:
  - Tier 3 — exact brand/model names (`openai`, `nasa`, `bitcoin`)
  - Tier 2 — domain jargon (`machine learning`, `ransomware`, `home assistant`)
  - Tier 1 — generic single words (`ai`, `hack`, `launch`)
- `DOMAIN_BOOSTS`: direct netloc → (channel, weight) overrides applied on top
  of keyword score (`arxiv.org +3 science`, `krebsonsecurity.com +3 security`, etc.)
- `_PRIORITY`: tie-breaking order when two channels score equally.
  `startup > science` so SpaceX funding stories win on priority even when
  `spacex (science tier-1)` ties with `funding (startup tier-1)`.

**`_HNItemProcessor`** (mixin)  
Shared normalisation and classification logic for both live and CSV ingestors.

- `_item_topics: Dict[int, str]` — item_id → channel. Comments inherit from
  their parent's channel via immediate-parent lookup.
- `_item_story: Dict[int, int]` — item_id → root story_id. Comments walk up
  the parent chain so all comments in a thread share the same story_id.
- `_story_meta: Dict[int, Dict]` — story_id → `{title, domain}`. Evicted
  only when the story_id is no longer referenced by any live item in
  `_item_story` (reference-safe eviction).
- `_process(items)`: filters deleted/dead items, classifies stories, inherits
  topic to comments, strips HTML, yields normalised dicts.

**`HackerNewsIngestor`** (live)  
Streams new HN items in real-time via the Firebase REST API.

- Architecture: sync `stream()` drains a `queue.Queue` fed by a background
  thread running a single persistent `aiohttp.ClientSession`. Avoids the
  `asyncio.run()`-per-cycle trap that destroys the event loop and TCP
  connection pool on every poll.
- `_bootstrap()`: seeds `_last_id` from `maxitem.json` on startup; historical
  items are skipped.
- `_fetch_new()`: polls `maxitem.json`, fetches all new item IDs concurrently
  with a semaphore (`concurrency=20`).
- Poll interval: 5 seconds.

**`HNCsvIngestor`** (backtest)  
Replays a HN CSV dump (`HN_2023_Nov8-23.csv`) through the same
`_HNItemProcessor` pipeline as the live ingestor. Rows are sorted by timestamp
before processing so stories arrive before their comments, matching real-time
order. CSV columns are normalised to Firebase-compatible schema before being
passed to `_process()`.

---

#### `src/ingestion/reddit.py` — `ZstFileIngestor` (backtest only)
Streams a pushshift `.zst` Reddit dump line-by-line using a 64KB chunk buffer
with `zstandard`. Normalises each line to
`{id, timestamp, subreddit, body, score, author, parent_id, link_id, permalink}`.
Used only by `backtest_driver.py` and `main.run()` — not in the live HN pipeline.

---

### 5.2 Filter Layer

#### `src/pipeline/filter.py` — `SubredditFilter`
Sits immediately after the ingestor. Two responsibilities:

1. **Channel routing** — passes only items whose `subreddit` field matches the
   target list (case-insensitive). For the live HN pipeline, targets are the 8
   virtual channel names; for Reddit backtest, targets are subreddit names.
2. **Bot filtering** — drops items from known bot accounts (`AutoModerator`,
   `dang`, `pg`, `BotDefense`, etc.) and items whose body matches any
   boilerplate phrase (`"i am a bot"`, `"your submission has been removed"`,
   HN moderation notices). Upstream removal prevents bot text from ever
   reaching Redis or the NLP engine.

---

### 5.3 State Management

#### `src/storage/state_manager.py`

**`StateManager`** (ABC)  
Interface between the tripwire and the storage backend. Five methods:
`ingest`, `get_window_count`, `get_window_items`, `get_history`,
`push_history`. The tripwire has zero knowledge of Redis.

**`RedisStateManager`**  
Production Redis implementation. Key schema:

```
window:{channel}   ZSET    score=Unix timestamp
                           member="{uuid_hex}:{json_payload}"

history:{channel}  LIST    last 168 hourly counts (7 days × 24 hours)
```

JSON member payload (stored per item):
```json
{
  "t":   "comment body (truncated to 120 chars)",
  "sid": 48339580,
  "st":  "Story headline",
  "d":   "techcrunch.com",
  "it":  "comment",
  "iid": 48341234,
  "ca":  1748700000
}
```

`get_window_items()` deserialises the JSON payload and returns a list of dicts
with keys `{item_id, text, story_id, story_title, domain, item_type, created_at}`.
Old plain-text members (pre-migration format `{uuid}:{text}`) are handled by a
try/except fallback that sets all metadata fields to `0` / `""`.

Window TTL: 2 hours (`ZREMRANGEBYSCORE` prunes on every read).  
History cap: 168 entries (`LTRIM` after every push).  
Sample cap: 500 items returned per `get_window_items()` call.

---

### 5.4 Detection Engine

#### `src/pipeline/tripwire.py` — `TumblingWindowTripwire` (Phase 1 / Reddit backtest)
1-hour tumbling windows. Used only by `main.run()` (Reddit November 2023
backtest). Not used in the live pipeline. Fires when `z > 3.0`; uses MAD
instead of std for `news` / `worldnews` (volatile subreddits prone to extreme
outliers from breaking news).

---

#### `src/pipeline/sliding_tripwire.py` — `SlidingWindowTripwire` (Phase 2 / live)
Redis-backed, fully decoupled from storage via `StateManager`. This is the
live detection engine.

**Tick contract:**
- `ingest(comment)` — called per incoming item; delegates to `state.ingest()`.
- `baseline_tick(now)` — called every 60 minutes; commits current window count
  to rolling history.
- `evaluation_tick(now)` — called every 5 minutes; computes Z-score and fires
  `AnomalyEvent` objects.

**Schmitt trigger (hysteresis) state machine per channel:**
```
CALM → ELEVATED   when Z ≥ 3.0   (trigger threshold)
ELEVATED → CALM   when Z ≤ 2.0   (release threshold)
Hysteresis zone   2.0 < Z < 3.0  — stays ELEVATED, keeps emitting events
```

**Baseline freeze:** While ELEVATED, `baseline_tick()` pushes `history[-1]`
(last clean count) instead of the anomalous current count. This prevents the
Z-score from decaying to zero during a sustained surge, keeping the baseline
anchored at pre-event ambient level. Known risk: unbounded freeze during
multi-week surges (see Issue #10).

**Guards:**
- `MIN_HISTORY = 24` — no alerting until 24 hourly samples exist (cold-start
  protection; covers the full diurnal cycle).
- `MIN_COUNT = 10` — ignores windows with fewer than 10 items before entering
  ELEVATED (prevents near-zero baselines from producing misleadingly high
  Z-scores on low-volume channels). Does not block the ELEVATED release path.

**Z-score computation:**
- Default channels: `(count − mean) / std`
- `news`, `worldnews`: `(count − median) / MAD` (robust to extreme outliers)

---

### 5.5 NLP Context Engine

#### `src/pipeline/context.py` — `DBSCANContextEngine`
Trigger-only: runs only when an anomaly fires. Never called in the hot ingestion
path.

**Pipeline:**
1. `SentenceTransformer("all-MiniLM-L6-v2")` encodes raw texts → 384-dim
   embeddings. Runs on CPU; ~80MB model.
2. `UMAP(n_components=2, random_state=42)` reduces to 2D. Necessary because
   DBSCAN degrades badly in high-dimensional space (curse of dimensionality).
3. `DBSCAN(eps=0.5, min_samples=5)` clusters the 2D points. Label `-1` = noise
   (excluded from keyword extraction).
4. `_ctfidf_keywords()` — manual class-based TF-IDF. Each DBSCAN cluster is
   treated as one "document." `top_keywords = 8`.

**Upstream stopword filtering:** Tokens are stripped *before* TF-IDF via
`_tokenize()`. Two sets:
- `_STOPWORDS` — applied always. Covers reaction words (`lol`, `omg`, `fuck`),
  internet acronyms (`ngl`, `imo`, `tbh`), contracted negatives (`didn`, `won`,
  `isn`), and HN conversational filler (`people`, `think`, `work`, `going`).
- `_SINGLE_CLUSTER_STOPWORDS` — superset applied only when DBSCAN finds exactly
  one cluster (IDF is flat in that case, so TF alone drives selection and
  generic verbs dominate without extra filtering).

**Dynamic baseline penalty:** A 7-day rolling `Counter` of token frequencies
(updated per `summarize_anomaly()` call via `update_baseline()`). Words that
are chronically common get their c-TF-IDF score penalised by
`score / (1 + log(baseline_rate + 1))`. Words spiking above 2× their baseline
rate bypass the penalty (`spike_ratio ≥ 2.0`).

**`text_indices`:** Each cluster result carries `text_indices: List[int]` —
the positions in the input `texts` list that belong to that cluster. Used by
`batch_enricher` to map cluster membership back to story IDs. This field is
ephemeral — not written to Parquet (see Issue #13).

**Minimum texts guard:** `batch_enricher` skips `summarize_anomaly()` when
`len(texts) < 50`. Windows below this threshold produce noisy clusters with
too many noise points to be useful.

---

### 5.6 Gate and Dispatch

#### `src/pipeline/alert_gate.py` — `AlertGate`
Cooldown gate between the detection engine and the notification layer.

- Every `AnomalyEvent` from the tripwire passes through `process()`.
- Only events that clear the per-channel cooldown (default 30 minutes) are
  forwarded as actionable alerts.
- Two backends:
  - **In-memory** (default): `dict` keyed by channel name. Fast; loses state
    on restart (see Issue #12 — not wired to Redis in `live_hn()` yet).
  - **Redis**: `SET alert:{ch} EX {cooldown}` — TTL-based, survives restarts,
    multi-process safe. Enabled by passing a Redis client: `AlertGate(r=r)`.

---

#### `src/alerting/webhooks.py` — `WebhookDispatcher`
Fire-and-forget webhook dispatcher. `dispatch()` spawns a daemon thread and
returns immediately — the math engine is never blocked by network I/O.

- Auto-detects Discord vs Slack vs plain text from `WEBHOOK_URL`.
- Named exception handling per failure type: HTTP 429 (rate limit), URLError
  (network), TimeoutError, unexpected exceptions — each logs a specific message
  and never raises.
- No-op when `WEBHOOK_URL` is unset (backtest / test environments).
- Currently passes `keywords=[]` — cluster keywords are computed offline by
  the enricher and are not available at dispatch time.

---

### 5.7 Storage and Archival

#### `src/storage/parquet_archiver.py` — `ParquetArchiver`
Writes each `AnomalyEvent` as one Snappy-compressed Parquet file and uploads
to GCS.

**File path layout:**
```
gs://hn-surge-dashboard-01/parquet/YYYY/MM/DD/{channel}_{window_end}.parquet
```
One row per file (one anomaly event).

**Atomic write:** Data goes to `{filename}.parquet.tmp` first, then
`tmp.replace(path)` on success. `retry_pending()` only scans `*.parquet` so a
crash during write never leaves a corrupt file eligible for upload.

**Delete after upload:** Local file is unlinked in a separate `try/except`
after confirmed GCS upload. Upload failure and unlink failure produce distinct
error messages and do not mask each other.

**`retry_pending()`:** Called at `live_hn()` startup. Scans
`data/parquet/*.parquet` and uploads any leftover files from prior crashed runs.

**Schema:** See Section 6.

---

### 5.8 Batch Enrichment (offline, laptop)

#### `src/reporting/batch_enricher.py` — `batch_enricher`
Runs locally. Downloads raw Parquet files from GCS, runs DBSCAN enrichment,
and writes enriched files to `data/parquet_enriched/`. Tracks processed blobs
in `data/parquet_enriched/.processed` to skip already-enriched files.

**Pipeline per file:**
1. Download blob to `tmp_download.parquet`.
2. `_normalise_row()` — maps old-schema rows to current schema:
   - Removes legacy `keywords` column if present.
   - Back-fills `items[*].item_id` and `items[*].created_at` to 0 for files
     predating the schema extension.
3. `DBSCANContextEngine.summarize_anomaly(texts)` if `len(texts) ≥ 50`,
   else `clusters = []`.
4. Attribution loop per cluster: uses `text_indices` to map cluster members
   back to `items[idx]`, then computes:
   - `top_story_id / top_story_title / top_story_pct` — dominant story by
     comment count among items with a known `story_id` (excludes `story_id=0`).
   - `unique_story_count` — distinct story IDs in the cluster.
   - `story_ids` — sorted list of all story IDs in the cluster.
   - `top_domains` — top 5 domains by comment count across all valid indices.
5. Writes enriched Parquet with explicit PyArrow struct types for `items` and
   `clusters` columns.

---

### 5.9 Analysis (backtest post-processing only)

#### `src/analysis/aggregator.py`
**Not wired into the live pipeline.** Post-hoc batch processor for the Reddit
historical backtest. Reads from a completed SQLite `anomalies` table and
routes raw anomalies into consolidated event types.

**Event types (processed in claim order — prevents double-counting):**

1. **FLASH** (runs first) — cross-channel viral burst.
   - Links anomalies across ≥ 2 channels within a 2-hour sliding window.
   - Match condition: any cluster pair shares ≥ 30% Szymkiewicz–Simpson overlap
     AND ≥ 2 shared keywords.
   - Cluster-to-cluster matching (not union): DBSCAN's topic separation is
     preserved — unrelated clusters score 0 and become invisible to the merge.
   - Union-Find groups linked anomalies; splits transitivity chains > 4 hours.

2. **SUSTAINED** (runs second, on unclaimed anomalies) — single-channel
   developing story.
   - Requires ≥ 4 consecutive hourly windows at ≥ 50% keyword overlap.
   - Multi-Track Anchor: Hour 1's DBSCAN clusters each get an isolated track.
     Hour 2 finds the best-scoring cluster pair and locks the dominant track.
     From Hour 3 onward, only the dominant track can extend the chain —
     prevents topic bleed from concurrent unrelated stories. The dominant
     track expands (`|=`) to allow within-story vocabulary evolution.

3. **ISOLATED** (catch-all) — any anomaly not claimed by FLASH or SUSTAINED.

**Porting status:** Issue #18 in `docs/ISSUES.md` tracks porting this into a
streaming `EventConsolidator` for the live pipeline.

---

### 5.10 Dashboard

#### `src/reporting/dashboard_generator.py`
Plotly-based live dashboard. Runs on the VM and serves a 4×2 trellis of all
8 channels.

- Reads live window data from Redis (`RedisStateManager.get_history()`).
- Reads anomaly event markers from `anomalies_hn_live.db` (SQLite).
- Downloads the most recent enriched Parquet for each channel from GCS to
  surface cluster keywords on anomaly markers.
- Auto-refreshes every 300 seconds.
- Channels: `ai`, `tech`, `security`, `startup`, `crypto`, `science`,
  `policy`, `general`.

---

## 6. Entry Points

| File | Function | Purpose |
|---|---|---|
| `src/main.py` | `live_hn()` | **Live HN pipeline.** Wires all components end-to-end; calls `fire_ticks()` to drive eval and baseline ticks from item timestamps. |
| `src/main.py` | `run()` | Reddit November 2023 backtest (tumbling windows, ZstFileIngestor). |
| `src/hn_backtest_driver.py` | `main()` | HN November 2023 backtest. Replays `HN_2023_Nov8-23.csv` via `HNCsvIngestor`; 7-day burn-in suppresses cold-start false positives; writes to `anomalies_hn_nov.db`. |
| `src/backtest_driver.py` | `main()` | Reddit Phase 2 backtest. Streams `RC_2023-11.zst` into `SlidingWindowTripwire` + `Phase2Aggregator`; writes to `anomalies_phase2_nov.db`. |
| `src/backfill_clusters.py` | `main()` | Recovery tool. Backfills the `clusters` table into older DBs that were generated before inline NLP was added. |
| `src/reporting/batch_enricher.py` | `run()` | Offline enrichment. Run locally: `python -m src.reporting.batch_enricher`. |
| `src/reporting/dashboard_generator.py` | `run()` | Live dashboard. Run on VM. |

---

## 7. Data Schemas

### 7.1 Redis

```
window:{channel}   ZSET   score=Unix timestamp
                          member="{uuid_hex}:{json}"
                          JSON keys: t, sid, st, d, it, iid, ca
                          TTL pruned on every read (2-hour window)

history:{channel}  LIST   up to 168 floats (hourly counts, oldest first)
                          LTRIM to 168 after every push

alert:{channel}    STRING last alert Unix timestamp
                          EX = cooldown TTL (Redis-backed AlertGate only)
```

### 7.2 Parquet (GCS — raw, written by VM)

One file per anomaly event. Path: `parquet/YYYY/MM/DD/{channel}_{window_end}.parquet`

| Column | Type | Notes |
|---|---|---|
| `channel` | `string` | Virtual channel name |
| `window_start` | `int64` | Unix timestamp |
| `window_end` | `int64` | Unix timestamp |
| `window_end_dt` | `string` | Human-readable UTC string |
| `count` | `int64` | Items in the 2-hour window |
| `z_score` | `float64` | Raw z-score (uncapped) |
| `mean` | `float64` | Baseline mean or median |
| `std` | `float64` | Baseline std or MAD |
| `texts` | `list<string>` | Redundant copy of item texts (Issue #11) |
| `items` | `list<_ITEM_STRUCT>` | Full item records — see below |
| `clusters` | `list<_CLUSTER_STRUCT>` | Empty on VM write; filled by enricher |

**`_ITEM_STRUCT`**
```
item_id      int64    HN Firebase item ID
text         string   Comment body (truncated to 120 chars)
story_id     int64    Root story ID (0 = unknown)
story_title  string   Root story headline
domain       string   Story URL netloc
item_type    string   "story" | "comment"
created_at   int64    Item creation Unix timestamp
```

**`_CLUSTER_STRUCT`** (populated by batch_enricher)
```
cluster_id          int64         DBSCAN label
size                int64         Items in cluster
noise_count         int64         Noise points in this anomaly window
keywords            list<string>  Top c-TF-IDF keywords
top_story_id        int64         Dominant story ID (0 = none)
top_story_title     string        Dominant story headline
top_story_pct       float64       Fraction of attributed items from top story
unique_story_count  int64         Distinct story IDs in cluster
story_ids           list<int64>   All story IDs present in cluster
top_domains         list<string>  Top 5 domains by comment count
```

### 7.3 SQLite

**`anomalies_hn_live.db`** — written by `live_hn()` in real-time:
```sql
CREATE TABLE anomalies (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    channel       TEXT    NOT NULL,
    window_start  INTEGER NOT NULL,
    window_end    INTEGER NOT NULL,
    window_end_dt TEXT    NOT NULL,
    count         INTEGER NOT NULL,
    z_score       REAL    NOT NULL,
    mean          REAL    NOT NULL,
    std           REAL    NOT NULL
);
```

**`anomalies_hn_nov.db`** — HN backtest; also has `clusters` and `anomaly_texts` tables.  
**`anomalies_phase2_nov.db`** — Reddit Phase 2 backtest; has `consolidated_events` table.

---

## 8. Infrastructure

| Resource | Detail |
|---|---|
| **VM** | GCP `e2-micro`, Ubuntu 22.04 LTS, `us-central1` |
| **GCS bucket** | `hn-surge-dashboard-01` (project `project-8299dfb6-57e5-4dcf-bc0`) |
| **Parquet prefix** | `gs://hn-surge-dashboard-01/parquet/` |
| **Redis** | Local `redis-server` on VM, port 6379, no auth |
| **Branch** | `feature/phase-2` |
| **Live process** | `tmux` session, Session 1 = `live_hn()`, Session 2 = dashboard |

---

## 9. Known Open Issues

See `docs/ISSUES.md` for full detail. Summary of open items:

| # | Issue | Severity |
|---|---|---|
| #10 | Baseline freeze corrupts 7-day history during multi-week surges | Medium |
| #11 | Redundant `texts` column doubles Parquet storage | Low |
| #12 | AlertGate cooldown lost on process restart (in-memory mode) | Medium |
| #13 | `text_indices` not written to Parquet — cluster attribution non-auditable | Low |
| #18 | Flash/sustained classification not wired into live pipeline | High |
