# Surge Detection Engine

> Real-time event detection across social discussion platforms — built to spot when the internet starts talking about something before it becomes mainstream news.

Monitors topic channels on social platforms, detects statistical volume surges using a Z-score Schmitt trigger, and classifies NLP clusters into typed event candidates: `viral_post`, `topic_surge`, or `event_candidate`.

**Current platform:** Hacker News (via Firebase REST API). Reddit and Twitter ingestion are planned — the pipeline is designed for multi-source ingestion from the ground up.

---

## How It Works

```
Platform API (HN Firebase)
  │
  ▼
HNTopicRouter          — classifies each story into 1 of 8 topic channels
  │
  ▼
RedisStateManager      — maintains a 2-hour sliding window per channel (ZSET)
  │
  ▼
SlidingWindowTripwire  — Z-score + Schmitt trigger; fires AnomalyEvent on surge
  │
  ├──▶ ParquetArchiver ──▶ GCS (raw anomaly Parquet, one file per event)
  ├──▶ SQLite           — anomaly metadata for dashboard markers
  └──▶ WebhookDispatcher ──▶ Discord alert

── offline, runs locally ──────────────────────────────────────────────────────

batch_enricher.py
  │  downloads raw Parquet from GCS
  ├──▶ DBSCANContextEngine  — SentenceTransformer → UMAP → DBSCAN → c-TF-IDF
  ├──▶ story attribution    — links cluster items back to source stories
  ├──▶ CandidateBuilder     — scores and classifies each cluster
  └──▶ CandidateArchiver    — writes EventCandidate Parquet locally
```

**Two-machine split:** the VM (e2-micro, 1 GB RAM) runs the live pipeline 24/7. The laptop runs the NLP enricher offline — SentenceTransformer alone exceeds VM memory.

---

## Platform Roadmap

| Platform | Status | Notes |
|---|---|---|
| Hacker News | Live | Firebase REST API, 8 topic channels |
| Reddit | Pending | API approval in progress |
| Twitter/X | Planned | Requires reworked domain-spread scoring |

All platform-specific logic is isolated in `src/ingestion/`. The anomaly detection, NLP, and EventCandidate layers are platform-neutral and require no changes when a new source is added.

---

## Repository Structure

```
src/
  main.py                     Entry points: live_hn() and Reddit backtest
  models.py                   AnomalyEvent, Comment dataclasses

  ingestion/
    base.py                   DataIngestor ABC — stream() → Iterator[Dict]
    hacker_news.py            HackerNewsIngestor, HNTopicRouter, HNCsvIngestor
    reddit.py                 ZstFileIngestor — backtest only

  pipeline/
    sliding_tripwire.py       Z-score Schmitt trigger detector
    tripwire.py               Tumbling window detector (Reddit backtest only)
    context.py                DBSCANContextEngine — embeddings → UMAP → DBSCAN
    alert_gate.py             Per-channel 30-min cooldown gate
    filter.py                 SubredditFilter — channel routing + bot removal

  storage/
    state_manager.py          RedisStateManager — ZSET sliding window
    parquet_archiver.py       Atomic Parquet writer → GCS (one file per anomaly)
    candidate_archiver.py     Atomic Parquet writer for EventCandidates (local)

  events/
    models.py                 EventCandidate dataclass
    classifier.py             classify() — kind + event_score from cluster features
    candidate_builder.py      CandidateBuilder — maps enriched rows to candidates

  alerting/
    webhooks.py               WebhookDispatcher — Discord/Slack fire-and-forget

  reporting/
    batch_enricher.py         Offline: GCS download → DBSCAN → story attribution
                              → EventCandidate classification → local Parquet
    dashboard_generator.py    Plotly 4×2 channel dashboard → GCS index.html

data/                         (gitignored)
  parquet_enriched/           NLP-enriched Parquet with cluster + item attribution
    .processed                Manifest of GCS blobs already enriched
  event_candidates/           EventCandidate Parquet (date-partitioned, local only)
  dbs/
    anomalies_hn_live.db      SQLite — anomaly metadata for dashboard markers
```

---

## Detection Pipeline

### 1. Channel Routing

Each HN story is scored against 8 topic channels using weighted keywords:

| Tier | Weight | Example |
|---|---|---|
| 3 | Exact brand / identifier | `openai`, `cve-`, `bitcoin` |
| 2 | Domain jargon | `machine learning`, `vulnerability` |
| 1 | Generic topic words | `ai`, `security`, `startup` |

Domain boosts apply on top: `arxiv.org → science+3`, `krebsonsecurity.com → security+3`. Comments inherit their parent story's channel. Unmatched stories → `general`.

### 2. Sliding Window + Z-Score

```
WINDOW_TTL        = 7200s   (2-hour sliding window)
HISTORY_SIZE      = 168     (7 days of hourly counts)
MIN_HISTORY       = 24      (cold-start guard)
MIN_COUNT         = 10      (low-volume noise floor)
Z_THRESHOLD       = 3.0     (enter ELEVATED)
RELEASE_THRESHOLD = 2.0     (exit ELEVATED)
EVAL_INTERVAL     = 300s
BASELINE_INTERVAL = 3600s
```

**Schmitt trigger hysteresis:** once ELEVATED (Z ≥ 3.0), stays ELEVATED until Z ≤ 2.0. Prevents false start/stop chatter during sustained surges.

**Baseline freeze:** while ELEVATED, the hourly push records the last clean count instead of the anomalous one — keeping the baseline anchored to pre-event ambient noise.

### 3. NLP Enrichment (offline)

```
texts (up to 500 per anomaly)
  ↓ SentenceTransformer("all-MiniLM-L6-v2")  — 384-dim embeddings
  ↓ UMAP(n_components=2)                      — 2D projection
  ↓ DBSCAN(eps=0.5, min_samples=5)            — density clusters
  ↓ c-TF-IDF per cluster                      — top 8 distinctive keywords
  ↓ story attribution                         — links items → source stories
  ↓ CandidateBuilder                          — EventCandidate classification
```

### 4. EventCandidate Classification

Each cluster is independently classified:

| Kind | Condition |
|---|---|
| `unknown` | No attributed items — dropped silently |
| `viral_post` | `top_story_pct ≥ 0.80` OR `unique_stories ≤ 1` |
| `event_candidate` | `unique ≥ 3` AND `domains ≥ 2` AND `size ≥ 10` AND `keywords ≥ 2` |
| `topic_surge` | Everything else |

**Event score** (additive, 0–1):

```
0.25 × min(unique_stories / 5,  1.0)   conversation diversity
0.20 × (1.0 - top_story_pct)           not dominated by one story
0.20 × min(domain_count / 3,   1.0)   multi-source spread
0.15 × min(cluster_size / 100, 1.0)   volume
0.10 × min(z_score / 10.0,     1.0)   anomaly strength
0.10 × min(keyword_count / 5,  1.0)   cluster coherence
```

---

## Data Schemas

### Raw Anomaly Parquet
`parquet/{YYYY}/{MM}/{DD}/{channel}_{window_end}.parquet` (GCS)

| Column | Type | Description |
|---|---|---|
| channel | string | Topic channel |
| window_start / window_end | int64 | Unix timestamps |
| window_end_dt | string | Human-readable UTC |
| count | int64 | Items in 2-hour window |
| z_score / mean / std | float64 | Detection statistics |
| texts | list\<string\> | Up to 500 raw comment bodies |
| items | list\<struct\> | Per-item metadata (see below) |
| clusters | list\<struct\> | Filled by enricher (empty on VM) |

**Item struct:** `{item_id, text, story_id, story_title, domain, item_type, created_at}`

**Cluster struct:** `{cluster_id, size, noise_count, keywords, top_story_id, top_story_title, top_story_pct, unique_story_count, story_ids, top_domains}`

### EventCandidate Parquet
`data/event_candidates/{YYYY}/{MM}/{DD}/candidates_{channel}_{window_end}.parquet` (local)

| Column | Type | Description |
|---|---|---|
| candidate_id | string | `{source}:{channel}:{window_end}:{cluster_id}` |
| source | string | `hacker_news`, `reddit`, … |
| channel | string | Topic channel |
| kind | string | `viral_post` / `topic_surge` / `event_candidate` |
| event_score | float64 | 0–1 composite score |
| unique_conversation_count | int64 | Distinct stories in cluster |
| top_conversation_pct | float64 | Fraction from dominant story |
| top_story_id / top_story_title | int64 / string | Dominant story |
| top_domains | list\<string\> | Up to 5 most common domains |
| keywords | list\<string\> | c-TF-IDF cluster labels |
| z_score / window_count | float64 / int64 | Parent anomaly stats |

---

## GCS Bucket

Bucket: `hn-surge-dashboard-01`

```
parquet/YYYY/MM/DD/{channel}_{window_end}.parquet   ← raw anomaly ticks
index.html                                           ← live Plotly dashboard
```

Public dashboard: `https://storage.googleapis.com/hn-surge-dashboard-01/index.html`

---

## VM Setup (e2-micro, Ubuntu 22.04)

```bash
# Redis
docker run --name hn-redis -p 6379:6379 -d \
  --restart unless-stopped \
  -v redis-data:/data \
  redis:alpine redis-server --save 60 1

# Repo
git clone https://github.com/Umi9973/Reddit-Surge-Detection.git
cd Reddit-Surge-Detection
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
gcloud auth application-default login
export WEBHOOK_URL="https://discord.com/api/webhooks/..."
```

```bash
# Session 1 — live pipeline
python3 -m src.main

# Session 2 — dashboard (separate tmux window)
python3 -m src.reporting.dashboard_generator
```

**Updating (no warm-up required):**
```bash
git pull origin feature/phase-2 && python3 -m src.main
```

| Action | Wipes Redis? | Re-warm needed? |
|---|---|---|
| `git pull` + restart | No | No |
| VM reboot (volume mounted) | No | No |
| `docker restart hn-redis` | Yes | Yes (24 h) |
| `redis-cli FLUSHDB` | Yes | Yes (24 h) |

---

## Laptop Setup (NLP enricher)

```bash
pip install sentence-transformers umap-learn scikit-learn pyarrow
gcloud auth application-default login
```

```bash
python -m src.reporting.batch_enricher
```

The enricher downloads raw Parquet from GCS, runs NLP enrichment, attributes items to source stories, classifies clusters into EventCandidates, and writes results locally. Re-run whenever you want to analyse recent anomalies. To force re-processing of all files, delete `data/parquet_enriched/.processed`.

---

## Branch

Active development: `feature/phase-2`
