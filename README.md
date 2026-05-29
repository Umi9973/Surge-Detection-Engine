# HN Surge Detection

A real-time anomaly detection pipeline for Hacker News. Monitors 8 topic
channels (ai, tech, security, startup, crypto, science, policy, general),
detects statistical volume surges using a Z-score Schmitt trigger, fires
Discord alerts, uploads interactive Plotly dashboards to Google Cloud Storage,
and runs NLP cluster analysis locally on enriched Parquet archives.

---

## Architecture Overview

The system is split into two machines to avoid OOM on the VM:

```
VM (e2-micro, 1 GB RAM)                  Laptop
─────────────────────────────            ──────────────────────────────
HackerNewsIngestor (Firebase API)  →     batch_enricher.py
  ↓                                        ↓ downloads from GCS
SubredditFilter (channel router)           ↓ runs SentenceTransformer
  ↓                                        ↓   + UMAP + DBSCAN
SlidingWindowTripwire (Z-score)            ↓ writes enriched Parquet
  ↓ anomaly fires                            locally
ParquetArchiver → GCS (raw Parquet)
  ↓
SQLite (anomalies_hn_live.db)
  ↓
WebhookDispatcher → Discord

dashboard_generator.py (separate process)
  Redis + SQLite → Plotly HTML → GCS (index.html, every 5 min)
```

---

## Repository Structure

```
src/
  main.py                        Entry point — live_hn() and Reddit backtest
  models.py                      AnomalyEvent, Comment dataclasses
  ingestion/
    hacker_news.py               Live Firebase ingestor + HNTopicRouter + CSV backtest
    reddit.py                    Zstandard .zst file ingestor (backtest only)
  pipeline/
    sliding_tripwire.py          Z-score detector with Schmitt trigger hysteresis
    tripwire.py                  Tumbling window detector (Reddit backtest only)
    context.py                   DBSCANContextEngine — SentenceTransformer+UMAP+DBSCAN
    alert_gate.py                Per-channel cooldown gate for Discord notifications
    filter.py                    SubredditFilter — routes raw stream to channels
  storage/
    state_manager.py             RedisStateManager — ZSET sliding window + history LIST
    parquet_archiver.py          Writes one Parquet file per anomaly → GCS
  alerting/
    webhooks.py                  WebhookDispatcher — Discord/Slack fire-and-forget
  reporting/
    dashboard_generator.py       Plotly 4×2 trellis → uploads index.html to GCS
    batch_enricher.py            Laptop NLP enricher — pulls GCS Parquet, adds clusters
data/                            (gitignored)
  dbs/
    anomalies_hn_live.db         SQLite — anomaly metadata for dashboard red markers
  parquet/                       Raw unenriched Parquet (one file per anomaly)
  parquet_enriched/              NLP-enriched Parquet with cluster structure
    .processed                   Manifest of GCS blobs already enriched
```

---

## Detection Model

### Channel Routing (HNTopicRouter)
Each HN story is classified into one channel by weighted keyword scoring:
- **Tier 3** (weight 3): exact brand names — `gemini`, `openai`, `bitcoin`, `cve`
- **Tier 2** (weight 2): domain jargon — `machine learning`, `vulnerability`, `blockchain`
- **Tier 1** (weight 1): generic words — `ai`, `security`, `crypto`
- **Domain boosts**: `arxiv.org` → science+3, `krebsonsecurity.com` → security+3, etc.

Comments inherit their parent story's channel via an in-memory `_item_topics` cache.
Stories with no keyword match go to `general`.

### Sliding Window (RedisStateManager)
- Each item stored in a Redis ZSET (`window:{channel}`) with Unix timestamp as score
- `get_window_count(channel, now)` = items with timestamp in `[now-7200, now]` (2-hour window)
- `baseline_tick()` fires every hour, pushes the 2-hour count to a Redis LIST (`history:{channel}`)
- History capped at 168 entries (7 days × 24 hours)

### Z-Score Detection (SlidingWindowTripwire)
```
MIN_HISTORY  = 24     # require 24 hourly samples before alerting (cold-start guard)
MIN_COUNT    = 10     # ignore windows with fewer than 10 items (low-volume noise floor)
Z_THRESHOLD  = 3.0   # enter ELEVATED state (anomaly begins)
RELEASE      = 2.0   # exit ELEVATED state (anomaly ends)
EVAL_INTERVAL     = 300s   (every 5 minutes)
BASELINE_INTERVAL = 3600s  (every 1 hour)
```

**Schmitt Trigger hysteresis**: once a channel enters ELEVATED (Z ≥ 3.0), it stays
ELEVATED and fires events on every eval tick until Z drops to ≤ 2.0. This captures
sustained surges without repeated false-start/stop noise.

**Baseline freeze**: while ELEVATED, `baseline_tick()` pushes the last clean count
instead of the anomalous current count, keeping the baseline anchored to pre-event
ambient noise so Z-score doesn't decay to zero during a prolonged surge.

---

## Parquet Schema

One file per anomaly tick: `data/parquet/{YYYY}/{MM}/{DD}/{channel}_{window_end}.parquet`

| Column | Type | Description |
|---|---|---|
| channel | string | HN topic channel (ai, tech, etc.) |
| window_start | int64 | Unix timestamp — start of 2-hour window |
| window_end | int64 | Unix timestamp — end of 2-hour window |
| window_end_dt | string | Human-readable UTC datetime |
| count | int64 | Number of items in the 2-hour window |
| z_score | float64 | Z-score at detection time |
| mean | float64 | Baseline mean at detection time |
| std | float64 | Baseline std at detection time |
| texts | list\<string\> | Up to 500 raw comment/title bodies |
| clusters | list\<struct\> | NLP clusters (empty on VM, filled by enricher) |

**Cluster struct**: `{cluster_id: int, size: int, noise_count: int, keywords: list<string>}`

Enriched files written to: `data/parquet_enriched/{YYYY}/{MM}/{DD}/{channel}_{window_end}.parquet`

---

## GCS Bucket Layout

Bucket: `hn-surge-dashboard-01` (project: `project-8299dfb6-57e5-4dcf-bc0`)

```
parquet/YYYY/MM/DD/{channel}_{window_end}.parquet   ← raw anomaly ticks (VM uploads)
index.html                                           ← live Plotly dashboard (refreshed every 5 min)
```

Public URL: `https://storage.googleapis.com/hn-surge-dashboard-01/index.html`

---

## VM Setup (e2-micro, Debian)

### First-time setup
```bash
# Install Docker
sudo apt-get update && sudo apt-get install -y docker.io
sudo usermod -aG docker $USER && newgrp docker

# Start Redis (persistent, auto-restarts on reboot)
docker run --name hn-redis -p 6379:6379 -d \
  --restart unless-stopped \
  -v redis-data:/data \
  redis:alpine redis-server --save 60 1

# Clone repo and install lean deps (no PyTorch)
git clone https://github.com/Umi9973/Reddit-Surge-Detection.git
cd Reddit-Surge-Detection
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# GCP auth
gcloud auth application-default login
```

### Environment variables
```bash
export WEBHOOK_URL="https://discord.com/api/webhooks/..."   # Discord webhook (never commit)
```

### Running (use tmux — two separate sessions)
```bash
# Session 1: live pipeline
source venv/bin/activate
python3 -m src.main

# Session 2: dashboard generator
source venv/bin/activate
python3 -m src.reporting.dashboard_generator
```

### Updating code (no warm-up required — Redis is untouched)
```bash
# Session 1 only (Ctrl+C, then):
git pull origin feature/phase-2
python3 -m src.main
# Leave Session 2 running — dashboard_generator doesn't need restart
```

### What wipes the 24-hour warm-up
| Action | Wipes Redis? | Warm-up needed? |
|---|---|---|
| `git pull` + restart `src.main` | No | No |
| VM reboot (with volume mount) | No (RDB snapshot) | No |
| `docker restart hn-redis` | Yes | Yes |
| `redis-cli FLUSHDB` | Yes | Yes |

---

## Laptop Setup (NLP enricher)

Requires the heavy ML stack (PyTorch, sentence-transformers) — do NOT install on VM.

```bash
pip install sentence-transformers umap-learn scikit-learn
gcloud auth application-default login
```

### Running the enricher
```bash
cd Reddit-Surge-Detection
python -m src.reporting.batch_enricher
```

The enricher:
1. Lists GCS blobs under `parquet/`
2. Skips blobs already in `data/parquet_enriched/.processed`
3. Downloads each new file, runs `DBSCANContextEngine` (SentenceTransformer → UMAP → DBSCAN → c-TF-IDF)
4. Writes enriched Parquet to `data/parquet_enriched/` with cluster structure
5. Marks blob as done in `.processed`

Run it whenever you want to analyse recent anomalies — no scheduling required.

---

## NLP Pipeline (DBSCANContextEngine)

Located in `src/pipeline/context.py`.

```
texts (up to 500)
  ↓ SentenceTransformer("all-MiniLM-L6-v2")   ~384-dim embeddings
  ↓ UMAP(n_components=2, random_state=42)       2D projection
  ↓ DBSCAN(eps=0.5, min_samples=5)              cluster labels (-1 = noise)
  ↓ c-TF-IDF per cluster                        top 8 keywords each
  ↓ dynamic baseline penalty                    suppresses chronically common words
```

Returns a list of cluster dicts: `{cluster_id, size, noise_count, keywords}`.
Returns `[]` if fewer than 5 texts or all points are noise.

---

## Discord Alerts (WebhookDispatcher)

Reads `WEBHOOK_URL` from environment. Fires in a background thread (fire-and-forget).
No-op if env var is unset. Rate-limit backoff: HTTP 429 → sleeps and retries once.

Alert format:
```
🚨 Surge Detected — ai
Count: 375 | Z-score: 3.54 | 2026-05-19 19:30 UTC
```

---

## Dashboard (dashboard_generator.py)

- Reads Redis `history:{channel}` LISTs → blue baseline lines (hourly counts)
- Reads SQLite `anomalies_hn_live.db` → red × markers (anomaly timestamps)
- 4×2 Plotly trellis: ai, tech, security, startup, crypto, science, policy, general
- Uploads `index.html` to GCS every 5 minutes
- Dark theme: `paper_bgcolor="#1a1a2e"`, `plot_bgcolor="#16213e"`

**Known behaviour**: baseline lines look perfectly straight for the first 2 hours of
operation. This is a fill-up artifact — the 2-hour ZSET window starts at 0 and
grows monotonically until it reaches steady state. After 2+ hours the lines
become naturally jagged.

---

## Key Constants (quick reference)

| Constant | Value | Location |
|---|---|---|
| MIN_HISTORY | 24 hours | sliding_tripwire.py |
| MIN_COUNT | 10 items | sliding_tripwire.py |
| Z_THRESHOLD | 3.0 | sliding_tripwire.py |
| RELEASE_THRESHOLD | 2.0 | sliding_tripwire.py |
| WINDOW_TTL | 7200s (2hr) | state_manager.py |
| HISTORY_SIZE | 168 (7 days) | state_manager.py |
| TEXT_CAP | 500 texts | state_manager.py |
| EVAL_INTERVAL | 300s (5 min) | sliding_tripwire.py |
| BASELINE_INTERVAL | 3600s (1 hr) | sliding_tripwire.py |
| REFRESH_SEC | 300s (5 min) | dashboard_generator.py |
| GCS_BUCKET | hn-surge-dashboard-01 | parquet_archiver.py |
| GCS_PROJECT | project-8299dfb6-57e5-4dcf-bc0 | parquet_archiver.py |

---

## Branch

Active development: `feature/phase-2`
