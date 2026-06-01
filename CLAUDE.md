# CLAUDE.md: AI Agent Instructions

## Project Context

You are acting as a Senior Data Engineer on the **HN Surge Detection Engine**.

- **Phase 1** (complete): Historical Reddit backtest on `.zst` pushshift dumps. Tumbling-window tripwire, DBSCAN NLP, cross-subreddit FLASH/SUSTAINED aggregator. Benchmark: Grade A, 88% F1.
- **Phase 2** (active): Live real-time pipeline ingesting Hacker News via the Firebase REST API. Redis sliding window, Schmitt-trigger anomaly detector, GCS Parquet archival, offline batch enrichment. Reddit live API is still pending approval.
- **Phase 3** (future): Predictive intelligence — NER tagging, multi-platform ingestion, event consolidator port.

The live pipeline runs 24/7 on a GCP `e2-micro` VM (Ubuntu 22.04). Offline enrichment and analysis run locally on the developer's laptop.

---

## File Structure

```
src/
├── models.py                  # Comment and AnomalyEvent dataclasses — pipeline contract
├── main.py                    # Entry points: live_hn(), run() (Reddit backtest)
├── hn_backtest_driver.py      # HN November 2023 CSV backtest
├── backtest_driver.py         # Reddit November 2023 backtest (Phase 2 aggregator)
├── backfill_clusters.py       # Recovery: backfill clusters into old DBs
│
├── ingestion/
│   ├── base.py                # DataIngestor ABC — stream() → Iterator[Dict]
│   ├── hacker_news.py         # HackerNewsIngestor (live), HNCsvIngestor (backtest),
│   │                          # HNTopicRouter (8-channel classifier), _HNItemProcessor
│   └── reddit.py              # ZstFileIngestor — backtest only
│
├── pipeline/
│   ├── filter.py              # SubredditFilter — channel routing + bot removal
│   ├── sliding_tripwire.py    # SlidingWindowTripwire — Schmitt trigger, Z-score, Redis
│   ├── tripwire.py            # TumblingWindowTripwire — Phase 1 / Reddit backtest only
│   ├── context.py             # DBSCANContextEngine — embeddings → UMAP → DBSCAN → c-TF-IDF
│   └── alert_gate.py          # AlertGate — 30-min cooldown gate
│
├── storage/
│   ├── state_manager.py       # StateManager ABC + RedisStateManager (ZSET sliding window)
│   └── parquet_archiver.py    # ParquetArchiver — atomic write → GCS upload → local delete
│
├── alerting/
│   └── webhooks.py            # WebhookDispatcher — Discord/Slack fire-and-forget
│
├── analysis/
│   └── aggregator.py          # FLASH/SUSTAINED/ISOLATED aggregator — backtest only
│
└── reporting/
    ├── batch_enricher.py      # Offline: GCS download → DBSCAN → story attribution → enriched Parquet
    └── dashboard_generator.py # Plotly 4×2 channel dashboard → GCS index.html
```

---

## Strict Engineering Rules

1. **Streaming only.** Never load a full dataset into memory. Use generators (`yield`), chunk-based reads, and `zstandard` streaming decompression. No `pandas.read_csv` on large files.

2. **Modularity.** Follow the pipeline architecture in `docs/ARCHITECTURE.md`. Every component communicates through an abstract interface. The math engine has no knowledge of Redis internals; the ingestor does zero math.

3. **Typing.** Use strict Python type hints (`-> Iterator[Dict]`, `List[Dict]`, etc.) throughout. Robust `try/except` on all external I/O boundaries (Firebase API, Redis, GCS, file reads).

4. **No premature abstraction.** Add only what the current task requires. No helper classes for hypothetical future use, no feature flags, no backwards-compatibility shims beyond what existing data requires.

5. **No comments unless the WHY is non-obvious.** Well-named identifiers are self-documenting. Only add a comment for hidden constraints, subtle invariants, or workarounds for specific bugs.

6. **Schema changes are additive.** New fields must have backward-compatible defaults in `_normalise_row()` (enricher) and the Redis fallback path (state manager). Never remove or rename existing Parquet columns while old files exist on GCS.

7. **Infrastructure is fixed.** GCS bucket: `hn-surge-dashboard-01`. Redis: `localhost:6379` on VM. Branch: `feature/phase-2`. Do not provision new cloud resources without discussion.

---

## Workflow

- **Always check `docs/TODO.md`** for the current objective before starting work.
- **Always check `docs/ISSUES.md`** before logging a new issue — deduplicate first.
- **Update `docs/TODO.md` checkboxes** when a step is complete and tested.
- **GIT DISCIPLINE:** Never run `git commit` or `git push` unless the user explicitly asks. These are always separate, user-approved steps.
- **Error documentation threshold:** Only log to `docs/ISSUES.md` if the issue demonstrates senior-level engineering insight — architecture shifts, subtle correctness bugs, performance bottlenecks, or concurrency hazards. Do not log syntax errors, typos, or basic API misuse.
- **Ask before adding new dependencies.** Especially anything that increases VM memory footprint or requires a new system package.
