from __future__ import annotations

import sqlite3
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, List

import fakeredis
import psutil

from .ingestion.reddit import ZstFileIngestor
from .pipeline.alert_gate import AlertGate
from .pipeline.context import DBSCANContextEngine
from .pipeline.filter import SubredditFilter
from .pipeline.sliding_tripwire import SlidingWindowTripwire
from .storage.state_manager import RedisStateManager
from .models import Comment, AnomalyEvent

TARGET_SUBREDDITS = [
    "gaming", "Games", "pcgaming", "PS5", "XboxSeriesX", "NintendoSwitch",
    "movies", "television", "entertainment", "popculturechat", "Music",
    "news", "worldnews",
]

NOV_START = 1698796800  # 2023-11-01 00:00:00 UTC
NOV_END   = 1701388800  # 2023-12-01 00:00:00 UTC

_ROOT       = Path(__file__).resolve().parent.parent
DATA_FILE   = str(_ROOT / "data" / "raw_dumps" / "RC_2023-11.zst")
PHASE1_DB   = str(_ROOT / "data" / "dbs" / "anomalies(Reddit November).db")
PHASE2_DB   = str(_ROOT / "data" / "dbs" / "anomalies_phase2_nov.db")
LOG_DIR     = _ROOT / "data" / "logs"
ARCHIVE_DIR = LOG_DIR / "archive"
LATEST_LOG  = LOG_DIR / "backtest_latest.txt"

EVAL_INTERVAL     = SlidingWindowTripwire.EVAL_INTERVAL      # 300s  (5 min)
BASELINE_INTERVAL = SlidingWindowTripwire.BASELINE_INTERVAL  # 3600s (1 hr)

_PROCESS = psutil.Process()


# ---------------------------------------------------------------------------
# Logging — dual-write to stdout + live log file
# ---------------------------------------------------------------------------

def _setup_logging() -> IO[str]:
    """Archive previous log if it exists, then open a fresh log file."""
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    if LATEST_LOG.exists():
        ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
        LATEST_LOG.rename(ARCHIVE_DIR / f"backtest_{ts}.txt")

    return LATEST_LOG.open("w", encoding="utf-8")


def _log(msg: str, f: IO[str]) -> None:
    print(msg)
    f.write(msg + "\n")
    f.flush()


# ---------------------------------------------------------------------------
# Lightweight per-hour stats accumulator
# ---------------------------------------------------------------------------

class _HourStats:
    def __init__(self) -> None:
        self.comments:      int   = 0
        self.ingest_ms:     float = 0.0
        self.eval_ms:       float = 0.0
        self.eval_calls:    int   = 0
        self.baseline_ms:   float = 0.0

    def reset(self) -> None:
        self.comments    = 0
        self.ingest_ms   = 0.0
        self.eval_ms     = 0.0
        self.eval_calls  = 0
        self.baseline_ms = 0.0

    @property
    def avg_ingest_us(self) -> float:
        return (self.ingest_ms / max(1, self.comments)) * 1000

    @property
    def avg_eval_ms(self) -> float:
        return self.eval_ms / max(1, self.eval_calls)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _init_phase2_db(path: str) -> sqlite3.Connection:
    """Drop and recreate tables — guarantees a clean slate on every run."""
    conn = sqlite3.connect(path)
    conn.executescript("""
        DROP TABLE IF EXISTS clusters;
        DROP TABLE IF EXISTS anomaly_texts;
        DROP TABLE IF EXISTS anomalies;
        DROP TABLE IF EXISTS hourly_metrics;
        CREATE TABLE anomalies (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            subreddit      TEXT    NOT NULL,
            window_start   INTEGER NOT NULL,
            window_end     INTEGER NOT NULL,
            window_end_dt  TEXT    NOT NULL,
            count          INTEGER NOT NULL,
            z_score        REAL    NOT NULL,
            mean           REAL    NOT NULL,
            std            REAL    NOT NULL
        );
        CREATE TABLE clusters (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            anomaly_id  INTEGER NOT NULL,
            keywords    TEXT    NOT NULL
        );
        CREATE TABLE anomaly_texts (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            anomaly_id  INTEGER NOT NULL,
            text        TEXT    NOT NULL
        );
        CREATE TABLE hourly_metrics (
            hour_ts        INTEGER NOT NULL,
            hour_dt        TEXT    NOT NULL,
            comments       INTEGER NOT NULL,
            avg_ingest_us  REAL    NOT NULL,
            avg_eval_ms    REAL    NOT NULL,
            baseline_ms    REAL    NOT NULL,
            memory_mb      REAL    NOT NULL
        );
    """)
    conn.commit()
    return conn


def _save_anomaly(conn: sqlite3.Connection, ev: AnomalyEvent) -> None:
    dt = datetime.fromtimestamp(ev.window_end, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    cur = conn.execute(
        "INSERT INTO anomalies "
        "(subreddit, window_start, window_end, window_end_dt, count, z_score, mean, std) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (ev.subreddit, ev.window_start, ev.window_end, dt, ev.count, ev.z_score, ev.mean, ev.std),
    )
    anomaly_id = cur.lastrowid
    if ev.items:
        conn.executemany(
            "INSERT INTO anomaly_texts (anomaly_id, text) VALUES (?, ?)",
            [(anomaly_id, item["text"]) for item in ev.items],
        )


def _save_metrics(conn: sqlite3.Connection, hour_ts: int, s: _HourStats, mem_mb: float) -> None:
    dt = datetime.fromtimestamp(hour_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    conn.execute(
        "INSERT INTO hourly_metrics "
        "(hour_ts, hour_dt, comments, avg_ingest_us, avg_eval_ms, baseline_ms, memory_mb) "
        "VALUES (?,?,?,?,?,?,?)",
        (hour_ts, dt, s.comments, s.avg_ingest_us, s.avg_eval_ms, s.baseline_ms, mem_mb),
    )


def _load_phase1(conn: sqlite3.Connection) -> list:
    return conn.execute("""
        SELECT subreddit, window_start, count, z_score
        FROM anomalies ORDER BY window_start
    """).fetchall()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _to_comment(raw: dict) -> Comment:
    return Comment(
        id=raw.get("id", ""),
        subreddit=raw.get("subreddit", ""),
        body=raw.get("body", ""),
        timestamp=int(raw.get("timestamp", 0)),
        author=raw.get("author", ""),
        score=int(raw.get("score", 0)),
    )


def run() -> None:
    sys.stdout.reconfigure(encoding="utf-8")

    lf = _setup_logging()
    run_ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    _log("=" * 70, lf)
    _log(f"  Phase 2 Backtest Driver — Full November 2023", lf)
    _log(f"  Run started : {run_ts}", lf)
    _log(f"  Log file    : {LATEST_LOG}", lf)
    _log(f"  Output DB   : {Path(PHASE2_DB).name}", lf)
    _log("=" * 70, lf)
    _log(f"\n  NOTE: metrics are fakeredis (in-process). ZADD/ZCOUNT timings", lf)
    _log(f"  reflect data-volume scaling, not real Redis wire latency.\n", lf)

    r2_conn  = _init_phase2_db(PHASE2_DB)
    r        = fakeredis.FakeRedis(decode_responses=True)
    state    = RedisStateManager(r)
    tripwire = SlidingWindowTripwire(state, TARGET_SUBREDDITS)
    gate     = AlertGate()
    ingestor = ZstFileIngestor(DATA_FILE)
    filter_  = SubredditFilter(TARGET_SUBREDDITS)

    phase2_anomalies: List[AnomalyEvent] = []
    total_comments = 0
    stats = _HourStats()
    ticks: dict = {"eval": None, "base": None}

    def fire_ticks(up_to: int) -> None:
        while True:
            e, b = ticks["eval"], ticks["base"]
            if e is None or min(e, b) > up_to:
                break

            if e <= b:
                t0         = time.perf_counter()
                raw_events = tripwire.evaluation_tick(e)
                stats.eval_ms    += (time.perf_counter() - t0) * 1000
                stats.eval_calls += 1
                events = gate.process(raw_events)

                for ev in events:
                    phase2_anomalies.append(ev)
                    _save_anomaly(r2_conn, ev)
                    dt = datetime.fromtimestamp(ev.window_end, tz=timezone.utc).strftime("%b %d %H:%M UTC")
                    _log(
                        f"  *** ANOMALY  r/{ev.subreddit:<16} | {dt} | "
                        f"count={ev.count:>5} | z={ev.z_score} ***",
                        lf,
                    )
                ticks["eval"] = e + EVAL_INTERVAL

            else:
                t0 = time.perf_counter()
                tripwire.baseline_tick(b)
                stats.baseline_ms = (time.perf_counter() - t0) * 1000

                mem_mb = _PROCESS.memory_info().rss / (1024 * 1024)
                _save_metrics(r2_conn, b, stats, mem_mb)
                r2_conn.commit()

                dt = datetime.fromtimestamp(b, tz=timezone.utc).strftime("%b %d %H:%M UTC")
                _log(
                    f"  [hr {dt}] "
                    f"comments={stats.comments:>6,} | "
                    f"ingest={stats.avg_ingest_us:>5.1f}µs avg | "
                    f"eval={stats.avg_eval_ms:>5.2f}ms avg | "
                    f"baseline={stats.baseline_ms:.2f}ms | "
                    f"mem={mem_mb:.1f}MB",
                    lf,
                )
                stats.reset()
                ticks["base"] = b + BASELINE_INTERVAL

    _log(f"  Streaming {Path(DATA_FILE).name} ...\n", lf)
    wall_start = time.perf_counter()

    for raw in filter_.stream(ingestor.stream()):
        ts = int(raw["timestamp"])

        if ts >= NOV_END:
            break
        if ts < NOV_START:
            continue

        if ticks["eval"] is None:
            ticks["eval"] = ((ts // EVAL_INTERVAL) + 1) * EVAL_INTERVAL
            ticks["base"] = ((ts // BASELINE_INTERVAL) + 1) * BASELINE_INTERVAL

        fire_ticks(ts - 1)

        t0 = time.perf_counter()
        tripwire.ingest(_to_comment(raw))
        stats.ingest_ms += (time.perf_counter() - t0) * 1000
        stats.comments  += 1
        total_comments  += 1

    fire_ticks(NOV_END)
    wall_sec = time.perf_counter() - wall_start

    # --- NLP enrichment (runs after streaming so the main loop isn't paused) ---
    _log(f"\n{'=' * 70}", lf)
    _log(f"  NLP ENRICHMENT", lf)
    _log(f"{'=' * 70}", lf)
    _log(f"  Loading sentence-transformer model...", lf)

    nlp_engine   = DBSCANContextEngine()
    anomaly_rows = r2_conn.execute(
        "SELECT id, subreddit, window_start FROM anomalies ORDER BY window_start"
    ).fetchall()

    total_clusters = 0
    for (anomaly_id, subreddit, window_start) in anomaly_rows:
        texts = [row[0] for row in r2_conn.execute(
            "SELECT text FROM anomaly_texts WHERE anomaly_id = ?", (anomaly_id,)
        ).fetchall()]

        if not texts:
            continue

        clusters = nlp_engine.summarize_anomaly(texts, window_start=window_start)

        for c in clusters:
            r2_conn.execute(
                "INSERT INTO clusters (anomaly_id, keywords) VALUES (?, ?)",
                (anomaly_id, ", ".join(c["keywords"])),
            )

        if clusters:
            total_clusters += len(clusters)
            dt         = datetime.fromtimestamp(window_start, tz=timezone.utc).strftime("%b %d %H:%M UTC")
            kw_preview = " | ".join(", ".join(c["keywords"][:4]) for c in clusters[:2])
            _log(f"  r/{subreddit:<16} {dt}  {len(clusters)} cluster(s)  [{kw_preview}]", lf)

    r2_conn.commit()
    _log(f"\n  NLP done: {total_clusters} clusters across {len(anomaly_rows)} anomalies", lf)

    # --- Overall performance summary ---
    peak_mem = max(
        r2_conn.execute("SELECT MAX(memory_mb) FROM hourly_metrics").fetchone()[0] or 0,
        _PROCESS.memory_info().rss / (1024 * 1024),
    )
    avg_ingest_us = r2_conn.execute("SELECT AVG(avg_ingest_us) FROM hourly_metrics").fetchone()[0] or 0
    avg_eval_ms   = r2_conn.execute("SELECT AVG(avg_eval_ms)   FROM hourly_metrics").fetchone()[0] or 0

    _log(f"\n{'=' * 70}", lf)
    _log(f"  PERFORMANCE SUMMARY", lf)
    _log(f"{'=' * 70}", lf)
    _log(f"  Wall time            : {wall_sec:.1f}s", lf)
    _log(f"  Comments ingested    : {total_comments:,}", lf)
    _log(f"  Throughput           : {total_comments / wall_sec:,.0f} comments/sec", lf)
    _log(f"  Avg ingest latency   : {avg_ingest_us:.1f}µs  (fakeredis ZADD — not real Redis)", lf)
    _log(f"  Avg eval tick time   : {avg_eval_ms:.2f}ms  (ZCOUNT + numpy across 13 subs)", lf)
    _log(f"  Peak RSS memory      : {peak_mem:.1f}MB  (fakeredis in-process heap)", lf)
    _log(f"  Phase 2 anomalies    : {len(phase2_anomalies)}", lf)

    # --- Phase 1 vs Phase 2 comparison by subreddit ---
    p1_conn  = sqlite3.connect(PHASE1_DB)
    p1_rows  = _load_phase1(p1_conn)
    p1_conn.close()

    p1_by_sub = Counter(r[0] for r in p1_rows)
    p2_by_sub = Counter(ev.subreddit for ev in phase2_anomalies)
    all_subs  = sorted(p1_by_sub.keys() | p2_by_sub.keys())

    _log(f"\n{'=' * 70}", lf)
    _log(f"  PHASE 1 vs PHASE 2 — anomaly counts by subreddit (full November)", lf)
    _log(f"{'=' * 70}", lf)
    _log(f"  {'subreddit':<18} {'Phase1':>7} {'Phase2':>7}", lf)
    _log(f"  {'-'*18} {'-'*7} {'-'*7}", lf)
    for sub in all_subs:
        _log(f"  r/{sub:<16} {p1_by_sub[sub]:>7} {p2_by_sub[sub]:>7}", lf)
    _log(f"  {'-'*18} {'-'*7} {'-'*7}", lf)
    _log(f"  {'TOTAL':<18} {len(p1_rows):>7} {len(phase2_anomalies):>7}", lf)

    _log(f"\n  Results saved to : {Path(PHASE2_DB).name}", lf)
    _log(f"  Log saved to     : {LATEST_LOG}", lf)

    lf.close()
    r2_conn.close()
    state.flush()
    gate.flush()


if __name__ == "__main__":
    run()
