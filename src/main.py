from __future__ import annotations

import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterator, List

import fakeredis

from .alerting.webhooks import WebhookDispatcher
from .ingestion.hacker_news import HackerNewsIngestor
from .ingestion.reddit import ZstFileIngestor
from .models import Comment
from .pipeline.alert_gate import AlertGate
from .pipeline.filter import SubredditFilter
from .pipeline.sliding_tripwire import SlidingWindowTripwire
from .pipeline.tripwire import TumblingWindowTripwire
from .storage.parquet_archiver import ParquetArchiver
from .storage.state_manager import RedisStateManager

# --- Config ---
TARGET_SUBREDDITS = [
    "gaming", "Games", "pcgaming", "PS5", "XboxSeriesX", "NintendoSwitch",
    "movies", "television", "entertainment", "popculturechat", "Music",
    "news", "worldnews",
]

# Full November 2023 backtest — Nov 1 builds rolling history, Nov 2–30 is detection
STREAM_CUTOFF_TS = 1701388800  # 2023-12-01 00:00:00 UTC

_ROOT       = Path(__file__).resolve().parent.parent
DB_PATH     = str(_ROOT / "data" / "dbs" / "anomalies.db")
LIVE_DB     = str(_ROOT / "data" / "dbs" / "anomalies_hn_live.db")
DATA_FILE   = str(_ROOT / "data" / "raw_dumps" / "RC_2023-11.zst")
PARQUET_DIR = _ROOT / "data" / "parquet"


# --- Database ---
def init_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS anomalies (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            subreddit     TEXT    NOT NULL,
            window_start  INTEGER NOT NULL,
            window_end    INTEGER NOT NULL,
            window_date   TEXT    NOT NULL,
            count         INTEGER NOT NULL,
            mean          REAL    NOT NULL,
            std           REAL    NOT NULL,
            z_score       REAL    NOT NULL,
            texts_sampled INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS clusters (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            anomaly_id  INTEGER NOT NULL REFERENCES anomalies(id),
            cluster_id  INTEGER NOT NULL,
            size        INTEGER NOT NULL,
            noise_count INTEGER NOT NULL,
            keywords    TEXT    NOT NULL
        );
    """)
    conn.commit()
    return conn


def save_anomaly(conn: sqlite3.Connection, anomaly: Dict, clusters: List[Dict]) -> int:
    window_date = datetime.fromtimestamp(anomaly["window_start"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    cur = conn.execute(
        """INSERT INTO anomalies
           (subreddit, window_start, window_end, window_date, count, mean, std, z_score, texts_sampled)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (
            anomaly["subreddit"], anomaly["window_start"], anomaly["window_end"],
            window_date, anomaly["count"],
            anomaly.get("mean", anomaly.get("median", 0.0)),
            anomaly.get("std",  anomaly.get("mad",    0.0)),
            anomaly["z_score"], len(anomaly["texts"]),
        ),
    )
    anomaly_id = cur.lastrowid
    for c in clusters:
        conn.execute(
            "INSERT INTO clusters (anomaly_id, cluster_id, size, noise_count, keywords) VALUES (?,?,?,?,?)",
            (anomaly_id, int(c["cluster_id"]), int(c["size"]), int(c["noise_count"]), ", ".join(c["keywords"])),
        )
    conn.commit()
    return anomaly_id


# --- Stream helpers ---
def capped_stream(source: Iterator[Dict]) -> Iterator[Dict]:
    for comment in source:
        if int(comment["timestamp"]) >= STREAM_CUTOFF_TS:
            return
        yield comment


# --- Main pipeline ---
def run() -> None:
    sys.stdout.reconfigure(encoding="utf-8")

    print("=" * 70)
    print("  Surge Detection Engine — Full November 2023 Backtest")
    print("=" * 70)

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    conn     = init_db(DB_PATH)
    ingestor = ZstFileIngestor(DATA_FILE)
    filter_  = SubredditFilter(TARGET_SUBREDDITS)
    tripwire = TumblingWindowTripwire(volatile_subreddits={"news", "worldnews"})
    nlp      = DBSCANContextEngine()

    print(f"\nStreaming full November 2023 UTC")
    print(f"Results will be saved to: anomalies.db\n")
    print("-" * 70)

    anomaly_count = 0
    source = capped_stream(filter_.stream(ingestor.stream()))

    for anomaly in tripwire.stream(source):
        anomaly_count += 1
        window_dt = datetime.fromtimestamp(anomaly["window_start"], tz=timezone.utc)
        date_str  = window_dt.strftime("%Y-%m-%d %H:00 UTC")

        clusters = nlp.summarize_anomaly(anomaly["texts"], window_start=anomaly["window_start"]) if anomaly["texts"] else []
        save_anomaly(conn, anomaly, clusters)

        print(
            f"[#{anomaly_count:03d}] r/{anomaly['subreddit']:<16} | {date_str} | "
            f"count={anomaly['count']:>5} | z={anomaly['z_score']:>5}"
        )
        for c in clusters:
            print(f"         Cluster {c['cluster_id']}: {c['size']} comments | keywords: {c['keywords']}")

    print("-" * 70)
    print(f"\nDone. {anomaly_count} total anomalies saved to anomalies.db")

    print("\n--- Top 20 Anomalies by Z-Score ---")
    rows = conn.execute("""
        SELECT subreddit, window_date, count, z_score
        FROM anomalies
        ORDER BY z_score DESC
        LIMIT 20
    """).fetchall()

    for r in rows:
        print(f"  r/{r[0]:<16} | {r[1]} | count={r[2]:>5} | z={r[3]}")

    conn.close()


def _init_live_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS anomalies (
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
    """)
    conn.commit()
    return conn


def _save_live_anomaly(conn: sqlite3.Connection, ev: AnomalyEvent) -> None:
    dt = datetime.fromtimestamp(ev.window_end, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    conn.execute(
        "INSERT INTO anomalies "
        "(channel, window_start, window_end, window_end_dt, count, z_score, mean, std) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (ev.subreddit, ev.window_start, ev.window_end, dt,
         ev.count, ev.z_score, ev.mean, ev.std),
    )
    conn.commit()


def _to_comment(raw: Dict) -> Comment:
    return Comment(
        id          = raw.get("id", ""),
        subreddit   = raw.get("subreddit", ""),
        body        = raw.get("body", ""),
        timestamp   = int(raw.get("timestamp", 0)),
        author      = raw.get("author", ""),
        score       = int(raw.get("score", 0)),
        story_id    = int(raw.get("story_id", 0)),
        story_title = raw.get("story_title", ""),
        domain      = raw.get("domain", ""),
        item_type   = raw.get("item_type", ""),
        item_id     = int(raw.get("item_id", 0)),
        created_at  = int(raw.get("created_at", 0)),
    )


# ---------------------------------------------------------------------------
# Live HN pipeline
# ---------------------------------------------------------------------------

TARGET_CHANNELS = ["ai", "security", "startup", "crypto", "science", "tech", "policy", "general"]

EVAL_INTERVAL     = SlidingWindowTripwire.EVAL_INTERVAL      # 300s
BASELINE_INTERVAL = SlidingWindowTripwire.BASELINE_INTERVAL  # 3600s


def live_hn(use_real_redis: bool = True) -> None:
    sys.stdout.reconfigure(encoding="utf-8")

    print("=" * 70)
    print("  Surge Detection Engine — Hacker News Live Feed")
    print("=" * 70)

    if use_real_redis:
        import redis
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    else:
        r = fakeredis.FakeRedis(decode_responses=True)

    state    = RedisStateManager(r)
    tripwire = SlidingWindowTripwire(state, TARGET_CHANNELS)
    gate     = AlertGate()
    ingestor = HackerNewsIngestor(poll_interval=5.0)
    filter_  = SubredditFilter(TARGET_CHANNELS)
    dispatcher = WebhookDispatcher()   # reads WEBHOOK_URL from env; no-op if unset

    ticks: dict = {"eval": None, "base": None}

    archiver   = ParquetArchiver(PARQUET_DIR)
    archiver.retry_pending()
    live_conn  = _init_live_db(LIVE_DB)

    def fire_ticks(up_to: int) -> None:
        while True:
            e, b = ticks["eval"], ticks["base"]
            if e is None or min(e, b) > up_to:
                break
            if e <= b:
                raw_events = tripwire.evaluation_tick(e)
                events     = gate.process(raw_events)
                for ev in events:
                    archiver.archive(ev)
                    _save_live_anomaly(live_conn, ev)
                    dispatcher.dispatch(ev, [])
                    dt    = datetime.fromtimestamp(ev.window_end, tz=timezone.utc).strftime("%b %d %H:%M UTC")
                    z_str = f"10.0+ raw={ev.z_score:.2f}" if ev.z_score > 10.0 else f"{ev.z_score:.2f}"
                    print(
                        f"  *** ANOMALY  {ev.subreddit:<12} | {dt} | "
                        f"count={ev.count:>5} | z={z_str} ***"
                    )
                ticks["eval"] = e + EVAL_INTERVAL
            else:
                tripwire.baseline_tick(b)
                dt = datetime.fromtimestamp(b, tz=timezone.utc).strftime("%b %d %H:%M UTC")
                print(f"  [baseline tick {dt}]")
                ticks["base"] = b + BASELINE_INTERVAL

    print("\n  Connecting to Hacker News Firebase API...")
    print("  (First poll seeds _last_id — historical items are skipped)\n")

    try:
        for raw in filter_.stream(ingestor.stream()):
            ts = int(raw["timestamp"])

            if ticks["eval"] is None:
                ticks["eval"] = ((ts // EVAL_INTERVAL) + 1) * EVAL_INTERVAL
                ticks["base"] = ((ts // BASELINE_INTERVAL) + 1) * BASELINE_INTERVAL

            fire_ticks(ts - 1)
            tripwire.ingest(_to_comment(raw))
    finally:
        archiver.close()
        live_conn.close()


if __name__ == "__main__":
    live_hn()
