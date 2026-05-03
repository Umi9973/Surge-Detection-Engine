from __future__ import annotations

import os
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Dict, Iterator, List

from context import DBSCANContextEngine
from filter import SubredditFilter
from ingestion import ZstFileIngestor
from tripwire import TumblingWindowTripwire

# --- Config ---
TARGET_SUBREDDITS = [
    "gaming", "Games", "pcgaming", "PS5", "XboxSeriesX", "NintendoSwitch",
    "movies", "television", "entertainment", "popculturechat", "Music",
    "news", "worldnews",
]

# Stream Dec 1–8: Dec 1–5 builds rolling history, Dec 6–8 is the detection target
TARGET_START_TS  = 1701820800  # 2023-12-06 00:00:00 UTC
STREAM_CUTOFF_TS = 1702080000  # 2023-12-09 00:00:00 UTC

DB_PATH   = os.path.join(os.path.dirname(__file__), "anomalies.db")
DATA_FILE = os.path.join(os.path.dirname(__file__), "RC_2023-12.zst")


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
    print("  Reddit Surge Detection — Backtest Dec 6–8")
    print("  Target: December 6–8, 2023 (The Game Awards)")
    print("=" * 70)

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    conn     = init_db(DB_PATH)
    ingestor = ZstFileIngestor(DATA_FILE)
    filter_  = SubredditFilter(TARGET_SUBREDDITS)
    tripwire = TumblingWindowTripwire(volatile_subreddits={"news", "worldnews"})
    nlp      = DBSCANContextEngine()

    print(f"\nStreaming from Dec 1 → Dec 8, 2023 UTC")
    print(f"Results will be saved to: anomalies.db\n")
    print("-" * 70)

    anomaly_count = 0
    source = capped_stream(filter_.stream(ingestor.stream()))

    for anomaly in tripwire.stream(source):
        anomaly_count += 1
        window_dt  = datetime.fromtimestamp(anomaly["window_start"], tz=timezone.utc)
        date_str   = window_dt.strftime("%Y-%m-%d %H:00 UTC")
        is_target  = anomaly["window_start"] >= TARGET_START_TS

        clusters = nlp.summarize_anomaly(anomaly["texts"]) if anomaly["texts"] else []
        save_anomaly(conn, anomaly, clusters)

        marker = "  <<<< DEC 6-8 TARGET" if is_target else ""
        print(
            f"[#{anomaly_count:02d}] r/{anomaly['subreddit']:<16} | {date_str} | "
            f"count={anomaly['count']:>5} | z={anomaly['z_score']:>5}{marker}"
        )
        for c in clusters:
            print(f"        Cluster {c['cluster_id']}: {c['size']} comments | keywords: {c['keywords']}")

    print("-" * 70)
    print(f"\nDone. {anomaly_count} total anomalies saved to anomalies.db")

    print("\n--- Dec 6-8 Summary (Game Awards Window) ---")
    rows = conn.execute("""
        SELECT subreddit, window_date, count, z_score
        FROM anomalies
        WHERE window_start >= ?
        ORDER BY z_score DESC
    """, (TARGET_START_TS,)).fetchall()

    if rows:
        for r in rows:
            print(f"  r/{r[0]:<16} | {r[1]} | count={r[2]} | z={r[3]}")
    else:
        print("  No anomalies detected in the Dec 6-8 window.")

    conn.close()


if __name__ == "__main__":
    run()
