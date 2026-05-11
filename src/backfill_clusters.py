from __future__ import annotations

"""
Recovery tool: backfill keyword clusters into a Phase 2 anomalies DB.

backtest_driver.py now runs NLP enrichment inline after streaming, so this
script is only needed to repair an older DB that is missing the clusters table
(e.g. generated before the NLP merge, or from a run that crashed mid-NLP).

  Phase 1 — Text collection
    Fast path : reads anomaly_texts table saved by backtest_driver (seconds).
    Slow path : re-streams RC_2023-11.zst, collecting only the comments that
                fall inside each anomaly's 2-hour window. No Redis, no
                sliding-window math — just timestamp filtering (~10-15 min).
                Used automatically when anomaly_texts is absent (old DB).

  Phase 2 — NLP enrichment
    Runs DBSCANContextEngine.summarize_anomaly() on each anomaly's texts in
    chronological order (so the dynamic TF-IDF baseline builds correctly).
    Writes results to the `clusters` table and backfills `window_start`.
"""

import random
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from .ingestion.reddit import ZstFileIngestor
from .pipeline.context import DBSCANContextEngine

_ROOT     = Path(__file__).resolve().parent.parent
DATA_FILE = str(_ROOT / "data" / "raw_dumps" / "RC_2023-11.zst")
PHASE2_DB = str(_ROOT / "data" / "dbs" / "anomalies_phase2_nov.db")

NOV_START  = 1698796800   # 2023-11-01 00:00 UTC
NOV_END    = 1701388800   # 2023-12-01 00:00 UTC
WINDOW_TTL = 7200         # 2-hour sliding window
TEXT_CAP   = 500          # match RedisStateManager._TEXT_CAP


# ---------------------------------------------------------------------------
# DB schema helpers
# ---------------------------------------------------------------------------

def _prepare_db(conn: sqlite3.Connection) -> None:
    """Add window_start column and create clusters table if not already present."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(anomalies)")}
    if "window_start" not in cols:
        conn.execute("ALTER TABLE anomalies ADD COLUMN window_start INTEGER")
        conn.execute("UPDATE anomalies SET window_start = window_end - ?", (WINDOW_TTL,))

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS clusters (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            anomaly_id  INTEGER NOT NULL,
            keywords    TEXT    NOT NULL
        );
    """)
    conn.commit()


def _load_anomalies(conn: sqlite3.Connection) -> List[Dict]:
    return [
        {
            "id":           row[0],
            "subreddit":    row[1],
            "window_start": row[2],
            "window_end":   row[3],
        }
        for row in conn.execute(
            "SELECT id, subreddit, window_start, window_end FROM anomalies ORDER BY window_start"
        )
    ]


def _clear_clusters(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM clusters")
    conn.commit()


def _save_clusters(conn: sqlite3.Connection, anomaly_id: int, clusters: List[Dict]) -> None:
    for c in clusters:
        conn.execute(
            "INSERT INTO clusters (anomaly_id, keywords) VALUES (?, ?)",
            (anomaly_id, ", ".join(c["keywords"])),
        )


# ---------------------------------------------------------------------------
# Phase 1 — Text collection
# ---------------------------------------------------------------------------

def _collect_from_db(conn: sqlite3.Connection) -> Dict[int, List[str]]:
    """Fast path: read texts saved by backtest_driver."""
    try:
        rows = conn.execute("SELECT anomaly_id, text FROM anomaly_texts").fetchall()
        if not rows:
            return {}
        result: Dict[int, List[str]] = defaultdict(list)
        for anomaly_id, text in rows:
            result[anomaly_id].append(text)
        return dict(result)
    except sqlite3.OperationalError:
        return {}


def _collect_from_stream(anomalies: List[Dict]) -> Dict[int, List[str]]:
    """Slow fallback: re-stream .zst, collecting texts per anomaly window."""
    # Build lookup: subreddit → [(anomaly_id, window_start, window_end)]
    windows_by_sub: Dict[str, List] = defaultdict(list)
    for a in anomalies:
        windows_by_sub[a["subreddit"]].append(
            (a["id"], a["window_start"], a["window_end"])
        )

    texts_by_id: Dict[int, List[str]] = {a["id"]: [] for a in anomalies}
    ingestor = ZstFileIngestor(DATA_FILE)

    scanned = 0
    t0 = time.perf_counter()
    last_print = t0

    for raw in ingestor.stream():
        ts  = int(raw.get("timestamp", 0))
        sub = raw.get("subreddit", "")

        if ts > NOV_END:
            break
        if ts < NOV_START or sub not in windows_by_sub:
            scanned += 1
            continue

        for (anomaly_id, ws, we) in windows_by_sub[sub]:
            if ws <= ts <= we:
                texts_by_id[anomaly_id].append(raw.get("body", "")[:120])

        scanned += 1
        now = time.perf_counter()
        if now - last_print >= 30:
            elapsed = now - t0
            rate    = scanned / elapsed
            print(f"  [stream] {scanned:>8,} comments scanned | {rate:,.0f}/sec | {elapsed:.0f}s elapsed")
            last_print = now

    # Cap to TEXT_CAP, random sample to match original behaviour
    for anomaly_id, texts in texts_by_id.items():
        if len(texts) > TEXT_CAP:
            texts_by_id[anomaly_id] = random.sample(texts, TEXT_CAP)

    elapsed = time.perf_counter() - t0
    print(f"  [stream] done — {scanned:,} comments in {elapsed:.1f}s")
    return texts_by_id


# ---------------------------------------------------------------------------
# Phase 2 — NLP enrichment
# ---------------------------------------------------------------------------

def _run_nlp(
    anomalies: List[Dict],
    texts_by_id: Dict[int, List[str]],
    conn: sqlite3.Connection,
) -> None:
    engine = DBSCANContextEngine()
    total_clusters = 0
    skipped = 0

    for a in anomalies:
        texts = texts_by_id.get(a["id"], [])
        dt    = datetime.fromtimestamp(a["window_start"], tz=timezone.utc).strftime("%b %d %H:%M UTC")

        if not texts:
            print(f"  [nlp] r/{a['subreddit']:<16} {dt}  — no texts, skipped")
            skipped += 1
            continue

        t0       = time.perf_counter()
        clusters = engine.summarize_anomaly(texts, window_start=a["window_start"])
        elapsed  = (time.perf_counter() - t0) * 1000

        if clusters:
            _save_clusters(conn, a["id"], clusters)
            conn.commit()
            kw_preview = " | ".join(", ".join(c["keywords"][:4]) for c in clusters[:2])
            print(
                f"  [nlp] r/{a['subreddit']:<16} {dt}  "
                f"{len(clusters)} cluster(s)  [{kw_preview}]  ({elapsed:.0f}ms)"
            )
            total_clusters += len(clusters)
        else:
            print(f"  [nlp] r/{a['subreddit']:<16} {dt}  — no clusters (noise only)")

    print(f"\n  NLP done: {total_clusters} clusters across {len(anomalies) - skipped} anomalies")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run() -> None:
    sys.stdout.reconfigure(encoding="utf-8")

    print("=" * 70)
    print("  Phase 2 NLP Enrichment Pass")
    print("=" * 70)

    conn = sqlite3.connect(PHASE2_DB)
    _prepare_db(conn)

    anomalies = _load_anomalies(conn)
    if not anomalies:
        print("  ERROR: no anomalies found in DB — run backtest_driver first.")
        conn.close()
        return

    print(f"  Loaded {len(anomalies)} anomalies from {Path(PHASE2_DB).name}\n")

    # --- Phase 1: text collection ---
    print("Phase 1 — Text collection")
    print("-" * 70)

    texts_by_id = _collect_from_db(conn)
    if texts_by_id:
        total_texts = sum(len(v) for v in texts_by_id.values())
        print(f"  Fast path: {total_texts:,} texts loaded from anomaly_texts table.\n")
    else:
        print(f"  anomaly_texts table not found — falling back to .zst re-stream.")
        print(f"  Streaming {Path(DATA_FILE).name} (progress every 30s) ...\n")
        texts_by_id = _collect_from_stream(anomalies)
        total_texts = sum(len(v) for v in texts_by_id.values())
        print(f"  Collected {total_texts:,} texts across {len(anomalies)} anomaly windows.\n")

    # --- Phase 2: NLP ---
    print("Phase 2 — NLP enrichment")
    print("-" * 70)
    _clear_clusters(conn)
    _run_nlp(anomalies, texts_by_id, conn)

    print(f"\n  Clusters written to: {Path(PHASE2_DB).name}")
    conn.close()


if __name__ == "__main__":
    run()
