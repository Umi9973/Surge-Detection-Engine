from __future__ import annotations

import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, List

import fakeredis

from .ingestion.hacker_news import HNCsvIngestor
from .models import AnomalyEvent, Comment
from .pipeline.alert_gate import AlertGate
from .pipeline.context import DBSCANContextEngine
from .pipeline.filter import SubredditFilter
from .pipeline.sliding_tripwire import SlidingWindowTripwire
from .storage.parquet_archiver import ParquetArchiver
from .storage.state_manager import RedisStateManager

TARGET_CHANNELS = [
    "ai", "security", "startup", "crypto", "science", "tech", "policy", "general",
]

_ROOT       = Path(__file__).resolve().parent.parent
CSV_FILE    = str(_ROOT / "data" / "raw_dumps" / "HN_2023_Nov8-23.csv")
HN_DB       = str(_ROOT / "data" / "dbs" / "anomalies_hn_nov.db")
LOG_DIR     = _ROOT / "data" / "logs"
LATEST_LOG  = LOG_DIR / "hn_backtest_latest.txt"
PARQUET_DIR = _ROOT / "data" / "parquet"

EVAL_INTERVAL     = SlidingWindowTripwire.EVAL_INTERVAL      # 300s
BASELINE_INTERVAL = SlidingWindowTripwire.BASELINE_INTERVAL  # 3600s
MIN_HISTORY       = SlidingWindowTripwire.MIN_HISTORY        # 24 hourly samples

BURN_IN_DAYS = 7
BURN_IN_SEC  = BURN_IN_DAYS * 86400


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _setup_logging() -> IO[str]:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    archive = LOG_DIR / "archive"
    archive.mkdir(exist_ok=True)
    if LATEST_LOG.exists():
        ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
        LATEST_LOG.rename(archive / f"hn_backtest_{ts}.txt")
    return LATEST_LOG.open("w", encoding="utf-8")


def _log(msg: str, f: IO[str]) -> None:
    print(msg)
    f.write(msg + "\n")
    f.flush()


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

def _init_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.executescript("""
        DROP TABLE IF EXISTS clusters;
        DROP TABLE IF EXISTS anomaly_texts;
        DROP TABLE IF EXISTS anomalies;
        CREATE TABLE anomalies (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            channel        TEXT    NOT NULL,
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
    """)
    conn.commit()
    return conn


def _save_anomaly(conn: sqlite3.Connection, ev: AnomalyEvent) -> None:
    dt = datetime.fromtimestamp(ev.window_end, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    cur = conn.execute(
        "INSERT INTO anomalies "
        "(channel, window_start, window_end, window_end_dt, count, z_score, mean, std) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (ev.subreddit, ev.window_start, ev.window_end, dt,
         ev.count, ev.z_score, ev.mean, ev.std),
    )
    anomaly_id = cur.lastrowid
    if ev.texts:
        conn.executemany(
            "INSERT INTO anomaly_texts (anomaly_id, text) VALUES (?, ?)",
            [(anomaly_id, t) for t in ev.texts],
        )


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
    _log(f"  HN Backtest Driver — Nov 8–22 2023 (Sam Altman Saga)", lf)
    _log(f"  Run started : {run_ts}", lf)
    _log(f"  Burn-in     : {BURN_IN_DAYS} days  (MIN_HISTORY={MIN_HISTORY})", lf)
    _log(f"  Input CSV   : {Path(CSV_FILE).name}", lf)
    _log(f"  Output DB   : {Path(HN_DB).name}", lf)
    _log("=" * 70, lf)

    conn     = _init_db(HN_DB)
    r        = fakeredis.FakeRedis(decode_responses=True)
    state    = RedisStateManager(r)
    tripwire = SlidingWindowTripwire(state, TARGET_CHANNELS)
    gate     = AlertGate()
    ingestor = HNCsvIngestor(CSV_FILE)
    filter_  = SubredditFilter(TARGET_CHANNELS)

    anomalies: List[AnomalyEvent] = []
    total_items = 0
    ticks: dict = {"eval": None, "base": None}
    detection_start_ts:    int = 0
    baseline_sample_count: int = 0

    def fire_ticks(up_to: int) -> None:
        nonlocal baseline_sample_count  # closure mutates a primitive — see plan note
        while True:
            e, b = ticks["eval"], ticks["base"]
            if e is None or min(e, b) > up_to:
                break
            if e <= b:
                if e >= detection_start_ts:
                    raw_events = tripwire.evaluation_tick(e)
                    events     = gate.process(raw_events)
                    for ev in events:
                        anomalies.append(ev)
                        _save_anomaly(conn, ev)
                        dt = datetime.fromtimestamp(ev.window_end, tz=timezone.utc).strftime("%b %d %H:%M UTC")
                        _log(
                            f"  *** ANOMALY  {ev.subreddit:<12} | {dt} | "
                            f"count={ev.count:>5} | z={ev.z_score} ***",
                            lf,
                        )
                # else: burn-in — evaluation tick skipped; Schmitt trigger stays clean
                ticks["eval"] = e + EVAL_INTERVAL
            else:
                tripwire.baseline_tick(b)
                baseline_sample_count += 1
                dt = datetime.fromtimestamp(b, tz=timezone.utc).strftime("%b %d %H:%M UTC")
                if b < detection_start_ts:
                    _log(
                        f"  [burn-in {baseline_sample_count:>3}/{MIN_HISTORY}] {dt}"
                        f"  items: {total_items:,}",
                        lf,
                    )
                else:
                    _log(f"  [baseline {dt}]  items ingested so far: {total_items:,}", lf)
                conn.commit()
                ticks["base"] = b + BASELINE_INTERVAL

    _log(f"\n  Loading and sorting {Path(CSV_FILE).name} ...", lf)
    wall_start = time.perf_counter()

    for raw in filter_.stream(ingestor.stream()):
        ts = int(raw["timestamp"])

        if ticks["eval"] is None:
            ticks["eval"] = ((ts // EVAL_INTERVAL) + 1) * EVAL_INTERVAL
            ticks["base"] = ((ts // BASELINE_INTERVAL) + 1) * BASELINE_INTERVAL
            # snap to midnight UTC of first data day + BURN_IN_DAYS
            # note: actual burn-in may be slightly < 168h (see plan — intentional)
            detection_start_ts = ((ts // 86400) + BURN_IN_DAYS) * 86400
            det_str = datetime.fromtimestamp(detection_start_ts, tz=timezone.utc).strftime("%b %d %H:%M UTC")
            _log(f"  Detection opens : {det_str}", lf)

        fire_ticks(ts - 1)
        tripwire.ingest(_to_comment(raw))
        total_items += 1

    # Flush remaining ticks to end of data
    if ticks["eval"] is not None:
        last_ts = ticks["base"] + BASELINE_INTERVAL
        fire_ticks(last_ts)

    wall_sec = time.perf_counter() - wall_start

    # --- NLP enrichment ---
    _log(f"\n{'=' * 70}", lf)
    _log(f"  NLP ENRICHMENT", lf)
    _log(f"{'=' * 70}", lf)

    nlp_engine   = DBSCANContextEngine()
    anomaly_rows = conn.execute(
        "SELECT id, channel, window_start FROM anomalies ORDER BY window_start"
    ).fetchall()

    events_by_key: dict = {
        (ev.subreddit, ev.window_start): ev for ev in anomalies
    }

    total_clusters = 0
    with ParquetArchiver(PARQUET_DIR) as archiver:
        for (anomaly_id, channel, window_start) in anomaly_rows:
            texts = [row[0] for row in conn.execute(
                "SELECT text FROM anomaly_texts WHERE anomaly_id = ?", (anomaly_id,)
            ).fetchall()]

            if not texts:
                continue

            clusters = nlp_engine.summarize_anomaly(texts, window_start=window_start)
            for c in clusters:
                conn.execute(
                    "INSERT INTO clusters (anomaly_id, keywords) VALUES (?, ?)",
                    (anomaly_id, ", ".join(c["keywords"])),
                )

            flat_kw = [kw for c in clusters for kw in c["keywords"]]
            ev = events_by_key.get((channel, window_start))
            if ev:
                archiver.archive(ev, flat_kw)

            if clusters:
                total_clusters += len(clusters)
                dt         = datetime.fromtimestamp(window_start, tz=timezone.utc).strftime("%b %d %H:%M UTC")
                kw_preview = " | ".join(", ".join(c["keywords"][:4]) for c in clusters[:2])
                _log(f"  {channel:<12} {dt}  {len(clusters)} cluster(s)  [{kw_preview}]", lf)

    conn.commit()
    _log(f"\n  NLP done: {total_clusters} clusters across {len(anomaly_rows)} anomalies", lf)

    # --- Summary ---
    _log(f"\n{'=' * 70}", lf)
    _log(f"  SUMMARY", lf)
    _log(f"{'=' * 70}", lf)
    _log(f"  Wall time      : {wall_sec:.1f}s", lf)
    _log(f"  Items ingested : {total_items:,}", lf)
    _log(f"  Anomalies      : {len(anomalies)}", lf)
    _log(f"  Clusters       : {total_clusters}", lf)

    _log(f"\n  --- Anomalies by channel ---", lf)
    rows = conn.execute("""
        SELECT channel, window_end_dt, count, z_score,
               (SELECT GROUP_CONCAT(keywords, ' | ') FROM clusters WHERE anomaly_id = anomalies.id)
        FROM anomalies ORDER BY z_score DESC
    """).fetchall()

    for ch, dt, cnt, z, kws in rows:
        _log(f"  {ch:<12} | {dt} | count={cnt:>5} | z={z:>6.2f} | {kws or '(no clusters)'}", lf)

    _log(f"\n  Results saved to : {Path(HN_DB).name}", lf)
    _log(f"  Log saved to     : {LATEST_LOG}", lf)

    lf.close()
    conn.close()
    state.flush()
    gate.flush()


if __name__ == "__main__":
    run()
