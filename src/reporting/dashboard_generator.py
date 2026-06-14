from __future__ import annotations

import html as _html
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import redis
from google.cloud import storage
from plotly.subplots import make_subplots
import plotly.graph_objects as go

from ..storage.state_manager import RedisStateManager

_ROOT = Path(__file__).resolve().parent.parent.parent

BUCKET_NAME     = os.environ.get("GCS_BUCKET",  "hn-surge-dashboard-01")
GCP_PROJECT     = os.environ.get("GCS_PROJECT", "project-8299dfb6-57e5-4dcf-bc0")
REFRESH_SEC     = 300
LIVE_DB         = str(_ROOT / "data" / "dbs" / "anomalies_hn_live.db")
HEALTH_PATH     = _ROOT / "data" / "health.json"
_STALE_SECONDS  = 15 * 60  # 3× the 5-min polling interval

# 4×2 trellis — (channel, row, col)
CHANNEL_GRID = [
    ("ai",       1, 1), ("tech",     1, 2),
    ("security", 2, 1), ("startup",  2, 2),
    ("crypto",   3, 1), ("science",  3, 2),
    ("policy",   4, 1), ("general",  4, 2),
]


# ---------------------------------------------------------------------------
# Dashboard builder
# ---------------------------------------------------------------------------

def build_dashboard(state: RedisStateManager, conn: sqlite3.Connection) -> str:
    now           = int(time.time())
    seven_days_ago = now - 7 * 86400

    fig = make_subplots(
        rows=4, cols=2,
        shared_xaxes=True,
        subplot_titles=[ch for ch, _, _ in CHANNEL_GRID],
        vertical_spacing=0.06,
        horizontal_spacing=0.08,
    )

    for channel, row, col in CHANNEL_GRID:
        # --- Baseline history (blue line) ---
        history = state.get_history(channel)
        n       = len(history)
        x_ts    = [
            datetime.fromtimestamp(now - (n - i) * 3600, tz=timezone.utc)
            for i in range(n)
        ]

        fig.add_trace(go.Scatter(
            x=x_ts,
            y=list(history),
            mode="lines",
            name=channel,
            line=dict(color="#4C78A8", width=1.5),
            showlegend=False,
            hovertemplate="%{x|%b %d %H:%M}<br>count=%{y}<extra>" + channel + "</extra>",
        ), row=row, col=col)

        # --- Anomaly markers (red ×) ---
        rows_db = conn.execute(
            "SELECT window_end, count, z_score FROM anomalies "
            "WHERE window_end >= ? AND channel = ? ORDER BY window_end",
            (seven_days_ago, channel),
        ).fetchall()

        if rows_db:
            ax = [datetime.fromtimestamp(r[0], tz=timezone.utc) for r in rows_db]
            ay = [r[1] for r in rows_db]
            az = [r[2] for r in rows_db]
            fig.add_trace(go.Scatter(
                x=ax,
                y=ay,
                mode="markers",
                marker=dict(color="red", size=9, symbol="x", line=dict(width=2)),
                showlegend=False,
                customdata=az,
                hovertemplate=(
                    "%{x|%b %d %H:%M}<br>count=%{y}<br>z=%{customdata:.1f}"
                    "<extra>ANOMALY</extra>"
                ),
            ), row=row, col=col)

    updated = datetime.fromtimestamp(now, tz=timezone.utc).strftime("%b %d %H:%M UTC")
    fig.update_layout(
        title=dict(
            text=f"HN Surge Detection — Live Baseline  ·  updated {updated}",
            font=dict(size=17),
        ),
        height=1000,
        paper_bgcolor="#1a1a2e",
        plot_bgcolor="#16213e",
        font=dict(color="#e0e0e0"),
        hovermode="x unified",
    )
    fig.update_xaxes(showgrid=False, zeroline=False)
    fig.update_yaxes(showgrid=True, gridcolor="#2a2a4a", zeroline=False)

    page = fig.to_html(full_html=True, include_plotlyjs="cdn")
    banner = _build_status_banner(HEALTH_PATH)
    return page.replace("<body>", f"<body>{banner}", 1)


# ---------------------------------------------------------------------------
# Health banner
# ---------------------------------------------------------------------------

def _build_status_banner(health_path: Path) -> str:
    try:
        data = json.loads(health_path.read_text(encoding="utf-8"))
    except Exception:
        return _banner_html("⚠️ No health data found — ingestion may not be running.", "#ffcc00")

    heartbeat_str = data.get("last_heartbeat_utc", "")
    try:
        heartbeat_dt = datetime.fromisoformat(heartbeat_str.replace("Z", "+00:00"))
        age_sec = (datetime.now(timezone.utc) - heartbeat_dt).total_seconds()
    except Exception:
        age_sec = float("inf")

    if age_sec > _STALE_SECONDS:
        mins     = int(age_sec // 60)
        last_err = _html.escape(data.get("last_error_message") or "unknown")
        return _banner_html(
            f"⚠️ HN ingestion stale — no heartbeat for {mins}m. Last error: {last_err}",
            "#ff4444",
        )
    if data.get("status") != "ok":
        msg = _html.escape(data.get("last_error_message") or "")
        return _banner_html(f"⚠️ HN ingestion warning: {msg}", "#ffcc00")

    last_fetch = _html.escape(heartbeat_str)
    return _banner_html(f"✓ HN ingestion OK — last fetch {last_fetch}", "#22bb55")


def _banner_html(msg: str, color: str) -> str:
    return (
        f'<div style="background:{color};color:#fff;padding:10px 16px;'
        f'font-family:sans-serif;font-size:14px;position:sticky;top:0;z-index:999">'
        f"{msg}</div>"
    )


# ---------------------------------------------------------------------------
# GCS upload
# ---------------------------------------------------------------------------

def upload_to_gcs(html: str, bucket_name: str) -> None:
    client = storage.Client(project=GCP_PROJECT)
    blob   = client.bucket(bucket_name).blob("index.html")
    blob.upload_from_string(html, content_type="text/html")
    print(f"  Uploaded → https://storage.googleapis.com/{bucket_name}/index.html")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run() -> None:
    sys.stdout.reconfigure(encoding="utf-8")

    r     = redis.Redis(host="localhost", port=6379, decode_responses=True)
    state = RedisStateManager(r)
    conn  = sqlite3.connect(LIVE_DB, check_same_thread=False)
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

    print(f"Dashboard generator — refreshing every {REFRESH_SEC}s")
    print(f"Reading Redis @ localhost:6379 | SQLite @ {Path(LIVE_DB).name}")

    while True:
        try:
            html = build_dashboard(state, conn)
            upload_to_gcs(html, BUCKET_NAME)
        except Exception as exc:
            print(f"  [dashboard] {type(exc).__name__}: {exc}", file=sys.stderr)
        time.sleep(REFRESH_SEC)


if __name__ == "__main__":
    run()
