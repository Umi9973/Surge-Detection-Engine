"""
CLI viewer for EventCandidate Parquet files.

Usage:
    python -m src.reporting.inspect_candidates [options]

Options:
    --date  YYYY-MM-DD | YYYY-MM  filter by date prefix
    --days  N                      last N days by window_end (default 7; ignored if --date set)
    --channel  CHANNEL             filter by channel name
    --kind  event_candidate|viral_post|topic_surge
    --min-score  FLOAT             minimum event_score
    --limit  N                     max rows to print (default 50)
    --sort  score|time             sort order (default: score)
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import pyarrow.parquet as pq

_ROOT           = Path(__file__).resolve().parent.parent.parent
_CANDIDATES_DIR = _ROOT / "data" / "event_candidates"

_KIND_LABEL = {
    "event_candidate": "[EVENT  ]",
    "viral_post":      "[VIRAL  ]",
    "topic_surge":     "[SURGE  ]",
}
_KIND_ORDER = {"event_candidate": 0, "viral_post": 1, "topic_surge": 2}


def _collect_files(date_filter: Optional[str], days: int) -> List[Path]:
    if not _CANDIDATES_DIR.exists():
        return []

    if date_filter:
        parts = date_filter.split("-")
        if len(parts) == 3:
            pattern = f"{parts[0]}/{parts[1]}/{parts[2]}/*.parquet"
        else:
            pattern = f"{parts[0]}/{parts[1]}/*/*.parquet"
        return sorted(_CANDIDATES_DIR.glob(pattern))

    cutoff = int(time.time()) - days * 86400
    result = []
    for f in sorted(_CANDIDATES_DIR.glob("**/*.parquet")):
        try:
            window_end = int(f.stem.rsplit("_", 1)[-1])
        except ValueError:
            result.append(f)
            continue
        if window_end >= cutoff:
            result.append(f)
    return result


def _load_rows(files: List[Path]) -> List[dict]:
    rows = []
    for path in files:
        try:
            table = pq.read_table(str(path))
            for i in range(table.num_rows):
                rows.append({col: table.column(col)[i].as_py() for col in table.schema.names})
        except Exception as exc:
            print(f"  [warn] could not read {path.name}: {exc}", file=sys.stderr)
    return rows


def _apply_filters(
    rows: List[dict],
    channel: Optional[str],
    kind: Optional[str],
    min_score: float,
) -> List[dict]:
    if channel:
        rows = [r for r in rows if r.get("channel") == channel]
    if kind:
        rows = [r for r in rows if r.get("kind") == kind]
    if min_score > 0.0:
        rows = [r for r in rows if (r.get("event_score") or 0.0) >= min_score]
    return rows


def _print_row(row: dict) -> None:
    window_end = row.get("window_end") or 0
    dt         = datetime.fromtimestamp(window_end, tz=timezone.utc).strftime("%b %d %H:%M UTC")
    channel    = row.get("channel", "")
    kind       = row.get("kind", "")
    score      = row.get("event_score") or 0.0
    top_pct    = int((row.get("top_conversation_pct") or 0.0) * 100)
    unique     = row.get("unique_conversation_count") or 0
    size       = row.get("size") or 0
    z          = row.get("z_score") or 0.0
    title      = (row.get("top_story_title") or "").strip()
    story_id   = row.get("top_story_id") or 0
    domains    = ", ".join((row.get("top_domains") or [])[:5]) or "—"
    kws        = ", ".join(row.get("keywords") or []) or "—"
    lbl        = _KIND_LABEL.get(kind, f"[{kind[:7]:<7}]")

    print("─" * 72)
    print(f"{lbl}  score={score:.4f}  {channel:<10}  {dt}")
    if title:
        print(f"  Story  : \"{title}\" (id={story_id})")
    else:
        print(f"  Story  : (no attribution)  id={story_id}")
    print(f"  Top%   : {top_pct}%  |  Unique: {unique}  |  Size: {size}  |  z={z:.2f}")
    print(f"  Domains: {domains}")
    print(f"  KW     : {kws}")


def main(argv: Optional[List[str]] = None) -> None:
    sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(
        prog="inspect_candidates",
        description="Browse EventCandidate Parquet files in data/event_candidates/",
    )
    parser.add_argument("--date",      metavar="YYYY-MM[-DD]",
                        help="filter by date prefix (exact day or month)")
    parser.add_argument("--days",      type=int, default=7,
                        help="last N days by window_end (default 7; ignored when --date set)")
    parser.add_argument("--channel",   help="filter by channel (ai, tech, security, …)")
    parser.add_argument("--kind",      choices=["event_candidate", "viral_post", "topic_surge"],
                        help="filter by kind")
    parser.add_argument("--min-score", type=float, default=0.0, dest="min_score",
                        metavar="FLOAT", help="minimum event_score (0.0–1.0)")
    parser.add_argument("--limit",     type=int, default=50,
                        help="max rows to print (default 50)")
    parser.add_argument("--sort",      choices=["score", "time"], default="score",
                        help="sort by score desc (default) or time desc")
    args = parser.parse_args(argv)

    files = _collect_files(args.date, args.days)
    if not files:
        print(f"No candidate files found in {_CANDIDATES_DIR}")
        return

    rows = _load_rows(files)
    rows = _apply_filters(rows, args.channel, args.kind, args.min_score)

    if not rows:
        print("No candidates match the given filters.")
        return

    if args.sort == "score":
        rows.sort(key=lambda r: (_KIND_ORDER.get(r.get("kind", ""), 9), -(r.get("event_score") or 0.0)))
    else:
        rows.sort(key=lambda r: -(r.get("window_end") or 0))

    rows = rows[: args.limit]

    label = f"  {len(rows)} candidate(s)"
    if args.channel:
        label += f"  channel={args.channel}"
    if args.kind:
        label += f"  kind={args.kind}"
    if args.min_score:
        label += f"  min_score={args.min_score}"
    label += f"  date={args.date}" if args.date else f"  last {args.days}d"

    print(label)
    for row in rows:
        _print_row(row)
    print("─" * 72)


if __name__ == "__main__":
    main()
