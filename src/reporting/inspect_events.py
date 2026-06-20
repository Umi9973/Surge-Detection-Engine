"""
CLI viewer for consolidated TrackedEvent JSONL files.

Usage:
    python -m src.reporting.inspect_events [options]

Options:
    --days  N             last N days by last_seen (default 7)
    --channel  CHANNEL    filter by primary_channel
    --source  SOURCE      filter by source (e.g. hn)
    --min-score  FLOAT    minimum peak_score
    --limit  N            max events to print (default 50)
    --sort  score|time|duration   sort order (default: score)
    --event-kind  KIND    filter by event_kind
    --duration-kind  KIND filter by duration_kind
    --active-only         show only status=active events
    --summary             print aggregate diagnostics instead of individual blocks
    --audit               print full candidate timeline + merge reasons per event
    --output  PATH        write report to file instead of stdout
"""
from __future__ import annotations

import argparse
import contextlib
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pyarrow.parquet as pq

from ..events.models import TrackedEvent
from ..events.store import EventStore

_ROOT       = Path(__file__).resolve().parent.parent.parent
_EVENTS_DIR = _ROOT / "data" / "events"


def _fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%b %d %H:%M UTC")


def _apply_filters(
    events: List[TrackedEvent],
    channel: Optional[str],
    source: Optional[str],
    min_score: float,
    event_kind: Optional[str],
    duration_kind: Optional[str],
    active_only: bool,
) -> List[TrackedEvent]:
    if channel:
        events = [e for e in events if channel in e.channels]
    if source:
        events = [e for e in events if source in e.sources]
    if min_score > 0.0:
        events = [e for e in events if e.peak_score >= min_score]
    if event_kind:
        events = [e for e in events if e.event_kind == event_kind]
    if duration_kind:
        events = [e for e in events if e.duration_kind == duration_kind]
    if active_only:
        events = [e for e in events if e.status == "active"]
    return events


def _print_event(ev: TrackedEvent) -> None:
    dur_h, dur_m = divmod(int(ev.duration_minutes), 60)
    dur_str  = f"{dur_h}h {dur_m}m" if dur_h else f"{dur_m}m"
    channels = ", ".join(ev.channels)
    sources  = ", ".join(ev.sources)
    kws      = ", ".join(ev.display_keywords[:10]) or "—"
    domains  = ", ".join(ev.domains[:5]) or "—"
    titles   = ev.top_titles[:3]

    print("─" * 76)
    print(
        f"[{ev.status.upper():<6}]  score={ev.peak_score:.4f}  z={ev.peak_z_score:.2f}"
        f"  cands={ev.candidate_count}  dur={dur_str}"
    )
    print(f"  Title  : \"{ev.representative_title}\"")
    print(f"  Channel: {channels}  |  Source: {sources}")
    print(f"  Time   : {_fmt_ts(ev.first_seen)}  →  {_fmt_ts(ev.last_seen)}")
    print(f"  KW     : {kws}")
    print(f"  Domains: {domains}")
    if len(titles) > 1:
        print(f"  Also   : {' / '.join(t[:50] for t in titles[1:])}")
    print(f"  ID     : {ev.event_id}")


def _print_summary(events: List[TrackedEvent], days: int) -> None:
    total = len(events)
    if total == 0:
        print("No tracked events.")
        return

    div = "─" * 76

    multi   = [e for e in events if e.candidate_count > 1]
    single  = [e for e in events if e.candidate_count == 1]
    active  = [e for e in events if e.status == "active"]
    closed  = [e for e in events if e.status == "closed"]

    print(div)
    print(f"  TrackedEvent Report  —  last {days}d  ({total} total)")
    print(div)

    print(f"\n  Status")
    print(f"    active : {len(active):>4}")
    print(f"    closed : {len(closed):>4}")

    print(f"\n  Merge quality")
    print(f"    multi-candidate events : {len(multi):>4}  ({100*len(multi)//max(total,1):>2}%)")
    print(f"    single-candidate       : {len(single):>4}  ({100*len(single)//max(total,1):>2}%)")

    # Channel breakdown
    print(f"\n  Events by primary_channel")
    ch_counts = Counter(e.primary_channel for e in events)
    for ch, n in ch_counts.most_common():
        print(f"    {ch:<12}: {n:>4}")

    # Duration distribution (multi-candidate only)
    if multi:
        print(f"\n  Duration of multi-candidate events")
        buckets: Counter = Counter()
        for e in multi:
            d = e.duration_minutes
            if d < 30:       buckets["< 30m"] += 1
            elif d < 120:    buckets["30m–2h"] += 1
            elif d < 360:    buckets["2h–6h"]  += 1
            else:            buckets["> 6h"]   += 1
        for label in ["< 30m", "30m–2h", "2h–6h", "> 6h"]:
            print(f"    {label:<10}: {buckets[label]:>4}")

    # Top keywords across all events
    kw_counter: Counter = Counter()
    for e in events:
        for kw in e.display_keywords:
            kw_counter[kw] += 1
    print(f"\n  Top keywords across all events")
    for kw, n in kw_counter.most_common(12):
        bar = "█" * min(n, 20)
        print(f"    {kw:<20} {n:>3}  {bar}")

    # Most-merged events
    print(f"\n  Most-merged events (top 8 by candidate_count)")
    for e in sorted(events, key=lambda e: -e.candidate_count)[:8]:
        dur_h, dur_m = divmod(int(e.duration_minutes), 60)
        dur_str = f"{dur_h}h{dur_m}m" if dur_h else f"{dur_m}m"
        print(
            f"    [{e.candidate_count:>2} cands]  {dur_str:<7}  "
            f"{e.primary_channel:<10}  \"{e.representative_title[:55]}\""
        )

    print(f"\n{div}")


def _build_candidate_index(candidates_dir: Path) -> Dict[str, dict]:
    """Load all candidate Parquet files into a dict keyed by candidate_id."""
    index: Dict[str, dict] = {}
    for path in sorted(candidates_dir.glob("**/*.parquet")):
        try:
            table = pq.read_table(str(path))
            for i in range(table.num_rows):
                row = {c: table.column(c)[i].as_py() for c in table.schema.names}
                index[row["candidate_id"]] = row
        except Exception as exc:
            print(f"[audit] skipping {path.name}: {exc}", file=sys.stderr)
    return index


def _print_audit(events: List[TrackedEvent], candidates_dir: Path) -> None:
    print("Building candidate index...", file=sys.stderr)
    cand_index = _build_candidate_index(candidates_dir)
    print(f"Indexed {len(cand_index)} candidates.\n", file=sys.stderr)

    div = "─" * 76
    for ev in events:
        dur_h, dur_m = divmod(int(ev.duration_minutes), 60)
        dur_str = f"{dur_h}h {dur_m}m" if dur_h else f"{dur_m}m"
        print(div)
        print(f"EVENT  {ev.primary_channel:<10}  {ev.duration_kind:<11}  "
              f"cands={ev.candidate_count}  dur={dur_str}  score={ev.peak_score:.3f}")
        print(f"  \"{ev.representative_title}\"")
        print(f"  kw: {', '.join(ev.display_keywords[:8])}")
        print()

        for trace in ev.merge_trace:
            cid  = trace["candidate_id"]
            cand = cand_index.get(cid)
            t    = _fmt_ts(trace["candidate_time"])

            if trace.get("was_seed_candidate"):
                role = "SEED "
                reason_str = ""
            else:
                role = "MERGE"
                s    = trace.get("merge_score") or 0.0
                r    = trace.get("merge_reason") or "?"
                co   = trace.get("conversation_overlap") or 0.0
                ko   = trace.get("keyword_overlap") or 0.0
                do_  = trace.get("domain_overlap") or 0.0
                tcm  = "✓" if trace.get("top_conversation_match") else "✗"
                reason_str = (
                    f"  score={s:.3f}  reason={r:<14}"
                    f"  top_conv={tcm}  conv={co:.2f}  kw={ko:.2f}  dom={do_:.2f}"
                )

            if cand:
                title  = (cand.get("top_story_title") or "")[:55]
                cs     = cand.get("event_score", 0.0)
                ckw    = ", ".join((cand.get("keywords") or [])[:5])
                cdom   = ", ".join((cand.get("top_domains") or [])[:3])
                print(f"  [{role}] {t}  s={cs:.3f}  \"{title}\"")
                print(f"          kw={ckw}")
                print(f"          dom={cdom}{reason_str}")
            else:
                print(f"  [{role}] {t}  [candidate not found]{reason_str}")
        print()

    print(div)


def main(argv: Optional[List[str]] = None) -> None:
    sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(
        prog="inspect_events",
        description="Browse TrackedEvent JSONL files in data/events/",
    )
    parser.add_argument("--days",          type=int,   default=7,   help="last N days by last_seen")
    parser.add_argument("--channel",                               help="filter by channel")
    parser.add_argument("--source",                                help="filter by source (e.g. hn)")
    parser.add_argument("--min-score",     type=float, default=0.0, dest="min_score")
    parser.add_argument("--limit",         type=int,   default=50)
    parser.add_argument("--sort",          choices=["score", "time", "duration"], default="score")
    parser.add_argument("--event-kind",    dest="event_kind")
    parser.add_argument("--duration-kind", dest="duration_kind")
    parser.add_argument("--active-only",   action="store_true", dest="active_only")
    parser.add_argument("--summary",       action="store_true")
    parser.add_argument("--audit",         action="store_true",
                        help="print full candidate timeline + merge reasons per event")
    parser.add_argument("--output",        metavar="PATH",
                        help="write report to file instead of stdout")
    args = parser.parse_args(argv)

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_file = open(out_path, "w", encoding="utf-8")
        ctx: contextlib.AbstractContextManager = contextlib.redirect_stdout(out_file)
    else:
        out_file = None
        ctx = contextlib.nullcontext()

    with ctx:
        _run(args)

    if out_file:
        out_file.close()
        print(f"Report written → {Path(args.output).resolve()}", file=sys.stderr)


def _run(args: argparse.Namespace) -> None:
    store  = EventStore(_EVENTS_DIR)
    events = store.read_all(days=args.days)

    if not events:
        print(f"No event files found in {_EVENTS_DIR}")
        return

    events = _apply_filters(
        events,
        channel       = args.channel,
        source        = args.source,
        min_score     = args.min_score,
        event_kind    = args.event_kind,
        duration_kind = args.duration_kind,
        active_only   = args.active_only,
    )

    if not events:
        print("No events match the given filters.")
        return

    if args.summary:
        _print_summary(events, args.days)
        return

    if args.audit:
        candidates_dir = _ROOT / "data" / "event_candidates"
        _print_audit(events, candidates_dir)
        return

    if args.sort == "score":
        events.sort(key=lambda e: -e.peak_score)
    elif args.sort == "time":
        events.sort(key=lambda e: -e.last_seen)
    else:  # duration
        events.sort(key=lambda e: -e.duration_minutes)

    events = events[: args.limit]

    label = f"  {len(events)} event(s)"
    if args.channel:
        label += f"  channel={args.channel}"
    if args.source:
        label += f"  source={args.source}"
    if args.min_score:
        label += f"  min_score={args.min_score}"
    label += f"  last {args.days}d"
    print(label)

    for ev in events:
        _print_event(ev)
    print("─" * 76)


if __name__ == "__main__":
    main()
