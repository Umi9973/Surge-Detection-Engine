"""
Power BI CSV exporter for Reddit Surge Detection.

Usage:
    python -m src.reporting.export_powerbi [--days N]

Outputs to data/bi_export/:
    events.csv              one row per TrackedEvent
    event_keywords.csv      one row per event × display_keyword
    event_domains.csv       one row per event × domain
    event_candidates.csv    one row per merge_trace entry (+ Parquet enrichment)
    evaluation_summary.csv  one row per date × channel (derived aggregate)
    baseline_results.csv    one row per golden pairwise test case
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import pyarrow.parquet as pq

from ..events.models import TrackedEvent
from ..events.store import EventStore

_ROOT           = Path(__file__).resolve().parent.parent.parent
_EVENTS_DIR     = _ROOT / "data" / "events"
_CANDIDATES_DIR = _ROOT / "data" / "event_candidates"
_BASELINE_PATH  = _ROOT / "data" / "audit" / "golden_merge_cases.json"
_BI_DIR         = _ROOT / "data" / "bi_export"


# ---------------------------------------------------------------------------
# Display keyword filtering (reporting layer only — does not touch JSONL)
# ---------------------------------------------------------------------------

# Stopwords applied when generating event_keywords.csv and display_keywords_text.
# Never used for merge scoring — SameChannelPolicy reads TrackedEvent.keywords directly.
NOISE_DISPLAY_KEYWORDS: frozenset = frozenset({
    # function / auxiliary words
    "into", "onto", "over", "also", "just", "even", "well", "here",
    "only", "then", "when", "what", "where", "back", "too", "more",
    "some", "such", "both", "very", "like", "said", "does", "will",
    "would", "could", "have", "been", "were", "from", "with", "them",
    "they", "this", "that", "there", "their", "about", "after",
    # generic display noise
    "using", "new", "better", "show", "first", "maybe", "those",
    "always", "part", "say", "says", "every", "right", "used",
    "never", "true", "clear", "great", "few", "old", "waiting",
    "really", "think", "know", "want", "going", "make", "many",
    "good", "need", "look", "take", "people", "time", "year",
    "years", "way", "nap", "fish", "working", "write", "wrote",
    "still", "already", "actually", "though", "without", "should",
    "much", "most", "made", "another", "same", "while", "until",
    "across", "under", "between", "each",
    # second-tier generic discussion words
    "saying", "yes", "please", "keep", "agree", "idea", "comment",
    "past", "seem", "seems", "getting", "add", "call", "calls",
    "something", "someone", "everything", "nothing", "anything", "exactly",
    "might", "said", "saying", "happen", "happens", "happened",
    "since", "again", "done", "being", "doing", "thing", "things",
})

# Minimal stop-list for title tokenisation fallback.
_TITLE_STOP: frozenset = frozenset({
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to",
    "for", "of", "with", "by", "is", "are", "was", "were", "be",
    "been", "being", "have", "has", "had", "do", "does", "did",
    "will", "would", "could", "should", "may", "might", "can",
    "not", "no", "nor", "if", "as", "that", "this", "it", "its",
    "from", "into", "now", "get", "let", "ask", "how", "who",
    "what", "when", "where", "why", "which", "about", "after",
    "new", "using",
})

_TOKEN_RE = re.compile(r"[a-zA-Z][a-zA-Z0-9]*")


def _get_display_keywords(ev: TrackedEvent) -> List[str]:
    """
    Return clean display keywords for Power BI output.

    Fallback order:
      1. ev.keywords filtered by NOISE_DISPLAY_KEYWORDS
      2. meaningful tokens from representative_title
      3. [] (blank — caller skips the row or leaves field empty)
    """
    filtered = [kw for kw in ev.keywords if kw.lower() not in NOISE_DISPLAY_KEYWORDS]
    if filtered:
        return filtered

    tokens = [
        t.lower() for t in _TOKEN_RE.findall(ev.representative_title)
        if len(t) >= 3 and t.lower() not in _TITLE_STOP
    ]
    seen: dict = {}
    deduped = [seen.setdefault(t, t) for t in tokens if t not in seen]
    return deduped[:10]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts_to_iso(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ts_to_date(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def _build_candidate_index(candidates_dir: Path) -> Dict[str, dict]:
    index: Dict[str, dict] = {}
    for path in sorted(candidates_dir.glob("**/*.parquet")):
        try:
            table = pq.read_table(str(path))
            for i in range(table.num_rows):
                row = {c: table.column(c)[i].as_py() for c in table.schema.names}
                index[row["candidate_id"]] = row
        except Exception as exc:
            print(f"[bi_export] skipping {path.name}: {exc}", file=sys.stderr)
    return index


def _nullable(value, round_digits: int = 0):
    """Return empty string for None; round floats if round_digits > 0."""
    if value is None:
        return ""
    if round_digits and isinstance(value, float):
        return round(value, round_digits)
    return value


# ---------------------------------------------------------------------------
# Table 1 — events.csv
# ---------------------------------------------------------------------------

def export_events(events: List[TrackedEvent], out_path: Path) -> int:
    fieldnames = [
        "event_id", "representative_title", "primary_channel",
        "status", "duration_kind", "event_kind",
        "first_seen_ts", "last_seen_ts", "first_seen_utc", "last_seen_utc",
        "duration_minutes", "candidate_count", "total_item_count",
        "peak_score", "avg_score", "peak_z_score",
        "top_conversation_id",
        "sources_joined", "display_keywords_text", "domains_top5", "top_titles_joined",
    ]
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for ev in events:
            w.writerow({
                "event_id":               ev.event_id,
                "representative_title":   ev.representative_title,
                "primary_channel":        ev.primary_channel,
                "status":                 ev.status,
                "duration_kind":          ev.duration_kind,
                "event_kind":             ev.event_kind,
                "first_seen_ts":          ev.first_seen,
                "last_seen_ts":           ev.last_seen,
                "first_seen_utc":         _ts_to_iso(ev.first_seen),
                "last_seen_utc":          _ts_to_iso(ev.last_seen),
                "duration_minutes":       round(ev.duration_minutes, 2),
                "candidate_count":        ev.candidate_count,
                "total_item_count":       ev.total_item_count,
                "peak_score":             round(ev.peak_score, 6),
                "avg_score":              round(ev.avg_score, 6),
                "peak_z_score":           round(ev.peak_z_score, 2),
                "top_conversation_id":    ev.top_conversation_id or "",
                "sources_joined":         "|".join(ev.sources),
                "display_keywords_text":  ",".join(_get_display_keywords(ev)[:10]),
                "domains_top5":           ",".join(ev.domains[:5]),
                "top_titles_joined":      "|".join(ev.top_titles[:3]),
            })
    return len(events)


# ---------------------------------------------------------------------------
# Table 2 — event_keywords.csv
# ---------------------------------------------------------------------------

def export_keywords(events: List[TrackedEvent], out_path: Path) -> int:
    count = 0
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["event_id", "keyword", "keyword_rank"])
        w.writeheader()
        for ev in events:
            kws = _get_display_keywords(ev)
            for rank, kw in enumerate(kws, start=1):
                w.writerow({"event_id": ev.event_id, "keyword": kw, "keyword_rank": rank})
                count += 1
            # if kws is [] (all filtered, no title fallback), no rows written for this event
    return count


# ---------------------------------------------------------------------------
# Table 3 — event_domains.csv
# ---------------------------------------------------------------------------

def export_domains(events: List[TrackedEvent], out_path: Path) -> int:
    count = 0
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["event_id", "domain", "domain_rank"])
        w.writeheader()
        for ev in events:
            for rank, dom in enumerate(ev.domains, start=1):
                w.writerow({"event_id": ev.event_id, "domain": dom, "domain_rank": rank})
                count += 1
    return count


# ---------------------------------------------------------------------------
# Table 4 — event_candidates.csv
# ---------------------------------------------------------------------------

def export_candidates(
    events: List[TrackedEvent],
    cand_index: Dict[str, dict],
    out_path: Path,
) -> Tuple[int, int]:
    fieldnames = [
        "event_id", "candidate_id",
        "candidate_time_ts", "candidate_time_utc",
        "was_seed_candidate",
        "merge_score", "merge_reason",
        "top_conversation_match", "conversation_overlap", "keyword_overlap", "domain_overlap",
        "candidate_channel", "candidate_kind",
        "candidate_event_score", "candidate_z_score", "candidate_size",
        "candidate_top_story_title", "candidate_top_story_id",
    ]
    count = 0
    unresolved = 0
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for ev in events:
            for trace in ev.merge_trace:
                cid  = trace["candidate_id"]
                cand = cand_index.get(cid)
                if cand is None:
                    unresolved += 1
                w.writerow({
                    "event_id":                 ev.event_id,
                    "candidate_id":             cid,
                    "candidate_time_ts":        trace["candidate_time"],
                    "candidate_time_utc":       _ts_to_iso(trace["candidate_time"]),
                    "was_seed_candidate":       trace.get("was_seed_candidate", False),
                    "merge_score":              _nullable(trace.get("merge_score"), 4),
                    "merge_reason":             trace.get("merge_reason") or "",
                    "top_conversation_match":   _nullable(trace.get("top_conversation_match")),
                    "conversation_overlap":     _nullable(trace.get("conversation_overlap"), 4),
                    "keyword_overlap":          _nullable(trace.get("keyword_overlap"), 4),
                    "domain_overlap":           _nullable(trace.get("domain_overlap"), 4),
                    # Parquet enrichment (empty if candidate file not found)
                    "candidate_channel":        cand.get("channel", "") if cand else "",
                    "candidate_kind":           cand.get("kind", "") if cand else "",
                    "candidate_event_score":    _nullable(cand.get("event_score"), 6) if cand else "",
                    "candidate_z_score":        _nullable(cand.get("z_score"), 2) if cand else "",
                    "candidate_size":           cand.get("size", "") if cand else "",
                    "candidate_top_story_title": (cand.get("top_story_title") or "") if cand else "",
                    "candidate_top_story_id":   cand.get("top_story_id", "") if cand else "",
                })
                count += 1
    return count, unresolved


# ---------------------------------------------------------------------------
# Table 5 — channel_daily_summary.csv  (date × channel rollup)
# ---------------------------------------------------------------------------

def export_channel_daily_summary(events: List[TrackedEvent], out_path: Path) -> int:
    agg: dict = defaultdict(lambda: {
        "event_count": 0,
        "total_candidates_merged": 0,
        "multi_candidate_events": 0,
        "single_candidate_events": 0,
        "peak_scores": [],
        "duration_minutes_list": [],
        "isolated_count": 0,
        "flash_count": 0,
        "developing_count": 0,
        "sustained_count": 0,
    })

    for ev in events:
        key = (_ts_to_date(ev.last_seen), ev.primary_channel)
        b = agg[key]
        b["event_count"] += 1
        b["total_candidates_merged"] += ev.candidate_count
        if ev.candidate_count > 1:
            b["multi_candidate_events"] += 1
        else:
            b["single_candidate_events"] += 1
        b["peak_scores"].append(ev.peak_score)
        b["duration_minutes_list"].append(ev.duration_minutes)
        dk = ev.duration_kind
        if dk in ("isolated", "flash", "developing", "sustained"):
            b[f"{dk}_count"] += 1

    fieldnames = [
        "report_date", "channel",
        "event_count", "total_candidates_merged",
        "multi_candidate_events", "single_candidate_events",
        "avg_peak_score", "max_peak_score", "avg_duration_minutes",
        "isolated_count", "flash_count", "developing_count", "sustained_count",
    ]
    rows = sorted(agg.items())
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for (date, channel), b in rows:
            scores = b["peak_scores"]
            durations = b["duration_minutes_list"]
            w.writerow({
                "report_date":             date,
                "channel":                 channel,
                "event_count":             b["event_count"],
                "total_candidates_merged": b["total_candidates_merged"],
                "multi_candidate_events":  b["multi_candidate_events"],
                "single_candidate_events": b["single_candidate_events"],
                "avg_peak_score":          round(sum(scores) / len(scores), 6),
                "max_peak_score":          round(max(scores), 6),
                "avg_duration_minutes":    round(sum(durations) / len(durations), 2),
                "isolated_count":          b["isolated_count"],
                "flash_count":             b["flash_count"],
                "developing_count":        b["developing_count"],
                "sustained_count":         b["sustained_count"],
            })
    return len(rows)


# ---------------------------------------------------------------------------
# Table 5b — evaluation_summary.csv  (one row per run)
# ---------------------------------------------------------------------------

def _count_recurring_threads(candidates_dir: Path, from_ts: int, to_ts: int) -> int:
    """Count recurring_thread candidates in the given window_end timestamp range."""
    count = 0
    for path in sorted(candidates_dir.glob("**/*.parquet")):
        try:
            table = pq.read_table(str(path), columns=["kind", "window_end"])
        except Exception:
            continue
        for i in range(table.num_rows):
            we = table.column("window_end")[i].as_py()
            if from_ts <= we <= to_ts and table.column("kind")[i].as_py() == "recurring_thread":
                count += 1
    return count


def _run_baseline_metrics(baseline_path: Path) -> dict:
    """Run pairwise + E2E baseline checks and return counts as a dict."""
    from tests.regression_merge_baseline import _build_index, _run_pairwise, _run_e2e

    data      = json.loads(baseline_path.read_text(encoding="utf-8"))
    meta      = data["meta"]
    date_from, date_to = meta["date_range"].split(" to ")
    pairwise  = data.get("pairwise", [])
    e2e_data  = data.get("end_to_end", {})

    must_merge     = [c for c in pairwise if c["expected"] == "should_merge"]
    must_not_merge = [c for c in pairwise if c["expected"] == "should_not_merge"]

    index = _build_index(date_from, date_to)

    mm_p,  mm_f,  _ = _run_pairwise(must_merge,     index, verbose=False)
    mnm_p, mnm_f, _ = _run_pairwise(must_not_merge, index, verbose=False)
    e2e_p, e2e_f, _, _ = _run_e2e(e2e_data, date_from, date_to, verbose=False)

    total_pw  = len(pairwise)
    e2e_total = (len(e2e_data.get("expected_same_event",    []))
               + len(e2e_data.get("expected_different_events", [])))

    return {
        "pairwise_must_merge_total":      len(must_merge),
        "pairwise_must_merge_passed":     mm_p,
        "pairwise_must_merge_failed":     mm_f,
        "pairwise_must_not_merge_total":  len(must_not_merge),
        "pairwise_must_not_merge_passed": mnm_p,
        "pairwise_must_not_merge_failed": mnm_f,
        "pairwise_pass_rate":             round((mm_p + mnm_p) / max(total_pw, 1), 4),
        "e2e_total":      e2e_total,
        "e2e_passed":     e2e_p,
        "e2e_failed":     e2e_f,
        "e2e_pass_rate":  round(e2e_p / max(e2e_total, 1), 4),
    }


def export_evaluation_summary(
    events: List[TrackedEvent],
    candidates_dir: Path,
    baseline_path: Path,
    days: int,
    out_path: Path,
) -> int:
    """Write one-row-per-run evaluation metrics CSV."""
    from collections import Counter

    total  = len(events)
    cands  = sum(ev.candidate_count for ev in events)
    ch_cnt = Counter(ev.primary_channel for ev in events)
    dk_cnt = Counter(ev.duration_kind   for ev in events)

    from_ts = min(ev.first_seen for ev in events)
    to_ts   = max(ev.last_seen  for ev in events)

    print("  Running baseline checks...", file=sys.stderr)
    bm = _run_baseline_metrics(baseline_path)

    print("  Counting recurring threads...", file=sys.stderr)
    recurring = _count_recurring_threads(candidates_dir, from_ts, to_ts)

    row = {
        "run_date":                       _ts_to_iso(int(datetime.now(tz=timezone.utc).timestamp())),
        "date_from":                      _ts_to_date(from_ts),
        "date_to":                        _ts_to_date(to_ts),
        "days_requested":                 days,
        "total_tracked_events":           total,
        "total_candidates_consolidated":  cands,
        "recurring_thread_count":         recurring,
        "candidate_to_event_ratio":       round(cands / max(total, 1), 3),
        "multi_candidate_event_count":    sum(1 for ev in events if ev.candidate_count > 1),
        "multi_candidate_event_rate":     round(sum(1 for ev in events if ev.candidate_count > 1) / max(total, 1), 4),
        "isolated_event_count":           dk_cnt["isolated"],
        "flash_event_count":              dk_cnt["flash"],
        "developing_event_count":         dk_cnt["developing"],
        "sustained_event_count":          dk_cnt["sustained"],
        "events_general":                 ch_cnt["general"],
        "events_tech":                    ch_cnt["tech"],
        "events_ai":                      ch_cnt["ai"],
        "events_security":                ch_cnt["security"],
        "events_startup":                 ch_cnt["startup"],
        "events_science":                 ch_cnt["science"],
        "events_policy":                  ch_cnt["policy"],
        **bm,
    }

    fieldnames = list(row.keys())
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerow(row)
    return 1


# ---------------------------------------------------------------------------
# Table 6 — baseline_results.csv
# ---------------------------------------------------------------------------

def export_baseline(baseline_path: Path, out_path: Path) -> int:
    if not baseline_path.exists():
        print(f"[bi_export] baseline not found: {baseline_path}", file=sys.stderr)
        return 0

    data = json.loads(baseline_path.read_text(encoding="utf-8"))
    pairwise = data.get("pairwise", [])

    fieldnames = [
        "case_id", "group", "expected",
        "candidate_a", "candidate_b", "notes",
        "actual_score", "passed",
        "merge_reason", "keyword_overlap", "conversation_overlap",
        "domain_overlap", "top_conversation_match",
    ]
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for case in pairwise:
            w.writerow({
                "case_id":                case.get("case_id", ""),
                "group":                  case.get("group", ""),
                "expected":               case.get("expected", ""),
                "candidate_a":            case.get("candidate_a", ""),
                "candidate_b":            case.get("candidate_b", ""),
                "notes":                  case.get("notes") or "",
                # populated when regression test is run — empty for now
                "actual_score":           "",
                "passed":                 "",
                "merge_reason":           "",
                "keyword_overlap":        "",
                "conversation_overlap":   "",
                "domain_overlap":         "",
                "top_conversation_match": "",
            })
    return len(pairwise)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv=None) -> None:
    sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(
        prog="export_powerbi",
        description="Export TrackedEvents to flat CSVs for Power BI in data/bi_export/",
    )
    parser.add_argument(
        "--days", type=int, default=30,
        help="Export events with last_seen within last N days (default 30)",
    )
    args = parser.parse_args(argv)

    print(f"Loading events (last {args.days}d)...", file=sys.stderr)
    events = EventStore(_EVENTS_DIR).read_all(days=args.days)
    if not events:
        print("No events found.", file=sys.stderr)
        return
    print(f"  {len(events)} events loaded.", file=sys.stderr)

    print("Building candidate index...", file=sys.stderr)
    cand_index = _build_candidate_index(_CANDIDATES_DIR)
    print(f"  {len(cand_index)} candidates indexed.", file=sys.stderr)

    _BI_DIR.mkdir(parents=True, exist_ok=True)

    n = export_events(events, _BI_DIR / "events.csv")
    print(f"  events.csv              {n:>5} rows")

    n = export_keywords(events, _BI_DIR / "event_keywords.csv")
    print(f"  event_keywords.csv      {n:>5} rows")

    n = export_domains(events, _BI_DIR / "event_domains.csv")
    print(f"  event_domains.csv       {n:>5} rows")

    n, unresolved = export_candidates(events, cand_index, _BI_DIR / "event_candidates.csv")
    msg = f"  ({unresolved} unresolved from Parquet)" if unresolved else ""
    print(f"  event_candidates.csv    {n:>5} rows{msg}")

    n = export_channel_daily_summary(events, _BI_DIR / "channel_daily_summary.csv")
    print(f"  channel_daily_summary.csv {n:>4} rows")

    n = export_evaluation_summary(
        events, _CANDIDATES_DIR, _BASELINE_PATH, args.days,
        _BI_DIR / "evaluation_summary.csv",
    )
    print(f"  evaluation_summary.csv    {n:>4} row  (run-level metrics)")

    n = export_baseline(_BASELINE_PATH, _BI_DIR / "baseline_results.csv")
    print(f"  baseline_results.csv    {n:>5} rows")

    print(f"\nDone → {_BI_DIR}")


if __name__ == "__main__":
    main()
