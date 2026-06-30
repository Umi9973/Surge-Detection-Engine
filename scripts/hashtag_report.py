"""
Hashtag discovery report generator.

Reads the latest hashtag_stats_*.json produced by BlueskyIngestor and writes
three report files to data/debug/bluesky/reports/:

  top_unmatched_hashtags.csv   — what the router is missing
  top_routed_hashtags.csv      — what is already working
  hashtag_suggestions.json     — ranked suggestions for router updates

Usage:
    python scripts/hashtag_report.py
    python scripts/hashtag_report.py --stats path/to/hashtag_stats.json
    python scripts/hashtag_report.py --top 200
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_DEBUG_BASE   = _PROJECT_ROOT / "data" / "debug" / "bluesky"
_REPORTS_DIR  = _DEBUG_BASE / "reports"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _find_latest_stats() -> Path:
    candidates = sorted((_DEBUG_BASE / "hashtag_stats").glob("hashtag_stats_*.json"),
                        key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        sys.exit("No hashtag_stats_*.json found. Run the shadow ingestor first.")
    return candidates[0]


def _load(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _pct(n: int, d: int) -> float:
    return round(n / d, 4) if d else 0.0


def _top_str(counter: dict, n: int = 5) -> str:
    """Format top-n items from a dict as 'k:v k:v …'."""
    items = sorted(counter.items(), key=lambda x: -x[1])[:n]
    return "  ".join(f"{k}:{v}" for k, v in items)


def _suggested_action(unmatched: int, total: int) -> str:
    rate = _pct(unmatched, total)
    if rate >= 0.8 and total >= 50:
        return "review_for_addition"
    if rate >= 0.5:
        return "monitor"
    return "ok"


def _confidence(total: int, unique_authors: int) -> str:
    if total >= 1_000 and unique_authors >= 50:
        return "high"
    if total >= 100:
        return "medium"
    return "low"


def _suggested_channel(entry: dict) -> str:
    """Infer likely channel from top channels-when-matched, else 'unknown'."""
    channels = entry.get("channels", {})
    if channels:
        return max(channels, key=channels.__getitem__)
    return "unknown"


# ---------------------------------------------------------------------------
# Report A — top_unmatched_hashtags.csv
# ---------------------------------------------------------------------------

def write_unmatched_csv(hashtags: dict, out_dir: Path, top: int) -> Path:
    rows = [
        (ht, d) for ht, d in hashtags.items()
        if d["unmatched"] > 0
    ]
    rows.sort(key=lambda x: -x[1]["unmatched"])
    rows = rows[:top]

    path = out_dir / "top_unmatched_hashtags.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "hashtag", "unmatched_count", "total_count", "unmatched_rate",
            "unique_authors_capped",
            "top_cohashtags", "top_domains",
            "sample_text_1", "sample_text_2", "sample_text_3",
            "suggested_channel", "suggested_action",
        ])
        for ht, d in rows:
            samples = d.get("samples", [])
            writer.writerow([
                ht,
                d["unmatched"],
                d["total"],
                _pct(d["unmatched"], d["total"]),
                d.get("unique_authors_capped", ""),
                _top_str(d.get("cohashtags", {})),
                _top_str(d.get("domains", {})),
                samples[0] if len(samples) > 0 else "",
                samples[1] if len(samples) > 1 else "",
                samples[2] if len(samples) > 2 else "",
                _suggested_channel(d),
                _suggested_action(d["unmatched"], d["total"]),
            ])
    return path


# ---------------------------------------------------------------------------
# Report B — top_routed_hashtags.csv
# ---------------------------------------------------------------------------

def write_routed_csv(hashtags: dict, out_dir: Path, top: int) -> Path:
    rows = [
        (ht, d) for ht, d in hashtags.items()
        if d["matched"] > 0
    ]
    rows.sort(key=lambda x: -x[1]["matched"])
    rows = rows[:top]

    path = out_dir / "top_routed_hashtags.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "hashtag", "total_count", "matched_count", "matched_rate",
            "top_channel", "channel_distribution",
            "unique_authors_capped", "top_domains",
        ])
        for ht, d in rows:
            channels = d.get("channels", {})
            top_ch   = max(channels, key=channels.__getitem__) if channels else ""
            writer.writerow([
                ht,
                d["total"],
                d["matched"],
                _pct(d["matched"], d["total"]),
                top_ch,
                _top_str(channels),
                d.get("unique_authors_capped", ""),
                _top_str(d.get("domains", {})),
            ])
    return path


# ---------------------------------------------------------------------------
# Report C — hashtag_suggestions.json
# ---------------------------------------------------------------------------

def write_suggestions_json(hashtags: dict, out_dir: Path, top: int) -> Path:
    rows = [
        (ht, d) for ht, d in hashtags.items()
        if d["unmatched"] > 0 and d.get("unique_authors_capped", 0) > 0
    ]
    rows.sort(key=lambda x: -x[1]["unmatched"])
    rows = rows[:top]

    suggestions = []
    for ht, d in rows:
        cohashtags = list(d.get("cohashtags", {}).keys())[:10]
        domains    = list(d.get("domains", {}).keys())[:10]
        suggestions.append({
            "hashtag":              ht,
            "total_count":          d["total"],
            "unmatched_count":      d["unmatched"],
            "matched_count":        d["matched"],
            "unmatched_rate":       _pct(d["unmatched"], d["total"]),
            "unique_authors_capped": d.get("unique_authors_capped", 0),
            "top_cohashtags":       cohashtags,
            "top_domains":          domains,
            "samples":              d.get("samples", []),
            "suggested_channel":    _suggested_channel(d),
            "suggested_action":     _suggested_action(d["unmatched"], d["total"]),
            "confidence":           _confidence(d["total"], d.get("unique_authors_capped", 0)),
        })

    path = out_dir / "hashtag_suggestions.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(suggestions, f, indent=2, ensure_ascii=False)
    return path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Bluesky hashtag discovery report")
    parser.add_argument("--stats", type=Path, default=None,
                        help="Path to hashtag_stats_*.json (default: latest)")
    parser.add_argument("--top",   type=int, default=200,
                        help="Number of entries per report (default: 200)")
    args = parser.parse_args()

    stats_path = args.stats or _find_latest_stats()
    print(f"Reading: {stats_path}")
    data       = _load(stats_path)
    hashtags   = data["hashtags"]

    generated  = datetime.fromtimestamp(data["generated_at"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"Snapshot: {generated}  |  unique hashtags: {data['unique_hashtags']:,}  |  overflow: {data['overflow_count']:,}")

    _REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    p1 = write_unmatched_csv(hashtags,   _REPORTS_DIR, args.top)
    p2 = write_routed_csv(hashtags,      _REPORTS_DIR, args.top)
    p3 = write_suggestions_json(hashtags, _REPORTS_DIR, args.top)

    print(f"Written:")
    print(f"  {p1}")
    print(f"  {p2}")
    print(f"  {p3}")


if __name__ == "__main__":
    main()
