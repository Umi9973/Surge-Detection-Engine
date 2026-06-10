"""
EventConsolidator — merges repeated EventCandidates across time windows into TrackedEvents.

Usage:
    python -m src.events.consolidator --date-from 2026-05-31 --date-to 2026-06-03 --verbose

Batch-only: each run starts from empty state. Run over a single wide date range
for correct consolidation across day boundaries.

Date range semantics: candidate files are partitioned by window_end date (how
CandidateArchiver writes them). --date-from / --date-to filter on that date.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pyarrow.parquet as pq

from .evidence import EventEvidence
from .matching_policy import MatchingPolicy, SameChannelPolicy
from .models import EventCandidate, TrackedEvent
from .store import EventStore

_ROOT            = Path(__file__).resolve().parent.parent.parent
_CANDIDATES_DIR  = _ROOT / "data" / "event_candidates"
_EVENTS_DIR      = _ROOT / "data" / "events"


class EventConsolidator:
    def __init__(
        self,
        candidates_dir: Path = _CANDIDATES_DIR,
        events_dir: Path     = _EVENTS_DIR,
        policy: Optional[MatchingPolicy] = None,
        stale_hours: float = 6.0,
    ) -> None:
        self._candidates_dir = Path(candidates_dir)
        self._store          = EventStore(events_dir)
        self._policy         = policy or SameChannelPolicy()
        self._stale_secs     = stale_hours * 3600

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        date_from: str,
        date_to: str,
        write: bool = True,
    ) -> List[TrackedEvent]:
        candidates = self._load_candidates(date_from, date_to)
        print(f"Loaded {len(candidates)} event_candidate(s) from {date_from} to {date_to}.")

        active_events: List[TrackedEvent] = []
        all_events:    List[TrackedEvent] = []

        for cand in candidates:
            evidence = EventEvidence.from_candidate(cand)

            # Close stale events — safe: build new list, no in-place mutation
            still_active: List[TrackedEvent] = []
            for ev in active_events:
                if evidence.window_start - ev.last_seen > self._stale_secs:
                    ev.status = "closed"
                    all_events.append(ev)
                else:
                    still_active.append(ev)
            active_events = still_active

            # Score evidence against all active events
            best_event, best_score = self._best_match(evidence, active_events)
            if best_event is not None:
                self._merge(best_event, evidence)
            else:
                active_events.append(self._new_event(evidence))

        # Close remaining active events
        for ev in active_events:
            ev.status = "closed"
            all_events.append(ev)

        for ev in all_events:
            _label_duration(ev)

        all_events.sort(key=lambda e: e.first_seen)

        if write:
            path = self._store.write(all_events, date_from, date_to)
            print(f"Wrote {len(all_events)} tracked event(s) → {path}")

        return all_events

    # ------------------------------------------------------------------
    # Matching
    # ------------------------------------------------------------------

    def _best_match(
        self,
        evidence: EventEvidence,
        active: List[TrackedEvent],
    ) -> Tuple[Optional[TrackedEvent], float]:
        best_ev    = None
        best_score = 0.0
        for ev in active:
            s = self._policy.score(evidence, ev)
            if s > best_score:
                best_ev, best_score = ev, s
        if best_score >= self._policy.merge_threshold:
            return best_ev, best_score
        return None, 0.0

    # ------------------------------------------------------------------
    # Event creation
    # ------------------------------------------------------------------

    def _new_event(self, evidence: EventEvidence) -> TrackedEvent:
        if evidence.top_conversation_id != 0:
            event_id = (
                f"{evidence.source}:{evidence.channel}"
                f":{evidence.window_start}:{evidence.top_conversation_id}"
            )
        else:
            # candidate_id is unique by construction: {source}:{channel}:{window_end}:{cluster_id}
            event_id = evidence.candidate_id

        title = evidence.top_conversation_title or ""
        return TrackedEvent(
            event_id             = event_id,
            sources              = [evidence.source],
            source_counts        = {evidence.source: 1},
            channels             = [evidence.channel],
            primary_channel      = evidence.channel,
            first_seen           = evidence.window_start,
            last_seen            = evidence.window_end,
            duration_minutes     = (evidence.window_end - evidence.window_start) / 60,
            candidate_ids        = [evidence.candidate_id],
            candidate_count      = 1,
            representative_title = title,
            top_titles           = [title] if title else [],
            keywords             = list(evidence.keywords),
            conversation_ids     = list(evidence.conversation_ids),
            top_conversation_id  = evidence.top_conversation_id,
            domains              = list(evidence.domains),
            communities          = list(evidence.communities),
            total_item_count     = evidence.item_count,
            peak_score           = evidence.event_score,
            avg_score            = evidence.event_score,
            peak_z_score         = evidence.z_score,
            status               = "active",
        )

    # ------------------------------------------------------------------
    # Event update
    # ------------------------------------------------------------------

    def _merge(self, event: TrackedEvent, evidence: EventEvidence) -> None:
        new_avg = (
            event.avg_score * event.candidate_count + evidence.event_score
        ) / (event.candidate_count + 1)

        # Update representative title to highest-scoring candidate's title
        if evidence.event_score >= event.peak_score and evidence.top_conversation_title:
            event.representative_title = evidence.top_conversation_title
            event.top_conversation_id  = evidence.top_conversation_id

        event.last_seen        = max(event.last_seen, evidence.window_end)
        event.duration_minutes = (event.last_seen - event.first_seen) / 60
        event.candidate_ids.append(evidence.candidate_id)
        event.candidate_count += 1
        event.avg_score        = round(new_avg, 6)
        event.peak_score       = max(event.peak_score, evidence.event_score)
        event.peak_z_score     = max(event.peak_z_score, evidence.z_score)
        event.total_item_count += evidence.item_count
        event.source_counts[evidence.source] = (
            event.source_counts.get(evidence.source, 0) + 1
        )
        if evidence.channel not in event.channels:
            event.channels.append(evidence.channel)

        # Extend lists, deduplicated, capped
        _extend_capped(event.conversation_ids, evidence.conversation_ids, cap=50)
        _extend_capped(event.keywords,         evidence.keywords,         cap=15)
        _extend_capped(event.domains,          evidence.domains,          cap=10)
        if evidence.top_conversation_title and evidence.top_conversation_title not in event.top_titles:
            event.top_titles.append(evidence.top_conversation_title)
            if len(event.top_titles) > 5:
                event.top_titles = event.top_titles[:5]

    # ------------------------------------------------------------------
    # Candidate loading
    # ------------------------------------------------------------------

    def _load_candidates(self, date_from: str, date_to: str) -> List[EventCandidate]:
        from_dt = datetime.strptime(date_from, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        to_dt   = datetime.strptime(date_to,   "%Y-%m-%d").replace(tzinfo=timezone.utc)
        from_ts = int(from_dt.timestamp())
        to_ts   = int(to_dt.timestamp()) + 86399  # inclusive: end of to_date

        candidates: List[EventCandidate] = []

        for path in sorted(self._candidates_dir.glob("**/*.parquet")):
            try:
                stem = path.stem                       # candidates_{channel}_{window_end}
                window_end = int(stem.rsplit("_", 1)[-1])
            except (ValueError, IndexError):
                continue
            if not (from_ts <= window_end <= to_ts):
                continue

            try:
                table = pq.read_table(str(path))
            except Exception as exc:
                print(f"[consolidator] skipping {path.name}: {exc}", file=sys.stderr)
                continue

            for i in range(table.num_rows):
                row = {col: table.column(col)[i].as_py() for col in table.schema.names}
                if row.get("kind") != "event_candidate":
                    continue
                candidates.append(_row_to_candidate(row))

        candidates.sort(key=lambda c: c.window_start)
        return candidates


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _label_duration(event: TrackedEvent) -> None:
    if event.candidate_count == 1:
        event.duration_kind = "isolated"
    elif event.duration_minutes < 120:
        event.duration_kind = "flash"
    elif event.duration_minutes < 240:
        event.duration_kind = "developing"
    else:
        event.duration_kind = "sustained"


def _extend_capped(target: list, source: list, cap: int) -> None:
    for item in source:
        if item not in target:
            target.append(item)
            if len(target) >= cap:
                break


def _row_to_candidate(row: Dict) -> EventCandidate:
    return EventCandidate(
        candidate_id              = row["candidate_id"],
        source                    = row["source"],
        channel                   = row["channel"],
        window_start              = row.get("window_start", 0),
        window_end                = row["window_end"],
        cluster_id                = row["cluster_id"],
        kind                      = row["kind"],
        event_score               = row["event_score"],
        size                      = row["size"],
        unique_conversation_count = row["unique_conversation_count"],
        top_conversation_pct      = row["top_conversation_pct"],
        top_story_id              = row["top_story_id"],
        top_story_title           = row.get("top_story_title", ""),
        conversation_ids          = list(row.get("conversation_ids") or []),
        top_domains               = list(row.get("top_domains") or []),
        keywords                  = list(row.get("keywords") or []),
        z_score                   = row.get("z_score", 0.0),
        window_count              = row.get("window_count", 0),
        dbscan_eps                = row.get("dbscan_eps", 0.5),
        dbscan_min_samples        = row.get("dbscan_min_samples", 5),
    )


# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------

def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description="Consolidate EventCandidates into TrackedEvents.")
    parser.add_argument("--date-from",    required=True, help="Start date YYYY-MM-DD (inclusive, by window_end)")
    parser.add_argument("--date-to",      required=True, help="End date YYYY-MM-DD (inclusive, by window_end)")
    parser.add_argument("--stale-hours",  type=float, default=6.0, help="Hours of inactivity before closing an event")
    parser.add_argument("--candidates-dir", default=str(_CANDIDATES_DIR))
    parser.add_argument("--events-dir",     default=str(_EVENTS_DIR))
    parser.add_argument("--verbose", action="store_true", help="Print one line per tracked event after writing")
    args = parser.parse_args()

    consolidator = EventConsolidator(
        candidates_dir = Path(args.candidates_dir),
        events_dir     = Path(args.events_dir),
        stale_hours    = args.stale_hours,
    )
    events = consolidator.run(args.date_from, args.date_to)

    if args.verbose:
        print()
        print(f"{'event_id':<55} {'ch':<10} {'cands':>5} {'dur_min':>8}  title")
        print("-" * 120)
        for ev in events:
            print(
                f"{ev.event_id:<55} "
                f"{ev.primary_channel:<10} "
                f"{ev.candidate_count:>5} "
                f"{ev.duration_minutes:>8.0f}  "
                f"{ev.representative_title[:60]}"
            )
        print()
        print(f"Total: {len(events)} tracked events from {len({c for ev in events for c in ev.candidate_ids})} candidates")


if __name__ == "__main__":
    main()
