#!/usr/bin/env python3
"""
Regression baseline for SameChannelPolicy merge decisions.

Loads data/audit/golden_merge_cases.json and runs two test suites:
  - Pairwise: seeds a TrackedEvent from candidate_a, scores candidate_b.
              No state accumulation — conservative for SHOULD_MERGE.
  - End-to-end: runs EventConsolidator over the full date range (write=False)
                and checks that known groups land in the same or different events.

Usage:
    python tests/regression_merge_baseline.py
    python tests/regression_merge_baseline.py --skip-e2e
    python tests/regression_merge_baseline.py --verbose

Exit code 1 if any tests fail.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import pyarrow.parquet as pq

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from src.events.consolidator import EventConsolidator, _row_to_candidate
from src.events.evidence import EventEvidence
from src.events.matching_policy import SameChannelPolicy
from src.events.models import EventCandidate

_GOLDEN_PATH    = _ROOT / "data" / "audit" / "golden_merge_cases.json"
_CANDIDATES_DIR = _ROOT / "data" / "event_candidates"


# ---------------------------------------------------------------------------
# Candidate index
# ---------------------------------------------------------------------------

def _build_index(date_from: str, date_to: str) -> Dict[str, EventCandidate]:
    from_ts = int(datetime.strptime(date_from, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
    to_ts   = int(datetime.strptime(date_to,   "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()) + 86399

    index: Dict[str, EventCandidate] = {}
    for path in sorted(_CANDIDATES_DIR.glob("**/*.parquet")):
        try:
            window_end = int(path.stem.rsplit("_", 1)[-1])
        except (ValueError, IndexError):
            continue
        if not (from_ts <= window_end <= to_ts):
            continue
        try:
            table = pq.read_table(str(path))
        except Exception as exc:
            print(f"[index] skipping {path.name}: {exc}", file=sys.stderr)
            continue
        for i in range(table.num_rows):
            row = {col: table.column(col)[i].as_py() for col in table.schema.names}
            if row.get("kind") != "event_candidate":
                continue
            cand = _row_to_candidate(row)
            index[cand.candidate_id] = cand

    return index


# ---------------------------------------------------------------------------
# Pairwise tests
# ---------------------------------------------------------------------------

def _run_pairwise(
    cases: List[dict],
    index: Dict[str, EventCandidate],
    verbose: bool,
) -> Tuple[int, int, List[dict]]:
    policy       = SameChannelPolicy()
    consolidator = EventConsolidator()
    passed = failed = 0
    failures: List[dict] = []

    for case in cases:
        cid      = case["case_id"]
        expected = case["expected"]

        cand_a = index.get(case["candidate_a"])
        cand_b = index.get(case["candidate_b"])

        if cand_a is None or cand_b is None:
            missing = case["candidate_a"] if cand_a is None else case["candidate_b"]
            print(f"  SKIP  {cid}: not in index: {missing}")
            continue

        ev_a  = EventEvidence.from_candidate(cand_a)
        ev_b  = EventEvidence.from_candidate(cand_b)
        seed  = consolidator._new_event(ev_a)

        score, breakdown = policy.score(ev_b, seed)
        did_merge  = score >= policy.merge_threshold
        want_merge = expected == "should_merge"
        ok         = did_merge == want_merge

        if ok:
            passed += 1
        else:
            failed += 1
            failures.append({
                "case_id":   cid,
                "expected":  expected,
                "score":     score,
                "breakdown": breakdown,
                "title_a":   cand_a.top_story_title,
                "title_b":   cand_b.top_story_title,
                "notes":     case.get("notes", ""),
            })

        if verbose or not ok:
            label = "PASS" if ok else "FAIL"
            merge = "MERGE" if did_merge else "NO-MERGE"
            bd    = breakdown
            print(
                f"  {label}  {cid:<48}  score={score:.3f} [{merge}]"
                f"  kw={bd.get('keyword_overlap', 0):.2f}"
                f"  conv={bd.get('conversation_overlap', 0):.2f}"
                f"  dom={bd.get('domain_overlap', 0):.2f}"
                f"  top={int(bool(bd.get('top_conversation_match')))}"
            )

    return passed, failed, failures


# ---------------------------------------------------------------------------
# End-to-end tests
# ---------------------------------------------------------------------------

def _run_e2e(
    e2e: dict,
    date_from: str,
    date_to: str,
    verbose: bool,
) -> Tuple[int, int, List[dict]]:
    print(f"\n  Running end-to-end consolidation {date_from} → {date_to}...")
    consolidator = EventConsolidator()
    events = consolidator.run(date_from, date_to, write=False)
    print(f"  Consolidator produced {len(events)} tracked event(s).")

    cand_to_event: Dict[str, str] = {}
    for ev in events:
        for cid in ev.candidate_ids:
            cand_to_event[cid] = ev.event_id

    passed = failed = 0
    failures: List[dict] = []

    # All IDs must land in the same TrackedEvent
    for group in e2e.get("expected_same_event", []):
        gid = group["group_id"]
        ids = group["candidate_ids"]
        missing = [cid for cid in ids if cid not in cand_to_event]
        if missing:
            print(f"  SKIP  {gid} (same_event): not in consolidation output: {missing}")
            continue

        event_ids = {cand_to_event[cid] for cid in ids}
        ok = len(event_ids) == 1
        if ok:
            passed += 1
            if verbose:
                print(f"  PASS  {gid} (same_event): all in {next(iter(event_ids))}")
        else:
            failed += 1
            failures.append({
                "group_id":  gid,
                "kind":      "same_event",
                "event_ids": sorted(event_ids),
                "notes":     group.get("notes", ""),
            })
            print(f"  FAIL  {gid} (same_event): split across {len(event_ids)} events")

    # Each ID must be in a different TrackedEvent
    for group in e2e.get("expected_different_events", []):
        gid = group["group_id"]
        ids = group["candidate_ids"]
        missing = [cid for cid in ids if cid not in cand_to_event]
        if missing:
            print(f"  SKIP  {gid} (diff_events): not in consolidation output: {missing}")
            continue

        event_ids = [cand_to_event[cid] for cid in ids]
        ok = len(set(event_ids)) == len(ids)
        if ok:
            passed += 1
            if verbose:
                print(f"  PASS  {gid} (diff_events): all separated")
        else:
            failed += 1
            merged = [cid for cid, eid in zip(ids, event_ids) if event_ids.count(eid) > 1]
            failures.append({
                "group_id":   gid,
                "kind":       "diff_events",
                "merged_ids": merged,
                "notes":      group.get("notes", ""),
            })
            print(f"  FAIL  {gid} (diff_events): unexpectedly merged — {merged}")

    return passed, failed, failures


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(description="Regression baseline for SameChannelPolicy.")
    parser.add_argument("--skip-e2e", action="store_true", help="Skip end-to-end consolidation tests")
    parser.add_argument("--verbose",  action="store_true", help="Print all cases, not just failures")
    args = parser.parse_args()

    golden    = json.loads(_GOLDEN_PATH.read_text(encoding="utf-8"))
    meta      = golden["meta"]
    date_from, date_to = meta["date_range"].split(" to ")

    print(f"\n{'='*62}")
    print(f"  Golden Merge Baseline  ({date_from} → {date_to})")
    print(f"{'='*62}")

    print(f"\nBuilding candidate index...")
    index = _build_index(date_from, date_to)
    print(f"Indexed {len(index)} event_candidate(s).")

    # --- Pairwise ---
    should_merge     = [c for c in golden["pairwise"] if c["expected"] == "should_merge"]
    should_not_merge = [c for c in golden["pairwise"] if c["expected"] == "should_not_merge"]

    print(f"\nPAIRWISE — should_merge ({len(should_merge)} cases):")
    sm_p, sm_f, sm_failures = _run_pairwise(should_merge, index, args.verbose)

    print(f"\nPAIRWISE — should_not_merge ({len(should_not_merge)} cases):")
    snm_p, snm_f, snm_failures = _run_pairwise(should_not_merge, index, args.verbose)

    # --- E2E ---
    e2e_p = e2e_f = 0
    e2e_failures: List[dict] = []
    if not args.skip_e2e:
        print(f"\nEND-TO-END:")
        e2e_p, e2e_f, e2e_failures = _run_e2e(golden["end_to_end"], date_from, date_to, args.verbose)

    # --- Summary ---
    total_p = sm_p + snm_p + e2e_p
    total_f = sm_f + snm_f + e2e_f

    print(f"\n{'='*62}")
    print(f"  Results")
    print(f"{'='*62}")
    print(f"  should_merge:     {sm_p:>2} passed, {sm_f:>2} failed")
    print(f"  should_not_merge: {snm_p:>2} passed, {snm_f:>2} failed")
    if not args.skip_e2e:
        print(f"  end_to_end:       {e2e_p:>2} passed, {e2e_f:>2} failed")
    print(f"  {'─'*32}")
    print(f"  total:            {total_p:>2} passed, {total_f:>2} failed")

    if sm_failures or snm_failures:
        print(f"\nPairwise failures:")
        for f in sm_failures + snm_failures:
            bd = f.get("breakdown", {})
            print(
                f"\n  [{f['case_id']}]  expected={f['expected']}  score={f['score']:.3f}"
                f"  kw={bd.get('keyword_overlap', 0):.3f}"
                f"  conv={bd.get('conversation_overlap', 0):.3f}"
                f"  dom={bd.get('domain_overlap', 0):.3f}"
                f"  top={int(bool(bd.get('top_conversation_match')))}"
            )
            print(f"    a: {f['title_a']!r}")
            print(f"    b: {f['title_b']!r}")
            if f.get("notes"):
                print(f"    note: {f['notes']}")

    if e2e_failures:
        print(f"\nE2E failures:")
        for f in e2e_failures:
            print(f"\n  [{f['group_id']}]  kind={f['kind']}")
            if f["kind"] == "same_event":
                print(f"    split across: {f['event_ids']}")
            else:
                print(f"    merged: {f['merged_ids']}")
            if f.get("notes"):
                print(f"    note: {f['notes']}")

    sys.exit(1 if total_f > 0 else 0)


if __name__ == "__main__":
    main()
