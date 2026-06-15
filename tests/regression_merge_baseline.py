#!/usr/bin/env python3
"""
Regression baseline for SameChannelPolicy merge decisions.

Three classification groups (from golden_merge_cases.json):
  must_merge      — pairwise cases that must pass. Failure = policy too strict.
  must_not_merge  — pairwise cases that must not merge. Failure = policy too permissive.
  scenario_e2e    — E2E grouping cases where direct pairwise merge is ambiguous.
                    Requires state accumulation. Failures are warnings, not hard failures.

Exit code 1 if any must_merge or must_not_merge tests fail.
scenario_e2e failures are reported but do not affect exit code.

Usage:
    python tests/regression_merge_baseline.py
    python tests/regression_merge_baseline.py --skip-e2e
    python tests/regression_merge_baseline.py --verbose
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
        group    = case.get("group", "")

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
                "group":     group,
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
                f"  [{group}]"
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
) -> Tuple[int, int, int, List[dict]]:
    """Returns (hard_passed, hard_failed, scenario_warned, failures)."""
    print(f"\n  Running end-to-end consolidation {date_from} → {date_to}...")
    consolidator = EventConsolidator()
    events = consolidator.run(date_from, date_to, write=False)
    print(f"  Consolidator produced {len(events)} tracked event(s).")

    # Map candidate_id → TrackedEvent object (not event_id string).
    # Object identity is correct: two events with the same event_id string are
    # distinct events and must be treated as such.
    from src.events.models import TrackedEvent as _TrackedEvent
    cand_to_ev: Dict[str, _TrackedEvent] = {}
    for ev in events:
        for cid in ev.candidate_ids:
            cand_to_ev[cid] = ev

    # Uniqueness assertion — event_ids must be unique across the run.
    all_ids = [ev.event_id for ev in events]
    dupes   = {eid for eid in all_ids if all_ids.count(eid) > 1}
    if dupes:
        print(f"  WARN  Duplicate event_ids detected: {dupes}", file=sys.stderr)

    hard_passed = hard_failed = scenario_warned = 0
    failures: List[dict] = []

    for group in e2e.get("expected_same_event", []):
        gid       = group["group_id"]
        ids       = group["candidate_ids"]
        grp_class = group.get("group", "must_merge")
        missing   = [cid for cid in ids if cid not in cand_to_ev]

        if missing:
            print(f"  SKIP  {gid} (same_event): not in output: {missing}")
            continue

        # Use object identity — same Python object means same TrackedEvent.
        obj_ids   = {id(cand_to_ev[cid]) for cid in ids}
        event_ids = {cand_to_ev[cid].event_id for cid in ids}
        ok = len(obj_ids) == 1

        if ok:
            if grp_class != "scenario_e2e":
                hard_passed += 1
            if verbose:
                print(f"  PASS  {gid} (same_event) [{grp_class}]: all in {next(iter(event_ids))}")
        else:
            if grp_class == "scenario_e2e":
                scenario_warned += 1
                label = "WARN"
            else:
                hard_failed += 1
                label = "FAIL"
            failures.append({
                "group_id":  gid,
                "group":     grp_class,
                "kind":      "same_event",
                "event_ids": sorted(event_ids),
                "notes":     group.get("notes", ""),
            })
            print(f"  {label}  {gid} (same_event) [{grp_class}]: split across {len(obj_ids)} events")

    for group in e2e.get("expected_different_events", []):
        gid       = group["group_id"]
        ids       = group["candidate_ids"]
        grp_class = group.get("group", "must_not_merge")
        missing   = [cid for cid in ids if cid not in cand_to_ev]

        if missing:
            print(f"  SKIP  {gid} (diff_events): not in output: {missing}")
            continue

        # Use object identity for grouping; event_id only for display.
        ev_obj_ids = [id(cand_to_ev[cid]) for cid in ids]
        event_ids  = [cand_to_ev[cid].event_id for cid in ids]
        ok = len(set(ev_obj_ids)) == len(ids)

        if ok:
            if grp_class != "scenario_e2e":
                hard_passed += 1
            if verbose:
                print(f"  PASS  {gid} (diff_events) [{grp_class}]: all separated")
        else:
            merged = [cid for cid, oid in zip(ids, ev_obj_ids) if ev_obj_ids.count(oid) > 1]
            if grp_class == "scenario_e2e":
                scenario_warned += 1
                label = "WARN"
            else:
                hard_failed += 1
                label = "FAIL"
            failures.append({
                "group_id":   gid,
                "group":      grp_class,
                "kind":       "diff_events",
                "merged_ids": merged,
                "notes":      group.get("notes", ""),
            })
            print(f"  {label}  {gid} (diff_events) [{grp_class}]: unexpectedly merged — {merged}")

    return hard_passed, hard_failed, scenario_warned, failures


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
    must_merge     = [c for c in golden["pairwise"] if c["expected"] == "should_merge"]
    must_not_merge = [c for c in golden["pairwise"] if c["expected"] == "should_not_merge"]

    print(f"\nPAIRWISE — must_merge ({len(must_merge)} cases):")
    mm_p, mm_f, mm_failures = _run_pairwise(must_merge, index, args.verbose)

    print(f"\nPAIRWISE — must_not_merge ({len(must_not_merge)} cases):")
    mnm_p, mnm_f, mnm_failures = _run_pairwise(must_not_merge, index, args.verbose)

    # --- E2E ---
    e2e_p = e2e_f = e2e_w = 0
    e2e_failures: List[dict] = []
    if not args.skip_e2e:
        print(f"\nEND-TO-END:")
        e2e_p, e2e_f, e2e_w, e2e_failures = _run_e2e(
            golden["end_to_end"], date_from, date_to, args.verbose
        )

    # --- Summary ---
    total_p = mm_p + mnm_p + e2e_p
    total_f = mm_f + mnm_f + e2e_f

    print(f"\n{'='*62}")
    print(f"  Results")
    print(f"{'='*62}")
    print(f"  must_merge     (pairwise): {mm_p:>2} passed, {mm_f:>2} failed")
    print(f"  must_not_merge (pairwise): {mnm_p:>2} passed, {mnm_f:>2} failed")
    if not args.skip_e2e:
        print(f"  must_*         (e2e):      {e2e_p:>2} passed, {e2e_f:>2} failed")
        if e2e_w:
            print(f"  scenario_e2e   (e2e):      {e2e_w:>2} warned  (not counted in total)")
    print(f"  {'─'*34}")
    print(f"  total:                     {total_p:>2} passed, {total_f:>2} failed")

    if mm_failures or mnm_failures:
        print(f"\nPairwise failures:")
        for f in mm_failures + mnm_failures:
            bd = f.get("breakdown", {})
            print(
                f"\n  [{f['case_id']}]  group={f['group']}  expected={f['expected']}  score={f['score']:.3f}"
                f"  kw={bd.get('keyword_overlap', 0):.3f}"
                f"  conv={bd.get('conversation_overlap', 0):.3f}"
                f"  dom={bd.get('domain_overlap', 0):.3f}"
                f"  top={int(bool(bd.get('top_conversation_match')))}"
            )
            print(f"    a: {f['title_a']!r}")
            print(f"    b: {f['title_b']!r}")
            if f.get("notes"):
                print(f"    note: {f['notes']}")

    hard_e2e = [f for f in e2e_failures if f.get("group") != "scenario_e2e"]
    warn_e2e = [f for f in e2e_failures if f.get("group") == "scenario_e2e"]

    if hard_e2e:
        print(f"\nE2E failures:")
        for f in hard_e2e:
            print(f"\n  [{f['group_id']}]  kind={f['kind']}  group={f['group']}")
            if f["kind"] == "same_event":
                print(f"    split across: {f['event_ids']}")
            else:
                print(f"    merged: {f['merged_ids']}")
            if f.get("notes"):
                print(f"    note: {f['notes']}")

    if warn_e2e:
        print(f"\nE2E warnings (scenario_e2e — aspirational, not blocking):")
        for f in warn_e2e:
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
