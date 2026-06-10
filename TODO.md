### C. Event Consolidator — Flash/Sustained Classification for Live Pipeline (`src/pipeline/event_consolidator.py`)
- [ ] **Context:** The aggregator in `src/analysis/aggregator.py` already implements FLASH/SUSTAINED/ISOLATED classification with Union-Find, Multi-Track Anchor keyword locking, and Szymkiewicz–Simpson overlap scoring. It was built for the Reddit historical backtest (batch, reads from SQLite). The live HN pipeline has no equivalent — every raw `AnomalyEvent` is treated identically regardless of whether it is a 13-hour sustained surge or a one-window blip. See **Issue #18** in `docs/ISSUES.md`.
- [ ] **Action:** Port aggregator logic into a streaming `EventConsolidator` component that buffers `AnomalyEvent` objects and emits labelled consolidated events with a short look-ahead delay (2 eval ticks = 10 min).
- [ ] **Core algorithms to port (minimal changes needed):**
  - FLASH: Union-Find linking channels with ≥30% cluster overlap within 2-hour window, ≥2 shared keywords
  - SUSTAINED: Multi-Track Anchor locking dominant DBSCAN cluster after Hour 2, ≥4 consecutive windows at ≥50% overlap
  - ISOLATED: catch-all for unclaimed events
  - Claim order: FLASH → SUSTAINED → ISOLATED (prevents double-counting)
- [ ] **Interface change:** Add `event_type: str` (`"FLASH"` / `"SUSTAINED"` / `"ISOLATED"`) and `event_duration_s: int` to consolidated event output.
- [ ] **Wire-up:** Insert `EventConsolidator` between `AlertGate` and `archiver/dispatcher` in `live_hn()`.
- [ ] **Key trade-off:** Look-ahead buffer adds notification latency. 2-tick (10 min) buffer gives accurate classification for most flash events; sustained events are confirmed only after ≥4 consecutive windows (20 min).

---

Plan: EventConsolidator Layer — General/Future-Ready Design

Goal:
Build a platform-neutral EventConsolidator layer that merges repeated EventCandidates across time into TrackedEvents.

Current layer:
EventCandidate = one cluster in one anomaly window.

New layer:
TrackedEvent = one event/topic tracked across multiple EventCandidates.

Main purpose:
Reduce repeated window-level candidates into coherent event-level records.

Important framing:
This should be HN-only for v1 data, but not HN-specific in structure.
Design it so Reddit/Twitter-like sources can be added later without rewriting the whole layer.
Phase 1 — Define the boundary

Input:
data/event_candidates/

Output:
data/events/

Input object:
EventCandidate

Output object:
TrackedEvent

The consolidator should not read raw Parquet.
The consolidator should not run DBSCAN.
The consolidator should not change routing.
The consolidator should not change EventCandidate scoring.
The consolidator should not touch batch_enricher.py.

Worth noticing:
This layer is not a classifier for one candidate.
It is a merger/tracker across candidates.
If you put this into batch_enricher.py, the architecture gets messy.
Phase 2 — Add platform-neutral event models

Add / extend under:
src/events/models.py

Add model:
TrackedEvent

TrackedEvent should use general names:
- event_id
- sources
- source_counts
- channels
- primary_channel
- first_seen
- last_seen
- duration_minutes
- candidate_ids
- candidate_count
- representative_title
- top_titles
- keywords
- conversation_ids
- domains
- communities
- total_item_count
- peak_score
- avg_score
- peak_z_score
- event_kind
- duration_kind
- status

Do not use HN-only names like:
- story_id
- story_title
- subreddit-only assumptions

Use:
- conversation_id
- conversation_title
- community

Worth noticing:
HN story = Reddit post/thread = Twitter root thread.
So the model should think in "conversation" language, not "story" language.
Phase 3 — Add EventEvidence normalization

Add file:
src/events/evidence.py

Purpose:
Convert EventCandidate into a matching-friendly evidence object.

Why:
Future sources may have different fields.
The consolidator should compare normalized evidence, not raw HN candidate details.

EventEvidence should contain:
- candidate_id
- source
- channel
- time window
- candidate_kind
- event_score
- keywords
- conversation_ids
- top_conversation_id
- top_conversation_title
- domains
- communities
- item_count
- z_score

For HN v1:
source = "hn"
community can be empty or "news.ycombinator"
conversation_id = story_id-derived candidate field

Worth noticing:
This is a small abstraction, but important.
It prevents future Reddit/Twitter support from forcing a rewrite of the consolidator.
Phase 4 — Create matching policy as a separate component

Add file:
src/events/matching_policy.py

Purpose:
Decide whether one EventEvidence should merge into one existing TrackedEvent.

Do not hardcode all rules inside consolidator.py.

Matching signals:
- time proximity
- conversation overlap
- top conversation match
- keyword overlap
- domain overlap
- community overlap
- source/channel compatibility

V1 policy:
- same source only
- same channel by default
- time gap <= 90 minutes
- merge if strong conversation overlap
- merge if same top_conversation_id
- merge if keyword overlap is strong and domain overlap exists

General channel rule:
Be stricter for general.
Do not merge general candidates on weak keyword overlap alone.

Worth noticing:
"same channel" should be a policy rule, not a permanent model assumption.
Later cross-channel merging should be possible.
Phase 5 — Implement EventConsolidator v1

Add file:
src/events/consolidator.py

Purpose:
Read EventCandidates in time order and update/create TrackedEvents.

Process:
1. Load candidates.
2. Filter to candidate_kind = event_candidate for v1.
3. Convert each candidate to EventEvidence.
4. Compare it with active TrackedEvents.
5. Use matching_policy to find best match.
6. If match is strong enough, merge candidate into existing event.
7. If no match, create new TrackedEvent.
8. Close events that have not been updated after a configured time gap.

V1 scope:
Same-source, same-channel consolidation.
No cross-platform merging yet.
No LLM labeling.
No dashboard.
No flash/sustained yet unless basic merge works first.

Worth noticing:
The goal is not to solve all event intelligence.
The goal is to reduce duplicated candidates.
Example: several systemd candidates across Jun 02 should become one tracked event.
Phase 6 — Event update behavior

When a candidate merges into an event, update:
- last_seen
- candidate_count
- candidate_ids
- source_counts
- channels
- conversation_ids
- keywords
- domains
- total_item_count
- peak_score
- avg_score
- peak_z_score
- top_titles
- representative_title

Representative title rule:
Use the highest-score candidate title or most frequent top conversation title.
Do not always overwrite with the newest title.

Worth noticing:
Event labels can drift if you always use the latest candidate.
This is especially dangerous in general.
Phase 7 — Event storage

Add file:
src/events/store.py

Output location:
data/events/

Recommended first format:
Parquet or JSONL.

JSONL is easier to inspect.
Parquet is more consistent with your current artifacts.

Keep candidates and events separate:
data/event_candidates/ = evidence
data/events/ = derived tracked events

Worth noticing:
Do not overwrite EventCandidate files.
They are the audit trail.
TrackedEvents are summaries built from them.
Phase 8 — Add inspect_events CLI

Add file:
src/reporting/inspect_events.py

Purpose:
Inspect consolidated events, not raw candidates.

Useful filters:
- date
- channel
- source
- min-score
- limit
- event_kind
- duration_kind
- active-only

Display:
- event title
- source/channels
- first_seen
- last_seen
- duration
- candidate_count
- peak_score
- peak_z
- keywords
- top domains
- top conversations

Worth noticing:
This is required before dashboard work.
Do not integrate dashboard until inspect_events output looks sane.
Phase 9 — Validation pass

Use May 31 to Jun 3 candidates first.

Expected successful merges:
- repeated Cloudflare Turnstile candidates become one event
- repeated United Airlines Bluetooth candidates become one event
- repeated systemd candidates become one event
- repeated Gemma / Google model candidates become one event if similarity is strong enough

Expected non-merges:
- unrelated general candidates should not become one giant "general event"
- systemd should not merge with Anthropic/Project Glasswing unless evidence is genuinely shared
- hiring threads should not reappear as tracked events

Success target:
87 event candidates should collapse to a smaller number of coherent tracked events.

Do not chase an exact number.
The target is coherence.

Worth noticing:
If everything merges into a few giant general events, matching is too loose.
If almost nothing merges, matching is too strict.
Phase 10 — Add basic duration labels only after merge quality is acceptable

Do not start with flash/sustained.

After tracked events look coherent, add duration_kind:
- isolated
- flash
- developing
- sustained

Possible meaning:
isolated = one candidate only
flash = short burst, small duration
developing = multiple windows but not long enough
sustained = long-running event across many windows

Worth noticing:
Flash/sustained should describe TrackedEvent, not raw EventCandidate.
A channel can be sustained while the underlying events are multiple separate bursts.
Phase 11 — Future-ready extensions

Keep these as later phases, not v1:

Cross-channel merging:
Example: same AI event appears in ai, tech, and general.

Cross-source merging:
Example: HN + Reddit + Twitter-like candidates point to the same event.

Entity-aware matching:
Use named entities later:
companies, products, CVEs, people, places.

URL/domain-aware matching:
Useful when several platforms link the same article.

Dashboard integration:
Only after inspect_events CLI is useful.

GCS upload:
Only after event artifact format is stable.

Worth noticing:
Do not design TrackedEvent as HN-only now.
But do not implement all future features now either.
Direct instruction to coding model

Implement EventConsolidator as a separate platform-neutral event layer.

Operationally, v1 only consumes current HN EventCandidate artifacts.
Structurally, it must use source/conversation/channel-neutral naming so Reddit or Twitter-like sources can be added later.

Add:
- src/events/evidence.py
- src/events/matching_policy.py
- src/events/consolidator.py
- src/events/store.py
- src/reporting/inspect_events.py

Do not modify batch_enricher.py except if absolutely needed for artifact path compatibility.
Do not change DBSCAN.
Do not change EventCandidate scoring.
Do not add dashboard integration.
Do not implement full flash/sustained first.

V1 goal:
Merge repeated EventCandidates across nearby windows into TrackedEvents, write them to data/events/, and inspect t
