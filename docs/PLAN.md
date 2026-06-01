# EventCandidate Layer Plan

> **Status:** Pending implementation
> **Branch:** `feature/phase-2`
> **Phase:** 2 (Live HN pipeline extension)

---

## Objective

Add an `EventCandidate` classification layer on top of the existing `AnomalyEvent` /
DBSCAN cluster output. Each cluster produced by the batch enricher is independently
scored and labelled as one of three kinds: `viral_post`, `topic_surge`, or
`event_candidate`. This runs offline (batch enricher) initially; the live pipeline
will receive a streaming port in Phase 3.

---

## Motivation

The current pipeline detects anomalies (volume spikes) and extracts topic clusters,
but stops short of distinguishing *why* a surge happened:

- **Viral post** — a single story is dominating discussion (noise, not an event)
- **Topic surge** — correlated activity across multiple stories in one domain
- **Event candidate** — multi-story, multi-domain, high-volume signal worth escalating

Without this layer the dashboard shows all anomalies as equally significant, forcing
manual triage.

---

## New Package Layout

```
src/events/
├── __init__.py
├── models.py            # EventCandidate dataclass
├── candidate_builder.py # Builds candidates from enriched rows; calls classifier
└── classifier.py        # Pure scoring + classification logic
```

`candidate_builder.py` is the only public entry point — it imports and calls
`classifier.py` internally. Callers never instantiate `classifier.py` directly.

---

## `EventCandidate` Dataclass (`src/events/models.py`)

```python
@dataclass
class EventCandidate:
    candidate_id:              str    # "{source}:{channel}:{window_end}:{cluster_id}"
    source:                    str    # "hacker_news", "reddit", etc.
    channel:                   str
    window_end:                int    # Unix timestamp
    cluster_id:                int
    kind:                      str    # "viral_post" | "topic_surge" | "event_candidate"
    event_score:               float  # 0.0–1.0 additive weighted score
    size:                      int    # cluster comment/item count
    unique_conversation_count: int    # distinct story/post IDs in cluster
    top_conversation_pct:      float  # fraction of cluster from top story
    top_story_id:              int    # platform item ID (0 if unknown)
    top_story_title:           str
    top_domains:               list[str]
    keywords:                  list[str]
    z_score:                   float  # from parent AnomalyEvent
    window_count:              int    # total comments in anomaly window
```

**Platform-neutral naming:** `conversation_id` / `unique_conversation_count` /
`top_conversation_pct` instead of `story_id` — works for Reddit threads, HN items,
and future sources without schema changes.

---

## Classification Rules (`src/events/classifier.py`)

Clusters are classified in this priority order:

### 1. Unknown guard (must be first)

```python
if cluster["unique_conversation_count"] == 0:
    kind = "unknown"   # no attributable items — skip scoring
```

Clusters where all items had `story_id=0` (pre-schema or bot traffic) must not be
misclassified as `event_candidate` just because their counts happen to be non-zero.

### 2. Viral post

```python
if (top_conversation_pct >= 0.80) or (unique_conversation_count <= 1):
    kind = "viral_post"
```

**Threshold: 0.80** — 80% or more of attributed items reference the same conversation.
Also triggers when only one distinct conversation is present regardless of percentage.

### 3. Event candidate

```python
elif (unique_conversation_count >= 3
      and len(top_domains) >= 2
      and size >= 10):
    kind = "event_candidate"
```

Requires: 3+ distinct stories, 2+ domains, minimum 10 items. All three conditions
must hold — any single-source or single-domain cluster is not an event.

### 4. Topic surge (default)

```python
else:
    kind = "topic_surge"
```

Everything else: correlated activity that doesn't meet viral or event thresholds.

---

## Scoring Formula

`event_score` is additive, range 0.0–1.0. Components:

| Component | Weight | Signal |
|---|---|---|
| Conversation diversity | 0.30 | `min(unique_conversation_count / 5, 1.0)` — saturates at 5 stories |
| Not dominated | 0.20 | `1.0 - top_conversation_pct` — penalises single-story clusters |
| Domain spread | 0.20 | `min(len(top_domains) / 3, 1.0)` — saturates at 3 domains |
| Volume | 0.15 | `min(size / 100, 1.0)` — saturates at 100 items |
| Z-score strength | 0.15 | `min(z_score / 10.0, 1.0)` — saturates at z=10 |

```python
event_score = (
    0.30 * min(unique_conversation_count / 5,  1.0) +
    0.20 * (1.0 - top_conversation_pct)             +
    0.20 * min(len(top_domains) / 3,           1.0) +
    0.15 * min(size / 100,                     1.0) +
    0.15 * min(z_score / 10.0,                 1.0)
)
```

Score is computed for all kinds including `viral_post` — it reflects raw signal
strength independent of classification, useful for ranking dashboards.

---

## `CandidateBuilder` (`src/events/candidate_builder.py`)

```python
class CandidateBuilder:
    def __init__(self, source: str = "hacker_news") -> None:
        self._source = source

    def from_enriched_row(self, row: dict) -> list[EventCandidate]:
        """Convert one enriched Parquet row to a list of EventCandidates.

        row keys used: channel, window_end, z_score, count, clusters
        """
        candidates = []
        for cluster in (row.get("clusters") or []):
            candidate = _build(self._source, row, cluster)
            if candidate is not None:
                candidates.append(candidate)
        return candidates
```

`_build()` is a module-level private function that:
1. Calls `classifier.classify(cluster, z_score, window_count)` to get `(kind, event_score)`
2. Constructs the `EventCandidate` dataclass
3. Returns `None` for `kind == "unknown"` — unknown clusters are silently dropped

**Candidate ID format:** `"{source}:{channel}:{window_end}:{cluster_id}"`
Example: `"hacker_news:tech:1748649600:0"` — deterministic, collision-free within a run.

---

## Integration Points

### Batch enricher (`src/reporting/batch_enricher.py`)

After the existing cluster enrichment loop, add:

```python
from ..events.candidate_builder import CandidateBuilder

builder = CandidateBuilder(source="hacker_news")
for enriched_row in enriched_rows:
    candidates = builder.from_enriched_row(enriched_row)
    # write to local candidates Parquet or log for now
```

Candidates are written to a separate Parquet file:
`data/parquet_enriched/{YYYY}/{MM}/{DD}/candidates_{channel}_{window_end}.parquet`

### Dashboard generator (`src/reporting/dashboard_generator.py`)

Future: read candidates Parquet alongside enriched events to add an
"Event Candidates" panel to the 4×2 channel dashboard.

### Live pipeline (Phase 3)

Port `CandidateBuilder` to receive a streaming `AnomalyEvent` + inline DBSCAN
output directly, replacing the offline batch step for real-time alerting.

---

## Candidate Output Schema

```python
_CANDIDATE_SCHEMA = pa.schema([
    pa.field("candidate_id",              pa.string()),
    pa.field("source",                    pa.string()),
    pa.field("channel",                   pa.string()),
    pa.field("window_end",                pa.int64()),
    pa.field("cluster_id",                pa.int64()),
    pa.field("kind",                      pa.string()),
    pa.field("event_score",               pa.float64()),
    pa.field("size",                      pa.int64()),
    pa.field("unique_conversation_count", pa.int64()),
    pa.field("top_conversation_pct",      pa.float64()),
    pa.field("top_story_id",              pa.int64()),
    pa.field("top_story_title",           pa.string()),
    pa.field("top_domains",               pa.list_(pa.string())),
    pa.field("keywords",                  pa.list_(pa.string())),
    pa.field("z_score",                   pa.float64()),
    pa.field("window_count",              pa.int64()),
])
```

---

## Implementation Steps

- [ ] Create `src/events/__init__.py`
- [ ] Implement `src/events/models.py` — `EventCandidate` dataclass
- [ ] Implement `src/events/classifier.py` — `classify()` pure function, returns `(kind, event_score)`
- [ ] Implement `src/events/candidate_builder.py` — `CandidateBuilder.from_enriched_row()`
- [ ] Add `_CANDIDATE_SCHEMA` and writer to `src/storage/parquet_archiver.py` (or new `candidate_archiver.py`)
- [ ] Wire `CandidateBuilder` into `src/reporting/batch_enricher.py` run loop
- [ ] Verify: re-run enricher on a recent file, print candidate list, confirm kind assignments match expectations
- [ ] Update `docs/ISSUES.md` Issue #18 if flash/sustained is superseded by this layer

---

## Known Risks

| Risk | Mitigation |
|---|---|
| `unique_conversation_count=0` clusters misclassified | Unknown guard is the first check in `classifier.py` |
| `top_conversation_pct` field absent in old enriched files | `_normalise_row` already backfills cluster fields with defaults |
| Threshold drift as HN activity patterns change | `event_score` stored independently — thresholds can be tuned post-hoc without re-enriching |
| Candidates Parquet growing unbounded | Same date-partitioned layout as main Parquet; GCS lifecycle policies apply |
