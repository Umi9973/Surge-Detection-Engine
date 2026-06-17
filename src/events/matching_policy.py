from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Tuple

if TYPE_CHECKING:
    from .evidence import EventEvidence
    from .models import TrackedEvent

_MAX_GAP_SECONDS = 90 * 60  # 90-minute time gap hard gate

# English function words that slip through per-channel TF-IDF on small corpora.
# Conservative — only words that are NEVER topical. Do not extend casually.
_NOISE_KW = frozenset({
    "into", "onto", "over", "also", "just", "even", "well", "here",
    "only", "then", "when", "what", "where", "back", "too", "more",
    "some", "such", "both", "very", "like", "said", "does", "will",
    "would", "could", "have", "been", "were", "from", "with", "them",
    "they", "this", "that",
})


class MatchingPolicy(ABC):
    """Decide whether one EventEvidence should merge into an existing TrackedEvent.

    score() returns (total_score, breakdown_dict).
    The consolidator merges when total_score >= merge_threshold.
    breakdown_dict is passed directly into the merge_trace entry.
    """

    @abstractmethod
    def score(
        self, evidence: "EventEvidence", event: "TrackedEvent"
    ) -> Tuple[float, dict]:
        """Return (merge_score, breakdown) for the evidence/event pair."""
        raise NotImplementedError

    @property
    @abstractmethod
    def merge_threshold(self) -> float:
        """Minimum score required to merge evidence into event."""
        raise NotImplementedError


class SameChannelPolicy(MatchingPolicy):
    """V1 policy: same source, same channel, 90-minute gap gate.

    Scoring weights:
      0.25 — top_conversation_id match (anchor set only)
      0.10 — reverse anchor match: event anchor appears in evidence.conversation_ids
             (only when top_match is False; does not bypass any kw=0 gate)
      0.30 — conversation set overlap (Jaccard: |A∩B|/|A∪B|)
      0.15 — keyword overlap (Szymkiewicz-Simpson, after _NOISE_KW filtering)
      0.05 — domain overlap (Szymkiewicz-Simpson)

    Metric note: conv_overlap uses Jaccard, not Simpson. Each candidate has a bounded
    conv_id set from its own DBSCAN cluster, so there is no multi-cluster union bloat
    (the concern that motivated Simpson for keyword overlap in Issue #3). Tiny candidate
    pools (3-4 conv_ids) fully contained in a larger set gave Simpson=1.0 on unrelated
    stories — Jaccard correctly down-weights this containment.

    Global gate:  kw=0 AND no anchor → blocked. Conv/domain alone is not reliable
                  because small pools give high overlap for unrelated stories.
    General gate: kw=0 → always blocked (top_match unreliable as standalone signal).
                  Unanchored (no top_match): require kw≥0.30 AND (dom≥0.50 OR conv≥0.65)
                  AND kw+conv+dom score ≥ 0.35.  Anchored: kw>0 suffices.
    Reverse anchor: does not bypass any gate. Contributes only when kw>0 passes the
                  applicable gate(s) and top_match is False.
    """

    @property
    def merge_threshold(self) -> float:
        return 0.25

    def score(
        self, evidence: "EventEvidence", event: "TrackedEvent"
    ) -> Tuple[float, dict]:
        _zero = (0.0, {})

        # Hard gates
        if evidence.source not in event.sources:
            return _zero
        if evidence.channel not in event.channels:
            return _zero
        time_gap = evidence.window_start - event.last_seen
        if time_gap > _MAX_GAP_SECONDS:
            return _zero

        ev_conv  = set(evidence.conversation_ids)
        evt_conv = set(event.conversation_ids)
        ev_kw    = set(evidence.keywords)
        evt_kw   = set(event.keywords)
        ev_dom   = set(evidence.domains)
        evt_dom  = set(event.domains)

        # Top conversation match — checked against anchor set only (not full conversation_ids).
        # anchor_conversation_ids is the small, stable identity set; evt_conv is provenance only.
        top_match = (
            evidence.top_conversation_id != 0
            and evidence.top_conversation_id in set(event.anchor_conversation_ids)
        )
        top_score = 0.25 if top_match else 0.0

        # Reverse anchor match: event's anchor story appears in evidence's conversation set.
        # Weaker than direct top_match (0.10 vs 0.25). Does not bypass any kw=0 gate —
        # only contributes after gates pass and only when top_match is False.
        _anchors = set(event.anchor_conversation_ids)
        reverse_anchor_match = bool(_anchors & ev_conv) if _anchors else False
        rev_anchor_score = 0.10 if (reverse_anchor_match and not top_match) else 0.0

        # Conversation overlap (Jaccard)
        conv_union = ev_conv | evt_conv
        conv_overlap = len(ev_conv & evt_conv) / len(conv_union) if conv_union else 0.0
        conv_score = 0.30 * conv_overlap

        # Keyword overlap (Szymkiewicz-Simpson, noise-filtered)
        ev_kw_f  = ev_kw  - _NOISE_KW
        evt_kw_f = evt_kw - _NOISE_KW
        if ev_kw_f and evt_kw_f:
            kw_overlap = len(ev_kw_f & evt_kw_f) / min(len(ev_kw_f), len(evt_kw_f))
        else:
            kw_overlap = 0.0
        kw_score = 0.15 * kw_overlap

        # Domain overlap (Szymkiewicz-Simpson)
        if ev_dom and evt_dom:
            dom_overlap = len(ev_dom & evt_dom) / min(len(ev_dom), len(evt_dom))
        else:
            dom_overlap = 0.0
        dom_score = 0.05 * dom_overlap

        # Global gate: no keyword evidence and no anchor hit is never enough to merge.
        if kw_overlap == 0.0 and not top_match:
            return _zero

        # General channel: two-mode gating.
        # kw=0 is always blocked — top_story_id reflects the window's dominant story,
        # not the cluster's topic, so top_match alone is not reliable here.
        # Unanchored (no top_match): stricter gate to block stateful conv accumulation
        # drift. The 0.35 sub-threshold means kw+domain must clear the bar without
        # conv carrying the merge.
        # Anchored (top_match=True): kw>0 suffices; final score decides.
        if evidence.channel == "general":
            if kw_overlap == 0.0:
                return _zero
            if not top_match:
                if kw_overlap < 0.30:
                    return _zero
                if dom_overlap < 0.50 and conv_overlap < 0.65:
                    return _zero
                if conv_score + kw_score + dom_score < 0.35:
                    return _zero

        raw = top_score + conv_score + kw_score + dom_score + rev_anchor_score

        # Derive primary merge reason
        if top_match:
            reason = "top_conv_match"
        elif reverse_anchor_match:
            reason = "reverse_anchor_match"
        elif conv_overlap >= 0.3:
            reason = "conv_overlap"
        elif kw_overlap > 0 and dom_overlap > 0:
            reason = "kw+domain"
        else:
            reason = "keyword"

        breakdown = {
            "top_conversation_match": top_match,
            "reverse_anchor_match":   reverse_anchor_match,
            "conversation_overlap":   round(conv_overlap, 4),
            "keyword_overlap":        round(kw_overlap, 4),
            "domain_overlap":         round(dom_overlap, 4),
            "merge_reason":           reason,
        }
        return round(raw, 4), breakdown
