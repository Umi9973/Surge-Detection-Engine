from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .evidence import EventEvidence
    from .models import TrackedEvent

_MAX_GAP_SECONDS = 90 * 60  # 90-minute time gap hard gate


class MatchingPolicy(ABC):
    """Decide whether one EventEvidence should merge into an existing TrackedEvent.

    Implementations return a float score in [0.0, 1.0].
    The consolidator merges when score >= merge_threshold.
    """

    @abstractmethod
    def score(self, evidence: "EventEvidence", event: "TrackedEvent") -> float:
        """Return a merge affinity score between evidence and event."""
        raise NotImplementedError

    @property
    @abstractmethod
    def merge_threshold(self) -> float:
        """Minimum score required to merge evidence into event."""
        raise NotImplementedError


class SameChannelPolicy(MatchingPolicy):
    """V1 policy: same source, same channel, 90-minute gap gate.

    Scoring weights:
      0.50 — top_conversation_id match (strongest signal)
      0.30 — conversation set overlap (Szymkiewicz-Simpson)
      0.15 — keyword overlap
      0.05 — domain overlap

    General channel penalty: if keyword overlap is weak (< 0.3) and there is
    no top-conversation match and no strong conversation overlap (>= 0.5),
    the score is forced to 0.0 to prevent incoherent general candidates merging
    on superficial similarity alone.
    """

    @property
    def merge_threshold(self) -> float:
        return 0.25

    def score(self, evidence: "EventEvidence", event: "TrackedEvent") -> float:
        # Hard gates
        if evidence.source not in event.sources:
            return 0.0
        if evidence.channel not in event.channels:
            return 0.0
        time_gap = evidence.window_start - event.last_seen
        if time_gap > _MAX_GAP_SECONDS:
            return 0.0

        ev_conv   = set(evidence.conversation_ids)
        evt_conv  = set(event.conversation_ids)
        ev_kw     = set(evidence.keywords)
        evt_kw    = set(event.keywords)
        ev_dom    = set(evidence.domains)
        evt_dom   = set(event.domains)

        # Top conversation match
        top_match = (
            evidence.top_conversation_id != 0
            and evidence.top_conversation_id in evt_conv
        )
        top_score = 0.50 if top_match else 0.0

        # Conversation overlap (Szymkiewicz-Simpson)
        if ev_conv and evt_conv:
            conv_overlap = len(ev_conv & evt_conv) / min(len(ev_conv), len(evt_conv))
        else:
            conv_overlap = 0.0
        conv_score = 0.30 * conv_overlap

        # Keyword overlap
        if ev_kw and evt_kw:
            kw_overlap = len(ev_kw & evt_kw) / min(len(ev_kw), len(evt_kw))
        else:
            kw_overlap = 0.0
        kw_score = 0.15 * kw_overlap

        # Domain overlap
        if ev_dom and evt_dom:
            dom_overlap = len(ev_dom & evt_dom) / min(len(ev_dom), len(evt_dom))
        else:
            dom_overlap = 0.0
        dom_score = 0.05 * dom_overlap

        raw = top_score + conv_score + kw_score + dom_score

        # General channel: block weak merges that lack a strong anchor
        if evidence.channel == "general":
            has_anchor = top_match or conv_overlap >= 0.5
            if not has_anchor and kw_overlap < 0.3:
                return 0.0

        return round(raw, 4)
