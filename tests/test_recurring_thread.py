import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.events.candidate_builder import CandidateBuilder

_builder = CandidateBuilder()


def _row(title: str, channel: str = "general") -> dict:
    return {
        "channel": channel, "window_start": 0, "window_end": 1,
        "z_score": 5.0, "count": 100,
        "clusters": [{
            "cluster_id": 0, "top_story_title": title, "top_story_id": 1,
            "size": 20, "unique_story_count": 5, "top_story_pct": 0.2,
            "top_domains": ["news.ycombinator.com", "github.com"],
            "keywords": ["hiring", "jobs", "remote"],
            "story_ids": list(range(5)),
        }],
    }


def test_who_is_hiring():
    cands = _builder.from_enriched_row(_row("Ask HN: Who is Hiring? (June 2026)"))
    assert len(cands) == 1
    assert cands[0].kind == "recurring_thread"
    assert cands[0].event_score == 0.0


def test_who_wants_to_be_hired():
    cands = _builder.from_enriched_row(_row("Ask HN: Who wants to be hired? (June 2026)"))
    assert len(cands) == 1
    assert cands[0].kind == "recurring_thread"
    assert cands[0].event_score == 0.0


def test_what_are_you_working_on():
    cands = _builder.from_enriched_row(_row("Ask HN: What are you working on? (June 2026)"))
    assert len(cands) == 1
    assert cands[0].kind == "recurring_thread"
    assert cands[0].event_score == 0.0


def test_ask_hn_who_generic_not_recurring():
    # "ask hn: who" removed from markers — generic "who" questions are not recurring megathreads
    cands = _builder.from_enriched_row(_row("Ask HN: Who has experience with Rust in production?"))
    assert len(cands) == 1
    assert cands[0].kind != "recurring_thread"


def test_normal_story_not_recurring():
    cands = _builder.from_enriched_row(_row("Apple announces WWDC 2026", channel="tech"))
    assert len(cands) == 1
    assert cands[0].kind != "recurring_thread"


if __name__ == "__main__":
    tests = [
        test_who_is_hiring,
        test_who_wants_to_be_hired,
        test_what_are_you_working_on,
        test_ask_hn_who_generic_not_recurring,
        test_normal_story_not_recurring,
    ]
    for t in tests:
        t()
        print(f"  PASS  {t.__name__}")
    print("All tests passed.")
