from __future__ import annotations

from typing import Dict, Iterator, List

# Bot accounts whose comments are never topically relevant
_BOT_AUTHORS: frozenset = frozenset({
    "AutoModerator", "reddit", "BotDefense", "RepostSleuthBot",
    "RemindMeBot", "sneakpeek_bot", "reddit-stream",
})

# Boilerplate phrases that identify automated/bot content regardless of author name
_BOILERPLATE_PHRASES: tuple = (
    "i am a bot, and this action was performed automatically",
    "your submission has been removed",
    "please contact the moderators of this subreddit",
    "this is a reminder to",
    "^i ^am ^a ^bot",
)


class SubredditFilter:
    def __init__(self, target_subreddits: List[str]) -> None:
        self._targets = {s.lower() for s in target_subreddits}

    @staticmethod
    def _is_bot(comment: Dict) -> bool:
        if comment.get("author") in _BOT_AUTHORS:
            return True
        body_lower = comment.get("body", "").lower()
        return any(phrase in body_lower for phrase in _BOILERPLATE_PHRASES)

    def stream(self, source: Iterator[Dict]) -> Iterator[Dict]:
        for comment in source:
            if comment["subreddit"].lower() in self._targets and not self._is_bot(comment):
                yield comment


if __name__ == "__main__":
    import sys
    from pathlib import Path

    sys.stdout.reconfigure(encoding="utf-8")
    _root = Path(__file__).resolve().parent.parent.parent
    sys.path.insert(0, str(_root))

    from src.ingestion.ingestion import ZstFileIngestor

    TARGET_SUBREDDITS = [
        # Gaming Platforms & Hubs
        "gaming", "Games", "pcgaming", "PS5", "XboxSeriesX", "NintendoSwitch",
        # Pop Culture & Entertainment Hubs
        "movies", "television", "entertainment", "popculturechat", "Music",
        # Macro News Hubs
        "news", "worldnews",
    ]
    DATA_FILE = str(_root / "data" / "raw_dumps" / "RC_2023-12.zst")

    ingestor = ZstFileIngestor(DATA_FILE)
    filter_ = SubredditFilter(TARGET_SUBREDDITS)

    found = 0
    for comment in filter_.stream(ingestor.stream()):
        print(comment)
        found += 1
        if found == 3:
            break

    print(f"\nFound {found} comments from target subreddits. Stream safely closed.")
