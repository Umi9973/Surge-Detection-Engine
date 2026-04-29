from __future__ import annotations

from typing import Dict, Iterator, List


class SubredditFilter:
    def __init__(self, target_subreddits: List[str]) -> None:
        self._targets = {s.lower() for s in target_subreddits}

    def stream(self, source: Iterator[Dict]) -> Iterator[Dict]:
        for comment in source:
            if comment["subreddit"].lower() in self._targets:
                yield comment


if __name__ == "__main__":
    import os
    import sys

    sys.stdout.reconfigure(encoding="utf-8")

    from ingestion import ZstFileIngestor

    TARGET_SUBREDDITS = [
        # Gaming Platforms & Hubs
        "gaming", "Games", "pcgaming", "PS5", "XboxSeriesX", "NintendoSwitch",
        # Pop Culture & Entertainment Hubs
        "movies", "television", "entertainment", "popculturechat", "Music",
        # Macro News Hubs
        "news", "worldnews",
    ]
    DATA_FILE = os.path.join(os.path.dirname(__file__), "RC_2023-12.zst")

    ingestor = ZstFileIngestor(DATA_FILE)
    filter_ = SubredditFilter(TARGET_SUBREDDITS)

    found = 0
    for comment in filter_.stream(ingestor.stream()):
        print(comment)
        found += 1
        if found == 3:
            break

    print(f"\nFound {found} comments from target subreddits. Stream safely closed.")
