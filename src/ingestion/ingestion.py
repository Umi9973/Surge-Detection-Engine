from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import Dict, Iterator

import zstandard as zstd


class DataIngestor(ABC):
    @abstractmethod
    def stream(self) -> Iterator[Dict]:
        raise NotImplementedError


class ZstFileIngestor(DataIngestor):
    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def stream(self) -> Iterator[Dict]:
        with open(self.file_path, "rb") as fh:
            dctx = zstd.ZstdDecompressor(max_window_size=2**31)
            with dctx.stream_reader(fh) as reader:
                buffer = b""
                while True:
                    chunk = reader.read(65536)
                    if not chunk:
                        break
                    buffer += chunk
                    lines = buffer.split(b"\n")
                    buffer = lines.pop()
                    for line in lines:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            raw: Dict = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        yield {
                            "id":        raw.get("id", ""),
                            "timestamp": raw.get("created_utc", 0),
                            "subreddit": raw.get("subreddit", ""),
                            "body":      raw.get("body", ""),
                            "score":     raw.get("score", 0),
                            "author":    raw.get("author", ""),
                            "parent_id": raw.get("parent_id", ""),
                            "link_id":   raw.get("link_id", ""),
                            "permalink": raw.get("permalink", ""),
                        }


if __name__ == "__main__":
    import itertools
    import sys
    from pathlib import Path

    sys.stdout.reconfigure(encoding="utf-8")
    DATA_FILE = str(Path(__file__).resolve().parent.parent.parent / "data" / "raw_dumps" / "RC_2023-12.zst")
    ingestor = ZstFileIngestor(DATA_FILE)

    for comment in itertools.islice(ingestor.stream(), 5):
        print(comment)

    print("\nStream safely closed.")
