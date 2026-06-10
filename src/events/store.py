from __future__ import annotations

import json
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Union

from .models import TrackedEvent


class EventStore:
    """Read/write TrackedEvent objects as JSONL files under out_dir.

    File naming: {out_dir}/{date_from}_{date_to}.jsonl
    Re-running the same date range overwrites the existing file — no duplicates
    across repeated runs of the same range.

    read_all() does NOT deduplicate across overlapping consolidation ranges.
    Avoid running overlapping ranges, or deduplicate by event_id in the caller.
    """

    def __init__(self, out_dir: Union[str, Path]) -> None:
        self._out_dir = Path(out_dir)

    def write(self, events: List[TrackedEvent], date_from: str, date_to: str) -> Path:
        self._out_dir.mkdir(parents=True, exist_ok=True)
        path     = self._out_dir / f"{date_from}_{date_to}.jsonl"
        tmp_path = path.with_suffix(".jsonl.tmp")

        with tmp_path.open("w", encoding="utf-8") as f:
            for ev in events:
                f.write(json.dumps(asdict(ev), ensure_ascii=False) + "\n")

        tmp_path.replace(path)
        return path

    def read_all(self, days: int = 7) -> List[TrackedEvent]:
        cutoff = datetime.now(tz=timezone.utc).timestamp() - days * 86400
        events: List[TrackedEvent] = []

        for path in sorted(self._out_dir.glob("*.jsonl")):
            try:
                with path.open("r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        data = json.loads(line)
                        ev = TrackedEvent(**data)
                        if ev.last_seen >= cutoff:
                            events.append(ev)
            except Exception as exc:
                print(f"[EventStore] skipping {path.name}: {exc}", file=sys.stderr)

        events.sort(key=lambda e: e.first_seen)
        return events
