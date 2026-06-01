from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Union

import pyarrow as pa
import pyarrow.parquet as pq

from ..events.models import EventCandidate

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


class CandidateArchiver:
    """Writes EventCandidates to date-partitioned local Parquet files.

    File layout:
        {out_dir}/{YYYY}/{MM}/{DD}/candidates_{channel}_{window_end}.parquet

    Write is atomic: .parquet.tmp is written first, then renamed to .parquet.
    No GCS upload — candidates are local-only until the dashboard consumes them.
    """

    def __init__(self, out_dir: Union[str, Path]) -> None:
        self._out_dir = Path(out_dir)

    def write(self, candidates: List[EventCandidate], window_end: int) -> Optional[Path]:
        """Persist a batch of candidates for one enriched row.

        Returns the output path, or None if candidates is empty.
        All candidates in the batch must share the same channel and window_end.
        """
        if not candidates:
            return None

        dt       = datetime.fromtimestamp(window_end, tz=timezone.utc)
        channel  = candidates[0].channel
        filename = f"candidates_{channel}_{window_end}.parquet"
        path     = self._out_dir / f"{dt:%Y/%m/%d}" / filename
        tmp_path = path.with_suffix(".parquet.tmp")
        path.parent.mkdir(parents=True, exist_ok=True)

        table = pa.table(
            {
                "candidate_id":              [c.candidate_id              for c in candidates],
                "source":                    [c.source                    for c in candidates],
                "channel":                   [c.channel                   for c in candidates],
                "window_end":                [c.window_end                for c in candidates],
                "cluster_id":                [c.cluster_id                for c in candidates],
                "kind":                      [c.kind                      for c in candidates],
                "event_score":               [c.event_score               for c in candidates],
                "size":                      [c.size                      for c in candidates],
                "unique_conversation_count": [c.unique_conversation_count for c in candidates],
                "top_conversation_pct":      [c.top_conversation_pct      for c in candidates],
                "top_story_id":              [c.top_story_id              for c in candidates],
                "top_story_title":           [c.top_story_title           for c in candidates],
                "top_domains":               [c.top_domains               for c in candidates],
                "keywords":                  [c.keywords                  for c in candidates],
                "z_score":                   [c.z_score                   for c in candidates],
                "window_count":              [c.window_count              for c in candidates],
            },
            schema=_CANDIDATE_SCHEMA,
        )

        pq.write_table(table, str(tmp_path), compression="snappy")
        tmp_path.replace(path)
        return path
