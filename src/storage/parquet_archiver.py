from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Union

import pyarrow as pa
import pyarrow.parquet as pq

from ..models import AnomalyEvent

_GCS_BUCKET   = os.environ.get("GCS_BUCKET",   "hn-surge-dashboard-01")
_GCS_PROJECT  = os.environ.get("GCS_PROJECT",  "project-8299dfb6-57e5-4dcf-bc0")
_GCS_PREFIX   = "parquet"

_CLUSTER_STRUCT = pa.struct([
    pa.field("cluster_id",         pa.int64()),
    pa.field("size",               pa.int64()),
    pa.field("noise_count",        pa.int64()),
    pa.field("keywords",           pa.list_(pa.string())),
    pa.field("top_story_id",       pa.int64()),
    pa.field("top_story_title",    pa.string()),
    pa.field("top_story_pct",      pa.float64()),
    pa.field("unique_story_count", pa.int64()),
    pa.field("story_ids",          pa.list_(pa.int64())),
    pa.field("top_domains",        pa.list_(pa.string())),
])

_ITEM_STRUCT = pa.struct([
    pa.field("item_id",    pa.int64()),
    pa.field("text",       pa.string()),
    pa.field("story_id",   pa.int64()),
    pa.field("story_title", pa.string()),
    pa.field("domain",     pa.string()),
    pa.field("item_type",  pa.string()),
    pa.field("created_at", pa.int64()),
])

_SCHEMA = pa.schema([
    pa.field("channel",       pa.string()),
    pa.field("window_start",  pa.int64()),
    pa.field("window_end",    pa.int64()),
    pa.field("window_end_dt", pa.string()),
    pa.field("count",         pa.int64()),
    pa.field("z_score",       pa.float64()),
    pa.field("mean",          pa.float64()),
    pa.field("std",           pa.float64()),
    pa.field("texts",         pa.list_(pa.string())),
    pa.field("items",         pa.list_(_ITEM_STRUCT)),
    pa.field("clusters",      pa.list_(_CLUSTER_STRUCT)),
])


class ParquetArchiver:
    """Writes each AnomalyEvent as its own complete Parquet file.

    File layout:
        {out_dir}/{YYYY}/{MM}/{DD}/{channel}_{window_end}.parquet

    Write is atomic: data goes to a .parquet.tmp file first, then renamed
    to .parquet on success. retry_pending() only scans *.parquet so a crash
    during write never leaves a corrupt file eligible for upload.
    """

    def __init__(self, out_dir: Union[str, Path]) -> None:
        self._out_dir = Path(out_dir)

    def archive(self, event: AnomalyEvent) -> None:
        dt       = datetime.fromtimestamp(event.window_end, tz=timezone.utc)
        date_str = dt.strftime("%Y-%m-%d %H:%M UTC")

        item_rows = event.items or []

        table = pa.table(
            {
                "channel":       [event.subreddit],
                "window_start":  [event.window_start],
                "window_end":    [event.window_end],
                "window_end_dt": [date_str],
                "count":         [event.count],
                "z_score":       [event.z_score],
                "mean":          [event.mean],
                "std":           [event.std],
                "texts":         [[i["text"] for i in item_rows]],
                "items":         pa.array([item_rows], type=pa.list_(_ITEM_STRUCT)),
                "clusters":      pa.array([[]], type=pa.list_(_CLUSTER_STRUCT)),
            },
            schema=_SCHEMA,
        )

        filename = f"{event.subreddit}_{event.window_end}.parquet"
        path     = self._out_dir / f"{dt:%Y/%m/%d}" / filename
        tmp_path = path.with_suffix(".parquet.tmp")
        path.parent.mkdir(parents=True, exist_ok=True)

        pq.write_table(table, str(tmp_path), compression="snappy")
        tmp_path.replace(path)
        self._upload_to_gcs(path)

    def _upload_to_gcs(self, local_path: Path) -> None:
        rel_path  = local_path.relative_to(self._out_dir)
        blob_name = f"{_GCS_PREFIX}/{rel_path.as_posix()}"

        try:
            from google.cloud import storage
            client = storage.Client(project=_GCS_PROJECT)
            blob   = client.bucket(_GCS_BUCKET).blob(blob_name)
            blob.upload_from_filename(str(local_path))
            print(f"  [archiver] → gs://{_GCS_BUCKET}/{blob_name}")
        except Exception as exc:
            print(f"  [archiver] GCS upload failed: {exc}", file=sys.stderr)
            return

        try:
            local_path.unlink()
        except Exception as exc:
            print(f"  [archiver] uploaded but failed to delete {local_path}: {exc}", file=sys.stderr)

    def retry_pending(self) -> None:
        leftover = list(self._out_dir.rglob("*.parquet"))
        if not leftover:
            return
        print(f"  [archiver] retrying {len(leftover)} pending upload(s)...")
        for path in leftover:
            self._upload_to_gcs(path)

    def close(self) -> None:
        pass

    def __enter__(self) -> "ParquetArchiver":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
