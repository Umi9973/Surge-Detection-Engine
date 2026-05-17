from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Union

import pyarrow as pa
import pyarrow.parquet as pq

from ..models import AnomalyEvent

_GCS_BUCKET   = "hn-surge-dashboard-01"
_GCS_PROJECT  = "project-8299dfb6-57e5-4dcf-bc0"
_GCS_PREFIX   = "parquet"

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
    pa.field("keywords",      pa.list_(pa.string())),
])


class ParquetArchiver:
    """Writes AnomalyEvents to date-partitioned, snappy-compressed Parquet files.

    File layout:
        {out_dir}/{YYYY}/{MM}/{DD}/anomalies_{run_ts}.parquet

    One file per archiver instance. The run timestamp in the filename
    prevents overwrite when the process restarts on the same day.

    Usage:
        with ParquetArchiver(root / "data" / "parquet") as archiver:
            archiver.archive(ev, flat_keywords)
    """

    def __init__(self, out_dir: Union[str, Path]) -> None:
        self._out_dir    = Path(out_dir)
        self._writer:     Optional[pq.ParquetWriter] = None
        self._local_path: Optional[Path] = None

    def _ensure_writer(self, window_end: int) -> pq.ParquetWriter:
        """Lazy-open the writer on the first archive() call."""
        if self._writer is None:
            run_ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            date   = datetime.fromtimestamp(window_end, tz=timezone.utc)
            path   = self._out_dir / f"{date:%Y/%m/%d}/anomalies_{run_ts}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            self._local_path = path
            self._writer = pq.ParquetWriter(str(path), _SCHEMA, compression="snappy")
        return self._writer

    def archive(
        self,
        event: AnomalyEvent,
        keywords: Optional[List[str]] = None,
    ) -> None:
        """Append one AnomalyEvent row to the Parquet file."""
        writer = self._ensure_writer(event.window_end)
        dt = datetime.fromtimestamp(event.window_end, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M UTC"
        )
        table = pa.table(
            {
                "channel":       [event.subreddit],
                "window_start":  [event.window_start],
                "window_end":    [event.window_end],
                "window_end_dt": [dt],
                "count":         [event.count],
                "z_score":       [event.z_score],
                "mean":          [event.mean],
                "std":           [event.std],
                "texts":         [event.texts or []],
                "keywords":      [keywords or []],
            },
            schema=_SCHEMA,
        )
        writer.write_table(table)

    def _upload_to_gcs(self, local_path: Path) -> None:
        import sys
        try:
            from google.cloud import storage
            client    = storage.Client(project=_GCS_PROJECT)
            blob_name = _GCS_PREFIX + "/" + "/".join(local_path.parts[-4:])
            blob      = client.bucket(_GCS_BUCKET).blob(blob_name)
            blob.upload_from_filename(str(local_path))
            print(f"  [archiver] → gs://{_GCS_BUCKET}/{blob_name}")
        except Exception as exc:
            print(f"  [archiver] GCS upload failed: {exc}", file=sys.stderr)

    def close(self) -> None:
        if self._writer:
            self._writer.close()
            self._writer = None
        if self._local_path:
            self._upload_to_gcs(self._local_path)
            self._local_path = None

    def __enter__(self) -> "ParquetArchiver":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
