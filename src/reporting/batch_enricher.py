"""
Laptop-side batch NLP enricher.

Usage:
    python -m src.reporting.batch_enricher

Downloads raw Parquet files from GCS (parquet/ prefix), skips already-processed
files (tracked in data/parquet_enriched/.processed), runs DBSCANContextEngine
on each row's texts, and writes enriched Parquet to data/parquet_enriched/.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage

from ..pipeline.context import DBSCANContextEngine

_ROOT           = Path(__file__).resolve().parent.parent.parent
_GCS_BUCKET     = "hn-surge-dashboard-01"
_GCS_PROJECT    = "project-8299dfb6-57e5-4dcf-bc0"
_GCS_PREFIX     = "parquet/"
_LOCAL_ENRICHED = _ROOT / "data" / "parquet_enriched"
_DONE_FILE      = _LOCAL_ENRICHED / ".processed"


def _load_done() -> set[str]:
    if _DONE_FILE.exists():
        return set(_DONE_FILE.read_text().splitlines())
    return set()


def _mark_done(blob_name: str) -> None:
    _DONE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with _DONE_FILE.open("a") as f:
        f.write(blob_name + "\n")


def run() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    _LOCAL_ENRICHED.mkdir(parents=True, exist_ok=True)

    client = storage.Client(project=_GCS_PROJECT)
    bucket = client.bucket(_GCS_BUCKET)
    nlp    = DBSCANContextEngine()
    done   = _load_done()

    blobs = [
        b for b in bucket.list_blobs(prefix=_GCS_PREFIX)
        if b.name.endswith(".parquet") and b.name not in done
    ]

    print(f"Found {len(blobs)} new Parquet file(s) to enrich.")

    for blob in blobs:
        local_tmp = _LOCAL_ENRICHED / "tmp_download.parquet"
        blob.download_to_filename(str(local_tmp))

        try:
            table = pq.read_table(str(local_tmp))
        except Exception as exc:
            print(f"  [skip] {blob.name} — corrupt file: {exc}", file=sys.stderr)
            local_tmp.unlink(missing_ok=True)
            _mark_done(blob.name)
            continue
        enriched_rows = []

        for i in range(table.num_rows):
            row      = {col: table.column(col)[i].as_py() for col in table.schema.names}
            texts    = row.get("texts") or []
            clusters = nlp.summarize_anomaly(texts, window_start=row["window_start"]) if texts else []
            row["keywords"] = [kw for c in clusters for kw in c["keywords"]]
            enriched_rows.append(row)

        rel_parts = Path(blob.name).parts[1:]  # strip leading "parquet/" segment
        out_path  = _LOCAL_ENRICHED.joinpath(*rel_parts)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        enriched_table = pa.table(
            {col: [r[col] for r in enriched_rows] for col in table.schema.names},
            schema=table.schema,
        )
        pq.write_table(enriched_table, str(out_path), compression="snappy")
        print(f"  Enriched {blob.name} → {out_path.name}")

        local_tmp.unlink(missing_ok=True)
        _mark_done(blob.name)

    print("Done.")


if __name__ == "__main__":
    run()
