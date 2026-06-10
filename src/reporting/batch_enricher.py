"""
Laptop-side batch NLP enricher.

Usage:
    python -m src.reporting.batch_enricher

Downloads raw Parquet files from GCS (parquet/ prefix), skips already-processed
files (tracked in data/parquet_enriched/.processed), runs DBSCANContextEngine
on each row's texts, and writes enriched Parquet to data/parquet_enriched/.
"""
from __future__ import annotations

import os
import sys
from collections import Counter
from pathlib import Path
from typing import Dict, List

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage

from ..events.candidate_builder import CandidateBuilder
from ..events.models import EventCandidate
from ..pipeline.context import DBSCANContextEngine
from ..storage.candidate_archiver import CandidateArchiver
from ..storage.parquet_archiver import _SCHEMA, _CLUSTER_STRUCT, _ITEM_STRUCT

_ROOT             = Path(__file__).resolve().parent.parent.parent
_GCS_BUCKET       = os.environ.get("GCS_BUCKET",  "hn-surge-dashboard-01")
_GCS_PROJECT      = os.environ.get("GCS_PROJECT", "project-8299dfb6-57e5-4dcf-bc0")
_GCS_PREFIX       = "parquet/"
_LOCAL_ENRICHED   = _ROOT / "data" / "parquet_enriched"
_LOCAL_CANDIDATES = _ROOT / "data" / "event_candidates"
_DONE_FILE        = _LOCAL_ENRICHED / ".processed"

_OUTPUT_COLS = [f.name for f in _SCHEMA]
_MIN_TEXTS_FOR_CLUSTERING = 50


def _load_done() -> set[str]:
    if _DONE_FILE.exists():
        return set(_DONE_FILE.read_text().splitlines())
    return set()


def _mark_done(blob_name: str) -> None:
    _DONE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with _DONE_FILE.open("a") as f:
        f.write(blob_name + "\n")


def _normalise_row(row: dict) -> dict:
    """Map old-schema rows to current schema, filling missing columns with defaults."""
    if "keywords" in row:
        del row["keywords"]
    row.setdefault("clusters", [])
    row.setdefault("items",    [])
    # Back-fill new item fields for old files that predate the schema extension
    normalised_items = []
    for item in (row["items"] or []):
        if item is None:
            normalised_items.append(None)
            continue
        item.setdefault("item_id",    0)
        item.setdefault("created_at", 0)
        normalised_items.append(item)
    row["items"] = normalised_items
    return row


def run() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    _LOCAL_ENRICHED.mkdir(parents=True, exist_ok=True)

    client     = storage.Client(project=_GCS_PROJECT)
    bucket     = client.bucket(_GCS_BUCKET)
    nlp        = DBSCANContextEngine()
    builder    = CandidateBuilder(
        source="hacker_news",
        dbscan_eps=nlp.dbscan_eps,
        dbscan_min_samples=nlp.dbscan_min_samples,
    )
    c_archiver = CandidateArchiver(_LOCAL_CANDIDATES)
    done       = _load_done()

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

        enriched_rows:   list = []
        blob_candidates: List[EventCandidate] = []

        for i in range(table.num_rows):
            row   = {col: table.column(col)[i].as_py() for col in table.schema.names}
            row   = _normalise_row(row)
            texts = row.get("texts") or []
            items = row.get("items") or []

            channel = row.get("channel", "")
            raw_clusters = (
                nlp.summarize_anomaly(texts, window_start=row["window_start"], channel=channel)
                if len(texts) >= _MIN_TEXTS_FOR_CLUSTERING
                else []
            )

            row["clusters"] = []
            for c in raw_clusters:
                indices = c.get("text_indices", [])
                valid   = [idx for idx in indices if idx < len(items)]
                known   = [idx for idx in valid if (items[idx] or {}).get("story_id", 0)]

                if known:
                    story_counts   = Counter((items[idx] or {}).get("story_id", 0) for idx in known)
                    top_sid, top_n = story_counts.most_common(1)[0]
                    top_pct        = top_n / len(known)
                    top_title      = next(
                        ((items[idx] or {}).get("story_title", "") for idx in known
                         if (items[idx] or {}).get("story_id") == top_sid),
                        "",
                    )
                    unique_story_ids  = sorted({(items[idx] or {}).get("story_id", 0) for idx in known} - {0})
                    domain_counts     = Counter((items[idx] or {}).get("domain", "") for idx in valid if (items[idx] or {}).get("domain", ""))
                    top_domains       = [d for d, _ in domain_counts.most_common(5)]
                else:
                    top_sid, top_title, top_pct = 0, "", 0.0
                    unique_story_ids = []
                    top_domains      = []

                row["clusters"].append({
                    "cluster_id":         c["cluster_id"],
                    "size":               c["size"],
                    "noise_count":        c["noise_count"],
                    "keywords":           c["keywords"],
                    "top_story_id":       top_sid,
                    "top_story_title":    top_title,
                    "top_story_pct":      round(top_pct, 3),
                    "unique_story_count": len(unique_story_ids),
                    "story_ids":          unique_story_ids,
                    "top_domains":        top_domains,
                })

            enriched_rows.append(row)
            actual_eps = nlp.eps_for_channel(channel)
            blob_candidates.extend(builder.from_enriched_row(row, dbscan_eps=actual_eps))

        groups: Dict[tuple, List[EventCandidate]] = {}
        for cand in blob_candidates:
            groups.setdefault((cand.channel, cand.window_end), []).append(cand)
        for (ch, we), group in groups.items():
            c_archiver.write(group, we)
            kind_summary = Counter(cand.kind for cand in group)
            print(f"    → {len(group)} candidate(s) [{ch}]: {dict(kind_summary)}")

        rel_parts = Path(blob.name).parts[1:]
        out_path  = _LOCAL_ENRICHED.joinpath(*rel_parts)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        cols = {}
        for col in _OUTPUT_COLS:
            if col == "items":
                cols[col] = pa.array([r["items"] for r in enriched_rows], type=pa.list_(_ITEM_STRUCT))
            elif col == "clusters":
                cols[col] = pa.array([r["clusters"] for r in enriched_rows], type=pa.list_(_CLUSTER_STRUCT))
            else:
                cols[col] = [r[col] for r in enriched_rows]
        enriched_table = pa.table(cols, schema=_SCHEMA)
        pq.write_table(enriched_table, str(out_path), compression="snappy")
        print(f"  Enriched {blob.name} → {out_path.name}")

        local_tmp.unlink(missing_ok=True)
        _mark_done(blob.name)

    print("Done.")


if __name__ == "__main__":
    run()
