"""
Bluesky Jetstream connectivity and volume test.

Usage:
    python tests/test_jetstream.py                     # print 20 posts (all languages)
    python tests/test_jetstream.py --en-only           # print 20 English posts
    python tests/test_jetstream.py --volume            # count all vs English for 30s
    python tests/test_jetstream.py --volume --secs 60  # run for 60s
    python tests/test_jetstream.py --channel-stats     # per-channel breakdown for 60s
    python tests/test_jetstream.py --limit 5 --raw     # dump full JSON of 5 posts
    python tests/test_jetstream.py --show-did          # include author DID prefix
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone

JETSTREAM_URL = (
    "wss://jetstream2.us-east.bsky.network/subscribe"
    "?wantedCollections=app.bsky.feed.post"
)


def _fmt_ts(time_us: int) -> str:
    dt = datetime.fromtimestamp(time_us / 1_000_000, tz=timezone.utc)
    return dt.strftime("%H:%M:%S UTC")


async def _run_volume(secs: int) -> None:
    import websockets
    print(f"Volume mode — counting for {secs}s (all posts vs English only).\n")
    total = en = 0
    start = time.monotonic()
    deadline = start + secs
    while time.monotonic() < deadline:
        try:
            async with websockets.connect(JETSTREAM_URL) as ws:
                async for raw in ws:
                    if time.monotonic() >= deadline:
                        break
                    msg = json.loads(raw)
                    if msg.get("kind") != "commit":
                        continue
                    commit = msg.get("commit", {})
                    if commit.get("collection") != "app.bsky.feed.post":
                        continue
                    if commit.get("operation") != "create":
                        continue
                    record = commit.get("record", {})
                    if not (record.get("text") or "").strip():
                        continue
                    total += 1
                    if "en" in (record.get("langs") or []):
                        en += 1
        except Exception as exc:
            if time.monotonic() < deadline:
                print(f"  [reconnecting after: {exc}]", flush=True)
            break
    elapsed = time.monotonic() - start
    print(f"  Duration     : {elapsed:.1f}s")
    print(f"  All posts    : {total:>6}  ({total/elapsed:.1f}/s  ~{int(total/elapsed*60)}/min)")
    print(f"  English only : {en:>6}  ({en/elapsed:.1f}/s  ~{int(en/elapsed*60)}/min)  ({100*en//max(total,1)}% of total)")


async def _run_channel_stats(secs: int) -> None:
    """
    Measure per-minute post rates after applying the BlueskyIngestor filters:
    English-only, then routed through BlueskyTopicRouter.
    Prints total/min, topical/min, drop rate, and per-channel breakdown.
    """
    import websockets
    from src.ingestion.bluesky import BlueskyTopicRouter, _normalize_post

    router  = BlueskyTopicRouter()
    channel_counts: Counter = Counter()
    minute_buckets: dict    = defaultdict(Counter)   # minute_index → channel → count

    total = en_total = topical = dropped_general = 0
    start    = time.monotonic()
    deadline = start + secs

    print(f"Channel stats mode — running for {secs}s with BlueskyTopicRouter.\n")

    while time.monotonic() < deadline:
        try:
            async with websockets.connect(JETSTREAM_URL) as ws:
                async for raw in ws:
                    now = time.monotonic()
                    if now >= deadline:
                        break

                    msg = json.loads(raw)
                    if msg.get("kind") != "commit":
                        continue
                    commit = msg.get("commit", {})
                    if commit.get("collection") != "app.bsky.feed.post":
                        continue
                    if commit.get("operation") != "create":
                        continue

                    record = commit.get("record", {})
                    if not (record.get("text") or "").strip():
                        continue
                    total += 1

                    if "en" not in (record.get("langs") or []):
                        continue
                    en_total += 1

                    item = _normalize_post(msg, router)
                    if item is None:
                        continue

                    ch  = item["subreddit"]
                    min_idx = int((now - start) // 60)
                    channel_counts[ch] += 1
                    minute_buckets[min_idx][ch] += 1

                    if ch == "general":
                        dropped_general += 1
                    else:
                        topical += 1
        except Exception as exc:
            if time.monotonic() < deadline:
                print(f"  [reconnecting after: {exc}]", flush=True)
            break

    elapsed = time.monotonic() - start
    per_min = 60 / elapsed   # scale factor to convert counts → per-minute rate

    div = "─" * 60
    print(div)
    print(f"  Duration           : {elapsed:.1f}s")
    print(f"  All posts          : {total:>5}  ({total*per_min:.0f}/min)")
    print(f"  English posts      : {en_total:>5}  ({en_total*per_min:.0f}/min)")
    print(f"  Topical (emitted)  : {topical:>5}  ({topical*per_min:.0f}/min)  "
          f"({100*topical//max(en_total,1)}% of English)")
    print(f"  Dropped (general)  : {dropped_general:>5}  ({dropped_general*per_min:.0f}/min)  "
          f"({100*dropped_general//max(en_total,1)}% of English)")
    print()
    print(f"  Per-channel breakdown (topical only):")

    topical_channels = {ch: n for ch, n in channel_counts.items() if ch != "general"}
    for ch, n in sorted(topical_channels.items(), key=lambda x: -x[1]):
        rate = n * per_min
        bar  = "█" * min(int(rate), 40)
        pct  = 100 * n // max(topical, 1)
        print(f"    {ch:<22} {rate:>5.1f}/min  {pct:>2}%  {bar}")

    # Per-minute breakdown if run was long enough
    if len(minute_buckets) >= 2:
        print()
        print(f"  Per-minute totals (topical):")
        for m, counts in sorted(minute_buckets.items()):
            m_topical = sum(v for ch, v in counts.items() if ch != "general")
            top_ch    = max((ch for ch in counts if ch != "general"),
                           key=lambda ch: counts[ch], default="—")
            print(f"    min {m+1:>2}: {m_topical:>4} posts   top channel: {top_ch}")

    print(div)


async def _run_stream(limit: int, show_did: bool, raw_mode: bool, en_only: bool) -> None:
    import websockets
    print(f"Connecting to Jetstream — will print {limit} {'English ' if en_only else ''}posts then exit.\n")

    count = 0
    async with websockets.connect(JETSTREAM_URL) as ws:
        async for raw in ws:
            msg = json.loads(raw)
            if msg.get("kind") != "commit":
                continue
            commit = msg.get("commit", {})
            if commit.get("collection") != "app.bsky.feed.post":
                continue
            if commit.get("operation") != "create":
                continue

            record = commit.get("record", {})
            text = (record.get("text") or "").strip()
            if not text:
                continue

            if en_only and "en" not in (record.get("langs") or []):
                continue

            if raw_mode:
                print(json.dumps(msg, indent=2, ensure_ascii=False))
                print("─" * 60)
            else:
                ts    = _fmt_ts(msg["time_us"])
                did   = msg["did"]
                langs = ",".join(record.get("langs") or ["-"])
                print(f"[{ts}] [{langs:<5}]", end="")
                if show_did:
                    print(f"  {did[:20]}…", end="")
                print(f"  {text[:120]}")

            count += 1
            if count >= limit:
                break

    print(f"\n{count} post(s) received. Connection OK.")


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(description="Bluesky Jetstream connection test")
    parser.add_argument("--limit",         type=int, default=20,  help="posts to print (stream mode)")
    parser.add_argument("--secs",          type=int, default=60,  help="duration for --volume / --channel-stats")
    parser.add_argument("--show-did",      action="store_true",   help="print author DID prefix")
    parser.add_argument("--raw",           action="store_true",   help="dump full JSON of each message")
    parser.add_argument("--en-only",       action="store_true",   dest="en_only", help="English posts only")
    parser.add_argument("--volume",        action="store_true",   help="count all vs English for --secs seconds")
    parser.add_argument("--channel-stats", action="store_true",   dest="channel_stats",
                        help="per-channel breakdown with router applied for --secs seconds")
    args = parser.parse_args()

    try:
        if args.channel_stats:
            asyncio.run(_run_channel_stats(args.secs))
        elif args.volume:
            asyncio.run(_run_volume(args.secs))
        else:
            asyncio.run(_run_stream(args.limit, args.show_did, args.raw, args.en_only))
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(0)


if __name__ == "__main__":
    main()
