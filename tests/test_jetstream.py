"""
Bluesky Jetstream connectivity and volume test.

Usage:
    python tests/test_jetstream.py                     # print 20 posts (all languages)
    python tests/test_jetstream.py --en-only           # print 20 English posts
    python tests/test_jetstream.py --volume            # count all vs English for 30s
    python tests/test_jetstream.py --limit 5 --raw     # dump full JSON of 5 posts
    python tests/test_jetstream.py --show-did          # include author DID prefix
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from datetime import datetime, timezone

JETSTREAM_URL = (
    "wss://jetstream2.us-east.bsky.network/subscribe"
    "?wantedCollections=app.bsky.feed.post"
)


def _fmt_ts(time_us: int) -> str:
    dt = datetime.fromtimestamp(time_us / 1_000_000, tz=timezone.utc)
    return dt.strftime("%H:%M:%S UTC")


async def _run(limit: int, show_did: bool, raw_mode: bool, en_only: bool, volume: bool) -> None:
    import websockets

    if volume:
        print("Volume mode — counting for 30 seconds (all posts vs English only).\n")
        total = en = 0
        start = time.monotonic()
        deadline = start + 30
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
        elapsed = time.monotonic() - start
        print(f"  Duration     : {elapsed:.1f}s")
        print(f"  All posts    : {total:>6}  ({total/elapsed:.1f}/s)")
        print(f"  English only : {en:>6}  ({en/elapsed:.1f}/s)  ({100*en//max(total,1)}% of total)")
        return

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
                ts = _fmt_ts(msg["time_us"])
                did = msg["did"]
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
    parser.add_argument("--limit",    type=int, default=20, help="posts to print before exiting")
    parser.add_argument("--show-did", action="store_true",  help="print author DID prefix")
    parser.add_argument("--raw",      action="store_true",  help="dump full JSON of each message")
    parser.add_argument("--en-only",  action="store_true",  dest="en_only", help="English posts only")
    parser.add_argument("--volume",   action="store_true",  help="count all vs English posts for 30s")
    args = parser.parse_args()

    try:
        asyncio.run(_run(args.limit, args.show_did, args.raw, args.en_only, args.volume))
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(0)


if __name__ == "__main__":
    main()
