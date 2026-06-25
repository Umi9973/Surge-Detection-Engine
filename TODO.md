Mission 1 — Plan Bluesky schema mapping
Inspect current ingestor contract, raw anomaly schema, Redis keying, and archiver expectations. Propose exact Bluesky → normalized item mapping. Do not implement yet.

Mission 2 — Implement BlueskyIngestor POC
Create src/ingestion/bluesky.py using Jetstream app.bsky.feed.post. Normalize posts and write local sample JSONL. Do not wire into live pipeline yet.

Mission 3 — Add Bluesky routing
Add BlueskyTopicRouter or wrapper. Test sample posts route into existing channels.

Mission 4 — Wire live_bluesky
Add src/main.py entry point. Ensure Redis/GCS/state keys are source-separated. Short smoke test only.

Mission 5 — Process Bluesky anomalies offline
Run batch_enricher, inspect candidates/events. Add source filters to CLI if missing.

Mission 6 — Evaluate and tune Bluesky-specific behavior
Only tune after sample output exists. Do not touch HN policy unless regression still passes.

Notice Points: 1. Current item schema still has HN/Reddit names: subreddit, story_id, story_title.
2. story_id/item_id are listed as ints, but Bluesky IDs are AT URIs/CIDs strings.
3. Redis/window keys must be source-separated, not just channel-separated.