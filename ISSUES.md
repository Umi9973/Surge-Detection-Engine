# Engineering Log & Issue Tracker

This document tracks significant architectural challenges, bugs, and bottlenecks encountered during the development of the Reddit Anomaly Engine, along with their resolutions.

---

### [Template] Issue #1: Short Title of the Problem
* **Date:** YYYY-MM-DD
* **Phase:** (e.g., Phase 1: Local Ingestion)
* **The Problem:** Describe what broke or what the bottleneck was. (e.g., "The script crashed with an OutOfMemory (OOM) error after running for 4 minutes.")
* **Root Cause:** Why did it happen? (e.g., "Python's garbage collector was not clearing the parsed JSON strings fast enough during the Zstandard decompression stream.")
* **The Resolution:** How did you fix it? (e.g., "Forced `del` on the parsed dictionary object at the end of the generator loop and explicitly called `gc.collect()` every 100,000 rows.")
* **Key Takeaway:** Why does this matter? (e.g., "Memory profiling is critical when streaming Big Data. We stabilized RAM usage at a flat 45MB.")

---

### [OPEN] Issue #1: r/PS5 not merging into GTA VI FLASH event

* **Date:** 2026-05-01
* **Phase:** Phase 1 — Step 6: Cross-Subreddit Aggregator
* **The Problem:** When the GTA VI trailer drops at Dec 4 23:00 UTC, `r/Games` and `r/gaming` correctly merge into a FLASH event on keywords `gta, trailer, game`. However, `r/PS5` fires at the same hour with keywords `game, trailer, florida` and remains ISOLATED instead of joining the FLASH.
* **Root Cause:** `detect_flash_isolated()` groups anomalies into fixed 2-hour buckets using `bucket = (window_start // 7200) * 7200`. If `r/PS5`'s `window_start` lands in a different bucket than `r/Games`/`r/gaming` — even by a small offset — they are never compared for Jaccard similarity and cannot be merged. This is a structural limitation of fixed bucketing, not a Jaccard threshold issue.
* **The Resolution:** Not yet implemented. Proposed fix: replace fixed 2-hour buckets with a **rolling/sliding window** that looks ±2 hours around each anomaly and merges any subreddit that overlaps in time AND passes Jaccard >= 0.3. This eliminates bucket boundary splits at the cost of O(n²) comparisons (acceptable given ~80 anomaly scale).
* **Key Takeaway:** Fixed-size time bucketing is simple but brittle at boundaries. Sliding windows are more accurate for event correlation but require more careful deduplication to avoid double-counting.

---