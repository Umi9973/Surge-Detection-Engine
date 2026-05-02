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

### [CLOSED] Issue #1: r/PS5 not merging into GTA VI FLASH event

* **Date Opened:** 2026-05-01
* **Date Closed:** 2026-05-01
* **Phase:** Phase 1 — Step 6: Cross-Subreddit Aggregator
* **The Problem:** When the GTA VI trailer drops at Dec 4 23:00 UTC, `r/Games` and `r/gaming` correctly merge into a FLASH event on keywords `gta, trailer, game`. However, `r/PS5` fires at the same hour and remains ISOLATED instead of joining the FLASH.
* **Root Cause:** Two compounding issues:
  1. **Structural** — `detect_flash_isolated()` used fixed 2-hour buckets. If `r/PS5`'s `window_start` landed in a different bucket than `r/Games`/`r/gaming`, they were never compared for Jaccard similarity.
  2. **Keyword noise** — Even after the sliding window was added, Jaccard scores stayed below 0.3 because keyword sets were polluted with Reddit meta-words (`compose`, `removed`, `submission`, `likes`, `views`, `karma`) and generic filler (`really`, `looks`, `wait`). These bloated the union without contributing to the intersection.
* **The Resolution:**
  1. Replaced fixed 2-hour buckets in `detect_flash_isolated()` with a **two-pointer sliding window** — each anomaly compares against all others within ±7200s. Includes a `MAX_EVENT_SPAN = 14400s` safeguard to break up transitivity chains longer than 4 hours.
  2. Extended `AGGREGATOR_STOPWORDS` in `aggregator.py` with observed Reddit meta/noise words: `comment`, `comments`, `karma`, `likes`, `views`, `broken`, `compose`, `removed`, `submission`, `really`, `looks`, `wait`, `man`, `also`, `much`.
  3. Added upstream bot filtering in `filter.py` — drops `AutoModerator` and comments containing boilerplate phrases before they reach the NLP engine, eliminating the modbot clusters that were generating noise keywords at the source.
* **Result:** GTA VI FLASH event now correctly consolidates **4 subreddits**: `r/Games, r/PS5, r/gaming, r/pcgaming` on keywords `gta, trailer, game, florida`. 80 raw anomalies → 66 consolidated events.
* **Remainder:** `filter.py` upstream bot filtering requires `main.py` to be re-run to regenerate `anomalies.db`. Current results use the stopword fix as a compensating control; the clean-room improvement is pending re-run.
* **Key Takeaway:** Fixed-size time bucketing is brittle at boundaries. Keyword quality is a prerequisite for Jaccard to work — noisy keyword sets make threshold tuning meaningless. Fix at the source (filter layer) before patching downstream math.

---