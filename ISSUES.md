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

### [CLOSED] Issue #2: SUSTAINED detection stealing anomalies before FLASH could see them

* **Date Opened:** 2026-05-02
* **Date Closed:** 2026-05-02
* **Phase:** Phase 2 Prep — Cross-Subreddit Aggregator
* **The Problem:** The Game Awards (Dec 8 01:00 UTC) produced a SUSTAINED event for `r/XboxSeriesX` alone instead of a FLASH across gaming subreddits. `r/XboxSeriesX` spiked for 4+ consecutive hours on `{game, xbox}`, and because SUSTAINED ran first it claimed all Xbox anomaly IDs. FLASH detection never saw them.
* **Root Cause:** `detect_sustained()` ran before `detect_flash_isolated()` and populated `claimed_ids`. The downstream FLASH detector was blind to any anomaly that formed a valid SUSTAINED chain — even when that anomaly was also part of a cross-subreddit event. The correct signal (4 subreddits reacting simultaneously) lost priority to the structural signal (one subreddit active for 4 hours).
* **The Resolution:** Split `detect_flash_isolated()` into three separate functions: `detect_flash()`, `detect_sustained()`, `detect_isolated()`. Flipped execution order — FLASH runs first and claims cross-subreddit anomaly IDs; SUSTAINED only processes what FLASH didn't claim; ISOLATED gets the rest.
* **Result:** Game Awards correctly produces a FLASH event (`r/Games, r/PS5, r/XboxSeriesX`) instead of an isolated SUSTAINED chain.
* **Key Takeaway:** Detector execution order encodes event type priority. A major real-world event should be allowed to be both sustained and cross-subreddit; the priority flip ensures the stronger, rarer cross-community signal wins.

---

### [CLOSED] Issue #3: Union-set Jaccard diluted by multi-cluster noise

* **Date Opened:** 2026-05-02
* **Date Closed:** 2026-05-02
* **Phase:** Phase 2 Prep — Cross-Subreddit Aggregator
* **The Problem:** `r/Games` at Dec 8 01:00 (z=8.02) failed to merge with `r/PS5` despite both reacting to the Game Awards. `r/Games` had 3 DBSCAN clusters that hour — awards discussion, a Warframe thread, and God of War. The aggregator took the union: `{game, trailer, warframe, clem, ragnarok, valhalla, god}`. Jaccard against r/PS5's `{game, dlc, free}` = 1/10 = 0.10.
* **Root Cause:** Flattening all clusters into one union before Jaccard loses the cluster structure DBSCAN worked to produce. A topically diverse hour accumulates noise words from every cluster, bloating the union denominator while the shared words stay small. The more active a subreddit in a given hour, the harder it is to merge — the opposite of what we want.
* **The Resolution:**
  1. **Cluster-to-cluster matching** — compare every cluster pair across two anomalies; link if any single pair exceeds threshold. Warframe and God of War clusters score 0.0 against r/PS5's awards cluster and become invisible.
  2. **Szymkiewicz–Simpson Overlap Coefficient** — `|A∩B| / min(|A|, |B|)`. Asks "how much of the smaller cluster is contained in the other?" rather than penalising vocabulary difference.
  3. **Minimum 2-keyword intersection guard** — prevents single generic words (`{game}`) from triggering false merges.
  4. **Split thresholds** — `FLASH_OVERLAP = 0.3`, `SUSTAINED_OVERLAP = 0.5`.
* **Result:** GTA VI FLASH consolidates all 4 subreddits with keywords `gta, rockstar, trailer, releas`. Game Awards FLASH keywords include `kojima, award, blade`.
* **Key Takeaway:** Cluster-to-cluster matching preserves DBSCAN's work. Jaccard on keyword unions punishes topically rich subreddits — the algorithm designed to find topics is undermined by flattening those topics before comparison.

---

### [CLOSED] Issue #4: Reaction words contaminating c-TF-IDF keyword extraction

* **Date Opened:** 2026-05-02
* **Date Closed:** 2026-05-02
* **Phase:** Phase 2 Prep — NLP Context Engine
* **The Problem:** After increasing `top_keywords` from 5 to 8, cluster keywords degraded instead of improving. `r/PS5` at Dec 8 01:00 surfaced `lol, fuck, roguelik, holi, week, dlc` — 4 of 6 keywords were reaction words with no topic signal.
* **Root Cause:** Reddit gaming comment sections during live events are dominated by short emotional reactions (`lol`, `fuck yeah`, `omg`). These have very high TF within a cluster because they appear in nearly every comment. With 8 slots, reaction words occupied the top positions and crowded out actual topical terms. Downstream AGGREGATOR_STOPWORDS stripping could remove them after extraction, but that left empty slots — the algorithm had already committed to its top 8.
* **The Resolution:** Added reaction words and internet acronyms to `_STOPWORDS` in `context.py`'s `_tokenize()` — `lol`, `lmao`, `omg`, `wtf`, `wow`, `fuck`, `shit`, `damn`, `bruh`, `ngl`, `imo`, `tbh`, and others. Filtering before tokenization means these words mathematically never exist when c-TF-IDF computes TF scores. The algorithm fills all 8 slots with real topic words.
* **Result:** Game Awards cluster keywords include `kojima`, `award`, `blade`. GTA VI: `gta, rockstar, trailer, releas`. News SUSTAINED: `hezbollah`, `hostag`, `territori`, `lebanes` instead of generic `hama, israel` repeated every hour.
* **Key Takeaway:** Filter junk at the extraction source, not downstream. Downstream stripping creates empty slots; upstream filtering forces the algorithm to find better replacements. Same principle as upstream bot filtering — the earlier you remove noise, the richer the signal flowing through the rest of the pipeline.

---