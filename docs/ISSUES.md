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

### [CLOSED] Issue #5: Topic bleeding inside SUSTAINED chains

* **Date Opened:** 2026-05-02
* **Date Closed:** 2026-05-03
* **Phase:** Phase 1 — Cross-Subreddit Aggregator
* **The Problem:** The `r/news` Dec 3 SUSTAINED chain (7hrs) included keywords from the Gaza conflict AND an Alaska/Hawaiian Airlines story — two distinct news stories chained into one event because they shared 2 incidental keywords in a consecutive hour window.
* **Root Cause:** The flat Anchor from Issue #6's fix made this worse. Flattening all DBSCAN clusters from Hour 1 into a single `Set[str]` created a "catch-all net" — r/news firing 3 breaking stories simultaneously produced an anchor containing keywords from all three. Any subsequent hour only needed 2 words from that polluted set to pass the threshold.
* **The Resolution:** Replaced the flat Anchor with a **Multi-Track Anchor** (`anchor_tracks: List[Set[str]]`) in `detect_sustained()`. Each DBSCAN cluster from Hour 1 gets its own isolated track. A new helper `find_best_cluster_pair(anchor_tracks, curr_clusters) → (anchor_idx, curr_idx, score)` finds the highest-scoring cluster pair. The first matching pair at Hour 2 locks the **dominant track** (`dominant_idx`). From Hour 3 onward, only `anchor_tracks[dominant_idx]` can extend the chain — other tracks are frozen. The dominant track expands (`|=`) as it matches, allowing vocabulary evolution within the single story while physically preventing cross-track contamination.
* **Measured Impact:**
  * Dec 3 r/news SUSTAINED: `airlin, tesla, babi, wolf, palestinian` (4 stories) → `civilian, hama, israel, militari` (pure Gaza chain)
  * Dec 31 r/news SUSTAINED: `border, boat, pirat, houthi` (Texas + Red Sea mixed) → `border, immigr, migrant, texa` (pure border chain)
  * Dec 26 r/entertainment SUSTAINED: dissolved — was held together by noise keywords, dominant track lock correctly broke it
* **Key Takeaway:** A flat Anchor set and a Multi-Track Anchor solve opposite problems and break each other when combined. The flat anchor solves drift (Issue #6) but enables bleeding (Issue #5). The Multi-Track Anchor solves both simultaneously: the dominant track lock prevents cross-topic contamination, while the track's own `|=` expansion handles within-story vocabulary evolution. The DBSCAN cluster structure must be preserved all the way through the aggregator — flattening it at any stage discards the topic separation DBSCAN was built to produce.

---

### [CLOSED] Issue #6: Keyword drift breaking valid SUSTAINED chains (Concept Drift)

* **Date Opened:** 2026-05-03
* **Date Closed:** 2026-05-03
* **Phase:** Phase 1 — Cross-Subreddit Aggregator
* **The Problem:** On Dec 31, r/news had a SUSTAINED event starting at 20:00 UTC (z=11.24, keywords: `immigr, border, traffick`) about the Texas Eagle Pass border standoff. However, the 19:00 hour (z=9.71) was a separate ISOLATED event. The hypothesis was that both hours were the same story split by vocabulary drift.
* **Root Cause:** `detect_sustained()` used a daisy-chain approach — each window compared only against its immediate predecessor. A single vocabulary gap anywhere in the chain broke it permanently.
* **The Resolution:** Replaced daisy-chaining with a **Sliding Anchor** in `detect_sustained()`. When a chain starts, `anchor_hour1` is frozen as the event's semantic identity. Each new window is compared against `anchor_hour1 | prev_hour_keywords` (Hour 1 + previous hour). The anchor never grows beyond two components, bounding the Overlap Coefficient denominator and preventing a bloated anchor from causing false merges via incidental common words.
* **Validation Finding:** The specific Dec 31 19:00 anomaly (`area, batteri, build, burn, car, colorado`) turned out to be a **different story** (Colorado vehicle fire) — not the Texas border standoff. Its ISOLATED classification was correct. The sliding anchor correctly blocked the merge since `{car, build, burn}` ∩ `{border, migrant, houthi}` = ∅.
* **Measured Impact:** SUSTAINED events increased from 7 → 8. Two spurious Dec 8 post-Game Awards FLASH events (`show, game` and `game, didn`) dissolved and their anomalies correctly merged into the main Game Awards FLASH (3 subreddits → 4, 6 alerts → 10). A previously invisible Dec 5 Gaza news FLASH (`hama, civilian, israel`) surfaced after upstream stopword cleanup unblocked it.
* **Side work — Upstream Stopwords:** Validation exposed filler tokens leaking through `context.py`: contracted negatives (`didn, don, won, isn, wasn, doesn, wouldn, couldn, hadn, shouldn` — apostrophe stripped by regex), and function words (`any → ani`, `being/been → be`, `these`, `other`). All added to `_STOPWORDS` in `context.py` upstream, forcing c-TF-IDF to replace them with real topic words.
* **Key Takeaway:** Daisy-chain overlap is brittle at vocabulary boundaries. The Sliding Anchor (Hour 1 + prev hour) gives the algorithm one step of drift tolerance while keeping the anchor bounded — a large unbounded anchor would invert the Overlap Coefficient, letting `min(|anchor|, |curr|) = |curr|` score any 2-word incidental overlap as 0.5.

---

### [CLOSED] Issue #8: Cold-start baseline produces false-positive flood on pipeline launch

* **Date Opened:** 2026-05-12
* **Date Closed:** 2026-05-12
* **Phase:** Phase 2 — Live Sliding Window Pipeline
* **The Problem:** When the pipeline starts with an empty Redis baseline (zero history), the first 12–24 hours generate an overwhelming number of false-positive anomalies. The HN Nov 15–22 backtest (old narrow CSV) produced 1,470 post-AlertGate alerts over 8 days, with the vast majority concentrated on Nov 15–16 — days where the Sam Altman firing had not yet occurred and no genuine surge was happening. In a live Discord-connected deployment this translates to hundreds of meaningless notifications before the baseline stabilises.
* **Root Cause:** `SlidingWindowTripwire` required only `MIN_HISTORY = 2` baseline samples before firing. With 2 samples, the standard deviation is computed from an almost-flat distribution. A typical early pattern is `history = [1, 3]` (mean=2, std=1). The first moderately active 5-minute window (count=20) produces `z = (20 − 2) / 1 = 18.0` — well above the `Z_THRESHOLD = 3.0` trigger. This is statistically meaningless: 2 samples are not enough to characterise the ambient noise floor of any channel.
* **Measured Impact:** HN backtest Nov 15 00:00–16 23:59 UTC: ~1,200 of the 1,470 total alerts were cold-start false positives.
* **The Resolution (two complementary fixes):**
  1. **`MIN_HISTORY = 2 → 24`** in `SlidingWindowTripwire` — `_compute_z()` returns `None` until 24 hourly baseline samples exist. One full day covers the diurnal cycle, giving the standard deviation a realistic floor. Protects both the live pipeline (against Redis wipes/reboots) and the backtest.
  2. **7-day burn-in period** in `hn_backtest_driver.py` — the detection window is suppressed for the first 7 days of CSV data (`detection_start_ts`). Evaluation ticks are skipped entirely during burn-in (keeping the Schmitt trigger clean); baseline ticks always run. Logged as `[burn-in N/24]` so the operator can track warmup progress. Detection opens at midnight UTC of Day 8.
* **Result:** HN backtest (wider CSV, Nov 8–22) with both fixes: **1,470 → 381 anomalies** (−74%). All remaining anomalies start from Nov 15 onward and carry legitimate NLP clusters (`board, sam, altman, openai, fired`). Zero cold-start false positives.
* **Note on midnight alignment:** `detection_start_ts` snaps to midnight UTC of the burn-in boundary, not exactly 168h from the first CSV item. If the CSV starts at 14:00 UTC Nov 8, the window opens Nov 15 00:00 UTC (154 hourly samples, not 168). This is acceptable — 154 >> 24 and the midnight boundary makes logs cleaner.
* **Key Takeaway:** Statistical anomaly detection requires a statistically valid baseline. `MIN_HISTORY = 2` satisfies the code contract but not the math contract. The burn-in fixes the backtest; the MIN_HISTORY gate is the always-on production guard against any cold-start scenario.

---

### [CLOSED] Issue #9: Low-volume channels generating false positives from tiny absolute counts

* **Date Opened:** 2026-05-12
* **Date Closed:** 2026-05-12
* **Phase:** Phase 2 — Live Sliding Window Pipeline
* **The Problem:** After the Issue #8 burn-in fix, 381 anomalies remained in the HN Nov 8–22 backtest. Inspecting by channel revealed that the `startup` channel was firing repeated alerts with `count=3` or `count=4` — e.g., `count=4, z=3.69` at Nov 15 04:15 UTC. These are statistically elevated but operationally meaningless. 54 of the 381 startup anomalies were this pattern; about 31 across all channels were pure low-count noise.
* **Root Cause:** Low-volume channels (startup, crypto at off-peak hours) have near-zero ambient activity. A baseline of `[0, 1, 1, 0, ...]` produces a mean ≈ 0.5 and std ≈ 0.5. A window with count=4 yields `z = (4 − 0.5) / 0.5 = 7.0` — high enough to trigger ELEVATED and hold it. The Z-score math is technically correct, but 4 items in a 5-minute window is not an actionable signal regardless of Z.
* **Measured Impact:** 31 anomalies across startup/crypto/science were count < 10 noise. In production these would generate Discord notifications for events with single-digit item counts.
* **The Resolution:** Added `MIN_COUNT = 10` class constant to `SlidingWindowTripwire`. In `evaluation_tick()`, if `count < MIN_COUNT` and the channel is not already `ELEVATED`, skip the channel entirely (no Z computation, no state change). If the channel is already ELEVATED, the check is bypassed so the Schmitt trigger release logic can still run — preventing channels from being stuck ELEVATED after a real surge winds down to low counts.
* **Result:** **381 → 350 anomalies** (−31). All remaining startup anomalies have count ≥ 10 and correspond to genuine Sam Altman–driven activity on Nov 15 and Nov 21–22.
* **Key Takeaway:** Z-score alone is not sufficient to qualify an anomaly on low-volume channels. A high Z from a near-zero baseline is a statistical artefact, not a signal. A minimum absolute count floor separates "statistically unusual" from "operationally interesting". The floor must not block the ELEVATED release path — gating only the entry transition preserves the Schmitt trigger's clean exit behaviour.

---

### [CLOSED] Issue #14: VM disk full — local Parquet files never deleted after GCS upload

* **Date Opened:** 2026-05-28
* **Date Closed:** 2026-05-28
* **Phase:** Phase 2 — Live HN Pipeline / Parquet Archiver
* **The Problem:** The VM (`e2-micro`, 1 GB RAM, ~10 GB disk) ran out of disk space, halting Parquet uploads after May 20. `ParquetArchiver.archive()` wrote each anomaly file locally then uploaded to GCS, but never deleted the local copy. Over several weeks, `data/parquet/` silently accumulated hundreds of files with no cleanup. Additionally, if the upload step threw an exception (network blip, auth failure), the local file was left behind with no retry mechanism — it became stranded and invisible until the next manual inspection.
* **Root Cause:** `pq.write_table()` writes locally first by design (Parquet must be complete before upload). The upload call was in the same function, but no deletion followed a successful upload. The `except` block only printed an error — it did not re-raise or schedule a retry. There was no startup sweep for leftover files from previous crashed runs.
* **The Resolution:**
  1. **Atomic write via temp file** — write to `.parquet.tmp`, then `rename()` to `.parquet` on success. `retry_pending()` scans only `*.parquet` so a crash mid-write never leaves a corrupt file eligible for upload.
  2. **Delete after confirmed upload** — `local_path.unlink()` runs in a separate `try/except` after the upload block, so upload failure and unlink failure produce distinct error messages and don't mask each other.
  3. **Startup retry sweep** — `retry_pending()` is called in `live_hn()` on startup. Scans `data/parquet/*.parquet` and uploads any leftover files from prior runs.
* **Key Takeaway:** Any pipeline that writes locally before uploading must treat local storage as a staging buffer, not a destination. If local files are not deleted on success, disk fills in proportion to runtime. The atomic temp-file pattern is the standard guard against corrupt-file retry bugs.

---

### [CLOSED] Issue #15: Silent crash on every anomaly — `flat_kw` NameError in `live_hn()`

* **Date Opened:** 2026-05-28
* **Date Closed:** 2026-05-28
* **Phase:** Phase 2 — Live HN Pipeline
* **The Problem:** No anomaly alerts were being dispatched to Discord. The pipeline appeared healthy (items ingesting, ticks firing) but `*** ANOMALY ***` lines were absent from logs and the SQLite DB had no new rows. The pipeline was silently swallowing every anomaly.
* **Root Cause:** A refactor had removed `flat_kw = [...]` and the `archiver.archive(ev, flat_kw)` call, but left `dispatcher.dispatch(ev, flat_kw)` unchanged. When an anomaly fired, `flat_kw` was referenced but undefined — a `NameError` was raised inside the `fire_ticks()` closure, which had no exception handler. The exception was silently dropped, `archiver.archive()` was never called, and the anomaly was lost.
* **The Resolution:** Changed `dispatcher.dispatch(ev, flat_kw)` to `dispatcher.dispatch(ev, [])`. The keywords argument is informational only (shown in the Discord embed) — passing an empty list is the correct default now that keywords come from the NLP enricher, not the live pipeline.
* **Key Takeaway:** Silent exception drops inside closures are the hardest bugs to detect — the system appears to be running normally. A bare `except` or missing handler on a closure turns logic errors into invisible no-ops. Anomaly pipelines should log a count of anomalies processed per tick so a sudden drop to zero is immediately visible.

---

### [CLOSED] Issue #16: Cluster explanation required reverse-engineering — no story provenance in Parquet

* **Date Opened:** 2026-05-29
* **Date Closed:** 2026-05-29
* **Phase:** Phase 2 — Parquet Archiver / Batch Enricher
* **The Problem:** Enriched Parquet files stored `texts: list<string>` only — raw comment bodies with no metadata. Verifying cluster quality required guessing which HN story each cluster corresponded to from keywords alone (e.g., "gemini + flash + google → probably Google I/O"). This is subjective and unscalable. There was no way to programmatically answer "which story drove 73% of this cluster's texts?"
* **Root Cause:** The ingestion pipeline stripped story metadata (title, URL, story ID, item type) when converting Firebase API responses to the internal `Comment` dataclass. Only the text body was carried through Redis, into `AnomalyEvent`, and into Parquet. Provenance was discarded at the first transformation boundary.
* **The Resolution:** Carried `story_id`, `story_title`, `domain`, and `item_type` through the full pipeline:
  1. `_HNItemProcessor`: added `_item_story` (item→root story ID) and `_story_meta` (story ID→title/domain) caches with reference-safe eviction.
  2. `RedisStateManager`: changed member format from `{uuid}:{text}` to `{uuid}:{json}` with backward-compat fallback for old plain-text entries.
  3. `AnomalyEvent`: replaced `texts: List[str]` with `items: List[Dict]` — single list, no parallel-list divergence risk.
  4. Parquet schema: added `items: list<struct<text, story_id, story_title, domain, item_type>>` and `top_story_id / top_story_title / top_story_pct` to `_CLUSTER_STRUCT`.
  5. `context.py`: added `text_indices` per cluster so the enricher can map cluster membership back to story IDs.
  6. `batch_enricher`: computes dominant story per cluster using `text_indices`, excluding `story_id=0` (unattributed) from the counter so deep-comment blanks don't crowd out real attribution.
* **Key Takeaway:** Provenance must be carried from the API boundary, not reconstructed downstream. Every transformation boundary (Firebase → dict → Comment → Redis → AnomalyEvent → Parquet) is an opportunity to lose metadata. Design the schema at the ingestion layer to include all fields you will ever want for analysis, even if they are not used immediately.

---

### [CLOSED] Issue #17: HNTopicRouter keyword gaps causing systematic mis-channelling

* **Date Opened:** 2026-05-29
* **Date Closed:** 2026-05-29
* **Phase:** Phase 2 — HN Topic Router
* **The Problem:** Several high-profile HN stories on May 29 routed to the wrong channel or fell through to `general`. "Blue Origin's New Glenn blows up during static fire test" (469 pts) routed to `startup` or `general` — it had no science keyword match. "Volkswagen blocks Home Assistant by requiring client assertion" (367 pts) routed to `general` — no tech keyword for IoT/OAuth. "SQLite is all you need" discussions also missed `tech`. The enrichment showed `startup` clusters containing "spacex", "blue origin", "rocket" — unambiguous signals that space content was accumulating in the wrong channel.
* **Root Cause:** The original `TOPIC_CHANNELS` had no space/aerospace vocabulary in `science`, and no home-automation or database vocabulary in `tech`. Domain boosts for `nasa.gov`, `space.com`, `spacenews.com` were absent. The router could only classify what its keyword vocabulary covered — everything else fell to `general`.
* **The Resolution:**
  * Added to `science`: `nasa (3)`, `blue origin (3)`, `new glenn (3)`, `starship (3)`, `rocket (2)`, `orbital (2)`, `aerospace (2)`, `booster (2)`, `spacex (1)` (tier-1 intentionally — prevents SpaceX funding stories from routing to science over startup's tier-3 business keywords via priority tie-break), `space (1)`, `launch (1)`.
  * Added to `tech`: `home assistant (2)`, `sqlite (2)`, `postgres (2)`, `oauth (2)`, `database (1)`.
  * Added domain boosts: `nasa.gov (+3 science)`, `space.com (+2 science)`, `spacenews.com (+2 science)`.
  * Verified with 11 `router.classify()` test cases before committing — all pass, including SpaceX funding staying in `startup`.
* **Key Takeaway:** A keyword router is only as good as its vocabulary. Gaps are invisible until you inspect enriched output and notice content aggregating in the wrong channel. The router needs periodic review against real traffic — running `router.classify()` on recent front-page titles is a fast, zero-infrastructure audit.

---

### [OPEN] Issue #10: Baseline freeze corrupts 7-day history during multi-week surges

* **Date Opened:** 2026-05-29
* **Phase:** Phase 2 — Live HN Pipeline
* **The Problem:** During a prolonged ELEVATED state, `baseline_tick()` pushes the last clean count (`history[-1]`) instead of the current anomalous count. This is intentional — it prevents the Z-score from decaying to zero during a surge. However, `HISTORY_SIZE = 168` (7 days). If a channel remains ELEVATED for more than 168 hours, the frozen count is pushed 168+ times and completely overwrites the real history. When the surge ends, the channel's baseline is now artificially anchored at the pre-surge floor, not the true long-run ambient level. For several days after the event ends, any moderate uptick will look anomalous against an incorrectly deflated baseline.
* **Root Cause:** Baseline freeze is an unbounded operation with no cap. The design assumes surges are short (hours, not days). A major ongoing story (geopolitical conflict, prolonged product launch cycle) can keep one channel ELEVATED for days.
* **Impact:** Silent correctness issue. After a multi-day surge, the detection sensitivity is artificially raised for the affected channel — legitimate moderate events get false-positive anomalies; the inflated baseline needs another 168 hours of real data to normalise.
* **Proposed Fix:** Cap the number of consecutive frozen pushes. After `N` frozen ticks (e.g. `N = 24`, one day), either push the current count regardless (accepting baseline drift), or insert a decayed value between frozen and current to soften re-entry. Alternatively, detect if the surge is still ELEVATED at baseline_tick time and log a warning so the operator knows the baseline is drifting.
* **Key Takeaway:** Baseline freeze is correct for short surges (hours) but incorrect for sustained ones (days). Unbounded state protection becomes a correctness liability when the protected condition never resolves.

---

### [OPEN] Issue #11: Redundant `texts` column doubles Parquet text storage with no active reader

* **Date Opened:** 2026-05-29
* **Phase:** Phase 2 — Parquet Archiver / Enricher
* **The Problem:** After adding the `items: list<struct<text, story_id, story_title, domain, item_type>>` column, the archiver still writes a parallel `texts: list<string>` column containing `[i["text"] for i in items]`. This is the same data stored twice. For a window with 500 texts of ~100 characters each, that's ~50 KB duplicated per Parquet file — times hundreds of anomaly files.
* **Root Cause:** `texts` was kept for "backward compatibility with existing readers." In practice, the dashboard generator reads from Redis and SQLite only, and `batch_enricher` uses `items` (falling back to `texts` for old files only). There is no live reader that requires `texts` when `items` is present.
* **Risk beyond storage:** Two representations of the same data creates an inconsistency vector. A future code path that reads `texts[i]` and `items[i]["story_id"]` assumes they are aligned. If they ever diverge (e.g., a bug in archive code writes `texts` from a different source), the enricher silently produces wrong attribution.
* **Proposed Fix:** Remove `texts` from `_SCHEMA` once backward compat for old files is no longer needed (i.e., after all pre-`items` files have been enriched or retired). Enricher already falls back to `texts` for old files via `_normalise_row()` — keep that fallback, but stop writing `texts` for new files.
* **Key Takeaway:** Backward-compat columns should have an explicit sunset plan. A column kept "just in case" becomes permanent technical debt and a latent inconsistency risk.

---

### [OPEN] Issue #12: AlertGate cooldown lost on process restart — alert spam on redeploy

* **Date Opened:** 2026-05-29
* **Phase:** Phase 2 — Live HN Pipeline
* **The Problem:** The default `AlertGate` uses an in-memory `dict` for cooldown tracking. If `src.main` is restarted during an active surge (e.g., for a code deploy, VM reboot, or crash), all cooldown state is lost. The next `evaluation_tick()` after restart will see the channel as ELEVATED with no cooldown and fire a Discord alert immediately — regardless of when the last alert was sent. During a prolonged surge with frequent redeploys, this generates repeated duplicate alerts.
* **Root Cause:** The Redis-backed `AlertGate` path exists (`AlertGate(r=redis.Redis(...))`) and correctly survives restarts via TTL, but `live_hn()` in `main.py` instantiates `AlertGate()` with no Redis argument, defaulting to in-memory mode.
* **Current Mitigation:** None. The operator notices duplicate alerts manually.
* **Proposed Fix:** Pass the live Redis client to `AlertGate`: `gate = AlertGate(r=r)`. One-line change in `live_hn()`. The Redis-backed implementation is already written and tested — it is simply not wired up.
* **Key Takeaway:** A dual-mode component (memory vs Redis) where the production-safe mode is not the default is a deployment trap. The safer mode should be the default, with the in-memory mode opt-in for tests.

---

### [OPEN] Issue #13: Story attribution in enriched Parquet is terminal — wrong routing cannot be corrected

* **Date Opened:** 2026-05-29
* **Phase:** Phase 2 — Batch Enricher / Parquet Schema
* **The Problem:** The `top_story_id`, `top_story_title`, and `top_story_pct` fields written to enriched Parquet are computed from `text_indices` (which cluster texts map to which story). But `text_indices` is a runtime-only field — it is computed during enrichment and discarded. Once the enriched file is written, the mapping from text to story is gone. If `top_story_*` attribution is later found to be wrong (e.g., a router bug mis-channelled comments, or the story metadata cache was evicted before the item was ingested), there is no way to re-derive correct attribution without re-running the full enrichment pipeline from scratch (re-download raw file, re-embed, re-cluster, re-attribute).
* **Root Cause:** `text_indices` was intentionally excluded from Parquet to save space ("ephemeral — computed during enrichment, not written"). But this makes the attribution fields opaque and non-auditable after the fact.
* **Impact:** Currently low — enrichment runs on fresh data and attribution is mostly correct. Becomes significant if routing bugs accumulate silently over days before detection.
* **Proposed Fix (two options):**
  1. Write `text_indices` per cluster to the enriched Parquet as `list<int64>`. Enables re-attribution without re-clustering. Storage cost: minor (one integer per clustered text).
  2. Write `story_id` per text in the cluster struct (a list of story IDs for the texts in that cluster), giving full traceability without needing to re-run DBSCAN.
* **Key Takeaway:** Derived fields that cannot be re-derived from stored data make pipelines non-auditable. In analytical systems, preserving the intermediate mapping (text → cluster, text → story) is usually worth the storage cost.

---

### [CLOSED] Issue #7: AGGREGATOR_STOPWORDS applied before stemming — inflected forms bypass filter

* **Date Opened:** 2026-05-06
* **Date Closed:** 2026-05-06
* **Phase:** Phase 1 — Cross-Subreddit Aggregator
* **The Problem:** After adding `"game"` and `"play"` to `AGGREGATOR_STOPWORDS` to suppress chronic gaming background noise, the November backtest still showed 5 weak gaming FLASH events with keywords `game, screen`, `game, year, new`, `fun, play`, etc. The stopword additions appeared to have no effect.
* **Root Cause:** `context.py`'s `_tokenize()` does not stem — it stores raw unstemmed tokens in the DB (`"games"`, `"playing"`, `"years"`). In `aggregator.py`, the pipeline was:
  1. Load raw keyword strings from DB into `kw_set` (e.g. `{"games", "playing"}`)
  2. `kw_set -= AGGREGATOR_STOPWORDS` — compares `"games"` against `"game"`. Not equal → survives.
  3. `kw_set = {_STEMMER.stem(kw) for kw in kw_set}` — `stem("games") = "game"`, `stem("playing") = "play"`.
  4. Result: `"game"` and `"play"` appear in the final keyword set despite being in `AGGREGATOR_STOPWORDS`.

  The comparison was in unstemmed space; the output was in stemmed space. The two spaces never met.
* **The Resolution:**
  1. Added `_STEMMED_STOPWORDS: Set[str] = {_STEMMER.stem(w) for w in AGGREGATOR_STOPWORDS}` computed once at module load time.
  2. Flipped the strip/stem order in `load_anomalies()`:
     ```python
     # Before (broken):
     kw_set -= AGGREGATOR_STOPWORDS          # unstemmed comparison — leaks inflections
     kw_set = {_STEMMER.stem(kw) for kw in kw_set}

     # After (fixed):
     kw_set = {_STEMMER.stem(kw) for kw in kw_set}   # stem first
     kw_set -= _STEMMED_STOPWORDS                      # compare in stemmed space
     ```
  3. Also added `"year"`, `"new"`, `"content"` to `AGGREGATOR_STOPWORDS` — these were the residual shared tokens driving the 2 remaining weak gaming FLASH merges after `"game"` was blocked.
* **Measured Impact:** November benchmark: 16 FLASH → 10 FLASH. All 5 weak gaming co-spike FLASHes dissolved (Nov 2 weekend co-spike, Nov 3 GOTY buildup, Nov 7 pre-GOTY, Nov 24 Black Friday, Nov 1 gaming noise). Zero false positives remaining. Precision 75% → 93%, F1 79% → 88%. **Grade B+ → Grade A.**
* **Key Takeaway:** Stopword filters and token transformations must operate in the same space. Any transformation applied after the filter (stemming, lowercasing, normalization) creates a gap where transformed forms bypass the filter undetected. Always apply filters last, or pre-transform the filter vocabulary to match the token space it will be compared against.

---