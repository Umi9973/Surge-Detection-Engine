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