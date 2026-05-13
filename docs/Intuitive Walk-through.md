Reddit Surge Detection — End-to-End Walkthrough
The Problem
Reddit has millions of comments per day spread across thousands of subreddits. When something big happens in the world — a game gets announced, a political crisis breaks, a celebrity dies — you see a sudden coordinated burst of activity across multiple communities at once. The goal of this system is to detect those bursts in real time, label them, and summarize what they're about.

The challenge: most hours are just background noise. You can't just count comments and set a fixed threshold, because some subreddits are always busy and some are always quiet. You need to know what's unusual for that specific community at that specific time.

Phase 1: Batch Backtesting on Historical Data
Phase 1 answered one question: can we detect known real events from archived Reddit data? We used November and December 2023 dumps — months that contained the Game Awards (Dec 8), the GTA VI trailer leak (Dec 5), and several breaking news cycles — as our ground truth.

Step 1 — Getting the data in: ZstFileIngestor
Reddit publishes monthly comment archives compressed with Zstandard (.zst). These files are enormous — gigabytes — so we can't load them into memory. Instead, ZstFileIngestor opens the file as a byte stream, decompresses it in 64KB chunks, and yields one comment dict at a time. Think of it like reading a book one sentence at a time instead of scanning every page at once.

Each comment comes out looking like:


{ "id": "abc123", "subreddit": "gaming", "body": "The trailer looks insane", "timestamp": 1701993600, ... }
Step 2 — Keeping only what matters: SubredditFilter
We only care about 13 subreddits — gaming hubs, entertainment hubs, and news hubs. The filter drops everything else and also strips bot comments (AutoModerator, RepostSleuthBot, etc.) by checking both the author name and boilerplate phrases in the body. The 13 targets were chosen to cover the three cultural categories we expect surges to flow through.

Step 3 — Detecting a surge: TumblingWindowTripwire
This is the core anomaly detector. It works on tumbling (fixed) windows — it slices time into clean 1-hour buckets aligned to the clock (18:00→19:00, 19:00→20:00, etc.).

As comments stream in, it counts them per subreddit within the current hour. The moment a comment arrives with a timestamp in the next hour, the current bucket "tumbles" — it locks in the count and asks: is this count unusual compared to recent history?

The math is a Z-score:

Z = (current count − mean of past 24 hours) / standard deviation

Think of it like asking: if the average gaming hour gets 100 comments and the standard deviation is 20, then 160 comments gives Z = (160−100)/20 = 3.0. That's 3 standard deviations above normal — statistically remarkable. We fire an anomaly at Z ≥ 3.0.

For volatile subreddits like r/news and r/worldnews, where the baseline itself fluctuates wildly (a slow Tuesday vs. a breaking news day look nothing alike), we swap mean/std for median/MAD (Median Absolute Deviation), which is more resistant to outliers in the history.

One practical detail: the tripwire does reservoir sampling to keep up to 500 comment bodies from each window for later NLP processing. It can't just take the first 500 (those would all be from the quietest part of the hour) — instead it uses a random replacement algorithm so the sample stays statistically representative of the whole hour.

Step 4 — Understanding what the surge is about: DBSCANContextEngine
Knowing that r/gaming spiked at 3.4σ is useful. Knowing why is more useful. When the tripwire fires, it passes the 500 sampled texts to the NLP engine.

Step 4a — Embedding: Every comment is converted to a 384-dimensional vector using a sentence transformer model (all-MiniLM-L6-v2). Semantically similar comments end up close together in that high-dimensional space. "The trailer is incredible" and "Best reveal I've ever seen" will be nearby; "I hate the new matchmaking" will be far away.

Step 4b — Dimensionality reduction (UMAP): 384 dimensions are hard to cluster. UMAP compresses them down to 2 dimensions while preserving the neighbourhood structure — comments that were close stay close.

Step 4c — Clustering (DBSCAN): DBSCAN groups the 2D points into dense islands, with sparse points labelled as noise (-1). Each island is a coherent topic thread. A gaming hour with a new trailer reveal might produce one big cluster (trailer reactions) and a smaller one (comparisons to last year's reveal), with scattered noise points.

Step 4d — Keywords (c-TF-IDF): For each cluster, we extract keywords using Class-based TF-IDF. The idea: treat each cluster as one big document, then find words that appear frequently in this cluster but rarely in the other clusters. This surfaces the distinctive vocabulary of each topic thread rather than just the most common words overall.

On top of that we apply a rolling 7-day baseline penalty: words that are chronically common on Reddit (like "game" or "trailer" during a gaming week) get penalized even if they're frequent in the current cluster. Words that are spiking right now compared to their baseline rate — like "GTA" on the day of the leak — bypass the penalty and score full weight.

Step 5 — Grouping events: aggregator.py
The tripwire fires one raw anomaly per subreddit per hour. The aggregator's job is to collapse these into meaningful narrative events.

It distinguishes three types:

FLASH — a burst that crosses community boundaries simultaneously. Example: the GTA VI trailer fires r/gaming, r/Games, r/pcgaming, and r/movies all within the same 2-hour window, all sharing keywords like "gta", "trailer", "rockstar". That's one FLASH event, not four separate anomalies. Detection uses a Union-Find structure: two anomalies get linked if they share ≥2 stemmed keywords with an Overlap Coefficient ≥ 0.3 (we use Overlap rather than Jaccard because cluster vocabularies naturally differ in size across communities).

SUSTAINED — a single subreddit stays elevated across ≥4 consecutive hours, with the same topic thread persisting. Example: r/news staying hot from 8pm to midnight during a political crisis. Detection uses a Multi-Track Anchor: the first hour's DBSCAN clusters each become their own "track", and only the dominant track (the one that matched hour 2) can extend the chain. This prevents a news story about topic A from accidentally merging with a later story about topic B just because both happened in r/news.

ISOLATED — anything that doesn't fit FLASH or SUSTAINED. A single-subreddit, single-hour spike with no neighbors. Could be a community-specific event.

FLASH is checked first and gets priority. Anomalies claimed by FLASH are removed from the SUSTAINED pool, and both are removed from ISOLATED.

The bugs we fixed
Three structural bugs were caught and fixed during Phase 1 validation:

The "PS5 Bug": The Z-score history was being updated before firing the anomaly. So a spike in hour N raised the baseline and made the Z-score for hour N look smaller than it actually was. Fix: fire first, then update history.

The "Keyword Drift" bug: We applied stopword filtering before stemming. So "games" (not in the stopword list) would pass through, then stem to "game" — which was supposed to be filtered. Fix: stem first, then check against a pre-stemmed stopword set.

The "Frankenstein Union" bug: Jaccard similarity requires both sets to be large to score high, so small-vocabulary clusters from one subreddit never matched large-vocabulary clusters from another. Fix: switch to Overlap Coefficient (intersection / min of the two set sizes), which doesn't penalize size asymmetry.

After these fixes, the November 2023 benchmark hit Grade A: 10/10 FLASH true positives, 0 false positives, 83% recall.

Phase 2: Real-Time Architecture
Phase 1 processed sorted historical files in one pass. A live Reddit stream is different — comments arrive one at a time, in near-real-time, and you need to answer "is this subreddit surging right now?" every few minutes, not every hour.

Why the tumbling window breaks for real-time
Imagine a surge that starts at 11:45pm. With a tumbling window, the 11pm–midnight bucket only captures 15 minutes of the surge before tumbling. The midnight–1am bucket captures the rest, but by then the baseline update has already happened and the signal is diluted. A surge that would have scored Z=6 in isolation might score Z=2.5 split across two buckets — below threshold.

The sliding window: Redis ZSETs
The fix is a sliding window: always look at the last 2 hours from right now, regardless of clock boundaries.

We store every comment in a Redis Sorted Set (ZSET) where:

The member is {uuid}:{comment text} (UUID makes every entry unique)
The score is the Unix timestamp of the comment
Redis keeps the ZSET sorted by score automatically. To answer "how many comments in the last 2 hours?":

Remove everything older than now − 7200: one ZREMRANGEBYSCORE call
Count what remains: one ZCOUNT call
Both are O(log N). The answer comes back in under 1ms regardless of how many comments are in the set.

The window literally slides: if you call it at 12:05 and again at 12:10, the second call automatically excludes anything that's now older than 2 hours. No buckets, no boundaries, no edge cases.

The decoupled architecture
In Phase 1, the math (Z-score) and the storage (Python dicts) were tangled together inside TumblingWindowTripwire. Phase 2 separates them cleanly.

StateManager is an abstract interface that defines 8 operations: store a comment, count the window, get text samples, read history, write history, check cooldown, set cooldown, flush. The math engine (SlidingWindowTripwire) only calls these 8 methods — it has no idea whether the backend is Redis, SQLite, or a Python dict.

RedisStateManager implements those 8 methods using Redis primitives. To swap from fakeredis (in-memory, for testing) to a real Redis server (for production), you change one line in the constructor — everything else is identical.

Two separate ticks
The engine runs on two different clocks:

evaluation_tick — fires every 5 minutes. For each subreddit: get the 2-hour window count, get the rolling history, compute Z-score, fire an AnomalyEvent if Z ≥ 3.0. After firing, a 30-minute cooldown is set (via a Redis key with a TTL) so the same subreddit can't spam alerts.

baseline_tick — fires every 1 hour. Records the current 2-hour window count into a rolling history list (last 24 entries). This is what the Z-score compares against.

Critical ordering: evaluation must happen before the baseline update within each hour. If you update the baseline first, the current hour's spike inflates the mean, and the Z-score becomes nearly zero — the system is blind to its own surge.

The load test
Since the live Reddit API is still pending approval, we validated the Phase 2 engine with a synthetic load test. It simulates 4 hours of fake comments:

Hours 1–2: normal traffic (80–120 comments/subreddit/hour) → builds 2 clean baseline entries
Hour 3: r/gaming gets 800 comments (the spike), everyone else stays normal
Hour 4: back to normal (verifies cooldown works and no false positives)
Expected: exactly 1 AnomalyEvent fires (r/gaming, hour 3), the text sample is random (not biased toward oldest), and after enough time passes the old comments are pruned from Redis so only recent data remains in the window. All four assertions pass.