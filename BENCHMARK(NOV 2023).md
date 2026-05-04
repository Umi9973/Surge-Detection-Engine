# BENCHMARK.md: November 2023 Validation Rubric

## 1. Objective
This document establishes the ground-truth benchmark for evaluating the Phase 1 anomaly detection pipeline against the **November 2023** Reddit dataset. 

Because the pipeline was aggressively tuned on December data (GTA VI Trailer, Game Awards, Christmas noise), running it blindly on November data acts as a **Holdout / Test Set validation**. This proves the architecture is robust, generalized, and immune to overfitting.

---

## 2. The Ground Truth Events (Recall Test)
To pass the benchmark, the pipeline MUST detect and correctly categorize the following real-world events that occurred in November 2023.

### Event A: The Rockstar Games Announcement (Nov 8)
* **What Happened:** Rockstar officially tweeted that the first trailer for the next Grand Theft Auto would drop in "early December." 
* **Expected Output:** `FLASH` event across `r/Games`, `r/gaming`, `r/PS5`, `r/XboxSeriesX`.
* **Success Criteria:** Must NOT merge with random late-November gaming chatter. 

### Event B: The OpenAI / Sam Altman Saga (Nov 17 - Nov 22)
* **What Happened:** Sam Altman was abruptly fired from OpenAI, hired by Microsoft, and then reinstated, causing absolute chaos in tech and news subreddits.
* **Expected Output:** A massive, multi-day `SUSTAINED` event (or linked series of FLASH events).
* **Success Criteria (The Concept Drift Test):** The Multi-Track Anchor must successfully track the vocabulary drift: `[altman, fired, board]` -> `[microsoft, nadella, twitch, emmett]` -> `[return, reinstated]`. It must NOT break into 20 isolated events.

### Event C: Thanksgiving "Noise" (Nov 23)
* **What Happened:** US holiday triggering massive generic traffic about "turkey," "family," "football," and "black friday."
* **Expected Output:** Suppression or `ISOLATED` tagging. 
* **Success Criteria (The Baseline Test):** The Dynamic Baseline TF-IDF penalty must successfully suppress generic Thanksgiving noise. It should not trigger a site-wide `FLASH` event just because everyone used the word "family" or "dinner."

---

## 3. Key Performance Indicators (The Confusion Matrix)

| Metric | Target | Description |
| :--- | :--- | :--- |
| **Recall** | **> 80%** | (True Positives) Out of all major November news stories, the percentage successfully detected and alerted. |
| **Precision** | **> 85%** | (Boy Who Cried Wolf) Out of all `FLASH/SUSTAINED` alerts fired, the percentage that were actual news stories rather than noise. |
| **F1-Score** | **> 82%** | The harmonic mean of Precision and Recall, proving the threshold trade-off is perfectly balanced. |
| **Frankenstein Rate** | **< 5%** | Less than 5% of `SUSTAINED` chains contain cross-contaminated news stories (The Lineage Tracking test). |

---

## 4. Grading Rubric

* **[GRADE A] Senior Ready:** Achieves an 85%+ F1-Score. Catches Altman and Rockstar perfectly. Thanksgiving noise is completely suppressed. The Frankenstein rate is zero.
* **[GRADE B] Minor Tuning Required:** Catches the major events but splits the OpenAI saga into 2 or 3 separate events due to extreme vocabulary shifts. High Recall, but Precision takes a hit as Thanksgiving triggers 1 or 2 minor false-positive `FLASH` alerts.
* **[GRADE C] The Overfit Trap:** Highly overfitted on December. Fails to suppress Thanksgiving noise (site-wide `FLASH` spam). Lineage tracking is too strict, causing the OpenAI story to fragment into 30+ `ISOLATED` spikes. 
* **[GRADE F] Pipeline Failure:** Frankenstein Unions return. The pipeline logic connects the Sam Altman firing with Thanksgiving turkey recipes.