from __future__ import annotations

import sqlite3
import sys
from collections import defaultdict, Counter
from datetime import datetime, timezone
from typing import Dict, List, Set

DB_PATH = "G:\\Umi\\Python Projects\\Reddit Surge Detection\\anomalies.db"

WINDOW_SECONDS     = 7200   # 2-hour rolling window
JACCARD_THRESHOLD  = 0.3
SUSTAINED_MIN_HOURS = 4

# Words observed to appear across 5+ unrelated anomalies in this dataset — pure Reddit
# conversational filler or sidebar/modbot artifacts. Stripped before Jaccard so only
# topical signal words (gta, hamas, caffeine, trailer…) drive similarity scores.
AGGREGATOR_STOPWORDS: Set[str] = {
    # High-frequency filler (10+ anomalies, zero topic signal)
    "people", "don", "because", "please", "good",
    # URL / meta fragments from modbot sidebars
    "https", "www", "com", "reddit", "message", "post", "questions", "audio",
    # 5–6 anomaly range filler
    "some", "think", "his", "were", "time",
}


# --- Math helpers ---

def jaccard(a: Set[str], b: Set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def top_shared_keywords(groups: List[Set[str]], top_n: int = 6) -> List[str]:
    """Keywords that appear in the most subreddit keyword sets, ranked by frequency."""
    counter: Counter = Counter()
    for kw_set in groups:
        for kw in kw_set:
            counter[kw] += 1
    multi = {kw for kw, cnt in counter.items() if cnt > 1}
    ranked = [kw for kw, _ in counter.most_common() if kw in multi] or \
             [kw for kw, _ in counter.most_common(top_n)]
    return ranked[:top_n]


# --- Data loading ---

def load_anomalies(conn: sqlite3.Connection) -> List[Dict]:
    rows = conn.execute("""
        SELECT a.id, a.subreddit, a.window_start, a.window_end,
               a.count, a.z_score,
               GROUP_CONCAT(c.keywords, '|') AS all_keywords
        FROM anomalies a
        LEFT JOIN clusters c ON c.anomaly_id = a.id
        GROUP BY a.id
        ORDER BY a.window_start
    """).fetchall()

    result = []
    for r in rows:
        kw_set: Set[str] = set()
        if r[6]:
            for chunk in r[6].split("|"):
                for kw in chunk.split(", "):
                    kw = kw.strip()
                    if kw:
                        kw_set.add(kw)
        kw_set -= AGGREGATOR_STOPWORDS
        result.append({
            "id":           r[0],
            "subreddit":    r[1],
            "window_start": r[2],
            "window_end":   r[3],
            "count":        r[4],
            "z_score":      r[5],
            "keywords":     kw_set,
        })
    return result


# --- SUSTAINED detection ---

def detect_sustained(anomalies: List[Dict]) -> tuple[List[Dict], Set[int]]:
    by_subreddit: Dict[str, List[Dict]] = defaultdict(list)
    for a in anomalies:
        by_subreddit[a["subreddit"]].append(a)

    events: List[Dict] = []
    claimed_ids: Set[int] = set()

    for subreddit, items in by_subreddit.items():
        items.sort(key=lambda x: x["window_start"])
        i = 0
        while i < len(items):
            chain = [items[i]]
            j = i + 1
            while j < len(items):
                prev, curr = items[j - 1], items[j]
                if (curr["window_start"] - prev["window_start"] == 3600 and
                        jaccard(prev["keywords"], curr["keywords"]) >= JACCARD_THRESHOLD):
                    chain.append(curr)
                    j += 1
                else:
                    break

            if len(chain) >= SUSTAINED_MIN_HOURS:
                ids = [a["id"] for a in chain]
                claimed_ids.update(ids)
                all_kws = set().union(*(a["keywords"] for a in chain))
                events.append({
                    "event_type":     "SUSTAINED",
                    "event_start":    chain[0]["window_start"],
                    "event_end":      chain[-1]["window_end"],
                    "subreddits":     [subreddit],
                    "shared_keywords": list(all_kws)[:8],
                    "peak_z_score":   max(a["z_score"] for a in chain),
                    "anomaly_count":  len(chain),
                    "anomaly_ids":    ids,
                })
                i = j
            else:
                i += 1

    return events, claimed_ids


# --- Union-Find for FLASH/ISOLATED grouping ---

class UnionFind:
    def __init__(self, n: int) -> None:
        self.parent = list(range(n))

    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, x: int, y: int) -> None:
        px, py = self.find(x), self.find(y)
        if px != py:
            self.parent[px] = py


def detect_flash_isolated(anomalies: List[Dict], claimed_ids: Set[int]) -> List[Dict]:
    remaining = [a for a in anomalies if a["id"] not in claimed_ids]

    # Bucket into 2-hour windows
    buckets: Dict[int, List[Dict]] = defaultdict(list)
    for a in remaining:
        bucket = (a["window_start"] // WINDOW_SECONDS) * WINDOW_SECONDS
        buckets[bucket].append(a)

    events: List[Dict] = []

    for bucket, group in sorted(buckets.items()):
        n = len(group)
        uf = UnionFind(n)

        for i in range(n):
            for j in range(i + 1, n):
                if group[i]["subreddit"] != group[j]["subreddit"]:
                    if jaccard(group[i]["keywords"], group[j]["keywords"]) >= JACCARD_THRESHOLD:
                        uf.union(i, j)

        components: Dict[int, List[Dict]] = defaultdict(list)
        for i, a in enumerate(group):
            components[uf.find(i)].append(a)

        for component in components.values():
            subreddits = list({a["subreddit"] for a in component})
            kw_sets    = [a["keywords"] for a in component]
            shared_kws = top_shared_keywords(kw_sets)
            event_type = "FLASH" if len(subreddits) > 1 else "ISOLATED"

            events.append({
                "event_type":      event_type,
                "event_start":     min(a["window_start"] for a in component),
                "event_end":       max(a["window_end"]   for a in component),
                "subreddits":      subreddits,
                "shared_keywords": shared_kws,
                "peak_z_score":    max(a["z_score"] for a in component),
                "anomaly_count":   len(component),
                "anomaly_ids":     [a["id"] for a in component],
            })

    return events


# --- DB output ---

def save_events(conn: sqlite3.Connection, events: List[Dict]) -> None:
    conn.execute("DROP TABLE IF EXISTS consolidated_events")
    conn.execute("""
        CREATE TABLE consolidated_events (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type       TEXT    NOT NULL,
            event_start      INTEGER NOT NULL,
            event_end        INTEGER NOT NULL,
            event_start_date TEXT    NOT NULL,
            event_end_date   TEXT    NOT NULL,
            subreddits       TEXT    NOT NULL,
            shared_keywords  TEXT    NOT NULL,
            peak_z_score     REAL    NOT NULL,
            anomaly_count    INTEGER NOT NULL
        )
    """)
    for e in events:
        start_str = datetime.fromtimestamp(e["event_start"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        end_str   = datetime.fromtimestamp(e["event_end"],   tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        conn.execute("""
            INSERT INTO consolidated_events
            (event_type, event_start, event_end, event_start_date, event_end_date,
             subreddits, shared_keywords, peak_z_score, anomaly_count)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            e["event_type"],
            e["event_start"], e["event_end"],
            start_str, end_str,
            ", ".join(sorted(e["subreddits"])),
            ", ".join(e["shared_keywords"]),
            e["peak_z_score"],
            e["anomaly_count"],
        ))
    conn.commit()


# --- Entry point ---

def run() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    conn = sqlite3.connect(DB_PATH)

    print("Loading anomalies...")
    anomalies = load_anomalies(conn)
    print(f"  {len(anomalies)} raw anomalies loaded.\n")

    sustained_events, claimed_ids = detect_sustained(anomalies)
    other_events = detect_flash_isolated(anomalies, claimed_ids)

    all_events = sorted(sustained_events + other_events, key=lambda e: e["event_start"])
    save_events(conn, all_events)

    flash_count     = sum(1 for e in all_events if e["event_type"] == "FLASH")
    sustained_count = sum(1 for e in all_events if e["event_type"] == "SUSTAINED")
    isolated_count  = sum(1 for e in all_events if e["event_type"] == "ISOLATED")

    TAGS = {"FLASH": "FLASH", "SUSTAINED": "SUSTAINED", "ISOLATED": "ISOLATED"}

    print("=" * 80)
    print("  CONSOLIDATED EVENTS")
    print("=" * 80)

    for e in all_events:
        start_str = datetime.fromtimestamp(e["event_start"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        end_str   = datetime.fromtimestamp(e["event_end"],   tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        print(f"\n[{e['event_type']}]")
        print(f"  Time       : {start_str}  ->  {end_str}")
        print(f"  Subreddits : {', '.join(sorted(e['subreddits']))}")
        print(f"  Keywords   : {', '.join(e['shared_keywords'])}")
        print(f"  Peak Z     : {e['peak_z_score']}")
        print(f"  Raw alerts : {e['anomaly_count']} collapsed")

    print(f"\n{'=' * 80}")
    print(f"Result : {len(anomalies)} raw anomalies  ->  {len(all_events)} events")
    print(f"         {flash_count} FLASH  |  {sustained_count} SUSTAINED  |  {isolated_count} ISOLATED")

    conn.close()


if __name__ == "__main__":
    run()
