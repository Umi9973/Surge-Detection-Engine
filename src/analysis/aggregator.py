from __future__ import annotations

import sqlite3
import sys
import time
from collections import defaultdict, Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Set, Tuple

from nltk.stem import SnowballStemmer

_STEMMER = SnowballStemmer("english")

_ROOT   = Path(__file__).resolve().parent.parent.parent
DB_PATH = str(_ROOT / "data" / "dbs" / "anomalies.db")

WINDOW_SECONDS      = 7200
FLASH_OVERLAP       = 0.3
SUSTAINED_OVERLAP   = 0.5
SUSTAINED_MIN_HOURS = 4
MAX_EVENT_SPAN      = WINDOW_SECONDS * 2

AGGREGATOR_STOPWORDS: Set[str] = {
    "people", "don", "because", "please", "good",
    "https", "www", "com", "reddit", "message", "post", "questions", "audio",
    "some", "think", "his", "were", "time",
    "comment", "comments", "karma", "upvote", "downvote", "thread",
    "moderator", "subreddit", "submission", "compose", "removed",
    "likes", "views", "million", "broken",
    "really", "looks", "wait", "man", "also", "much",
    "people", "person",
    "game", "play",
    "year", "new", "content",
    # Automod/modbot template — every subreddit uses the same reply boilerplate,
    # creating artificial cross-subreddit keyword overlap with zero topic signal.
    "thank", "unfortunately", "deleted", "following", "reason", "per",
    "hey", "scope", "limit", "rule",
    # Image metadata artifacts — r/popculturechat and similar embed image URL
    # fragments in comment bodies; DBSCAN clusters these as false "topics".
    "jpeg", "pjpg", "webp", "width", "format", "gif", "giphy",
    "redd", "auto", "preview",
}

_STEMMED_STOPWORDS: Set[str] = {_STEMMER.stem(w) for w in AGGREGATOR_STOPWORDS}


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class AggregatorConfig:
    window_seconds:      int   = WINDOW_SECONDS
    flash_overlap:       float = FLASH_OVERLAP
    sustained_overlap:   float = SUSTAINED_OVERLAP
    sustained_min_hours: int   = SUSTAINED_MIN_HOURS
    max_event_span:      int   = MAX_EVENT_SPAN


# ---------------------------------------------------------------------------
# Math helpers
# ---------------------------------------------------------------------------

def max_cluster_overlap(clusters_a: List[Set[str]], clusters_b: List[Set[str]]) -> float:
    """Szymkiewicz–Simpson overlap coefficient across all cluster pairs.
    Requires at least 2 shared keywords to prevent single-word false merges."""
    best = 0.0
    for ca in clusters_a:
        for cb in clusters_b:
            inter = ca & cb
            if len(inter) < 2:
                continue
            score = len(inter) / min(len(ca), len(cb))
            best = max(best, score)
    return best


def find_best_cluster_pair(
    anchor_tracks: List[Set[str]], curr_clusters: List[Set[str]]
) -> tuple[int, int, float]:
    """Returns (anchor_idx, curr_idx, score) of the highest-scoring cluster pair.
    Returns (-1, -1, 0.0) if no pair meets the min-2-intersection guard."""
    best_score = 0.0
    best_ai, best_ci = -1, -1
    for ai, ca in enumerate(anchor_tracks):
        for ci, cb in enumerate(curr_clusters):
            inter = ca & cb
            if len(inter) < 2:
                continue
            score = len(inter) / min(len(ca), len(cb))
            if score > best_score:
                best_score, best_ai, best_ci = score, ai, ci
    return best_ai, best_ci, best_score


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


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

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
        clusters: List[Set[str]] = []
        if r[6]:
            for chunk in r[6].split("|"):
                kw_set: Set[str] = set()
                for kw in chunk.split(", "):
                    kw = kw.strip()
                    if kw:
                        kw_set.add(kw)
                kw_set = {_STEMMER.stem(kw) for kw in kw_set}
                kw_set -= _STEMMED_STOPWORDS
                if kw_set:
                    clusters.append(kw_set)
        result.append({
            "id":           r[0],
            "subreddit":    r[1],
            "window_start": r[2],
            "window_end":   r[3],
            "count":        r[4],
            "z_score":      r[5],
            "clusters":     clusters,
        })
    return result


# ---------------------------------------------------------------------------
# Shared detectors (logic unchanged between phases)
# ---------------------------------------------------------------------------

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


def _split_by_span(component: List[Dict], max_span: int) -> List[List[Dict]]:
    sorted_c = sorted(component, key=lambda a: a["window_start"])
    groups: List[List[Dict]] = []
    current = [sorted_c[0]]
    for a in sorted_c[1:]:
        if a["window_start"] - current[0]["window_start"] <= max_span:
            current.append(a)
        else:
            groups.append(current)
            current = [a]
    groups.append(current)
    return groups


def detect_flash(
    anomalies: List[Dict], config: AggregatorConfig
) -> Tuple[List[Dict], Set[int]]:
    remaining = sorted(anomalies, key=lambda a: a["window_start"])
    n = len(remaining)
    uf = UnionFind(n)

    left = 0
    for right in range(1, n):
        while remaining[right]["window_start"] - remaining[left]["window_start"] > config.window_seconds:
            left += 1
        for k in range(left, right):
            if remaining[k]["subreddit"] != remaining[right]["subreddit"]:
                if max_cluster_overlap(remaining[k]["clusters"], remaining[right]["clusters"]) >= config.flash_overlap:
                    uf.union(k, right)

    components: Dict[int, List[Dict]] = defaultdict(list)
    for i, a in enumerate(remaining):
        components[uf.find(i)].append(a)

    flash_events: List[Dict] = []
    claimed_ids:  Set[int]   = set()

    for component in components.values():
        span = max(a["window_start"] for a in component) - min(a["window_start"] for a in component)
        sub_components = _split_by_span(component, config.max_event_span) if span > config.max_event_span else [component]

        for sub in sub_components:
            subreddits = list({a["subreddit"] for a in sub})
            if len(subreddits) < 2:
                continue
            kw_sets    = [set().union(*a["clusters"]) if a["clusters"] else set() for a in sub]
            shared_kws = top_shared_keywords(kw_sets)
            flash_events.append({
                "event_type":      "FLASH",
                "event_start":     min(a["window_start"] for a in sub),
                "event_end":       max(a["window_end"]   for a in sub),
                "subreddits":      subreddits,
                "shared_keywords": shared_kws,
                "peak_z_score":    max(a["z_score"] for a in sub),
                "anomaly_count":   len(sub),
                "anomaly_ids":     [a["id"] for a in sub],
            })
            claimed_ids.update(a["id"] for a in sub)

    return flash_events, claimed_ids


def detect_isolated(anomalies: List[Dict], claimed_ids: Set[int]) -> List[Dict]:
    events: List[Dict] = []
    for a in anomalies:
        if a["id"] in claimed_ids:
            continue
        shared_kws = top_shared_keywords([set().union(*a["clusters"]) if a["clusters"] else set()])
        events.append({
            "event_type":      "ISOLATED",
            "event_start":     a["window_start"],
            "event_end":       a["window_end"],
            "subreddits":      [a["subreddit"]],
            "shared_keywords": shared_kws,
            "peak_z_score":    a["z_score"],
            "anomaly_count":   1,
            "anomaly_ids":     [a["id"]],
        })
    return events


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


# ---------------------------------------------------------------------------
# Strategy classes
# ---------------------------------------------------------------------------

class Phase1Aggregator:
    """Tumbling-window aggregator for hourly anomaly data. Frozen — do not modify."""

    def __init__(self, config: AggregatorConfig = None) -> None:
        self.config = config or AggregatorConfig()

    def detect_sustained(
        self, anomalies: List[Dict], claimed_ids: Set[int]
    ) -> Tuple[List[Dict], Set[int]]:
        by_subreddit: Dict[str, List[Dict]] = defaultdict(list)
        for a in anomalies:
            if a["id"] not in claimed_ids:
                by_subreddit[a["subreddit"]].append(a)

        events: List[Dict] = []
        new_claimed: Set[int] = set()

        for subreddit, items in by_subreddit.items():
            items.sort(key=lambda x: x["window_start"])
            i = 0
            while i < len(items):
                chain = [items[i]]
                anchor_tracks: List[Set[str]] = (
                    [set(c) for c in items[i]["clusters"]] if items[i]["clusters"] else []
                )
                dominant_idx: int = -1

                j = i + 1
                while j < len(items):
                    prev, curr = items[j - 1], items[j]
                    if curr["window_start"] - prev["window_start"] != 3600:
                        break
                    if not anchor_tracks or not curr["clusters"]:
                        break
                    if dominant_idx == -1:
                        match = find_best_cluster_pair(anchor_tracks, curr["clusters"])
                        if match[2] >= self.config.sustained_overlap:
                            dominant_idx = match[0]
                            anchor_tracks[dominant_idx] |= curr["clusters"][match[1]]
                            chain.append(curr)
                            j += 1
                        else:
                            break
                    else:
                        match = find_best_cluster_pair([anchor_tracks[dominant_idx]], curr["clusters"])
                        if match[2] >= self.config.sustained_overlap:
                            anchor_tracks[dominant_idx] |= curr["clusters"][match[1]]
                            chain.append(curr)
                            j += 1
                        else:
                            break

                if len(chain) >= self.config.sustained_min_hours:
                    ids = [a["id"] for a in chain]
                    new_claimed.update(ids)
                    shared_kws = (
                        sorted(anchor_tracks[dominant_idx])[:8]
                        if dominant_idx != -1
                        else sorted(set().union(*chain[0]["clusters"]) if chain[0]["clusters"] else set())[:8]
                    )
                    events.append({
                        "event_type":      "SUSTAINED",
                        "event_start":     chain[0]["window_start"],
                        "event_end":       chain[-1]["window_end"],
                        "subreddits":      [subreddit],
                        "shared_keywords": shared_kws,
                        "peak_z_score":    max(a["z_score"] for a in chain),
                        "anomaly_count":   len(chain),
                        "anomaly_ids":     ids,
                    })
                    i = j
                else:
                    i += 1

        return events, new_claimed

    def run(self, conn: sqlite3.Connection) -> None:
        t_total = time.perf_counter()

        t0 = time.perf_counter()
        print("Loading anomalies...")
        anomalies = load_anomalies(conn)
        print(f"  {len(anomalies)} raw anomalies loaded.  ({time.perf_counter() - t0:.3f}s)\n")

        t0 = time.perf_counter()
        flash_events, flash_claimed = detect_flash(anomalies, self.config)
        print(f"  FLASH detection     : {len(flash_events)} events  ({time.perf_counter() - t0:.3f}s)")

        t0 = time.perf_counter()
        sustained_events, sustained_claimed = self.detect_sustained(anomalies, flash_claimed)
        print(f"  SUSTAINED detection : {len(sustained_events)} events  ({time.perf_counter() - t0:.3f}s)")

        t0 = time.perf_counter()
        isolated_events = detect_isolated(anomalies, flash_claimed | sustained_claimed)
        print(f"  ISOLATED            : {len(isolated_events)} events  ({time.perf_counter() - t0:.3f}s)")

        t0 = time.perf_counter()
        all_events = sorted(flash_events + sustained_events + isolated_events, key=lambda e: e["event_start"])
        save_events(conn, all_events)
        print(f"  Saved to DB         : {len(all_events)} total events  ({time.perf_counter() - t0:.3f}s)\n")

        flash_count     = sum(1 for e in all_events if e["event_type"] == "FLASH")
        sustained_count = sum(1 for e in all_events if e["event_type"] == "SUSTAINED")
        isolated_count  = sum(1 for e in all_events if e["event_type"] == "ISOLATED")

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
        print(f"         Total time: {time.perf_counter() - t_total:.3f}s")

        VAL_START = 1704045600
        VAL_END   = 1704074400
        val_anomalies = [
            a for a in anomalies
            if a["subreddit"] == "news" and VAL_START <= a["window_start"] <= VAL_END
        ]
        if val_anomalies:
            print(f"\n{'─' * 80}")
            print(f"Validation: Dec 31 18:00 → Jan 1 02:00  (r/news)  —  {len(val_anomalies)} anomalies")
            print(f"{'─' * 80}")
            for a in sorted(val_anomalies, key=lambda x: x["window_start"]):
                dt  = datetime.fromtimestamp(a["window_start"], tz=timezone.utc).strftime("%b %d %H:%M UTC")
                kws = sorted({kw for cluster in a["clusters"] for kw in cluster})[:8]
                tag = ("SUSTAINED" if a["id"] in sustained_claimed else
                       "FLASH"     if a["id"] in flash_claimed     else "ISOLATED")
                print(f"  {dt}  z={a['z_score']:>6.2f}  [{tag:9}]  {kws}")


class Phase2Aggregator(Phase1Aggregator):
    """Sliding-window aggregator for 5-minute continuous anomaly data.

    Overrides detect_sustained: collapses dense 5-minute rows into one hourly
    representative per clock-hour for keyword chain matching (preserving Phase 1
    semantics), then claims all fine-grained rows within the confirmed event window.
    """

    def _hourly_reps(self, items: List[Dict]) -> List[Dict]:
        """Peak-Z anomaly per clock-hour — one rep per hour for keyword matching."""
        by_hour: Dict[int, Dict] = {}
        for a in items:
            hour = a["window_end"] // 3600
            if hour not in by_hour or a["z_score"] > by_hour[hour]["z_score"]:
                by_hour[hour] = a
        return sorted(by_hour.values(), key=lambda x: x["window_start"])

    def detect_sustained(
        self, anomalies: List[Dict], claimed_ids: Set[int]
    ) -> Tuple[List[Dict], Set[int]]:
        by_subreddit: Dict[str, List[Dict]] = defaultdict(list)
        for a in anomalies:
            if a["id"] not in claimed_ids:
                by_subreddit[a["subreddit"]].append(a)

        events: List[Dict] = []
        new_claimed: Set[int] = set()

        for subreddit, items in by_subreddit.items():
            items.sort(key=lambda x: x["window_start"])
            reps = self._hourly_reps(items)

            i = 0
            while i < len(reps):
                chain = [reps[i]]
                anchor_tracks: List[Set[str]] = (
                    [set(c) for c in reps[i]["clusters"]] if reps[i]["clusters"] else []
                )
                dominant_idx: int = -1

                j = i + 1
                while j < len(reps):
                    prev, curr = reps[j - 1], reps[j]
                    # Allow up to 2-hour gap between hourly reps (handles sparse elevation hours)
                    if curr["window_start"] - prev["window_start"] > 7200:
                        break
                    if not anchor_tracks or not curr["clusters"]:
                        break
                    if dominant_idx == -1:
                        match = find_best_cluster_pair(anchor_tracks, curr["clusters"])
                        if match[2] >= self.config.sustained_overlap:
                            dominant_idx = match[0]
                            anchor_tracks[dominant_idx] |= curr["clusters"][match[1]]
                            chain.append(curr)
                            j += 1
                        else:
                            break
                    else:
                        match = find_best_cluster_pair([anchor_tracks[dominant_idx]], curr["clusters"])
                        if match[2] >= self.config.sustained_overlap:
                            anchor_tracks[dominant_idx] |= curr["clusters"][match[1]]
                            chain.append(curr)
                            j += 1
                        else:
                            break

                if len(chain) >= self.config.sustained_min_hours:
                    event_start = chain[0]["window_start"]
                    event_end   = chain[-1]["window_end"]
                    # Claim ALL fine-grained anomalies within the confirmed event window
                    all_ids = [
                        a["id"] for a in items
                        if a["window_start"] >= event_start and a["window_end"] <= event_end
                    ]
                    new_claimed.update(all_ids)
                    shared_kws = (
                        sorted(anchor_tracks[dominant_idx])[:8]
                        if dominant_idx != -1
                        else sorted(set().union(*chain[0]["clusters"]) if chain[0]["clusters"] else set())[:8]
                    )
                    peak_id_set = set(all_ids)
                    events.append({
                        "event_type":      "SUSTAINED",
                        "event_start":     event_start,
                        "event_end":       event_end,
                        "subreddits":      [subreddit],
                        "shared_keywords": shared_kws,
                        "peak_z_score":    max(a["z_score"] for a in items if a["id"] in peak_id_set),
                        "anomaly_count":   len(all_ids),
                        "anomaly_ids":     all_ids,
                    })
                    i = j
                else:
                    i += 1

        return events, new_claimed


# ---------------------------------------------------------------------------
# Entry point — wire Phase2Aggregator as the active strategy
# ---------------------------------------------------------------------------

def run() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    conn = sqlite3.connect(DB_PATH)
    Phase2Aggregator().run(conn)
    conn.close()


if __name__ == "__main__":
    run()
