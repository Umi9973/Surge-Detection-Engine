from __future__ import annotations

import math
import re
from abc import ABC, abstractmethod
from collections import Counter, deque
from typing import Dict, List

import numpy as np

_BASELINE_WINDOW  = 7 * 24 * 3600  # 7-day rolling window in seconds
_SPIKE_THRESHOLD  = 3.0             # spike_ratio above which baseline penalty is skipped
_BASELINE_MIN_HRS = 24              # cold-start guard: no penalty until 24 hrs of history
from sentence_transformers import SentenceTransformer
from sklearn.cluster import DBSCAN
from umap import UMAP


class NLPContextEngine(ABC):
    @abstractmethod
    def summarize_anomaly(self, texts: List[str]) -> List[Dict]:
        raise NotImplementedError


class DBSCANContextEngine(NLPContextEngine):
    _STOPWORDS = {
        "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
        "of", "with", "is", "it", "this", "that", "was", "are", "be", "have",
        "has", "had", "do", "does", "did", "will", "would", "could", "should",
        "i", "you", "he", "she", "we", "they", "my", "your", "its", "their",
        "not", "no", "so", "if", "as", "up", "out", "just", "like", "get",
        "got", "can", "all", "one", "been", "from", "by", "about", "more",
        "what", "when", "how", "who", "which", "there", "they", "them",
        # Reaction / exclamation words — high frequency on Reddit, zero topic signal
        "lol", "lmao", "omg", "wtf", "wow", "omfg", "bruh", "bro", "dude",
        "haha", "hehe", "nah", "yep", "yup", "yeah",
        "damn", "hell", "fuck", "shit", "crap",
        # Internet acronyms
        "ngl", "imo", "tbh", "btw", "fyi", "iirc",
        # Weak function words and pronouns not caught by base stopwords
        "him", "her", "his", "why", "only", "even", "than", "see", "now",
        "most", "use", "very", "still", "then", "here",
        # Function words confirmed leaking into keywords via validation run
        # "any" → stems to "ani", "being"/"been" → stems to "be" via SnowballStemmer
        "any", "been", "being", "these", "other",
        # Contracted negatives — apostrophe stripped by regex leaves "didn", "don", "isn" etc.
        "didn", "don", "won", "isn", "wasn", "doesn", "wouldn", "couldn", "hadn", "shouldn",
    }

    # When DBSCAN finds only one cluster, IDF is flat (every word scores 1.0),
    # so TF alone drives selection and high-frequency generic words dominate.
    # This extended set is applied as a secondary filter in that case only.
    _SINGLE_CLUSTER_STOPWORDS = _STOPWORDS | {
        # Sentiment / modifier filler
        "really", "actually", "pretty", "quite", "still", "already",
        "never", "always", "maybe", "probably", "literally", "very",
        # Conversational words with no topic value
        "looks", "wait", "man", "guy", "yeah", "good", "bad", "great",
        "thing", "things", "someone", "anyone", "everyone", "anything",
        # Common verbs that carry no topic signal
        "want", "need", "know", "make", "take", "give", "look", "come",
        "think", "say", "use", "try", "keep", "let", "feel", "seem",
        # Reddit modbot / sidebar artifacts
        "removed", "submission", "compose", "moderator", "subreddit",
        "thread", "edit", "update", "deleted", "see", "also", "much",
    }

    def __init__(
        self,
        model_name: str = "all-MiniLM-L6-v2",
        umap_components: int = 2,
        dbscan_eps: float = 0.5,
        dbscan_min_samples: int = 5,
        top_keywords: int = 8,
    ) -> None:
        self._model = SentenceTransformer(model_name)
        self._umap = UMAP(n_components=umap_components, random_state=42, verbose=False)
        self._dbscan_eps = dbscan_eps
        self._dbscan_min_samples = dbscan_min_samples
        self._top_keywords = top_keywords
        # Rolling 7-day baseline for dynamic TF-IDF penalty
        self._baseline_counts: Counter = Counter()
        self._hour_buckets: deque = deque()  # (window_start: int, counts: Counter)

    def update_baseline(self, window_start: int, token_counts: Counter) -> None:
        """Accumulate token counts into the rolling baseline; prune buckets older than 7 days."""
        self._hour_buckets.append((window_start, token_counts))
        self._baseline_counts.update(token_counts)
        while self._hour_buckets:
            oldest_ts, oldest_counts = self._hour_buckets[0]
            if window_start - oldest_ts > _BASELINE_WINDOW:
                self._hour_buckets.popleft()
                self._baseline_counts.subtract(oldest_counts)
            else:
                break

    def _tokenize(self, text: str) -> List[str]:
        tokens = re.findall(r"\b[a-z]{3,}\b", text.lower())
        return [t for t in tokens if t not in self._STOPWORDS]

    def _ctfidf_keywords(self, cluster_texts: List[List[str]], target_idx: int) -> List[str]:
        """Class-based TF-IDF: treats each cluster as one document."""
        cluster_docs = [" ".join(tokens) for tokens in cluster_texts]
        n_clusters = len(cluster_docs)

        target_tokens = cluster_texts[target_idx]
        if not target_tokens:
            return []

        tf_raw = Counter(target_tokens)
        total = sum(tf_raw.values())
        tf = {word: count / total for word, count in tf_raw.items()}

        all_tokens = [set(tokens) for tokens in cluster_texts]
        idf = {}
        for word in tf:
            doc_freq = sum(1 for token_set in all_tokens if word in token_set)
            idf[word] = math.log((n_clusters + 1) / (doc_freq + 1)) + 1

        scores = {word: tf[word] * idf[word] for word in tf}

        # Dynamic baseline penalty: suppress chronically common words, preserve spikes
        n_hours = len(self._hour_buckets)
        if n_hours >= _BASELINE_MIN_HRS:
            dynamic_scores: dict = {}
            for word, raw_score in scores.items():
                C_baseline    = self._baseline_counts.get(word, 0)
                baseline_rate = C_baseline / n_hours
                C_current     = tf_raw.get(word, 0)
                spike_ratio   = C_current / max(0.1, baseline_rate)
                if spike_ratio >= _SPIKE_THRESHOLD:
                    dynamic_scores[word] = raw_score          # spiking — no penalty
                else:
                    dynamic_scores[word] = raw_score / (1 + math.log(baseline_rate + 1))
            scores = dynamic_scores

        ranked = [w for w, _ in sorted(scores.items(), key=lambda x: -x[1])][: self._top_keywords]

        if n_clusters == 1:
            filtered = [w for w in ranked if w not in self._SINGLE_CLUSTER_STOPWORDS]
            return filtered or ranked
        return ranked

    def summarize_anomaly(self, texts: List[str], window_start: int = 0) -> List[Dict]:
        if len(texts) < self._dbscan_min_samples:
            return []

        embeddings = self._model.encode(texts, show_progress_bar=False)
        reduced = self._umap.fit_transform(embeddings)
        labels = DBSCAN(eps=self._dbscan_eps, min_samples=self._dbscan_min_samples).fit_predict(reduced)

        clusters: Dict[int, List[str]] = {}
        for label, text in zip(labels, texts):
            clusters.setdefault(label, []).append(text)

        cluster_token_lists = {
            label: [token for t in txts for token in self._tokenize(t)]
            for label, txts in clusters.items()
            if label != -1
        }

        if not cluster_token_lists:
            return []

        # Update rolling baseline from all texts (including DBSCAN noise points)
        if window_start:
            all_tokens = Counter(token for text in texts for token in self._tokenize(text))
            self.update_baseline(window_start, all_tokens)

        valid_labels = list(cluster_token_lists.keys())
        all_token_lists = [cluster_token_lists[l] for l in valid_labels]

        results = []
        for i, label in enumerate(valid_labels):
            keywords = self._ctfidf_keywords(all_token_lists, i)
            results.append({
                "cluster_id":  int(label),
                "size":        len(clusters[label]),
                "keywords":    keywords,
                "noise_count": len(clusters.get(-1, [])),
            })

        return sorted(results, key=lambda x: -x["size"])


if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding="utf-8")

    game_delay_comments = [
        f"The game delay is really disappointing, I was looking forward to the release date so much. Comment {i}"
        for i in range(40)
    ]
    noise_comments = [
        "I love eating pizza on weekends with my friends.",
        "The weather today is absolutely beautiful outside.",
        "Just finished reading a great book about history.",
        "My cat knocked over my coffee this morning again.",
        "Does anyone have a good recipe for chocolate cake?",
        "Just got back from a long hiking trip in the mountains.",
        "The new restaurant downtown has amazing pasta dishes.",
        "I can't believe how fast this year has gone by.",
        "Looking forward to visiting my family next holiday.",
        "Finally finished that home renovation project today.",
    ]

    mock_texts = game_delay_comments + noise_comments

    engine = DBSCANContextEngine()
    results = engine.summarize_anomaly(mock_texts)

    print(f"Total comments passed: {len(mock_texts)}")
    print(f"Clusters found: {len(results)}\n")
    for r in results:
        print(f"  Cluster {r['cluster_id']} | size={r['size']} | noise={r['noise_count']} | keywords={r['keywords']}")
