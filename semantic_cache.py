from __future__ import annotations
"""
semantic_cache.py
=================
Concept Demonstrated:
  4. Semantic Caching — cache SQL query results so semantically
     similar questions (not just identical strings) get instant answers
     without calling the LLM or database again.

Implementation:
  We use TF-IDF vectorization + cosine similarity (pure scikit-learn).
  No external embedding server, no API key needed.
  "What are the top products by revenue?" and
  "Show me best-selling products by total revenue?" will both hit
  the same cache entry if their TF-IDF similarity > threshold.
"""

import json
import os
import pickle
import time
from dataclasses import dataclass, field, asdict
from typing import Any

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


CACHE_FILE = os.path.join(os.path.dirname(__file__), ".sql_cache.pkl")
DEFAULT_THRESHOLD = 0.82   # Cosine similarity threshold for a "hit"


@dataclass
class CacheEntry:
    """One cached result — question, SQL query, and the result data."""
    question:   str
    sql:        str
    result:     list[dict]    # rows as list-of-dicts
    columns:    list[str]
    db_label:   str
    created_at: float         = field(default_factory=time.time)
    hit_count:  int           = 0


@dataclass
class CacheHit:
    """Returned when a cache lookup succeeds."""
    entry:      CacheEntry
    similarity: float
    is_exact:   bool


class SemanticCache:
    """
    Concept 4 — Semantic Caching with TF-IDF + Cosine Similarity:

    How it works:
      1. When a question is answered, store (question → SQL → result).
      2. Next time a question comes in, vectorize it with TF-IDF.
      3. Compute cosine similarity against all cached question vectors.
      4. If similarity > threshold → CACHE HIT → return cached result.
      5. Otherwise → cache miss → proceed to LLM.

    Why TF-IDF instead of a neural embedding model?
      - Zero dependencies beyond scikit-learn (already required).
      - Fast on small caches (< 1000 entries).
      - Transparent: you can inspect the feature vectors.
      - Teaches the concept without the magic of embeddings.

    Why semantic (not exact string) matching?
      "Revenue by region" and "Total revenue per region breakdown"
      are the same question. Exact string matching would miss this.
    """

    def __init__(self, threshold: float = DEFAULT_THRESHOLD):
        self.threshold   = threshold
        self._entries:   list[CacheEntry] = []
        self._vectorizer = TfidfVectorizer(
            analyzer="word",
            ngram_range=(1, 2),   # unigrams + bigrams
            stop_words="english",
            lowercase=True,
            min_df=1,
        )
        self._matrix     = None   # TF-IDF matrix (n_entries × n_features)
        self._load()

    # ──────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────

    def lookup(self, question: str) -> CacheHit | None:
        """
        Searches the cache for a semantically similar question.
        Returns CacheHit if found, None if miss.
        """
        if not self._entries:
            return None

        try:
            q_vec    = self._vectorizer.transform([question])
            sims     = cosine_similarity(q_vec, self._matrix).flatten()
            best_idx = int(np.argmax(sims))
            best_sim = float(sims[best_idx])

            if best_sim >= self.threshold:
                entry = self._entries[best_idx]
                entry.hit_count += 1
                self._save()
                return CacheHit(
                    entry=entry,
                    similarity=best_sim,
                    is_exact=(best_sim == 1.0),
                )
        except Exception:
            # Cache not yet fitted or other error — treat as miss
            pass

        return None

    def store(
        self,
        question: str,
        sql: str,
        rows: list[list],
        columns: list[str],
        db_label: str = "",
    ) -> None:
        """
        Adds a new entry to the cache and re-fits the TF-IDF model.
        Re-fitting is cheap for small caches (< 1000 entries).
        """
        # Convert rows (list of lists) to list of dicts for storage
        result_dicts = [dict(zip(columns, row)) for row in rows]

        entry = CacheEntry(
            question=question,
            sql=sql,
            result=result_dicts,
            columns=columns,
            db_label=db_label,
        )
        self._entries.append(entry)
        self._refit_vectorizer()
        self._save()

    def similarity(self, q1: str, q2: str) -> float:
        """Returns the cosine similarity between two questions (for debugging)."""
        try:
            vecs = self._vectorizer.transform([q1, q2])
            return float(cosine_similarity(vecs[0:1], vecs[1:2])[0][0])
        except Exception:
            return 0.0

    def clear(self) -> None:
        """Clears the entire cache."""
        self._entries = []
        self._matrix  = None
        self._save()

    def stats(self) -> dict:
        """Returns cache statistics."""
        return {
            "total_entries": len(self._entries),
            "total_hits":    sum(e.hit_count for e in self._entries),
            "threshold":     self.threshold,
        }

    # ──────────────────────────────────────────────────────────
    # Internal Helpers
    # ──────────────────────────────────────────────────────────

    def _refit_vectorizer(self) -> None:
        """Re-trains the TF-IDF model on all stored questions."""
        if not self._entries:
            self._matrix = None
            return
        questions    = [e.question for e in self._entries]
        self._matrix = self._vectorizer.fit_transform(questions)

    def _save(self) -> None:
        """Persists the cache to disk."""
        try:
            with open(CACHE_FILE, "wb") as f:
                pickle.dump({
                    "entries":    self._entries,
                    "vectorizer": self._vectorizer,
                    "matrix":     self._matrix,
                }, f)
        except Exception:
            pass

    def _load(self) -> None:
        """Loads the cache from disk if it exists."""
        if not os.path.exists(CACHE_FILE):
            return
        try:
            with open(CACHE_FILE, "rb") as f:
                data = pickle.load(f)
            self._entries    = data.get("entries", [])
            self._vectorizer = data.get("vectorizer", self._vectorizer)
            self._matrix     = data.get("matrix", None)
        except Exception:
            # Corrupted cache — start fresh
            self._entries = []
