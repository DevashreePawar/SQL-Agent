from __future__ import annotations
"""
db_router.py
============
Concept Demonstrated:
  5. Multi-Database Routing — classifies the user question and picks
     the right database before any SQL is written.

In enterprise environments, data lives across multiple databases.
A smart agent must first decide WHERE to look, then HOW to query.
"""

import re
import os

# ── Keyword Maps ──────────────────────────────────────────────
# Each DB is associated with keywords that signal its domain.

SALES_KEYWORDS = [
    "sales", "revenue", "order", "orders", "product", "products",
    "customer", "customers", "purchase", "purchases", "region",
    "category", "categories", "item", "items", "price", "quantity",
    "sold", "bought", "top products", "best selling", "refund",
    "e-commerce", "ecommerce", "shopping",
]

HR_KEYWORDS = [
    "employee", "employees", "staff", "salary", "salaries",
    "department", "departments", "performance", "hire", "hired",
    "job title", "title", "bonus", "rating", "headcount",
    "workforce", "compensation", "review", "reviews", "manager",
    "hr", "human resources",
]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class DBRouter:
    """
    Concept 5 — Multi-Database Routing:

    Instead of always querying a single hardcoded database, the agent
    first classifies the user's question using keyword matching.

    This teaches a fundamental enterprise pattern:
      1. User asks a question in natural language.
      2. Agent classifies the question domain (sales vs HR vs both).
      3. Agent routes to the correct database(s).
      4. Only THEN does it write SQL.

    The classification uses keyword counting (deterministic, fast).
    A production system could use an LLM call for classification,
    but keyword matching is transparent and debuggable.
    """

    DATABASES = {
        "sales": os.path.join(BASE_DIR, "databases", "sales.db"),
        "hr":    os.path.join(BASE_DIR, "databases", "hr.db"),
    }

    def classify_question(self, question: str) -> str:
        """
        Returns one of: "sales", "hr", or "both".

        Scoring:
          - Count how many sales keywords appear in the question.
          - Count how many HR keywords appear.
          - If both are > 0 and close in score → "both"
          - Otherwise pick the dominant one.
        """
        q = question.lower()

        sales_score = sum(1 for kw in SALES_KEYWORDS if re.search(r'\b' + re.escape(kw) + r'\b', q))
        hr_score    = sum(1 for kw in HR_KEYWORDS    if re.search(r'\b' + re.escape(kw) + r'\b', q))

        if sales_score == 0 and hr_score == 0:
            # No clear signal — default to sales (most common)
            return "sales"
        if sales_score > 0 and hr_score > 0:
            # Both topics mentioned
            if abs(sales_score - hr_score) <= 1:
                return "both"
            return "sales" if sales_score > hr_score else "hr"
        return "sales" if sales_score > hr_score else "hr"

    def get_db_path(self, classification: str) -> str | list[str]:
        """
        Returns the file path(s) for the classified database.

        For "both", returns a list so the agent can query each
        database separately and merge the results.
        """
        if classification == "both":
            return [self.DATABASES["sales"], self.DATABASES["hr"]]
        return self.DATABASES.get(classification, self.DATABASES["sales"])

    def route(self, question: str) -> dict:
        """
        Full routing pipeline.
        Returns a routing_result dict with classification + db_path(s).

        Example:
          {
            "question":       "What is the average salary by dept?",
            "classification": "hr",
            "db_path":        "/path/to/hr.db",
            "db_label":       "HR Database",
            "sales_score":    0,
            "hr_score":       3,
          }
        """
        q          = question.lower()
        sales_sc   = sum(1 for kw in SALES_KEYWORDS if re.search(r'\b' + re.escape(kw) + r'\b', q))
        hr_sc      = sum(1 for kw in HR_KEYWORDS    if re.search(r'\b' + re.escape(kw) + r'\b', q))
        cls        = self.classify_question(question)
        db_path    = self.get_db_path(cls)

        label_map  = {"sales": "Sales Database", "hr": "HR Database", "both": "Both Databases"}

        return {
            "question":       question,
            "classification": cls,
            "db_path":        db_path,
            "db_label":       label_map[cls],
            "sales_score":    sales_sc,
            "hr_score":       hr_sc,
        }
