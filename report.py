"""
report.py
=========
Markdown report generator.

Produces a rich, human-readable analysis report containing:
  - The original question
  - Database used + routing decision (Concept 5)
  - Final SQL query
  - Data table
  - Self-correction history (if any retries — Concept 3)
  - LLM-generated insights (Concept 2: schema-aware analysis)
  - Cache status (Concept 4)
"""

import os
import time
from datetime import datetime
from sql_executor import ExecutionResult, SQLExecutor
from memory import AgentMemory


class ReportGenerator:
    """Generates a structured Markdown report from the agent's session."""

    def __init__(self, output_dir: str = "output"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def generate(
        self,
        question:      str,
        routing:       dict,
        pipeline:      dict,
        insights:      str,
        memory:        AgentMemory,
        cache_hit:     bool = False,
        cache_sim:     float | None = None,
    ) -> str:
        """
        Builds the full Markdown report and saves it to output/.
        Returns the file path.
        """
        result: ExecutionResult = pipeline["result"]
        now    = datetime.now()
        fname  = f"report_{now.strftime('%Y%m%d_%H%M%S')}.md"
        fpath  = os.path.join(self.output_dir, fname)

        lines = []

        # ── Header ───────────────────────────────────────────
        lines += [
            "# 📊 SQL Data Analyst Agent — Report",
            "",
            f"**Generated:** {now.strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "---",
            "",
        ]

        # ── Question ──────────────────────────────────────────
        lines += [
            "## ❓ Question",
            "",
            f"> {question}",
            "",
            "---",
            "",
        ]

        # ── Routing Decision (Concept 5) ─────────────────────
        lines += [
            "## 🗂️ Database Routing  _(Concept 5: Multi-DB Routing)_",
            "",
            f"| Field | Value |",
            f"|---|---|",
            f"| Database Selected | **{routing['db_label']}** |",
            f"| Classification | `{routing['classification']}` |",
            f"| Sales Keywords Found | {routing['sales_score']} |",
            f"| HR Keywords Found | {routing['hr_score']} |",
            "",
            "---",
            "",
        ]

        # ── Cache Status (Concept 4) ──────────────────────────
        if cache_hit:
            lines += [
                "## ⚡ Semantic Cache  _(Concept 4: Semantic Caching)_",
                "",
                f"> **Cache HIT** — Similarity score: `{cache_sim:.4f}`",
                "> This result was retrieved from the semantic cache without calling the LLM or database.",
                "",
                "---",
                "",
            ]
        else:
            lines += [
                "## ⚡ Semantic Cache  _(Concept 4: Semantic Caching)_",
                "",
                "> **Cache MISS** — Result computed fresh and stored for future queries.",
                "",
                "---",
                "",
            ]

        # ── SQL Query (Concepts 1, 2, 6) ─────────────────────
        lines += [
            "## 🔍 SQL Query  _(Concepts 1, 2, 6: Dynamic Tools · Schema-Aware · NL2SQL)_",
            "",
            "```sql",
            pipeline["final_sql"],
            "```",
            "",
        ]

        # ── Self-Correction History (Concept 3) ──────────────
        attempts = pipeline.get("attempts", 1)
        history  = pipeline.get("retry_history", [])
        if attempts > 1:
            lines += [
                "## 🔄 Self-Correction Log  _(Concept 3: Iterative Query Refinement)_",
                "",
                f"The agent required **{attempts} attempt(s)** to produce a working query.",
                "",
            ]
            for h in history:
                status = "✅ Success" if h["success"] else f"❌ Failed — `{h['error']}`"
                lines += [
                    f"### Attempt {h['attempt']}: {status}",
                    "```sql",
                    h["sql"],
                    "```",
                    "",
                ]
            lines += ["---", ""]
        else:
            lines += [
                "## 🔄 Self-Correction  _(Concept 3)_",
                "",
                "> ✅ Query succeeded on the **first attempt** — no self-correction needed.",
                "",
                "---",
                "",
            ]

        # ── Results Table ─────────────────────────────────────
        lines += [
            "## 📋 Results",
            "",
        ]
        if result.success:
            lines += [
                f"**{result.row_count} rows returned.**",
                "",
            ]
            # Build markdown table
            executor = SQLExecutor.__new__(SQLExecutor)
            md_table = executor.to_markdown_table(result, max_rows=25)
            lines += [md_table, ""]
        else:
            lines += [
                f"> ❌ Final query failed after {attempts} attempts.",
                f"> Error: `{result.error_msg}`",
                "",
            ]
        lines += ["---", ""]

        # ── Insights (LLM analysis) ───────────────────────────
        lines += [
            "## 💡 Insights & Analysis",
            "",
            insights,
            "",
            "---",
            "",
        ]

        # ── Session Summary ───────────────────────────────────
        lines += [
            "## 📈 Session Summary",
            "",
            "```",
            memory.summary(),
            "```",
            "",
            "---",
            "",
            "_Report generated by SQL Data Analyst Agent — Project 3_",
        ]

        # ── Write file ────────────────────────────────────────
        content = "\n".join(lines)
        with open(fpath, "w", encoding="utf-8") as f:
            f.write(content)

        return fpath
