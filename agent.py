from __future__ import annotations
"""
agent.py
========
Core ReAct-style agent orchestrator with 3 phases:

  DISCOVER → QUERY → ANALYZE

This is the "brain" that ties all modules together:
  - DBRouter          (Concept 5: Multi-DB Routing)
  - SchemaInspector   (Concepts 1 & 2: Dynamic Tools + Schema-Aware Prompting)
  - SemanticCache     (Concept 4: Semantic Caching)
  - QueryRefiner      (Concepts 3 & 6: Iterative Refinement + NL2SQL)
  - AgentMemory       (structured session memory)
  - ReportGenerator   (markdown output)
"""

import os
from groq import Groq
from dotenv import load_dotenv
from colorama import Fore, Style, init as colorama_init

from db_router import DBRouter
from schema_inspector import SchemaInspector
from semantic_cache import SemanticCache
from query_refiner import QueryRefiner
from sql_executor import SQLExecutor
from memory import AgentMemory
from report import ReportGenerator
from tools import list_tables, describe_table

load_dotenv()
colorama_init(autoreset=True)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def _banner(phase: str, msg: str) -> None:
    colors = {
        "DISCOVER": Fore.CYAN,
        "QUERY":    Fore.YELLOW,
        "ANALYZE":  Fore.GREEN,
        "CACHE":    Fore.MAGENTA,
        "ERROR":    Fore.RED,
        "DONE":     Fore.GREEN,
    }
    c = colors.get(phase, Fore.WHITE)
    print(f"\n{c}{'─'*55}")
    print(f"{c}[{phase}] {msg}")
    print(f"{c}{'─'*55}{Style.RESET_ALL}")


class SQLAnalystAgent:
    """
    SQL Data Analyst Agent — 3-phase ReAct orchestrator.

    Phase 1: DISCOVER
      - Route question to correct database (Concept 5)
      - Check semantic cache (Concept 4)
      - Inspect schema, generate dynamic tools (Concepts 1 & 2)

    Phase 2: QUERY
      - Run NL2SQL pipeline with self-correction (Concepts 3 & 6)
      - Store successful result in semantic cache

    Phase 3: ANALYZE
      - Send results + schema to LLM for insight generation
      - Generate final Markdown report
    """

    def __init__(self):
        self.router    = DBRouter()
        self.cache     = SemanticCache()
        self.memory    = AgentMemory()
        self.reporter  = ReportGenerator(
            output_dir=os.path.join(BASE_DIR, "output")
        )
        self.client    = Groq(api_key=os.getenv("GROQ_API_KEY"))
        self.model     = "llama-3.3-70b-versatile"

    # ──────────────────────────────────────────────────────────
    # Main Entry Point
    # ──────────────────────────────────────────────────────────

    def run(self, question: str) -> str:
        """
        Runs the full 3-phase pipeline for a given question.
        Returns the path to the generated report.
        """
        self.memory.log_phase("START", question)
        self.memory.set("question", question)

        # ═══════════════════════════════════════════════════
        # PHASE 1: DISCOVER
        # ═══════════════════════════════════════════════════
        _banner("DISCOVER", "Routing question to correct database...")
        routing = self._phase_discover(question)
        db_path = routing["db_path"]
        if isinstance(db_path, list):
            db_path = db_path[0]   # For "both": use sales DB first

        _banner("DISCOVER", f"→ {routing['db_label']}  (sales: {routing['sales_score']} | hr: {routing['hr_score']})")
        self.memory.set("routing", routing)
        self.memory.set("db_path",  db_path)
        self.memory.set("db_label", routing["db_label"])

        # ═══════════════════════════════════════════════════
        # PHASE 2: QUERY  (with cache check first)
        # ═══════════════════════════════════════════════════
        _banner("QUERY", "Checking semantic cache...")

        cache_result = self._check_cache(question)
        if cache_result:
            # ── CACHE HIT ──
            _banner("CACHE", f"Hit! Similarity={cache_result.similarity:.4f}  (skipping LLM + DB)")
            cached_sql    = cache_result.entry.sql
            cached_rows   = [list(r.values()) for r in cache_result.entry.result]
            cached_cols   = cache_result.entry.columns

            # Reconstruct a pipeline-like dict from cache
            from sql_executor import ExecutionResult
            fake_result = ExecutionResult(
                success=True,
                query=cached_sql,
                columns=cached_cols,
                rows=cached_rows,
                row_count=len(cached_rows),
            )
            pipeline = {
                "success":       True,
                "final_sql":     cached_sql,
                "result":        fake_result,
                "attempts":      1,
                "retry_history": [{"attempt": 1, "sql": cached_sql, "success": True, "error": None}],
                "schema_text":   "",
            }
            cache_hit = True
            cache_sim = cache_result.similarity

        else:
            # ── CACHE MISS → run NL2SQL pipeline ──
            _banner("QUERY", "Cache miss — running NL2SQL pipeline...")
            pipeline  = self._phase_query(question, db_path)
            cache_hit = False
            cache_sim = None

            # Store in cache if successful
            if pipeline["success"]:
                r = pipeline["result"]
                self.cache.store(
                    question=question,
                    sql=pipeline["final_sql"],
                    rows=r.rows,
                    columns=r.columns,
                    db_label=routing["db_label"],
                )
                _banner("CACHE", "Result stored in semantic cache for future queries.")

        # Record in memory
        self.memory.record_query(
            question=question,
            sql=pipeline["final_sql"],
            success=pipeline["success"],
            row_count=pipeline["result"].row_count if pipeline["result"] else 0,
            attempts=pipeline["attempts"],
            db_label=routing["db_label"],
        )

        # ═══════════════════════════════════════════════════
        # PHASE 3: ANALYZE
        # ═══════════════════════════════════════════════════
        _banner("ANALYZE", "Generating insights from results...")
        insights = self._phase_analyze(question, pipeline, routing)

        # ── Generate Report ───────────────────────────────
        report_path = self.reporter.generate(
            question=question,
            routing=routing,
            pipeline=pipeline,
            insights=insights,
            memory=self.memory,
            cache_hit=cache_hit,
            cache_sim=cache_sim,
        )

        _banner("DONE", f"Report saved → {report_path}")
        return report_path

    # ──────────────────────────────────────────────────────────
    # Phase 1: DISCOVER
    # ──────────────────────────────────────────────────────────

    def _phase_discover(self, question: str) -> dict:
        """Routes question to the correct database."""
        self.memory.log_phase("DISCOVER", question)
        routing = self.router.route(question)

        # Log schema (if not already cached in memory)
        db_path = routing["db_path"]
        if isinstance(db_path, list):
            db_path = db_path[0]

        if not self.memory.get_schema(db_path):
            inspector = SchemaInspector(db_path)
            schema    = inspector.discover_schema()
            self.memory.store_schema(db_path, schema)

            # Log table overview
            tables = list_tables(db_path)
            print(Fore.CYAN + tables)

        return routing

    def _check_cache(self, question: str):
        """Looks up the semantic cache. Returns CacheHit or None."""
        return self.cache.lookup(question)

    # ──────────────────────────────────────────────────────────
    # Phase 2: QUERY
    # ──────────────────────────────────────────────────────────

    def _phase_query(self, question: str, db_path: str) -> dict:
        """
        Runs the full NL2SQL + self-correction pipeline.
        Logs errors to memory as they happen.
        """
        self.memory.log_phase("QUERY", question)
        refiner  = QueryRefiner(db_path)
        pipeline = refiner.run_pipeline(question)

        # Log any errors that occurred during retry
        for h in pipeline.get("retry_history", []):
            if not h["success"] and h["error"]:
                self.memory.log_error(
                    sql=h["sql"],
                    error_type="SQLError",
                    error_msg=h["error"],
                    feedback=h["error"],
                    attempt=h["attempt"],
                )

        # Print attempt summary
        attempts = pipeline["attempts"]
        if attempts > 1:
            print(Fore.YELLOW + f"⚠  Self-correction needed: {attempts} attempts (max {3})")
        else:
            print(Fore.GREEN + "✅  Query succeeded on first attempt.")

        print(Fore.YELLOW + f"\nFinal SQL:\n{pipeline['final_sql']}\n")
        return pipeline

    # ──────────────────────────────────────────────────────────
    # Phase 3: ANALYZE
    # ──────────────────────────────────────────────────────────

    def _phase_analyze(self, question: str, pipeline: dict, routing: dict) -> str:
        """
        Sends the query results + schema to the LLM for insight generation.
        Uses Schema-Aware Prompting (Concept 2) — the schema is in the prompt.
        """
        self.memory.log_phase("ANALYZE", question)
        result = pipeline["result"]

        if not result or not result.success:
            return "❌ Could not generate insights — the SQL query failed."

        # Build a text summary of the results for the LLM
        executor = SQLExecutor.__new__(SQLExecutor)
        md_table = executor.to_markdown_table(result, max_rows=20)

        schema_text = pipeline.get("schema_text", "")

        system_prompt = (
            "You are a senior data analyst. You have been given query results "
            "from a business database. Provide a concise, insightful analysis "
            "with bullet points. Focus on key trends, anomalies, and actionable "
            "recommendations. Keep it under 300 words."
        )

        user_msg = (
            f"Question asked: {question}\n\n"
            f"SQL used:\n```sql\n{pipeline['final_sql']}\n```\n\n"
            f"Results ({result.row_count} rows):\n{md_table}\n\n"
            f"Database: {routing['db_label']}\n\n"
            "Please provide 3–5 bullet-point insights."
        )

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_msg},
                ],
                temperature=0.4,
                max_tokens=512,
            )
            insights = response.choices[0].message.content.strip()
        except Exception as e:
            insights = f"_(Insight generation failed: {e})_"

        print(Fore.GREEN + f"\nInsights:\n{insights}\n")
        return insights
