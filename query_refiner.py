from __future__ import annotations
"""
query_refiner.py
================
Concepts Demonstrated:
  3. Iterative Query Refinement — retries SQL generation up to 3× with
     structured error feedback from the executor.
  6. NL2SQL with Self-Correction — full pipeline: question → SQL →
     execute → fix (if needed) → verified result.

This is the heart of the agent's SQL intelligence.
"""

import re
import os
from groq import Groq
from dotenv import load_dotenv
from sql_executor import SQLExecutor, ExecutionResult
from schema_inspector import SchemaInspector

load_dotenv()

MAX_RETRIES = 3   # Maximum self-correction attempts


class QueryRefiner:
    """
    Concepts 3 & 6 — Iterative NL2SQL Self-Correction:

    Concept 6 — NL2SQL Pipeline:
      Natural language question → LLM generates SQL → Execute.

    Concept 3 — Iterative Refinement:
      If execution fails, the error (plus available columns) is sent
      BACK to the LLM as structured feedback so it can fix the query.
      This repeats up to MAX_RETRIES times.

    Why iterative refinement?
      Even the best LLMs make SQL mistakes:
        - Wrong column name (hallucination)
        - Incorrect JOIN syntax
        - Non-existent table
      Instead of giving up, we loop: generate → execute → fix → repeat.
      This mirrors how a human SQL developer debugs: write, run, fix.

    The key insight: by providing the LLM with STRUCTURED feedback
    (exact error type + available columns), not just "error occurred",
    we dramatically increase self-correction success rates.
    """

    def __init__(self, db_path: str):
        self.db_path  = db_path
        self.executor = SQLExecutor(db_path)
        self.inspector= SchemaInspector(db_path)
        self.client   = Groq(api_key=os.getenv("GROQ_API_KEY"))
        self.model    = "llama-3.3-70b-versatile"

    # ──────────────────────────────────────────────────────────
    # Concept 6: NL2SQL — Generate SQL from natural language
    # ──────────────────────────────────────────────────────────

    def generate_sql(self, question: str, schema_text: str) -> str:
        """
        First-pass SQL generation: sends the question + schema to the LLM.
        Returns a raw SQL string.
        """
        system_prompt = f"""You are a SQL expert. Generate a valid SQLite SELECT query.

{schema_text}

Rules:
- Use only SELECT statements. Never use DROP, DELETE, UPDATE, INSERT.
- Only reference tables and columns that exist in the schema above.
- Use proper SQLite syntax (e.g., strftime for dates).
- Return ONLY the SQL query — no explanation, no markdown fences.
- End the query with a semicolon."""

        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": f"Question: {question}"},
            ],
            temperature=0.1,   # Low temp for deterministic SQL generation
            max_tokens=512,
        )
        raw = response.choices[0].message.content.strip()
        return self._clean_sql(raw)

    # ──────────────────────────────────────────────────────────
    # Concept 3: Iterative Refinement — fix bad SQL with feedback
    # ──────────────────────────────────────────────────────────

    def refine(self, question: str, bad_sql: str, error_feedback: str, schema_text: str) -> str:
        """
        Self-correction pass: sends the failed query + structured error
        back to the LLM with a "fix this" instruction.

        The error_feedback comes from SQLExecutor._handle_operational_error()
        which includes: exact error, what column was wrong, what columns exist.
        This rich context is what makes self-correction effective.
        """
        system_prompt = f"""You are a SQL expert fixing a broken SQLite query.

{schema_text}

The following query failed with an error. Fix it and return ONLY the corrected SQL.
Return ONLY the fixed SQL query — no explanation, no markdown fences."""

        user_message = f"""Original question: {question}

Failed query:
{bad_sql}

Error feedback:
{error_feedback}

Fix the query and return ONLY the corrected SQL."""

        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_message},
            ],
            temperature=0.1,
            max_tokens=512,
        )
        raw = response.choices[0].message.content.strip()
        return self._clean_sql(raw)

    # ──────────────────────────────────────────────────────────
    # Full Pipeline: generate → execute → self-correct loop
    # ──────────────────────────────────────────────────────────

    def run_pipeline(self, question: str) -> dict:
        """
        Full NL2SQL + self-correction pipeline.

        Steps:
          1. Discover schema (Concept 2: Schema-Aware Prompting)
          2. Generate SQL from the question (Concept 6)
          3. Execute SQL
          4. If error → format feedback → refine SQL → retry (Concept 3)
          5. Return structured pipeline result

        Returns:
          {
            "success":          bool,
            "final_sql":        str,
            "result":           ExecutionResult,
            "attempts":         int,
            "retry_history":    [...],  # each attempt's SQL + error
            "schema_text":      str,
          }
        """
        # Step 1 — Schema-Aware Prompting
        schema      = self.inspector.discover_schema()
        schema_text = self.inspector.format_for_prompt(schema)

        retry_history = []
        current_sql   = None
        result        = None

        for attempt in range(1, MAX_RETRIES + 1):
            # Step 2/4 — Generate or Refine SQL
            if attempt == 1:
                current_sql = self.generate_sql(question, schema_text)
            else:
                current_sql = self.refine(
                    question, current_sql, result.llm_feedback, schema_text
                )

            # Step 3 — Execute
            result = self.executor.execute(current_sql)

            retry_history.append({
                "attempt":  attempt,
                "sql":      current_sql,
                "success":  result.success,
                "error":    result.error_msg if not result.success else None,
            })

            if result.success:
                break   # ✅ Success — exit the loop

            # If we've exhausted retries, exit with failure
            if attempt == MAX_RETRIES:
                break

        return {
            "success":       result.success if result else False,
            "final_sql":     current_sql,
            "result":        result,
            "attempts":      len(retry_history),
            "retry_history": retry_history,
            "schema_text":   schema_text,
        }

    # ──────────────────────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────────────────────

    def _clean_sql(self, raw: str) -> str:
        """
        Strips markdown code fences and extra whitespace from LLM output.
        LLMs frequently wrap SQL in ```sql ... ``` blocks despite instructions.
        """
        # Remove ```sql ... ``` or ``` ... ```
        raw = re.sub(r"```(?:sql)?\s*", "", raw, flags=re.IGNORECASE)
        raw = re.sub(r"```",            "", raw)
        # Remove lines that are just plain text explanations
        lines = [l for l in raw.splitlines() if l.strip()]
        # Return the first complete SELECT statement
        sql_lines = []
        in_select = False
        for line in lines:
            if re.match(r'^\s*SELECT\b', line, re.IGNORECASE):
                in_select = True
            if in_select:
                sql_lines.append(line)
                if line.rstrip().endswith(";"):
                    break
        return "\n".join(sql_lines).strip() if sql_lines else raw.strip()
