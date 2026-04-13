"""
sql_executor.py
===============
Concept Demonstrated:
  3. Iterative Query Refinement (the execution side) — safe SQL runner
     that captures structured error messages suitable for LLM feedback.

Key safety feature: only SELECT queries are allowed.
DROP, DELETE, UPDATE, INSERT are blocked at the gate.
"""

import sqlite3
import re
import pandas as pd
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ExecutionResult:
    """
    Structured result returned by SQLExecutor.execute().

    Having a typed result (vs a raw tuple) lets the agent code
    handle success and failure uniformly without nested if/else.
    """
    success:      bool
    query:        str
    columns:      list[str]          = field(default_factory=list)
    rows:         list[list[Any]]    = field(default_factory=list)
    row_count:    int                = 0
    error_type:   str | None        = None   # e.g. "OperationalError"
    error_msg:    str | None        = None   # raw sqlite error
    error_hint:   str | None        = None   # human-readable hint for LLM
    llm_feedback: str | None        = None   # formatted feedback for self-correction


# ── SQL Safety Guard ─────────────────────────────────────────
BLOCKED_KEYWORDS = re.compile(
    r'\b(DROP|DELETE|UPDATE|INSERT|ALTER|CREATE|TRUNCATE|REPLACE|PRAGMA\s+\w+=)\b',
    re.IGNORECASE,
)


class SQLExecutor:
    """
    Concept 3 — Safe SQL Execution + Structured Error Feedback:

    When the LLM generates a bad SQL query, we don't just crash.
    We:
      1. Catch the specific sqlite3 exception type.
      2. Format a human-readable error message.
      3. Include the available columns / tables as hints.
      4. Return everything as llm_feedback so the QueryRefiner
         can send it back to the LLM to fix the query.

    This structured feedback loop is what makes self-correction work.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path

    # ──────────────────────────────────────────────────────────
    # Core Execution
    # ──────────────────────────────────────────────────────────

    def execute(self, query: str) -> ExecutionResult:
        """
        Runs a SQL query and returns a fully-structured ExecutionResult.
        Rejects any non-SELECT statement before it reaches SQLite.
        """
        query = query.strip().rstrip(";")

        # Safety: block destructive SQL
        if BLOCKED_KEYWORDS.search(query):
            return ExecutionResult(
                success=False,
                query=query,
                error_type="SecurityError",
                error_msg="Query contains disallowed keywords.",
                error_hint="Only SELECT statements are permitted.",
                llm_feedback=(
                    "SECURITY BLOCK: Your query contains a disallowed keyword "
                    "(DROP/DELETE/UPDATE/INSERT/etc). "
                    "You may only write SELECT queries. Please rewrite the query."
                ),
            )

        # Only allow SELECT
        if not re.match(r'^\s*SELECT\b', query, re.IGNORECASE):
            return ExecutionResult(
                success=False,
                query=query,
                error_type="NotASelectError",
                error_msg="Query does not start with SELECT.",
                llm_feedback=(
                    "ERROR: Your query must be a SELECT statement. "
                    "Please rewrite it to start with SELECT."
                ),
            )

        # Execute
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cur  = conn.cursor()
            cur.execute(query)
            raw_rows = cur.fetchall()
            columns  = [d[0] for d in cur.description] if cur.description else []
            rows     = [list(r) for r in raw_rows]
            conn.close()

            return ExecutionResult(
                success=True,
                query=query,
                columns=columns,
                rows=rows,
                row_count=len(rows),
            )

        except sqlite3.OperationalError as e:
            return self._handle_operational_error(query, e)
        except sqlite3.ProgrammingError as e:
            return ExecutionResult(
                success=False,
                query=query,
                error_type="ProgrammingError",
                error_msg=str(e),
                llm_feedback=f"SQL ProgrammingError: {e}\nPlease fix the syntax and rewrite the query.",
            )
        except Exception as e:
            return ExecutionResult(
                success=False,
                query=query,
                error_type=type(e).__name__,
                error_msg=str(e),
                llm_feedback=f"Unexpected error ({type(e).__name__}): {e}\nPlease rewrite the query.",
            )

    # ──────────────────────────────────────────────────────────
    # Structured Error Formatting (key to self-correction)
    # ──────────────────────────────────────────────────────────

    def _handle_operational_error(self, query: str, error: Exception) -> ExecutionResult:
        """
        Parses sqlite3.OperationalError and generates rich LLM feedback.

        This is the core of Concept 3:
        Instead of just saying "error", we tell the LLM:
          - What went wrong
          - What the valid alternatives are
          - What to fix
        """
        msg = str(error)
        hint = None
        llm_feedback = None

        # Pattern: "no such column: X"
        m = re.search(r"no such column: (\S+)", msg, re.IGNORECASE)
        if m:
            bad_col = m.group(1)
            # Fetch real columns from the tables referenced in the query
            real_cols = self._get_columns_from_query(query)
            hint = f"Column '{bad_col}' does not exist."
            llm_feedback = (
                f"SQL Error: Column '{bad_col}' does not exist.\n"
                f"Actual columns available: {real_cols}\n"
                f"Original query was:\n{query}\n"
                f"Please rewrite using only the listed columns."
            )

        # Pattern: "no such table: X"
        elif re.search(r"no such table: (\S+)", msg, re.IGNORECASE):
            m2 = re.search(r"no such table: (\S+)", msg, re.IGNORECASE)
            bad_table = m2.group(1) if m2 else "unknown"
            real_tables = self._get_real_tables()
            llm_feedback = (
                f"SQL Error: Table '{bad_table}' does not exist.\n"
                f"Available tables: {real_tables}\n"
                f"Please rewrite using one of these table names."
            )

        # Syntax error
        elif "syntax error" in msg.lower():
            llm_feedback = (
                f"SQL Syntax Error: {msg}\n"
                f"Original query:\n{query}\n"
                f"Please fix the SQL syntax and rewrite the query."
            )

        else:
            llm_feedback = (
                f"SQL OperationalError: {msg}\n"
                f"Original query:\n{query}\n"
                f"Please analyze the error and rewrite the query."
            )

        return ExecutionResult(
            success=False,
            query=query,
            error_type="OperationalError",
            error_msg=msg,
            error_hint=hint,
            llm_feedback=llm_feedback,
        )

    def _get_real_tables(self) -> list[str]:
        """Lists all tables in the connected database."""
        try:
            conn = sqlite3.connect(self.db_path)
            cur  = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [r[0] for r in cur.fetchall()]
            conn.close()
            return tables
        except Exception:
            return []

    def _get_columns_from_query(self, query: str) -> dict[str, list[str]]:
        """
        Extracts table names from the FROM/JOIN clauses of a query,
        then returns their actual column names.
        """
        table_pattern = re.compile(
            r'\bFROM\s+(\w+)|\bJOIN\s+(\w+)', re.IGNORECASE
        )
        tables_in_query = []
        for m in table_pattern.finditer(query):
            t = m.group(1) or m.group(2)
            if t:
                tables_in_query.append(t)

        result = {}
        try:
            conn = sqlite3.connect(self.db_path)
            cur  = conn.cursor()
            for table in set(tables_in_query):
                cur.execute(f"PRAGMA table_info({table})")
                cols = [row[1] for row in cur.fetchall()]
                if cols:
                    result[table] = cols
            conn.close()
        except Exception:
            pass
        return result

    # ──────────────────────────────────────────────────────────
    # Result Conversion
    # ──────────────────────────────────────────────────────────

    def to_dataframe(self, result: ExecutionResult) -> pd.DataFrame | None:
        """Converts a successful ExecutionResult to a pandas DataFrame."""
        if not result.success or not result.columns:
            return None
        return pd.DataFrame(result.rows, columns=result.columns)

    def to_markdown_table(self, result: ExecutionResult, max_rows: int = 20) -> str:
        """Returns a markdown table string for the report."""
        if not result.success:
            return f"❌ Query failed: {result.error_msg}"
        df = self.to_dataframe(result)
        if df is None or df.empty:
            return "_(No rows returned)_"
        return df.head(max_rows).to_markdown(index=False)
