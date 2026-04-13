from __future__ import annotations
"""
memory.py
=========
Upgraded memory module with SQL-specific slots.

Compared to Project 2's memory (conversation history + reflection log),
this adds:
  - schema_memory   : stores the discovered DB schema so it's not re-fetched
  - query_history   : all SQL queries + results for this session
  - error_log       : structured error records for self-correction context
  - phase_log       : records which phase the agent is in and when
"""

from dataclasses import dataclass, field
from typing import Any
import time


@dataclass
class QueryRecord:
    """A record of one SQL query attempt and its result."""
    question:   str
    sql:        str
    success:    bool
    row_count:  int
    attempts:   int        # how many tries before success (Concept 3)
    db_label:   str
    timestamp:  float = field(default_factory=time.time)


@dataclass
class ErrorRecord:
    """Structured error entry for self-correction context."""
    sql:        str
    error_type: str
    error_msg:  str
    feedback:   str
    attempt:    int
    timestamp:  float = field(default_factory=time.time)


class AgentMemory:
    """
    Central memory store for the SQL Data Analyst Agent.

    Slots:
      conversation_history  — list of {role, content} dicts for LLM calls
      schema_memory         — discovered DB schema (dict, keyed by db_path)
      query_history         — list of QueryRecord
      error_log             — list of ErrorRecord
      phase_log             — list of {phase, message, timestamp}
      session_metadata      — misc session info (question, db used, etc.)
    """

    def __init__(self):
        self.conversation_history: list[dict]          = []
        self.schema_memory:        dict[str, Any]      = {}  # db_path → schema dict
        self.query_history:        list[QueryRecord]   = []
        self.error_log:            list[ErrorRecord]   = []
        self.phase_log:            list[dict]          = []
        self.session_metadata:     dict[str, Any]      = {}

    # ── Conversation ─────────────────────────────────────────

    def add_message(self, role: str, content: str) -> None:
        """Appends a message to the conversation history."""
        self.conversation_history.append({"role": role, "content": content})

    def get_messages(self) -> list[dict]:
        return self.conversation_history

    def trim_history(self, keep_last: int = 10) -> None:
        """Keeps only the last N messages to avoid token overflow."""
        if len(self.conversation_history) > keep_last:
            self.conversation_history = self.conversation_history[-keep_last:]

    # ── Schema Memory ─────────────────────────────────────────

    def store_schema(self, db_path: str, schema: dict) -> None:
        self.schema_memory[db_path] = schema

    def get_schema(self, db_path: str) -> dict | None:
        return self.schema_memory.get(db_path)

    # ── Query History ─────────────────────────────────────────

    def record_query(
        self,
        question: str,
        sql: str,
        success: bool,
        row_count: int,
        attempts: int,
        db_label: str,
    ) -> None:
        self.query_history.append(QueryRecord(
            question=question,
            sql=sql,
            success=success,
            row_count=row_count,
            attempts=attempts,
            db_label=db_label,
        ))

    def get_query_history(self) -> list[QueryRecord]:
        return self.query_history

    # ── Error Log ─────────────────────────────────────────────

    def log_error(
        self,
        sql: str,
        error_type: str,
        error_msg: str,
        feedback: str,
        attempt: int,
    ) -> None:
        self.error_log.append(ErrorRecord(
            sql=sql,
            error_type=error_type,
            error_msg=error_msg,
            feedback=feedback,
            attempt=attempt,
        ))

    def get_error_context(self, last_n: int = 3) -> str:
        """Returns the last N errors as a plain-text block for the LLM."""
        if not self.error_log:
            return "No previous errors."
        errors = self.error_log[-last_n:]
        lines = []
        for e in errors:
            lines.append(f"Attempt {e.attempt}: {e.error_type} — {e.error_msg}")
        return "\n".join(lines)

    # ── Phase Log ─────────────────────────────────────────────

    def log_phase(self, phase: str, message: str = "") -> None:
        self.phase_log.append({
            "phase":     phase,
            "message":   message,
            "timestamp": time.time(),
        })

    def get_phase_log(self) -> list[dict]:
        return self.phase_log

    # ── Session Metadata ──────────────────────────────────────

    def set(self, key: str, value: Any) -> None:
        self.session_metadata[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        return self.session_metadata.get(key, default)

    # ── Summary ───────────────────────────────────────────────

    def summary(self) -> str:
        """Returns a brief session summary string."""
        q = self.session_metadata.get("question", "—")
        db = self.session_metadata.get("db_label", "—")
        n_queries = len(self.query_history)
        n_errors  = len(self.error_log)
        n_phases  = len(self.phase_log)
        return (
            f"Question: {q}\n"
            f"Database: {db}\n"
            f"Queries run: {n_queries} | Errors: {n_errors} | Phases: {n_phases}"
        )
