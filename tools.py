from __future__ import annotations
"""
tools.py
========
SQL-specific tools available to the agent during the QUERY phase.

Each tool is a regular Python function that the agent can invoke.
The tool registry maps tool names → functions so the agent can
call them by name from its ReAct reasoning loop.

Tools here wrap the executor and inspector to provide a clean,
agent-facing interface.
"""

import pandas as pd
from sql_executor import SQLExecutor, ExecutionResult
from schema_inspector import SchemaInspector
from typing import Any


def list_tables(db_path: str) -> str:
    """
    Tool: list_tables
    Returns all table names and row counts in the database.
    Useful during DISCOVER phase to orient the agent.
    """
    inspector = SchemaInspector(db_path)
    schema    = inspector.discover_schema()
    lines     = ["Available tables:\n"]
    for table, info in schema.items():
        lines.append(f"  • {table}  ({info['row_count']:,} rows)")
    return "\n".join(lines)


def describe_table(db_path: str, table_name: str) -> str:
    """
    Tool: describe_table
    Returns column names, types, and a sample row for a given table.
    """
    inspector = SchemaInspector(db_path)
    schema    = inspector.discover_schema()
    if table_name not in schema:
        tables = list(schema.keys())
        return f"Table '{table_name}' not found. Available tables: {tables}"

    info  = schema[table_name]
    lines = [f"TABLE: {table_name}  ({info['row_count']:,} rows)"]
    for col in info["columns"]:
        lines.append(f"  {col['name']:<25} {col['type']}")
    if info["sample_rows"]:
        lines.append(f"\nSample row: {info['sample_rows'][0]}")
    return "\n".join(lines)


def execute_sql(db_path: str, sql: str) -> ExecutionResult:
    """
    Tool: execute_sql
    Runs a SQL SELECT query and returns a structured ExecutionResult.
    Enforces read-only safety (no DROP/DELETE/UPDATE).
    """
    executor = SQLExecutor(db_path)
    return executor.execute(sql)


def profile_table(db_path: str, table_name: str) -> str:
    """
    Tool: profile_table
    Returns basic statistics for numeric columns in a table:
    min, max, avg, null count. Helpful for understanding data quality.
    """
    inspector = SchemaInspector(db_path)
    schema    = inspector.discover_schema()
    if table_name not in schema:
        return f"Table '{table_name}' not found."

    numeric_cols = [
        c["name"] for c in schema[table_name]["columns"]
        if c["type"].upper() in ("INTEGER", "REAL", "NUMERIC", "FLOAT", "DOUBLE")
    ]
    if not numeric_cols:
        return f"No numeric columns found in '{table_name}'."

    executor = SQLExecutor(db_path)
    lines    = [f"Profile of '{table_name}':\n"]

    for col in numeric_cols:
        result = executor.execute(
            f"SELECT MIN({col}) as min_val, MAX({col}) as max_val, "
            f"AVG({col}) as avg_val, "
            f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) as null_count "
            f"FROM {table_name}"
        )
        if result.success and result.rows:
            mn, mx, avg, nulls = result.rows[0]
            lines.append(
                f"  {col:<25} min={mn:.2f}  max={mx:.2f}  avg={avg:.2f}  nulls={int(nulls) if nulls else 0}"
            )

    return "\n".join(lines)


def result_to_markdown(result: ExecutionResult, max_rows: int = 15) -> str:
    """
    Tool: result_to_markdown
    Converts an ExecutionResult into a markdown table string.
    Used by the report generator.
    """
    executor = SQLExecutor.__new__(SQLExecutor)
    return executor.to_markdown_table(result, max_rows)


# ── Tool Registry ─────────────────────────────────────────────
# Maps tool names (as the agent uses them) to callable functions.
# The agent's ReAct loop looks up tools here by name.

TOOL_REGISTRY: dict[str, Any] = {
    "list_tables":       list_tables,
    "describe_table":    describe_table,
    "execute_sql":       execute_sql,
    "profile_table":     profile_table,
    "result_to_markdown": result_to_markdown,
}


def get_tool(name: str):
    """Returns a tool function by name, or None if not found."""
    return TOOL_REGISTRY.get(name)


def list_tool_descriptions() -> str:
    """Returns tool descriptions for injection into the agent prompt."""
    descriptions = {
        "list_tables":    "list_tables(db_path) — list all tables and row counts",
        "describe_table": "describe_table(db_path, table_name) — show columns and sample row",
        "execute_sql":    "execute_sql(db_path, sql) — run a SELECT query safely",
        "profile_table":  "profile_table(db_path, table_name) — stats for numeric columns",
    }
    return "\n".join(f"  • {v}" for v in descriptions.values())
