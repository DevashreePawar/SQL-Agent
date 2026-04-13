"""
schema_inspector.py
====================
Concepts Demonstrated:
  1. Dynamic Tool Generation  — generates tool specs from live DB schema
  2. Schema-Aware Prompting   — formats schema into LLM-injectable text

The SchemaInspector reads the actual SQLite schema at runtime.
It doesn't hard-code anything — the tools and prompt change based on
whatever tables exist in the database. This is the "Dynamic" part.
"""

import sqlite3
from typing import Any


class SchemaInspector:
    """
    Discovers and formats database schema information.

    Concept 1 – Dynamic Tool Generation:
      Instead of defining static tools like execute_sql(query),
      we generate table-specific tool descriptions such as:
        query_orders_table(columns, filter_by, group_by, order_by, limit)
      These are regenerated every time the agent connects to a new DB.

    Concept 2 – Schema-Aware Prompting:
      The LLM system prompt is rebuilt each turn with the ACTUAL schema.
      This prevents hallucination of non-existent column names.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._schema: dict[str, Any] = {}

    # ──────────────────────────────────────────────────────────
    # Core Schema Discovery
    # ──────────────────────────────────────────────────────────

    def discover_schema(self) -> dict[str, Any]:
        """
        Reads ALL tables, columns (name + type), and 3 sample rows.
        Returns a dict shaped like:
          {
            "table_name": {
              "columns": [{"name": "col", "type": "TEXT"}, ...],
              "sample_rows": [[val, ...], ...]
            },
            ...
          }
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Get all user-created tables
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [row["name"] for row in cur.fetchall()]

        schema: dict[str, Any] = {}
        for table in tables:
            # Column info
            cur.execute(f"PRAGMA table_info({table})")
            columns = [
                {"name": row["name"], "type": row["type"]}
                for row in cur.fetchall()
            ]

            # Sample rows (up to 3)
            try:
                cur.execute(f"SELECT * FROM {table} LIMIT 3")
                rows = [list(row) for row in cur.fetchall()]
            except Exception:
                rows = []

            # Row count
            cur.execute(f"SELECT COUNT(*) as cnt FROM {table}")
            count = cur.fetchone()["cnt"]

            schema[table] = {
                "columns":     columns,
                "sample_rows": rows,
                "row_count":   count,
            }

        conn.close()
        self._schema = schema
        return schema

    # ──────────────────────────────────────────────────────────
    # Concept 2: Schema-Aware Prompt Formatting
    # ──────────────────────────────────────────────────────────

    def format_for_prompt(self, schema: dict[str, Any] | None = None) -> str:
        """
        Converts the schema dict into a clean, human-readable block
        that can be injected directly into the LLM system prompt.

        Example output:
          TABLE: orders (1234 rows)
            - order_id     : INTEGER
            - customer_id  : INTEGER
            - order_date   : TEXT
            - status       : TEXT
          Sample: [1, 12, '2024-01-15', 'completed']
        """
        if schema is None:
            schema = self._schema or self.discover_schema()

        lines = ["=== DATABASE SCHEMA ===\n"]
        for table, info in schema.items():
            lines.append(f"TABLE: {table}  ({info['row_count']:,} rows)")
            for col in info["columns"]:
                lines.append(f"  - {col['name']:<25} {col['type']}")
            if info["sample_rows"]:
                lines.append(f"  Sample row: {info['sample_rows'][0]}")
            lines.append("")

        lines.append("=== END SCHEMA ===")
        return "\n".join(lines)

    # ──────────────────────────────────────────────────────────
    # Concept 1: Dynamic Tool Generation
    # ──────────────────────────────────────────────────────────

    def generate_dynamic_tools(self, schema: dict[str, Any] | None = None) -> list[dict]:
        """
        Generates a list of tool-spec dicts based on the LIVE schema.
        Each table gets its own query tool with actual column names
        listed in the description.

        This is 'dynamic' because:
          - The tools are created AT RUNTIME, not hard-coded.
          - If you point the agent at a different DB, it gets
            completely different tools automatically.

        Returns a list of dicts:
          [
            {
              "name": "query_orders_table",
              "description": "...",
              "columns": ["order_id", "customer_id", ...],
            },
            ...
          ]
        """
        if schema is None:
            schema = self._schema or self.discover_schema()

        tools = []
        for table, info in schema.items():
            col_names = [c["name"] for c in info["columns"]]
            col_list  = ", ".join(col_names)

            tools.append({
                "name":        f"query_{table}_table",
                "table":       table,
                "description": (
                    f"Query the '{table}' table ({info['row_count']:,} rows). "
                    f"Available columns: {col_list}. "
                    f"Use SQL to filter, group, aggregate, or join with other tables."
                ),
                "columns":     col_names,
            })

        # Also add a generic execute_sql tool for complex multi-table queries
        tools.append({
            "name":        "execute_sql",
            "table":       None,
            "description": (
                "Execute any valid SQL SELECT query against the database. "
                "Use this for JOINs across multiple tables or complex aggregations."
            ),
            "columns":     [],
        })

        return tools

    def get_column_names(self, table: str) -> list[str]:
        """Quick helper — returns column names for a specific table."""
        schema = self._schema or self.discover_schema()
        if table not in schema:
            return []
        return [c["name"] for c in schema[table]["columns"]]

    def get_all_tables(self) -> list[str]:
        """Returns all table names in the database."""
        schema = self._schema or self.discover_schema()
        return list(schema.keys())
