"""Load the widget queries in dashboard_queries.sql and run them against an output schema."""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

from app.output_tables import _quote_multipart_identifier

QUERIES_FILE = Path(__file__).resolve().parent.parent / "dashboard_queries.sql"
PLACEHOLDER_SCHEMA = "incontext.`incontext-schema`"

_HEADER = re.compile(r"^--\s*(\d+)\.\s*(.+?)\s*$")
_PAGE = re.compile(r"^--\s*([A-Z][A-Z ]+ PAGE)\s*$")


@dataclass(frozen=True)
class DashboardQuery:
    number: int
    title: str
    page: str
    sql: str


def load_dashboard_queries(path: Path = QUERIES_FILE) -> List[DashboardQuery]:
    """Split the SQL file into one query per numbered '-- N. Title' header."""
    queries: List[DashboardQuery] = []
    page = ""
    current: Dict[str, Any] | None = None

    def flush() -> None:
        if current is not None:
            sql = "\n".join(current["lines"]).strip().rstrip(";").strip()
            if sql:
                queries.append(DashboardQuery(current["number"], current["title"], current["page"], sql))

    for line in path.read_text().splitlines():
        page_match = _PAGE.match(line)
        if page_match:
            flush()
            current = None
            page = page_match.group(1).title()
            continue
        header = _HEADER.match(line)
        if header:
            flush()
            current = {"number": int(header.group(1)), "title": header.group(2), "page": page, "lines": []}
            continue
        if current is not None and not line.lstrip().startswith("--"):
            current["lines"].append(line)
    flush()
    return queries


def bind_schema(sql: str, output_schema: str) -> str:
    """Point a query at the given catalog.schema instead of the placeholder."""
    parts = output_schema.split(".")
    if len(parts) != 2 or any(not p.strip() for p in parts):
        raise ValueError("output_schema must be in catalog.schema form")
    quoted = _quote_multipart_identifier(output_schema.strip())
    return sql.replace(f"{PLACEHOLDER_SCHEMA}.", f"{quoted}.")


def run_dashboard_query(spark: Any, query: DashboardQuery, output_schema: str) -> Any:
    """Run one widget query and return a pandas DataFrame."""
    return spark.sql(bind_schema(query.sql, output_schema)).toPandas()
