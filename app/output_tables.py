from __future__ import annotations

from typing import Any, Dict


def _quote_multipart_identifier(identifier: str) -> str:
    parts = identifier.split(".")
    if any(not part for part in parts):
        raise ValueError("Identifiers must not contain empty components.")
    return ".".join(f"`{part.replace('`', '``')}`" for part in parts)


def build_output_table_names(output_schema: str) -> Dict[str, str]:
    return {
        "asset_scores": f"{output_schema}.asset_scores",
        "domain_scores": f"{output_schema}.domain_scores",
        "findings": f"{output_schema}.findings",
        "metric_measures": f"{output_schema}.metric_measures",
        "glossary_health": f"{output_schema}.glossary_health",
    }


def ensure_schema_exists(spark: Any, output_schema: str) -> None:
    parts = output_schema.split(".")
    if len(parts) != 2 or any(not part for part in parts):
        raise ValueError("output_schema must be in catalog.schema form")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_quote_multipart_identifier(output_schema)}")


def save_result_tables(spark: Any, output_schema: str, result_tables: Dict[str, Any]) -> Dict[str, str]:
    if spark is None:
        raise ValueError("A Spark session is required to write output tables.")

    ensure_schema_exists(spark, output_schema)
    output_names = build_output_table_names(output_schema)
    written = {}

    for table_name, frame in result_tables.items():
        full_name = output_names.get(table_name, f"{output_schema}.{table_name}")
        frame.write.mode("append").option("mergeSchema", "true").saveAsTable(_quote_multipart_identifier(full_name))
        written[table_name] = full_name

    return written
