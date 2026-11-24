from __future__ import annotations

from app.config import AppConfig
from app.scorer import ContextTrustScoreEngine

# Databricks notebook template for running the same engine used by the app.
# Replace the values below with your actual catalog names and target output schema.


def build_databricks_config() -> AppConfig:
    return AppConfig(
        catalogs=["main"],
        output_schema="governance.trust",
        domain_tag="domain",
        certified_tag="certified",
        pii_tag="pii",
        glossary_table=None,
        freshness_days=7,
        lookback_days=30,
        run_mode="databricks_notebook",
    )


def run_context_trust_in_notebook(spark) -> dict:
    config = build_databricks_config()
    engine = ContextTrustScoreEngine(config)
    return engine.run(spark=spark)


# Example notebook cell usage:
# result = run_context_trust_in_notebook(spark)
# display(result)
