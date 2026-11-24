from __future__ import annotations

from app.config import AppConfig
from app.scorer import ContextTrustScoreEngine


def build_runtime_config() -> AppConfig:
    """Example configuration builder for notebook-based Databricks runtime.

    This is used when the project is run from a notebook directly in Databricks.
    """
    return AppConfig(
        catalogs=["main"],
        output_schema="governance.trust",
        domain_tag="domain",
        certified_tag="certified",
        pii_tag="pii",
        glossary_table=None,
        freshness_days=7,
        lookback_days=30,
        run_mode="notebook",
    )


def run_notebook_job(spark) -> dict:
    config = build_runtime_config()
    engine = ContextTrustScoreEngine(config)
    return engine.run(spark=spark)
