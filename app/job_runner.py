from __future__ import annotations

from typing import Any, Dict

from app.config import AppConfig
from app.scorer import ContextTrustScoreEngine


def get_spark_session() -> Any:
    """Reuse an active Spark session or connect to Databricks serverless compute."""
    from pyspark.sql import SparkSession

    active_session = SparkSession.getActiveSession()
    if active_session is not None:
        return active_session

    try:
        from databricks.connect import DatabricksSession
    except ImportError as exc:
        raise RuntimeError(
            "No active Spark session is available. Install databricks-connect to connect "
            "the app to Databricks serverless compute."
        ) from exc

    return DatabricksSession.builder.serverless().getOrCreate()


def run_context_trust_score(config: dict | AppConfig, spark: Any = None) -> Dict[str, Any]:
    """Run the trust score from jobs or from the app.

    This keeps notebook logic, app execution, and scheduled jobs on the same path.
    """
    cfg = config if isinstance(config, AppConfig) else AppConfig.from_dict(config)
    engine = ContextTrustScoreEngine(cfg)
    return engine.run(spark=spark if spark is not None else get_spark_session())


def run_from_job(config: dict) -> Dict[str, Any]:
    return run_context_trust_score(config)
