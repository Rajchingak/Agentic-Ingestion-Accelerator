from __future__ import annotations

import json
import logging
import math
import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import yaml
from pyspark.sql import functions as F
from pyspark.sql import types as T

from app.config import AppConfig
from app.output_tables import build_output_table_names, save_result_tables

WEIGHTS = {
    "semantic_clarity": 30,
    "governance": 25,
    "consistency": 15,
    "structural": 10,
    "lineage": 10,
    "freshness": 10,
}

GRADES = [(85, "Trusted"), (65, "Use with caution"), (0, "Not AI-ready")]
CAP_NO_OWNER = 40
CAP_UNTAGGED_PII = 50
MIN_TABLE_COMMENT = 20
MIN_COLUMN_COMMENT = 15
PII_PATTERN = (
    r"(?i)(^|_)(e_?mail|phone|mobile|ssn|passport|national_id|tax_id|dob|date_of_birth|birth_date|"
    r"street_address|home_address|postcode|zip_code|credit_card|card_number|iban)(_|$)"
)
DATA_TYPES = ["MANAGED", "EXTERNAL", "MATERIALIZED_VIEW", "STREAMING_TABLE"]
PK_TYPES = ["MANAGED", "EXTERNAL"]
logger = logging.getLogger(__name__)


def _norm_py(value: Optional[str]) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (value or "").strip().lower()).strip("_")


def _catalog_in_sql(catalogs: Iterable[str]) -> str:
    return ", ".join(f"'{catalog.lower()}'" for catalog in catalogs)


class ContextTrustScoreEngine:
    """Reusable scoring engine for Databricks App and notebook execution."""

    def __init__(self, config: AppConfig):
        self.config = config
        self.config.validate()

    def _build_query_features(self, spark: Any) -> Any:
        """Collect the asset feature layer from Unity Catalog system tables."""
        catalog_in = _catalog_in_sql(self.config.catalogs)
        domain_tag = self.config.domain_tag.lower()
        certified_tag = self.config.certified_tag.lower()
        pii_tag = self.config.pii_tag.lower()
        freshness_days = self.config.freshness_days
        lookback_days = self.config.lookback_days

        features = spark.sql(
            f"""
            WITH t AS (
              SELECT table_catalog, table_schema, table_name,
                     lower(concat_ws('.', table_catalog, table_schema, table_name)) AS full_name,
                     table_type, table_owner, comment AS table_comment, last_altered
              FROM system.information_schema.tables
              WHERE lower(table_catalog) IN ({catalog_in})
                AND table_schema <> 'information_schema'
            ),
            cols AS (
              SELECT table_catalog, table_schema, table_name,
                     count(*) AS n_cols,
                     count_if(length(trim(coalesce(comment, ''))) >= {MIN_COLUMN_COMMENT}) AS n_cols_documented
              FROM system.information_schema.columns
              WHERE lower(table_catalog) IN ({catalog_in})
              GROUP BY ALL
            ),
            pii AS (
              SELECT c.table_catalog, c.table_schema, c.table_name,
                     count(*) AS n_pii_untagged,
                     array_join(collect_list(c.column_name), ', ') AS pii_untagged_columns
              FROM system.information_schema.columns c
              LEFT JOIN system.information_schema.column_tags ct
                ON ct.catalog_name = c.table_catalog AND ct.schema_name = c.table_schema
               AND ct.table_name = c.table_name AND ct.column_name = c.column_name
               AND lower(ct.tag_name) = '{pii_tag}'
              WHERE lower(c.table_catalog) IN ({catalog_in})
                AND c.column_name RLIKE '{PII_PATTERN}'
                AND ct.tag_name IS NULL
              GROUP BY ALL
            ),
            pk AS (
              SELECT DISTINCT table_catalog, table_schema, table_name
              FROM system.information_schema.table_constraints
              WHERE constraint_type = 'PRIMARY KEY' AND lower(table_catalog) IN ({catalog_in})
            ),
            ttags AS (
              SELECT catalog_name, schema_name, table_name,
                     max(CASE WHEN lower(tag_name) = '{domain_tag}' THEN tag_value END) AS table_domain,
                     max(CASE WHEN lower(tag_name) = '{certified_tag}'
                               AND lower(coalesce(tag_value, 'true')) NOT IN ('false', 'no', '0')
                              THEN true ELSE false END) AS is_certified
              FROM system.information_schema.table_tags
              WHERE lower(catalog_name) IN ({catalog_in})
              GROUP BY ALL
            ),
            stags AS (
              SELECT catalog_name, schema_name,
                     max(CASE WHEN lower(tag_name) = '{domain_tag}' THEN tag_value END) AS schema_domain
              FROM system.information_schema.schema_tags
              WHERE lower(catalog_name) IN ({catalog_in})
              GROUP BY ALL
            ),
            lin AS (
              SELECT lower(source_table_full_name) AS source_table_full_name, source_path,
                     lower(target_table_full_name) AS target_table_full_name,
                     created_by, event_time
              FROM system.access.table_lineage
              WHERE event_time >= current_timestamp() - INTERVAL 365 DAYS
                AND (lower(source_table_catalog) IN ({catalog_in}) OR lower(target_table_catalog) IN ({catalog_in}))
            ),
            writes AS (
              SELECT target_table_full_name AS full_name,
                     count(DISTINCT coalesce(source_table_full_name, source_path)) AS n_upstream,
                     max(event_time) AS last_write
              FROM lin
              WHERE target_table_full_name IS NOT NULL
              GROUP BY 1
            ),
            reads AS (
              SELECT source_table_full_name AS full_name,
                     count(*) AS n_reads,
                     count(DISTINCT created_by) AS n_readers,
                     count(DISTINCT target_table_full_name) AS n_downstream
              FROM lin
              WHERE source_table_full_name IS NOT NULL
                AND event_time >= current_timestamp() - INTERVAL {lookback_days} DAYS
              GROUP BY 1
            )
            SELECT t.full_name, t.table_catalog, t.table_schema, t.table_name, t.table_type,
                   t.table_owner, t.table_comment, t.last_altered,
                   coalesce(tt.table_domain, st.schema_domain, t.table_schema) AS domain,
                   CASE WHEN tt.table_domain IS NOT NULL THEN 'table tag'
                        WHEN st.schema_domain IS NOT NULL THEN 'schema tag'
                        ELSE 'schema name (fallback)' END AS domain_source,
                   coalesce(tt.is_certified, false) AS is_certified,
                   coalesce(c.n_cols, 0) AS n_cols,
                   coalesce(c.n_cols_documented, 0) AS n_cols_documented,
                   pk.table_name IS NOT NULL AS has_pk,
                   coalesce(p.n_pii_untagged, 0) AS n_pii_untagged,
                   p.pii_untagged_columns,
                   coalesce(w.n_upstream, 0) AS n_upstream,
                   w.last_write,
                   coalesce(r.n_reads, 0) AS n_reads,
                   coalesce(r.n_readers, 0) AS n_readers,
                   coalesce(r.n_downstream, 0) AS n_downstream
            FROM t
            LEFT JOIN cols c ON c.table_catalog = t.table_catalog AND c.table_schema = t.table_schema AND c.table_name = t.table_name
            LEFT JOIN pk ON pk.table_catalog = t.table_catalog AND pk.table_schema = t.table_schema AND pk.table_name = t.table_name
            LEFT JOIN pii p ON p.table_catalog = t.table_catalog AND p.table_schema = t.table_schema AND p.table_name = t.table_name
            LEFT JOIN ttags tt ON tt.catalog_name = t.table_catalog AND tt.schema_name = t.table_schema AND tt.table_name = t.table_name
            LEFT JOIN stags st ON st.catalog_name = t.table_catalog AND st.schema_name = t.table_schema
            LEFT JOIN writes w ON w.full_name = t.full_name
            LEFT JOIN reads r ON r.full_name = t.full_name
            """
        )
        return features

    def _build_metric_measures(self, spark: Any) -> Tuple[Any, Any, Any]:
        """Parse metric-view definitions and return measure rows and per-view statistics."""
        catalog_in = _catalog_in_sql(self.config.catalogs)
        metric_views = spark.sql(
            f"""
            SELECT lower(concat_ws('.', t.table_catalog, t.table_schema, t.table_name)) AS full_name,
                   t.table_catalog, t.table_schema, t.table_name,
                   v.view_definition
            FROM system.information_schema.tables t
            LEFT JOIN system.information_schema.views v
              ON v.table_catalog = t.table_catalog
             AND v.table_schema = t.table_schema
             AND v.table_name = t.table_name
            WHERE t.table_type = 'METRIC_VIEW'
              AND lower(t.table_catalog) IN ({catalog_in})
            """
        ).collect()

        measure_records = []
        parse_records = []

        def load_yaml(text: Optional[str]) -> Optional[Dict[str, Any]]:
            if not text:
                return None
            try:
                candidate = yaml.safe_load(text)
            except yaml.YAMLError:
                return None
            if isinstance(candidate, dict) and isinstance(candidate.get("measures"), list):
                return candidate
            return None

        for row in metric_views:
            definition = load_yaml(row.view_definition)
            if definition is None:
                def quote(part: str) -> str:
                    return f"`{part.replace('`', '``')}`"

                qualified_name = ".".join(
                    quote(part) for part in (row.table_catalog, row.table_schema, row.table_name)
                )
                try:
                    metadata = json.loads(
                        spark.sql(f"DESCRIBE TABLE EXTENDED {qualified_name} AS JSON").collect()[0][0]
                    )
                except Exception:
                    logger.warning("Could not read metric-view definition for %s", row.full_name, exc_info=True)
                else:
                    if isinstance(metadata, dict):
                        definition = load_yaml(metadata.get("view_text"))

            parsed = definition is not None
            if definition is not None:
                for measure in definition["measures"]:
                    if not isinstance(measure, dict) or not measure.get("name"):
                        continue
                    name = str(measure["name"]).strip()
                    expression = str(measure.get("expr") or "").strip()
                    comment = str(measure.get("comment") or measure.get("description") or "").strip()
                    measure_records.append(
                        (
                            row.full_name,
                            name,
                            _norm_py(name),
                            expression,
                            re.sub(r"\s+", " ", expression.replace("`", "").lower()).strip(),
                            comment,
                            len(comment) >= MIN_COLUMN_COMMENT,
                        )
                    )
            parse_records.append((row.full_name, parsed))

        measure_schema = T.StructType(
            [
                T.StructField("metric_view", T.StringType(), False),
                T.StructField("measure_name", T.StringType(), False),
                T.StructField("measure_norm", T.StringType(), False),
                T.StructField("expr", T.StringType(), False),
                T.StructField("expr_norm", T.StringType(), False),
                T.StructField("comment", T.StringType(), False),
                T.StructField("is_documented", T.BooleanType(), False),
            ]
        )
        measures = spark.createDataFrame(measure_records, measure_schema)
        conflicts = (
            measures.groupBy("measure_norm")
            .agg(
                F.countDistinct("expr_norm").alias("n_definitions"),
                F.collect_set("metric_view").alias("defined_in"),
            )
            .filter(F.col("n_definitions") > 1)
        )
        measures = (
            measures.join(conflicts, "measure_norm", "left")
            .withColumn("is_conflicting", F.col("n_definitions").isNotNull())
            .withColumn("conflicting_views", F.array_join(F.col("defined_in"), ", "))
            .drop("n_definitions", "defined_in")
        )
        measure_stats = measures.groupBy(F.col("metric_view").alias("full_name")).agg(
            F.count("*").alias("n_measures"),
            F.count_if("is_documented").alias("n_measures_documented"),
        )
        parse_schema = T.StructType(
            [
                T.StructField("full_name", T.StringType(), False),
                T.StructField("mv_parse_ok", T.BooleanType(), False),
            ]
        )
        parse_status = spark.createDataFrame(parse_records, parse_schema)
        return measures, measure_stats, parse_status

    def _score_asset_features(self, spark: Any, features: Any) -> Any:
        """Compute per-asset trust score and dimension scores."""
        col = F.col
        is_mv = col("table_type") == "METRIC_VIEW"
        has_glossary = F.lit(self.config.glossary_table is not None)

        comment_len = F.length(F.trim(F.coalesce(col("table_comment"), F.lit(""))))
        table_doc = F.when(comment_len >= MIN_TABLE_COMMENT, 100.0).when(comment_len > 0, 40.0).otherwise(0.0)
        col_doc = F.when(col("n_cols") > 0, 100.0 * col("n_cols_documented") / col("n_cols"))
        semantic = F.when(is_mv, F.when(col("n_measures").isNull(), 0.0).otherwise(
            100.0 * col("n_measures_documented") / col("n_measures")
        )).otherwise(F.when(col("n_cols") > 0, 100.0 * col("n_cols_documented") / col("n_cols")).otherwise(0.0))

        owner_score = F.when(col("table_owner").isNull() | (F.trim(col("table_owner")) == ""), 0.0) \
            .when(col("table_owner").contains("@"), 60.0).otherwise(100.0)
        domain_tagged = F.when(col("domain_source") != "schema name (fallback)", 100.0).otherwise(0.0)
        pii_ok = F.when(col("n_pii_untagged") == 0, 100.0).otherwise(0.0)
        governance = (0.5 * owner_score + 0.25 * domain_tagged + 0.25 * pii_ok)

        lineage = F.when(col("n_upstream") > 0, 100.0) \
            .when((col("n_downstream") > 0) | (col("n_reads") > 0), 50.0).otherwise(0.0)

        days_since_update = F.datediff(F.current_timestamp(), F.coalesce(col("last_write"), col("last_altered")))
        freshness = F.when(col("table_type").isin(DATA_TYPES),
                          F.when(days_since_update <= self.config.freshness_days, 100.0)
                           .when(days_since_update <= 4 * self.config.freshness_days, 50.0)
                           .otherwise(0.0))

        asset_scores = (
            features
            .withColumn("semantic_clarity", F.coalesce(semantic, F.lit(0.0)))
            .withColumn("governance", F.coalesce(governance, F.lit(0.0)))
            .withColumn("consistency", F.lit(100.0))
            .withColumn("structural", F.when(col("table_type").isin(PK_TYPES), F.when(col("has_pk"), 100.0).otherwise(0.0)).otherwise(0.0))
            .withColumn("lineage", F.coalesce(lineage, F.lit(0.0)))
            .withColumn("freshness", F.coalesce(freshness, F.lit(0.0)))
            .withColumn("days_since_update", days_since_update)
        )

        weighted_components = [(col(name), weight) for name, weight in WEIGHTS.items()]
        weighted_sum = sum(F.coalesce(c, F.lit(0.0)) * w for c, w in weighted_components)
        total_weight = sum(F.when(c.isNotNull(), F.lit(float(w))).otherwise(F.lit(0.0)) for c, w in weighted_components)
        raw_score = F.when(total_weight > 0, weighted_sum / total_weight).otherwise(F.lit(0.0))

        no_owner = col("table_owner").isNull() | (F.trim(col("table_owner")) == "")
        untagged_pii = col("n_pii_untagged") > 0
        capped = F.when(no_owner, F.least(raw_score, F.lit(float(CAP_NO_OWNER)))).otherwise(raw_score)
        capped = F.when(untagged_pii, F.least(capped, F.lit(float(CAP_UNTAGGED_PII)))).otherwise(capped)

        asset_scores = (
            asset_scores.withColumn("raw_score", F.round(raw_score, 1))
            .withColumn("trust_score", F.round(capped, 1))
            .withColumn("grade", F.when(F.col("trust_score") >= 85, "Trusted")
                .when(F.col("trust_score") >= 65, "Use with caution")
                .otherwise("Not AI-ready"))
            .withColumn("gates_applied", F.concat_ws(", ",
                F.when(no_owner & (raw_score > CAP_NO_OWNER), F.lit("no_owner_cap")),
                F.when(untagged_pii & (raw_score > CAP_UNTAGGED_PII), F.lit("untagged_pii_cap"))))
            .withColumn("importance_weight", (1 + F.log1p(col("n_reads"))) * F.when(col("is_certified"), 2.0).otherwise(1.0))
            .withColumn("run_ts", F.lit(datetime.now(timezone.utc).replace(tzinfo=None)))
        )

        return asset_scores

    def _build_domain_scores(self, asset_scores: Any) -> Any:
        w = F.col("importance_weight")
        dimension_avgs = [F.round(F.avg(name), 1).alias(f"avg_{name}") for name in WEIGHTS]
        domain_scores = asset_scores.groupBy(F.col("domain").alias("domain")).agg(
            F.round(F.sum(F.col("trust_score") * w) / F.sum(w), 1).alias("domain_score"),
            F.round(F.avg("trust_score"), 1).alias("unweighted_score"),
            F.count("*").alias("n_assets"),
            F.count_if(F.col("grade") == "Trusted").alias("n_trusted"),
            F.count_if(F.col("grade") == "Use with caution").alias("n_caution"),
            F.count_if(F.col("grade") == "Not AI-ready").alias("n_not_ready"),
            F.count_if(F.col("is_certified")).alias("n_certified"),
            F.count_if(F.col("table_type") == "METRIC_VIEW").alias("n_metric_views"),
            *dimension_avgs,
        )
        all_domain = asset_scores.agg(
            F.lit("__ALL__").alias("domain"),
            F.round(F.sum(F.col("trust_score") * w) / F.sum(w), 1).alias("domain_score"),
            F.round(F.avg("trust_score"), 1).alias("unweighted_score"),
            F.count("*").alias("n_assets"),
            F.count_if(F.col("grade") == "Trusted").alias("n_trusted"),
            F.count_if(F.col("grade") == "Use with caution").alias("n_caution"),
            F.count_if(F.col("grade") == "Not AI-ready").alias("n_not_ready"),
            F.count_if(F.col("is_certified")).alias("n_certified"),
            F.count_if(F.col("table_type") == "METRIC_VIEW").alias("n_metric_views"),
            *dimension_avgs,
        )
        return domain_scores.unionByName(all_domain).withColumn(
            "domain_grade",
            F.when(F.col("domain_score") >= 85, "Trusted")
            .when(F.col("domain_score") >= 65, "Use with caution")
            .otherwise("Not AI-ready"),
        ).withColumn("run_ts", F.lit(datetime.now(timezone.utc).replace(tzinfo=None)))

    def _build_findings(self, asset_scores: Any) -> Any:
        no_owner = F.col("table_owner").isNull() | (F.trim(F.col("table_owner")) == "")
        untagged_pii = F.col("n_pii_untagged") > 0
        findings = asset_scores.filter(no_owner | untagged_pii).select(
            F.col("full_name").alias("asset"),
            F.col("table_type").alias("asset_type"),
            F.col("domain"),
            F.col("table_owner").cast("string").alias("owner"),
            F.lit("high").alias("severity"),
            F.lit(1).alias("severity_rank"),
            F.when(no_owner, F.lit("no_owner")).otherwise(F.lit("untagged_pii")).alias("rule"),
            F.when(no_owner, F.lit("Asset has no owner")).otherwise(F.lit("Likely PII columns are not tagged")).alias("detail"),
            F.when(no_owner, F.lit("Set an accountable team owner")).otherwise(F.lit("Review and tag likely PII columns")).alias("recommendation"),
            F.col("run_ts"),
        )
        return findings

    def _write_outputs(self, spark: Any, result_tables: Dict[str, Any]) -> Dict[str, str]:
        return save_result_tables(spark, self.config.output_schema, result_tables)

    @classmethod
    def from_databricks_widgets(cls, spark: Optional[Any] = None, widget_values: Optional[Dict[str, str]] = None) -> "ContextTrustScoreEngine":
        """Create a config from Databricks widget values when running inside a notebook."""
        values = widget_values or {}
        cfg = AppConfig.from_dict(
            {
                "catalogs": values.get("catalogs", "main"),
                "output_schema": values.get("output_schema", "governance.trust"),
                "domain_tag": values.get("domain_tag", "domain"),
                "certified_tag": values.get("certified_tag", "certified"),
                "pii_tag": values.get("pii_tag", "pii"),
                "glossary_table": values.get("glossary_table") or None,
                "freshness_days": values.get("freshness_days", "7"),
                "lookback_days": values.get("lookback_days", "30"),
            }
        )
        return cls(cfg)

    def run(self, spark: Optional[Any] = None) -> Dict[str, Any]:
        """Execute the extraction flow and write output tables for the Databricks app."""
        if spark is None:
            raise ValueError("A Spark session is required to run the context trust scoring engine.")

        features = self._build_query_features(spark)
        metric_measures, measure_stats, parse_status = self._build_metric_measures(spark)
        features = features.join(measure_stats, "full_name", "left").join(parse_status, "full_name", "left")
        asset_scores = self._score_asset_features(spark, features)
        domain_scores = self._build_domain_scores(asset_scores)
        findings = self._build_findings(asset_scores)

        metric_measures = metric_measures.withColumn("run_ts", F.lit(datetime.now(timezone.utc).replace(tzinfo=None)))

        results = {
            "asset_scores": asset_scores,
            "domain_scores": domain_scores,
            "findings": findings,
            "metric_measures": metric_measures,
        }
        written = self._write_outputs(spark, results)

        return {
            "status": "success",
            "catalogs": self.config.catalogs,
            "output_schema": self.config.output_schema,
            "tables": written,
            "outputs": build_output_table_names(self.config.output_schema),
        }

    def validate_catalog_access(self, spark: Optional[Any] = None) -> Dict[str, Any]:
        if spark is None:
            raise ValueError("A Spark session is required to validate catalog access.")
        catalog_in = _catalog_in_sql(self.config.catalogs)
        query = f"SELECT 1 FROM system.information_schema.tables WHERE lower(table_catalog) IN ({catalog_in}) LIMIT 1"
        spark.sql(query).count()
        return {"status": "ok", "catalogs": self.config.catalogs}

    def run_with_metrics(self, spark: Optional[Any] = None) -> Dict[str, Any]:
        return self.run(spark=spark)
