# Databricks notebook source
# MAGIC %md
# MAGIC # Context Trust Score — MVP audit
# MAGIC
# MAGIC Audits Unity Catalog tables, views and **metric views**, plus an optional **business glossary**, and scores how
# MAGIC trustworthy each asset is as business context (for people and for AI agents such as Genie).
# MAGIC
# MAGIC | Output table | Audience | What it holds |
# MAGIC |---|---|---|
# MAGIC | `asset_scores` | Engineers | One row per asset: dimension scores, overall trust score, grade |
# MAGIC | `domain_scores` | Leadership | Usage-weighted trust score per business domain (+ an `__ALL__` row) |
# MAGIC | `findings` | Engineers | Actionable issues with severity, owner and recommended fix |
# MAGIC | `metric_measures` | Engineers | Every metric-view measure: documented? in glossary? conflicting definition? |
# MAGIC | `glossary_health` | Both | Every glossary term: defined? owned? implemented anywhere? |
# MAGIC
# MAGIC Every output is **appended** with a `run_ts`, so you can chart trends. Schedule this notebook as a daily/weekly job.

# COMMAND ----------

import json
import re
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql import types as T

dbutils.widgets.text("catalogs", "main", "1. Catalogs to audit (comma-separated)")
dbutils.widgets.text("output_schema", "governance.trust", "2. Output catalog.schema")
dbutils.widgets.text("domain_tag", "domain", "3. Tag name holding the business domain")
dbutils.widgets.text("certified_tag", "certified", "4. Tag name marking certified assets")
dbutils.widgets.text("pii_tag", "pii", "5. Tag name marking PII columns")
dbutils.widgets.text("glossary_table", "", "6. Glossary table (optional, catalog.schema.table)")
dbutils.widgets.text("freshness_days", "7", "7. Freshness SLA in days")
dbutils.widgets.text("lookback_days", "30", "8. Usage lookback in days")

_IDENT = re.compile(r"^[A-Za-z0-9_\-]+$")


def ident(value, label):
    """Validate a name before it is interpolated into SQL."""
    value = value.strip()
    if not _IDENT.match(value):
        raise ValueError(f"Invalid {label}: {value!r}")
    return value


def qualified(name, label, parts):
    pieces = [ident(p, label) for p in name.split(".")]
    if len(pieces) != parts:
        raise ValueError(f"{label} must have {parts} dot-separated parts, got {name!r}")
    return ".".join(f"`{p}`" for p in pieces)


CATALOGS = [ident(c, "catalog") for c in dbutils.widgets.get("catalogs").split(",") if c.strip()]
if not CATALOGS:
    raise ValueError("Provide at least one catalog to audit")
CATALOG_IN = ", ".join(f"'{c.lower()}'" for c in CATALOGS)

OUT = qualified(dbutils.widgets.get("output_schema"), "output_schema", 2)
DOMAIN_TAG = ident(dbutils.widgets.get("domain_tag"), "domain_tag").lower()
CERTIFIED_TAG = ident(dbutils.widgets.get("certified_tag"), "certified_tag").lower()
PII_TAG = ident(dbutils.widgets.get("pii_tag"), "pii_tag").lower()
_glossary = dbutils.widgets.get("glossary_table").strip()
GLOSSARY_TABLE = qualified(_glossary, "glossary_table", 3) if _glossary else None
FRESHNESS_DAYS = int(dbutils.widgets.get("freshness_days"))
LOOKBACK_DAYS = int(dbutils.widgets.get("lookback_days"))
RUN_TS = datetime.now(timezone.utc).replace(tzinfo=None)

print(f"Auditing catalogs {CATALOGS} -> writing to {OUT} (run_ts={RUN_TS})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scoring configuration
# MAGIC Tune these to match what your business cares about. Dimensions that don't apply to an asset
# MAGIC (e.g. *freshness* for a plain view) are skipped and the remaining weights are re-normalised.

# COMMAND ----------

WEIGHTS = {
    "semantic_clarity": 30,  # descriptions on tables / columns / measures, glossary coverage
    "governance": 25,        # owner, domain tag, PII tagged
    "consistency": 15,       # metric views only: no conflicting definitions of the same measure
    "structural": 10,        # managed/external tables only: primary key declared
    "lineage": 10,           # provenance known
    "freshness": 10,         # tables with data only: updated within SLA
}

# Grade bands (checked top-down)
GRADES = [(85, "Trusted"), (65, "Use with caution"), (0, "Not AI-ready")]

# Critical gates: a weighted average must not hide these
CAP_NO_OWNER = 40
CAP_UNTAGGED_PII = 50

MIN_TABLE_COMMENT = 20   # chars for a "meaningful" table / metric view description
MIN_COLUMN_COMMENT = 15  # chars for a "meaningful" column / measure description

# Heuristic for columns that probably hold PII. Matches whole name segments, e.g. customer_email, phone_number.
PII_PATTERN = (
    "(?i)(^|_)(e_?mail|phone|mobile|ssn|passport|national_id|tax_id|dob|date_of_birth|birth_date|"
    "street_address|home_address|postcode|zip_code|credit_card|card_number|iban)(_|$)"
)

DATA_TYPES = ["MANAGED", "EXTERNAL", "MATERIALIZED_VIEW", "STREAMING_TABLE"]  # hold data -> freshness applies
PK_TYPES = ["MANAGED", "EXTERNAL"]                                           # primary key expected

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Collect asset features from Unity Catalog system tables

# COMMAND ----------

features = spark.sql(f"""
WITH t AS (
  SELECT table_catalog, table_schema, table_name,
         lower(concat_ws('.', table_catalog, table_schema, table_name)) AS full_name,
         table_type, table_owner, comment AS table_comment, last_altered
  FROM system.information_schema.tables
  WHERE lower(table_catalog) IN ({CATALOG_IN})
    AND table_schema <> 'information_schema'
),
cols AS (
  SELECT table_catalog, table_schema, table_name,
         count(*) AS n_cols,
         count_if(length(trim(coalesce(comment, ''))) >= {MIN_COLUMN_COMMENT}) AS n_cols_documented
  FROM system.information_schema.columns
  WHERE lower(table_catalog) IN ({CATALOG_IN})
  GROUP BY ALL
),
pii AS (
  SELECT c.table_catalog, c.table_schema, c.table_name,
         count(*) AS n_pii_untagged,
         array_join(collect_list(c.column_name), ', ') AS pii_untagged_columns
  FROM system.information_schema.columns c
  LEFT JOIN system.information_schema.column_tags ct
    ON  ct.catalog_name = c.table_catalog AND ct.schema_name = c.table_schema
    AND ct.table_name = c.table_name     AND ct.column_name = c.column_name
    AND lower(ct.tag_name) = '{PII_TAG}'
  WHERE lower(c.table_catalog) IN ({CATALOG_IN})
    AND c.column_name RLIKE '{PII_PATTERN}'
    AND ct.tag_name IS NULL
  GROUP BY ALL
),
pk AS (
  SELECT DISTINCT table_catalog, table_schema, table_name
  FROM system.information_schema.table_constraints
  WHERE constraint_type = 'PRIMARY KEY' AND lower(table_catalog) IN ({CATALOG_IN})
),
ttags AS (
  SELECT catalog_name, schema_name, table_name,
         max(CASE WHEN lower(tag_name) = '{DOMAIN_TAG}' THEN tag_value END) AS table_domain,
         max(CASE WHEN lower(tag_name) = '{CERTIFIED_TAG}'
                   AND lower(coalesce(tag_value, 'true')) NOT IN ('false', 'no', '0')
                  THEN true ELSE false END) AS is_certified
  FROM system.information_schema.table_tags
  WHERE lower(catalog_name) IN ({CATALOG_IN})
  GROUP BY ALL
),
stags AS (
  SELECT catalog_name, schema_name,
         max(CASE WHEN lower(tag_name) = '{DOMAIN_TAG}' THEN tag_value END) AS schema_domain
  FROM system.information_schema.schema_tags
  WHERE lower(catalog_name) IN ({CATALOG_IN})
  GROUP BY ALL
),
lin AS (
  SELECT lower(source_table_full_name) AS source_table_full_name, source_path,
         lower(target_table_full_name) AS target_table_full_name,
         created_by, event_time
  FROM system.access.table_lineage
  WHERE event_time >= current_timestamp() - INTERVAL 365 DAYS
    AND (lower(source_table_catalog) IN ({CATALOG_IN}) OR lower(target_table_catalog) IN ({CATALOG_IN}))
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
    AND event_time >= current_timestamp() - INTERVAL {LOOKBACK_DAYS} DAYS
  GROUP BY 1
)
SELECT t.full_name, t.table_catalog, t.table_schema, t.table_name, t.table_type,
       t.table_owner, t.table_comment, t.last_altered,
       coalesce(tt.table_domain, st.schema_domain, t.table_schema) AS domain,
       CASE WHEN tt.table_domain IS NOT NULL THEN 'table tag'
            WHEN st.schema_domain IS NOT NULL THEN 'schema tag'
            ELSE 'schema name (fallback)' END AS domain_source,
       coalesce(tt.is_certified, false)     AS is_certified,
       coalesce(c.n_cols, 0)                AS n_cols,
       coalesce(c.n_cols_documented, 0)     AS n_cols_documented,
       pk.table_name IS NOT NULL            AS has_pk,
       coalesce(p.n_pii_untagged, 0)        AS n_pii_untagged,
       p.pii_untagged_columns,
       coalesce(w.n_upstream, 0)            AS n_upstream,
       w.last_write,
       coalesce(r.n_reads, 0)               AS n_reads,
       coalesce(r.n_readers, 0)             AS n_readers,
       coalesce(r.n_downstream, 0)          AS n_downstream
FROM t
LEFT JOIN cols c   ON c.table_catalog = t.table_catalog AND c.table_schema = t.table_schema AND c.table_name = t.table_name
LEFT JOIN pk       ON pk.table_catalog = t.table_catalog AND pk.table_schema = t.table_schema AND pk.table_name = t.table_name
LEFT JOIN pii p    ON p.table_catalog = t.table_catalog AND p.table_schema = t.table_schema AND p.table_name = t.table_name
LEFT JOIN ttags tt ON tt.catalog_name = t.table_catalog AND tt.schema_name = t.table_schema AND tt.table_name = t.table_name
LEFT JOIN stags st ON st.catalog_name = t.table_catalog AND st.schema_name = t.table_schema
LEFT JOIN writes w ON w.full_name = t.full_name
LEFT JOIN reads r  ON r.full_name = t.full_name
""").cache()

print(f"Assets found: {features.count()}")
display(features.groupBy("table_type").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Load the business glossary (optional)
# MAGIC Expected columns: `term` (required), `definition`, `domain`, `owner`, `synonyms` (comma-separated).
# MAGIC If your glossary lives on a wiki page, export it to CSV and load it into a Delta table first.

# COMMAND ----------


def norm_py(s):
    return re.sub(r"[^a-z0-9]+", "_", (s or "").strip().lower()).strip("_")


def norm(col):
    """Spark equivalent of norm_py: 'Net Revenue' / 'net-revenue' / 'NET_REVENUE' -> 'net_revenue'."""
    return F.regexp_replace(F.regexp_replace(F.lower(F.trim(col)), "[^a-z0-9]+", "_"), "^_+|_+$", "")


glossary = None
glossary_names = None
glossary_norms = set()

if GLOSSARY_TABLE:
    g = spark.table(GLOSSARY_TABLE)
    gcols = {c.lower(): c for c in g.columns}
    if "term" not in gcols:
        raise ValueError(f"{GLOSSARY_TABLE} must have a 'term' column; found {g.columns}")

    def opt(name):
        return (F.col(gcols[name]) if name in gcols else F.lit(None)).cast("string").alias(name)

    glossary = g.select(F.col(gcols["term"]).cast("string").alias("term"),
                        opt("definition"), opt("domain"), opt("owner"), opt("synonyms")).cache()

    names = F.when(F.col("synonyms").isNull(), F.array(F.col("term"))) \
             .otherwise(F.concat(F.array(F.col("term")), F.split(F.col("synonyms"), ",")))
    glossary_names = (glossary.select("term", F.explode(names).alias("name"))
                      .withColumn("name_norm", norm(F.col("name")))
                      .filter(F.col("name_norm") != "")
                      .select("term", "name_norm").distinct().cache())
    glossary_norms = {r.name_norm for r in glossary_names.select("name_norm").distinct().collect()}
    print(f"Glossary: {glossary.count()} terms, {len(glossary_norms)} names incl. synonyms")
else:
    print("No glossary table configured — glossary checks will be skipped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Parse metric views and check every measure

# COMMAND ----------

try:
    import yaml
except ImportError:  # PyYAML ships with Databricks Runtime, but be safe
    yaml = None
    print("PyYAML not available — measure-level checks will be skipped (%pip install pyyaml to enable).")

mv_rows = spark.sql(f"""
  SELECT lower(concat_ws('.', t.table_catalog, t.table_schema, t.table_name)) AS full_name,
         t.table_catalog, t.table_schema, t.table_name, v.view_definition
  FROM system.information_schema.tables t
  LEFT JOIN system.information_schema.views v
    ON v.table_catalog = t.table_catalog AND v.table_schema = t.table_schema AND v.table_name = t.table_name
  WHERE t.table_type = 'METRIC_VIEW' AND lower(t.table_catalog) IN ({CATALOG_IN})
""").collect()


def load_definition(row):
    """Return the metric view YAML as a dict, falling back to DESCRIBE ... AS JSON."""
    texts = [row.view_definition]
    try:
        q = ".".join(f"`{p}`" for p in (row.table_catalog, row.table_schema, row.table_name))
        meta = json.loads(spark.sql(f"DESCRIBE TABLE EXTENDED {q} AS JSON").collect()[0][0])
        texts.append(meta.get("view_text"))
    except Exception:
        pass
    for text in texts:
        if not text:
            continue
        try:
            doc = yaml.safe_load(text)
            if isinstance(doc, dict) and "measures" in doc:
                return doc
        except Exception:
            continue
    return None


measure_records, parse_failed = [], []
if yaml is not None:
    for row in mv_rows:
        doc = load_definition(row)
        if doc is None:
            parse_failed.append(row.full_name)
            continue
        for m in doc.get("measures") or []:
            if not isinstance(m, dict) or not m.get("name"):
                continue
            name = str(m["name"]).strip()
            expr = str(m.get("expr") or "").strip()
            comment = str(m.get("comment") or m.get("description") or "").strip()
            measure_records.append((
                row.full_name, name, norm_py(name), expr,
                re.sub(r"\s+", " ", expr.replace("`", "").lower()).strip(),
                comment,
                len(comment) >= MIN_COLUMN_COMMENT,
                (norm_py(name) in glossary_norms) if GLOSSARY_TABLE else None,
            ))

measure_schema = T.StructType([
    T.StructField("metric_view", T.StringType()),
    T.StructField("measure_name", T.StringType()),
    T.StructField("measure_norm", T.StringType()),
    T.StructField("expr", T.StringType()),
    T.StructField("expr_norm", T.StringType()),
    T.StructField("comment", T.StringType()),
    T.StructField("is_documented", T.BooleanType()),
    T.StructField("in_glossary", T.BooleanType()),
])
measures = spark.createDataFrame(measure_records, measure_schema)

# A measure name defined with different expressions in different metric views = conflicting definition
conflicts = (measures.groupBy("measure_norm")
             .agg(F.countDistinct("expr_norm").alias("n_definitions"),
                  F.collect_set("metric_view").alias("defined_in"))
             .filter(F.col("n_definitions") > 1))

measures = (measures.join(conflicts, "measure_norm", "left")
            .withColumn("is_conflicting", F.col("n_definitions").isNotNull())
            .withColumn("conflicting_views", F.array_join(F.col("defined_in"), ", "))
            .drop("n_definitions", "defined_in")
            .cache())

mv_stats = (measures.groupBy(F.col("metric_view").alias("full_name"))
            .agg(F.count("*").alias("n_measures"),
                 F.count_if("is_documented").alias("n_measures_documented"),
                 F.count_if("in_glossary").alias("n_measures_in_glossary"),
                 F.count_if("is_conflicting").alias("n_measures_conflicting")))

print(f"Metric views: {len(mv_rows)}, measures parsed: {len(measure_records)}, unparseable views: {len(parse_failed)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Score every asset

# COMMAND ----------


def wavg(pairs):
    """Weighted average that ignores null components and re-normalises the remaining weights."""
    num = sum(F.coalesce(c, F.lit(0.0)) * w for c, w in pairs)
    den = sum(F.when(c.isNotNull(), F.lit(float(w))).otherwise(F.lit(0.0)) for c, w in pairs)
    return F.when(den > 0, num / den)


col = F.col
is_mv = col("table_type") == "METRIC_VIEW"
has_glossary = F.lit(GLOSSARY_TABLE is not None)

df = (features.join(mv_stats, "full_name", "left")
      .withColumn("mv_parse_ok", F.when(is_mv, col("n_measures").isNotNull())))

comment_len = F.length(F.trim(F.coalesce(col("table_comment"), F.lit(""))))
table_doc = F.when(comment_len >= MIN_TABLE_COMMENT, 100.0).when(comment_len > 0, 40.0).otherwise(0.0)
col_doc = F.when(col("n_cols") > 0, 100.0 * col("n_cols_documented") / col("n_cols"))
measure_doc = F.when(col("n_measures") > 0, 100.0 * col("n_measures_documented") / col("n_measures"))
glossary_cov = F.when(has_glossary & (col("n_measures") > 0),
                      100.0 * col("n_measures_in_glossary") / col("n_measures"))

semantic = F.when(is_mv, wavg([(table_doc, 0.3), (measure_doc, 0.5), (glossary_cov, 0.2)])) \
            .otherwise(wavg([(table_doc, 0.4), (col_doc, 0.6)]))

owner_score = F.when(col("table_owner").isNull() | (F.trim(col("table_owner")) == ""), 0.0) \
               .when(col("table_owner").contains("@"), 60.0) \
               .otherwise(100.0)  # groups / service principals survive people leaving
domain_tagged = F.when(col("domain_source") != "schema name (fallback)", 100.0).otherwise(0.0)
pii_ok = F.when(col("n_pii_untagged") == 0, 100.0).otherwise(0.0)
governance = wavg([(owner_score, 0.5), (domain_tagged, 0.25), (pii_ok, 0.25)])

consistency = F.when(is_mv & (col("n_measures") > 0),
                     100.0 * (1 - col("n_measures_conflicting") / col("n_measures")))

structural = F.when(col("table_type").isin(PK_TYPES), F.when(col("has_pk"), 100.0).otherwise(0.0))

lineage = F.when(col("n_upstream") > 0, 100.0) \
           .when((col("n_downstream") > 0) | (col("n_reads") > 0), 50.0) \
           .otherwise(0.0)

days_since_update = F.datediff(F.current_timestamp(), F.coalesce(col("last_write"), col("last_altered")))
freshness = F.when(col("table_type").isin(DATA_TYPES),
                   F.when(days_since_update <= FRESHNESS_DAYS, 100.0)
                    .when(days_since_update <= 4 * FRESHNESS_DAYS, 50.0)
                    .otherwise(0.0))

df = (df.withColumn("semantic_clarity", semantic)
        .withColumn("governance", governance)
        .withColumn("consistency", consistency)
        .withColumn("structural", structural)
        .withColumn("lineage", lineage)
        .withColumn("freshness", freshness)
        .withColumn("days_since_update", days_since_update))

raw = wavg([(col(name), w) for name, w in WEIGHTS.items()])
no_owner = col("table_owner").isNull() | (F.trim(col("table_owner")) == "")
untagged_pii = col("n_pii_untagged") > 0

capped = F.when(no_owner, F.least(raw, F.lit(float(CAP_NO_OWNER)))).otherwise(raw)
capped = F.when(untagged_pii, F.least(capped, F.lit(float(CAP_UNTAGGED_PII)))).otherwise(capped)

gate = F.concat_ws("; ",
                   F.when(no_owner, F.lit(f"no owner (capped at {CAP_NO_OWNER})")),
                   F.when(untagged_pii, F.lit(f"untagged PII (capped at {CAP_UNTAGGED_PII})")))

grade = None
for threshold, label in GRADES:
    grade = F.when(col("trust_score") >= threshold, label) if grade is None \
        else grade.when(col("trust_score") >= threshold, label)

asset_scores = (df.withColumn("raw_score", F.round(raw, 1))
                  .withColumn("trust_score", F.round(capped, 1))
                  .withColumn("gates_applied", F.when(gate != "", gate))
                  .withColumn("grade", grade)
                  # Importance for roll-ups: heavily used and certified assets count more
                  .withColumn("importance_weight",
                              (1 + F.log1p(col("n_reads"))) * F.when(col("is_certified"), 2.0).otherwise(1.0))
                  .withColumn("run_ts", F.lit(RUN_TS)))

for c in WEIGHTS:
    asset_scores = asset_scores.withColumn(c, F.round(col(c), 1))
asset_scores = asset_scores.cache()

display(asset_scores.select("full_name", "table_type", "domain", "trust_score", "grade", "gates_applied",
                            *WEIGHTS.keys()).orderBy("trust_score"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Roll up to business domains (leadership view)

# COMMAND ----------


def rollup(df, domain_col):
    w = col("importance_weight")
    return df.groupBy(domain_col.alias("domain")).agg(
        F.round(F.sum(col("trust_score") * w) / F.sum(w), 1).alias("domain_score"),
        F.round(F.avg("trust_score"), 1).alias("unweighted_score"),
        F.count("*").alias("n_assets"),
        F.count_if(col("grade") == "Trusted").alias("n_trusted"),
        F.count_if(col("grade") == "Use with caution").alias("n_caution"),
        F.count_if(col("grade") == "Not AI-ready").alias("n_not_ready"),
        F.count_if(col("is_certified")).alias("n_certified"),
        F.count_if(col("table_type") == "METRIC_VIEW").alias("n_metric_views"),
        *[F.round(F.avg(c), 1).alias(f"avg_{c}") for c in WEIGHTS],
    )


domain_scores = (rollup(asset_scores, col("domain"))
                 .unionByName(rollup(asset_scores, F.lit("__ALL__")))
                 .withColumn("domain_grade",
                             F.when(col("domain_score") >= GRADES[0][0], GRADES[0][1])
                              .when(col("domain_score") >= GRADES[1][0], GRADES[1][1])
                              .otherwise(GRADES[2][1]))
                 .withColumn("run_ts", F.lit(RUN_TS)))

display(domain_scores.orderBy("domain_score"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Glossary health

# COMMAND ----------

glossary_health = None
if glossary is not None:
    asset_names = (asset_scores.select(norm(col("table_name")).alias("name_norm"))
                   .unionByName(spark.table("system.information_schema.columns")
                                .filter(F.lower(col("table_catalog")).isin([c.lower() for c in CATALOGS]))
                                .select(norm(col("column_name")).alias("name_norm")))
                   .unionByName(measures.select(col("measure_norm").alias("name_norm")))
                   .distinct())

    implemented = (glossary_names.join(asset_names, "name_norm", "inner")
                   .groupBy("term").agg(F.countDistinct("name_norm").alias("n_matching_names")))

    def_len = F.length(F.trim(F.coalesce(col("definition"), F.lit(""))))
    glossary_health = (glossary.join(implemented, "term", "left")
                       .withColumn("has_definition", def_len >= MIN_TABLE_COMMENT)
                       .withColumn("has_owner", F.coalesce(F.trim(col("owner")), F.lit("")) != "")
                       .withColumn("is_implemented", F.coalesce(col("n_matching_names"), F.lit(0)) > 0)
                       .withColumn("term_score", F.round(
                           (col("has_definition").cast("int") * 40 + col("has_owner").cast("int") * 30
                            + col("is_implemented").cast("int") * 30).cast("double"), 1))
                       .withColumn("run_ts", F.lit(RUN_TS))
                       .cache())
    display(glossary_health.orderBy("term_score"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Findings (engineer to-do list)

# COMMAND ----------

SEVERITY_RANK = {"high": 1, "medium": 2, "low": 3}


def finding(source, cond, severity, rule_id, detail, recommendation,
            asset=col("full_name"), asset_type=col("table_type"), domain=col("domain"), owner=col("table_owner")):
    return source.filter(cond).select(
        asset.alias("asset"), asset_type.alias("asset_type"), domain.alias("domain"),
        owner.cast("string").alias("owner"),
        F.lit(severity).alias("severity"), F.lit(SEVERITY_RANK[severity]).alias("severity_rank"),
        F.lit(rule_id).alias("rule"), detail.cast("string").alias("detail"),
        F.lit(recommendation).alias("recommendation"))


a = asset_scores
rules = [
    finding(a, no_owner, "high", "no_owner", F.lit("Asset has no owner"),
            "ALTER ... OWNER TO a group or service principal accountable for this data"),
    finding(a, untagged_pii, "high", "untagged_pii", F.concat(F.lit("Likely PII columns: "), col("pii_untagged_columns")),
            f"Review the columns and tag them with '{PII_TAG}' (and apply a column mask if needed)"),
    finding(a, col("is_certified") & (col("trust_score") < GRADES[1][0]), "high", "certified_but_untrusted",
            F.concat(F.lit("Certified asset scores "), col("trust_score").cast("string")),
            "Fix the issues below or remove the certification tag"),
    finding(a, col("table_owner").contains("@"), "medium", "individual_owner",
            F.concat(F.lit("Owned by an individual: "), col("table_owner")),
            "Transfer ownership to a team group so it survives people changing roles"),
    finding(a, comment_len < MIN_TABLE_COMMENT, "medium", "missing_description",
            F.when(comment_len == 0, F.lit("No description")).otherwise(F.lit("Description too short")),
            "COMMENT ON TABLE ... with what one row represents, source system and intended use"),
    finding(a, (~is_mv) & (col("n_cols") > 0) & (col("n_cols_documented") < 0.5 * col("n_cols")), "medium",
            "columns_undocumented",
            F.format_string("%d of %d columns documented", col("n_cols_documented"), col("n_cols")),
            "Add column comments (business meaning, units, allowed values)"),
    finding(a, col("freshness") == 0, "medium", "stale",
            F.format_string("Last update %d days ago", col("days_since_update")),
            f"Check the pipeline, or adjust the SLA ({FRESHNESS_DAYS}d) if this is static reference data"),
    finding(a, is_mv & (col("mv_parse_ok") == False), "medium", "metric_view_unparsed",  # noqa: E712
            F.lit("Could not read the metric view YAML definition"),
            "Check the definition manually; measure-level checks were skipped"),
    finding(a, col("structural") == 0, "low", "no_primary_key", F.lit("No primary key declared"),
            "ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY (informational) on the business key"),
    finding(a, col("domain_source") == "schema name (fallback)", "low", "no_domain_tag",
            F.lit("Domain inferred from schema name"),
            f"Tag the table or schema with '{DOMAIN_TAG}'"),
    finding(a, col("lineage") == 0, "low", "no_lineage", F.lit("No lineage or usage recorded"),
            "Confirm it is still needed; unused assets confuse people and AI agents"),
]

m = measures.withColumn("asset", F.concat(col("metric_view"), F.lit(" :: "), col("measure_name")))
m = m.join(asset_scores.select(col("full_name").alias("metric_view"), "domain", "table_owner"), "metric_view", "left")
mkw = dict(asset=col("asset"), asset_type=F.lit("MEASURE"))
rules += [
    finding(m, col("is_conflicting"), "high", "conflicting_measure",
            F.concat(F.lit("Defined differently in: "), col("conflicting_views")),
            "Agree one definition (or rename the variants, e.g. gross_revenue vs net_revenue)", **mkw),
    finding(m, ~col("is_documented"), "medium", "measure_undocumented", F.lit("Measure has no comment"),
            "Add a comment to the measure in the metric view YAML", **mkw),
]
if GLOSSARY_TABLE:
    rules.append(finding(m, col("in_glossary") == False, "medium", "measure_not_in_glossary",  # noqa: E712
                         F.lit("No matching glossary term or synonym"),
                         "Add the business term to the glossary, or add a synonym", **mkw))

if glossary_health is not None:
    gkw = dict(asset=F.concat(F.lit("glossary :: "), col("term")), asset_type=F.lit("GLOSSARY_TERM"),
               domain=col("domain"), owner=col("owner"))
    rules += [
        finding(glossary_health, ~col("has_definition"), "medium", "term_undefined",
                F.lit("Definition missing or too short"), "Write a clear business definition", **gkw),
        finding(glossary_health, ~col("has_owner"), "medium", "term_unowned",
                F.lit("No owner"), "Assign a business owner / data steward", **gkw),
        finding(glossary_health, ~col("is_implemented"), "low", "term_not_implemented",
                F.lit("No table, column or measure matches this term"),
                "Implement it in a metric view, add a synonym, or retire the term", **gkw),
    ]

findings = rules[0]
for r in rules[1:]:
    findings = findings.unionByName(r)
findings = findings.withColumn("run_ts", F.lit(RUN_TS)).cache()

display(findings.groupBy("severity", "rule").count().orderBy("count", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 — Save results

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OUT}")


def save(df, name):
    df.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{OUT}.`{name}`")
    print(f"Wrote {OUT}.{name}")


save(asset_scores, "asset_scores")
save(domain_scores, "domain_scores")
save(findings, "findings")
save(measures.withColumn("run_ts", F.lit(RUN_TS)), "metric_measures")
if glossary_health is not None:
    save(glossary_health, "glossary_health")

overall = domain_scores.filter(col("domain") == "__ALL__").first()
print(f"\nOverall Context Trust Score: {overall.domain_score} ({overall.domain_grade}) across {overall.n_assets} assets")
