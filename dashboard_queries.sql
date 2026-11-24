-- Context Trust Score — dashboard queries
-- Output schema: incontext.`incontext-schema` (tables: asset_scores, domain_scores, findings, metric_measures).
-- Each query is one dashboard widget. Only columns present in those tables are used.

-- =====================================================================
-- LEADERSHIP PAGE
-- =====================================================================

-- 1. Headline: overall score, grade and change since the previous run (counter widget)
WITH runs AS (
  SELECT run_ts, domain_score, n_assets,
         LAG(domain_score) OVER (ORDER BY run_ts) AS prev_score
  FROM incontext.`incontext-schema`.domain_scores
  WHERE domain = '__ALL__'
)
SELECT domain_score AS overall_score,
       CASE WHEN domain_score >= 85 THEN 'Trusted'
            WHEN domain_score >= 65 THEN 'Use with caution'
            ELSE 'Not AI-ready' END AS domain_grade,
       n_assets,
       round(domain_score - prev_score, 1) AS change_since_last_run
FROM runs
ORDER BY run_ts DESC
LIMIT 1;

-- 2. Score by business domain, latest run (bar chart)
SELECT domain, domain_score,
       CASE WHEN domain_score >= 85 THEN 'Trusted'
            WHEN domain_score >= 65 THEN 'Use with caution'
            ELSE 'Not AI-ready' END AS domain_grade,
       n_assets, n_trusted, n_caution, n_not_ready, n_certified, n_metric_views
FROM incontext.`incontext-schema`.domain_scores
WHERE run_ts = (SELECT max(run_ts) FROM incontext.`incontext-schema`.domain_scores)
  AND domain <> '__ALL__'
ORDER BY domain_score;

-- 3. Trend by domain over time (line chart: x = run_date, y = domain_score, series = domain)
SELECT date(run_ts) AS run_date,
       CASE WHEN domain = '__ALL__' THEN 'All domains' ELSE domain END AS domain,
       max_by(domain_score, run_ts) AS domain_score
FROM incontext.`incontext-schema`.domain_scores
GROUP BY ALL
ORDER BY run_date;

-- 4. Where each domain is weak, from per-asset dimension scores (heatmap / table: domain x dimension)
SELECT domain,
       round(avg(semantic_clarity), 1) AS avg_semantic_clarity,
       round(avg(governance), 1)       AS avg_governance,
       round(avg(consistency), 1)      AS avg_consistency,
       round(avg(structural), 1)       AS avg_structural,
       round(avg(lineage), 1)          AS avg_lineage,
       round(avg(freshness), 1)        AS avg_freshness
FROM incontext.`incontext-schema`.asset_scores
WHERE run_ts = (SELECT max(run_ts) FROM incontext.`incontext-schema`.asset_scores)
GROUP BY domain
ORDER BY domain;

-- 5. Metric definitions in conflict (counter; high = business risk)
-- metric_measures has no run_ts column, so this counts across all runs written to the table.
SELECT count(DISTINCT measure_norm) AS conflicting_metrics
FROM incontext.`incontext-schema`.metric_measures
WHERE is_conflicting;

-- =====================================================================
-- ENGINEERING PAGE
-- =====================================================================

-- 6. Fix-first list: important assets with low scores
SELECT full_name, table_type, domain, table_owner, trust_score, raw_score, grade,
       concat_ws(', ',
         CASE WHEN (table_owner IS NULL OR trim(table_owner) = '') AND raw_score > trust_score THEN 'no_owner_cap' END,
         CASE WHEN n_pii_untagged > 0 AND raw_score > trust_score THEN 'untagged_pii_cap' END) AS caps_applied,
       n_reads, is_certified,
       round((100 - trust_score) * importance_weight, 1) AS priority
FROM incontext.`incontext-schema`.asset_scores
WHERE run_ts = (SELECT max(run_ts) FROM incontext.`incontext-schema`.asset_scores)
  AND grade <> 'Trusted'
ORDER BY priority DESC
LIMIT 50;

-- 7. All findings, latest run (filterable table: domain, owner, severity, rule)
SELECT severity, rule, asset, asset_type, domain, owner, detail, recommendation
FROM incontext.`incontext-schema`.findings
WHERE run_ts = (SELECT max(run_ts) FROM incontext.`incontext-schema`.findings)
ORDER BY severity_rank, domain, asset;

-- 8. Findings per owner (who needs to act)
SELECT coalesce(owner, '(no owner)') AS owner,
       count_if(severity = 'high')   AS high,
       count_if(severity = 'medium') AS medium,
       count_if(severity = 'low')    AS low
FROM incontext.`incontext-schema`.findings
WHERE run_ts = (SELECT max(run_ts) FROM incontext.`incontext-schema`.findings)
GROUP BY ALL
ORDER BY high DESC, medium DESC;

-- 9. Conflicting measure definitions in detail
-- metric_measures has no run_ts column, so DISTINCT removes rows repeated by earlier runs.
SELECT DISTINCT measure_name, metric_view, expr, conflicting_views, measure_norm
FROM incontext.`incontext-schema`.metric_measures
WHERE is_conflicting
ORDER BY measure_norm, metric_view;

-- 10. Documentation and PII gaps per asset, latest run
SELECT full_name, domain, table_owner, n_cols, n_cols_documented,
       round(100.0 * n_cols_documented / nullif(n_cols, 0), 1) AS pct_cols_documented,
       n_pii_untagged, pii_untagged_columns, days_since_update
FROM incontext.`incontext-schema`.asset_scores
WHERE run_ts = (SELECT max(run_ts) FROM incontext.`incontext-schema`.asset_scores)
ORDER BY n_pii_untagged DESC, pct_cols_documented;
