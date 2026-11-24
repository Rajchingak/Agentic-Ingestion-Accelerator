# Context Trust Score — User Guide

This guide covers the deployed Databricks App and what its current features do. The app is available at:

<https://context-trust-score-8259556333772628.gcp.databricksapps.com>

Open the link while signed in to the Databricks workspace that hosts the app. You can also find
**context-trust-score** from the workspace's **Databricks Apps** page.

## What the app does

The app has two tabs:

- **Context trust** runs a Unity Catalog metadata audit and appends asset scores, domain scores, and findings to
  Delta tables.
- **AI answer quality** lets you enter answers to three built-in benchmark questions and calculates heuristic
  quality scores in the page.

The context tab currently returns run details and output table names; it does not yet display a score dashboard.
The benchmark tab does not call an AI model, save results, or use custom benchmark questions.

## Before you start

Ask your Databricks administrator to confirm that:

1. The app's configured identity can read the Unity Catalog catalogs you want to audit.
2. The app can query `system.information_schema` and `system.access.table_lineage`.
3. The app can create the output schema and tables in the catalog you choose.
4. The workspace app has the Databricks compute / Spark access required to run the scoring engine.
5. Databricks Connect serverless compute is available to the app's service principal.

The app connects to Databricks serverless compute through Databricks Connect. If this connection fails, check
that serverless compute is enabled and that the app's service principal is authorized to use it and access the
requested catalogs.

## Run a context-trust audit

1. Open the app and select **Context trust**.
2. Fill in the configuration:

   | Field | What to enter |
   |---|---|
   | Catalogs to audit | One or more Unity Catalog catalog names, separated by commas. The deployed app defaults to `incontext`. Example: `sales_prod, finance_prod`. |
   | Output schema | A destination in `catalog.schema` form. The deployed app defaults to `incontext.incontext-schema`. The schema is created if it does not exist, provided the app identity has permission. |
   | Domain tag | The Unity Catalog table/schema tag name used to assign a business domain. Default: `domain`. |
   | Certified tag | The tag name used to mark certified assets. Default: `certified`. |
   | PII tag | The column tag name used to identify tagged PII. Default: `pii`. |
   | Glossary table (optional) | An optional table identifier. It is accepted by the form, but the current app scoring path does not yet read this table. |
   | Freshness SLA in days | Age threshold for the freshness dimension. Assets older than this may lose freshness points; the next threshold is four times this value. Default: `7`. |
   | Usage lookback in days | Window used to count recent reads in Unity Catalog lineage activity and weight domain scores. Default: `30`. |

3. Select **Run score** and wait for the success message or error.
4. On success, note the output table names shown in the returned run details.

Catalog names are comma-separated; do not include schema names in the catalog list. The output schema is a
separate destination and is not one of the catalogs to audit unless you also include it.

### Results written by the current scorer

For the default output schema of `incontext.incontext-schema`, the current run appends to:

- `incontext.incontext-schema.asset_scores` — per-asset metadata features, dimension scores, overall trust score, grade,
  importance weight, and `run_ts`.
- `incontext.incontext-schema.domain_scores` — weighted and unweighted scores and asset counts per domain, including the
  `__ALL__` roll-up.
- `incontext.incontext-schema.findings` — high-severity findings for assets without an owner or with likely-PII columns
  that lack the configured PII tag.

Each run is appended, not replaced. Use `run_ts` to select or compare runs. The current scorer writes these
three tables; additional output names listed elsewhere in the code are not populated by this app flow yet.

## Understand the trust score

Scores range from 0 to 100. The current scorer grades assets as:

- **Trusted:** 85–100
- **Use with caution:** 65–84.9
- **Not AI-ready:** below 65

The score considers semantic documentation, governance, consistency, structural keys, lineage, and freshness.
Weights are defined in the scoring engine and are re-normalized when a dimension does not apply. Two critical
gates cap the final score regardless of the weighted average:

- No asset owner: maximum score of 40.
- Likely-PII column without a matching PII tag: maximum score of 50.

Domain scores are weighted by recent reads and certification. A frequently read or certified asset therefore
has more influence on its domain's roll-up than a low-usage, uncertified asset.

Treat results as review guidance, not a compliance determination. PII detection is based on column-name
patterns; freshness can penalize static reference data; and lineage reflects only activity captured by Unity
Catalog.

## Evaluate AI answers

1. Select **AI answer quality**.
2. Enter an answer for each displayed question. The questions are fixed examples covering a revenue KPI,
   metric selection, and a churn-table lookup.
3. Select **Evaluate answers**.
4. Review the overall average and the per-question table.

The evaluator compares normalized word overlap with a fixed expected answer, gives bonuses for mentioning the
expected metric and table, and scores answer length and references to “table” or “metric.” It is a lightweight
heuristic, not semantic evaluation: a high score does not prove that an answer is factually correct or properly
cited. Results are not persisted, so copy any results you need before leaving or refreshing the page.

## Glossary setup

The repository includes [`glossary_setup.sql`](./glossary_setup.sql) as a starting point for a business glossary
Delta table. It defines `term` plus optional definition, domain, owner, and synonyms columns. The current app
form accepts a glossary table name, but the current scoring engine does not yet consume it; setting the field
does not currently change scores.

## Troubleshooting

| What you see | What to check |
|---|---|
| Workspace sign-in prompt | Sign in to the Databricks workspace where the app is deployed, then open the app link again. |
| Catalog access or permission error | Confirm the app identity has access to each requested catalog and to the relevant system tables. |
| Cannot create output schema/table | Confirm `output_schema` is a valid `catalog.schema` and the app identity has schema/table creation privileges. |
| Unable to connect to serverless compute | Confirm Databricks serverless compute is enabled and the app service principal can use it and read the requested catalogs. |
| No useful lineage or freshness signal | Confirm Unity Catalog has captured lineage activity and choose an SLA/lookback suitable for the data. |
| Low score from a missing owner or PII tag | Assign an accountable owner and review/tag likely-PII columns in Unity Catalog, then run the audit again. |

## Current scope

The app currently provides the two interactive tabs described above. Domain dashboards, asset grids, persisted
benchmark summaries, trends, glossary health, and metric-conflict review are planned views and are not currently
rendered by the UI.
