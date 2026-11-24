canyou# Context Trust Score

Audits a Databricks Unity Catalog estate (tables, views, metric views and a business glossary) and scores how
trustworthy it is as **business context**, for people and for AI agents such as Genie.

- **Leadership** sees one score per business domain, a trend over time, and conflicting metric definitions.
- **Data engineers** get a prioritised list of findings, each with an owner and a recommended fix.

## Files

| File | Purpose |
|---|---|
| `context_trust_score.py` | Databricks notebook (import via *Workspace → Import*). Computes scores and writes the output tables. |
| `dashboard_queries.sql` | One query per widget for an AI/BI dashboard (leadership page + engineering page). |
| `glossary_setup.sql` | Creates the business glossary table with example rows. |

## Requirements

- Unity Catalog with read access to `system.information_schema` and `system.access.table_lineage`
- Databricks Runtime 14.3 LTS or newer (or serverless)
- `CREATE SCHEMA` / `CREATE TABLE` on the output catalog

## How to run

1. Import the notebook and fill in the widgets:
   - `catalogs`: catalogs to audit, e.g. `sales_prod,finance_prod`
   - `output_schema`: where the results go, e.g. `governance.trust`
   - `domain_tag`, `certified_tag`, `pii_tag`: the tag names your organisation uses
   - `glossary_table`: optional, see below
2. Run it once and review the results in the notebook.
3. Schedule it as a job (daily or weekly). Every run is appended with `run_ts`, so trends build up over time.
4. Build a dashboard from `dashboard_queries.sql`.

For instructions for the deployed Databricks App, its current UI, output tables, and troubleshooting, see the
[User Guide](./USER_GUIDE.md).

## Glossary format

A Delta table with a `term` column (required) and optional `definition`, `domain`, `owner` and `synonyms`
(comma-separated) columns. Create it with `glossary_setup.sql`.
Terms are matched to measure, column and table names after normalising case and punctuation, so
`Net Revenue`, `net-revenue` and `net_revenue` all match.

## Databricks App architecture

This repo is structured so the scoring logic can evolve from a notebook prototype into a production Databricks App without duplicating code.

### Recommended production layout

- `app/config.py` — app configuration model and validation
- `app/scorer.py` — reusable scoring engine entry point
- `app/job_runner.py` — scheduled-job / app runner wrapper
- `ui/main.py` — Streamlit-based Databricks App interface
- `context_trust_score.py` — notebook reference implementation and rapid prototyping environment

### How it fits together

1. The notebook stays as the development and debugging reference.
2. The app reuses the same config and scoring contract.
3. Databricks Jobs trigger the same engine for scheduled refreshes.
4. Unity Catalog tables store asset scores, findings and domain rollups.
5. The UI reads the latest results and renders them to leadership and engineering users.

### Suggested runtime flow

- User opens the app and enters catalog / schema configuration.
- The app validates the inputs.
- It triggers `ContextTrustScoreEngine.run()`.
- The scoring engine computes trust metrics and writes to the output tables.
- The app reads those tables and displays domain scores, findings and trend views.

This keeps the notebook and app aligned while allowing the project to grow into a real Databricks Application.

## AI answer-quality layer

The app should not stop at metadata trust. To evaluate whether the AI is actually answering business questions correctly, add a second layer that benchmarks agent answers against known-good responses.

### Recommended benchmark inputs

- `prompt_id`
- `domain`
- `question`
- `expected_answer`
- `expected_metric`
- `expected_table`
- `notes`

### Recommended scoring dimensions

- `correctness` — did the answer match the expected business fact?
- `groundedness` — was the answer tied to the right table, metric or source?
- `completeness` — did it include context such as time period or units?
- `citation_quality` — did it reference the correct asset or metric?

This produces a separate answer-quality score that can be combined with the trust score into an overall AI readiness score.

## How the score works

Each asset gets up to six dimension scores (0–100). Dimensions that don't apply to an asset are skipped, and the
remaining weights are re-normalised.

| Dimension | Weight | Applies to | Measures |
|---|---|---|---|
| Semantic clarity | 30 | all | Table description; column descriptions (tables/views); measure descriptions and glossary coverage (metric views) |
| Governance | 25 | all | Owner (group/service principal 100, individual 60, none 0); domain tag; likely-PII columns tagged |
| Consistency | 15 | metric views | Share of measures whose name isn't defined differently in another metric view |
| Structural | 10 | managed/external tables | Primary key declared |
| Lineage | 10 | all | Upstream lineage known (100), only downstream/usage known (50), nothing (0) |
| Freshness | 10 | tables with data | Updated within the SLA (100), within 4× SLA (50), older (0) |

**Critical gates** stop a good average from hiding a serious problem:
- No owner → score capped at 40
- Likely-PII column without a PII tag → score capped at 50

**Grades:** 85+ *Trusted*, 65–84 *Use with caution*, below 65 *Not AI-ready*.

**Domain roll-up** is weighted by importance: `(1 + ln(1 + reads in lookback)) × 2 if certified`, so heavily used
and certified assets count more than scratch tables.

All weights, thresholds, caps and the PII pattern are in the *Scoring configuration* cell.

## Known limitations

- **Not yet tested in your workspace.** System table columns and metric view metadata vary by Databricks
  release. If a step fails, the error usually names the column involved.
- **Metric view parsing** reads the YAML definition from `information_schema.views`, falling back to
  `DESCRIBE TABLE EXTENDED ... AS JSON`. Views it can't parse are reported as `metric_view_unparsed`.
- **Conflict detection** compares normalised expressions, so two formulas that are equivalent but written
  differently are still flagged. That's usually still worth a human look.
- **PII detection** is a column-name heuristic. Treat it as a prompt to review, not a classification.
- **Freshness** penalises static reference tables. Raise the SLA or exclude them.
- Lineage only covers activity Unity Catalog has captured (roughly the last year).

## Roadmap

1. **Data quality dimension:** pull pass rates from Lakehouse Monitoring and pipeline expectations.
2. **Foreign keys and relationships:** check that the ontology's entity relationships are declared.
3. **LLM description scoring:** grade descriptions for quality, not just length.
4. **Genie readiness:** score the tables and instructions behind each Genie space.
