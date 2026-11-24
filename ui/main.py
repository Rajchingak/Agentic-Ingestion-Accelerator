from __future__ import annotations

import re

import pandas as pd
import streamlit as st

from app.answer_quality import AnswerQualityEvaluator, default_benchmark_questions
from app.config import AppConfig
from app.dashboard import load_dashboard_queries, run_dashboard_query
from app.job_runner import get_spark_session, run_context_trust_score


st.set_page_config(page_title="Context Trust Score", page_icon="📊", layout="wide")
st.title("Context Trust Score")
st.caption("Databricks App for Unity Catalog trust, business-context quality, and AI answer readiness")

context_tab, dashboard_tab, quality_tab = st.tabs(["Context trust", "Score dashboard", "AI answer quality"])

with context_tab:
    with st.form("config_form"):
        catalogs = st.text_input("Catalogs to audit", value="incontext")
        output_schema = st.text_input("Output schema", value="incontext.incontext-schema")
        domain_tag = st.text_input("Domain tag", value="domain")
        certified_tag = st.text_input("Certified tag", value="certified")
        pii_tag = st.text_input("PII tag", value="pii")
        glossary_table = st.text_input("Glossary table (optional)", value="")
        freshness_days = st.number_input("Freshness SLA in days", min_value=1, value=7)
        lookback_days = st.number_input("Usage lookback in days", min_value=1, value=30)

        submitted = st.form_submit_button("Run score")

    if submitted:
        cfg = AppConfig(
            catalogs=[c.strip() for c in catalogs.split(",") if c.strip()],
            output_schema=output_schema,
            domain_tag=domain_tag,
            certified_tag=certified_tag,
            pii_tag=pii_tag,
            glossary_table=glossary_table or None,
            freshness_days=int(freshness_days),
            lookback_days=int(lookback_days),
            run_mode="manual",
        )

        try:
            cfg.validate()
            result = run_context_trust_score(cfg)
            st.success("Context trust score run completed successfully.")
            st.json(result)
        except Exception as exc:  # pragma: no cover - app-level safety
            st.error(f"Scoring run failed: {exc}")

@st.cache_data(ttl=300, show_spinner=False)
def _query_widget(number: int, output_schema: str):
    query = next(q for q in load_dashboard_queries() if q.number == number)
    return run_dashboard_query(get_spark_session(), query, output_schema)


def _widget(number: int, output_schema: str, missing_message: str = "No output table for this widget yet. Run the score first."):
    """Return the widget's DataFrame, or None after showing why it could not load."""
    try:
        df = _query_widget(number, output_schema)
    except Exception as exc:  # pragma: no cover - app-level safety
        message = str(exc)
        if "TABLE_OR_VIEW_NOT_FOUND" in message:
            st.info(missing_message)
        else:
            st.warning(f"Could not load this widget: {message.splitlines()[0][:300]}")
        return None
    if df.empty:
        st.info("No rows for the latest run.")
        return None
    return df


def _title(number: int, queries) -> str:
    """Widget title from its '-- N. Title' header, without the trailing '(chart type)' hint."""
    q = queries.get(number)
    return re.sub(r"\s*\([^)]*\)\s*$", "", q.title) if q else f"Query {number}"


with dashboard_tab:
    queries = {q.number: q for q in load_dashboard_queries()}
    top_left, top_right = st.columns([3, 1])
    with top_left:
        dash_schema = st.text_input("Output schema to read", value="incontext.incontext-schema", key="dash_schema")
    with top_right:
        st.write("")
        if st.button("Refresh data"):
            _query_widget.clear()

    with st.expander("Dashboard SQL (dashboard_queries.sql)"):
        for q in queries.values():
            st.markdown(f"**{q.number}. {q.title}**")
            st.code(q.sql, language="sql")

    with st.spinner("Loading dashboard..."):
        st.header("Leadership")

        head = _widget(1, dash_schema)
        conflicts = None
        m1, m2, m3, m4 = st.columns(4)
        if head is not None:
            row = head.iloc[0]
            delta = row["change_since_last_run"]
            m1.metric("Overall trust score", f"{row['overall_score']:.1f}",
                      delta=None if pd.isna(delta) else f"{delta:+.1f}")
            m2.metric("Grade", row["domain_grade"])
            m3.metric("Assets scored", int(row["n_assets"]))
        with m4:
            conflicts = _widget(5, dash_schema)
            if conflicts is not None:
                st.metric("Conflicting metric definitions", int(conflicts.iloc[0]["conflicting_metrics"]))

        st.subheader(_title(2, queries))
        by_domain = _widget(2, dash_schema)
        if by_domain is not None:
            st.bar_chart(by_domain.set_index("domain")["domain_score"], horizontal=True)
            st.dataframe(by_domain, hide_index=True, use_container_width=True)

        st.subheader(_title(3, queries))
        trend = _widget(3, dash_schema)
        if trend is not None:
            st.line_chart(trend.pivot_table(index="run_date", columns="domain", values="domain_score"))

        st.subheader(_title(4, queries))
        weak = _widget(4, dash_schema)
        if weak is not None:
            dims = [c for c in weak.columns if c.startswith("avg_")]
            st.dataframe(
                weak.replace({"domain": {"__ALL__": "All domains"}}),
                hide_index=True,
                use_container_width=True,
                column_config={
                    c: st.column_config.ProgressColumn(c.removeprefix("avg_").replace("_", " ").title(),
                                                       min_value=0, max_value=100, format="%.0f")
                    for c in dims
                },
            )

        st.header("Engineering")

        st.subheader(_title(6, queries))
        fix_first = _widget(6, dash_schema)
        if fix_first is not None:
            st.dataframe(fix_first, hide_index=True, use_container_width=True)

        st.subheader(_title(8, queries))
        per_owner = _widget(8, dash_schema)
        if per_owner is not None:
            st.bar_chart(per_owner.set_index("owner")[["high", "medium", "low"]], horizontal=True)

        st.subheader(_title(7, queries))
        findings = _widget(7, dash_schema)
        if findings is not None:
            f1, f2, f3 = st.columns(3)
            sev = f1.multiselect("Severity", sorted(findings["severity"].dropna().unique()))
            dom = f2.multiselect("Domain", sorted(findings["domain"].dropna().unique()))
            own = f3.multiselect("Owner", sorted(findings["owner"].fillna("(no owner)").unique()))
            view = findings.assign(owner=findings["owner"].fillna("(no owner)"))
            if sev:
                view = view[view["severity"].isin(sev)]
            if dom:
                view = view[view["domain"].isin(dom)]
            if own:
                view = view[view["owner"].isin(own)]
            st.dataframe(view, hide_index=True, use_container_width=True)

        st.subheader(_title(9, queries))
        measures = _widget(9, dash_schema)
        if measures is not None:
            st.dataframe(measures, hide_index=True, use_container_width=True)

        st.subheader(_title(10, queries))
        gaps = _widget(10, dash_schema)
        if gaps is not None:
            st.dataframe(gaps, hide_index=True, use_container_width=True)

        handled = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
        extra = [q for n, q in sorted(queries.items()) if n not in handled]
        if extra:
            st.header("More from dashboard_queries.sql")
            for q in extra:
                st.subheader(_title(q.number, queries))
                df = _widget(q.number, dash_schema)
                if df is not None:
                    st.dataframe(df, hide_index=True, use_container_width=True)

with quality_tab:
    st.subheader("AI answer quality benchmark")

    questions = default_benchmark_questions()

    answer_map = {}
    for q in questions:
        answer_map[q.prompt_id] = st.text_area(
            f"{q.domain} — {q.question}",
            value="",
            height=100,
            key=q.prompt_id,
        )

    if st.button("Evaluate answers"):
        evaluator = AnswerQualityEvaluator(questions)
        rows = evaluator.evaluate(answer_map)
        summary = evaluator.summary(answer_map)

        st.metric("Overall answer-quality score", f"{summary['overall']:.1f}/100")
        st.write(summary)
        st.dataframe(rows)

st.markdown("---")
st.subheader("Planned app views")
st.markdown(
    """
    - Asset quality grid
    - AI answer-quality benchmark summary
    """
)
