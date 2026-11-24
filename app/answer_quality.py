from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Dict, Iterable, List, Optional


@dataclass
class BenchmarkQuestion:
    prompt_id: str
    domain: str
    question: str
    expected_answer: str
    expected_metric: Optional[str] = None
    expected_table: Optional[str] = None
    notes: Optional[str] = None


def _normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (value or "").lower()).strip()


def _token_set(value: str) -> set[str]:
    return set(_normalize_text(value).split())


def default_benchmark_questions() -> List[BenchmarkQuestion]:
    return [
        BenchmarkQuestion(
            prompt_id="rev_q4_2025",
            domain="finance",
            question="What was revenue for Q4 2025?",
            expected_answer="Revenue for Q4 2025 was $12.4 million.",
            expected_metric="revenue",
            expected_table="sales_fact",
            notes="A simple benchmark for a known business KPI.",
        ),
        BenchmarkQuestion(
            prompt_id="net_revenue_definition",
            domain="finance",
            question="Which metric should be used for net revenue?",
            expected_answer="Use net_revenue, not gross_revenue.",
            expected_metric="net_revenue",
            expected_table="finance_metrics",
            notes="Checks whether the model selects the correct metric definition.",
        ),
        BenchmarkQuestion(
            prompt_id="churn_table_lookup",
            domain="customer",
            question="Which table contains customer churn?",
            expected_answer="Customer churn is defined in the churn_metrics table.",
            expected_metric="churn_rate",
            expected_table="churn_metrics",
            notes="Validates source table selection and grounding.",
        ),
    ]


class AnswerQualityEvaluator:
    def __init__(self, questions: Optional[Iterable[BenchmarkQuestion]] = None):
        self.questions = list(questions) if questions is not None else default_benchmark_questions()

    def score_answer(self, question: BenchmarkQuestion, answer_text: str) -> Dict[str, float]:
        gold = _normalize_text(question.expected_answer)
        answer = _normalize_text(answer_text)

        gold_tokens = _token_set(question.expected_answer)
        answer_tokens = _token_set(answer_text)
        overlap = len(gold_tokens & answer_tokens)
        total = max(len(gold_tokens), 1)
        lexical_overlap = overlap / total

        metric_bonus = 1.0 if question.expected_metric and question.expected_metric.lower() in answer else 0.0
        table_bonus = 1.0 if question.expected_table and question.expected_table.lower() in answer else 0.0

        correctness = min(1.0, 0.7 * lexical_overlap + 0.2 * metric_bonus + 0.1 * table_bonus)
        groundedness = 1.0 if answer and len(answer) >= 10 else 0.0
        completeness = 1.0 if len(answer) >= 20 else 0.5 if len(answer) >= 10 else 0.1
        citation_bonus = 1.0 if "table" in answer or "metric" in answer else 0.6

        overall = round(
            (correctness * 0.50 + groundedness * 0.25 + completeness * 0.15 + citation_bonus * 0.10) * 100,
            1,
        )

        return {
            "correctness": round(correctness * 100, 1),
            "groundedness": round(groundedness * 100, 1),
            "completeness": round(completeness * 100, 1),
            "citation_quality": round(citation_bonus * 100, 1),
            "overall": overall,
        }

    def evaluate(self, answer_map: Dict[str, str]) -> List[Dict[str, object]]:
        rows: List[Dict[str, object]] = []
        for question in self.questions:
            answer = answer_map.get(question.prompt_id, "")
            scores = self.score_answer(question, answer)
            rows.append(
                {
                    "prompt_id": question.prompt_id,
                    "domain": question.domain,
                    "question": question.question,
                    "expected_answer": question.expected_answer,
                    "expected_metric": question.expected_metric,
                    "expected_table": question.expected_table,
                    "answer": answer,
                    "correctness": scores["correctness"],
                    "groundedness": scores["groundedness"],
                    "completeness": scores["completeness"],
                    "citation_quality": scores["citation_quality"],
                    "overall": scores["overall"],
                }
            )
        return rows

    def summary(self, answer_map: Dict[str, str]) -> Dict[str, float]:
        rows = self.evaluate(answer_map)
        if not rows:
            return {"overall": 0.0, "correctness": 0.0, "groundedness": 0.0, "completeness": 0.0}
        return {
            "overall": round(sum(r["overall"] for r in rows) / len(rows), 1),
            "correctness": round(sum(r["correctness"] for r in rows) / len(rows), 1),
            "groundedness": round(sum(r["groundedness"] for r in rows) / len(rows), 1),
            "completeness": round(sum(r["completeness"] for r in rows) / len(rows), 1),
        }
