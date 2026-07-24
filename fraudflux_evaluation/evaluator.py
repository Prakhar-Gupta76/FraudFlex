"""Deterministic offline fraud-decision evaluation."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from typing import Callable, Iterable

from .models import EvaluationCase, EvaluationPolicy, EvaluationReport


class FraudDecisionEvaluator:
    """Calculate classification and monetary metrics without retraining."""

    def __init__(
        self,
        policy: EvaluationPolicy | None = None,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self.policy = policy or EvaluationPolicy()
        self.clock = clock

    def evaluate(self, cases: Iterable[EvaluationCase]) -> EvaluationReport:
        records = tuple(cases)
        _validate_unique_transactions(records)
        positive_categories = set(self.policy.positive_categories)

        true_positives = true_negatives = 0
        false_positives = false_negatives = 0
        fraud_amount_detected = 0
        legitimate_amount_held = 0
        for case in records:
            predicted_positive = case.category in positive_categories
            if case.is_fraud and predicted_positive:
                true_positives += 1
                fraud_amount_detected += case.amount_minor
            elif case.is_fraud:
                false_negatives += 1
            elif predicted_positive:
                false_positives += 1
            else:
                true_negatives += 1

            if (
                not case.is_fraud
                and case.recommended_action == "hold_for_review"
            ):
                legitimate_amount_held += case.amount_minor

        precision = _divide(
            true_positives,
            true_positives + false_positives,
        )
        recall = _divide(
            true_positives,
            true_positives + false_negatives,
        )
        f1_score = _divide(2 * precision * recall, precision + recall)
        false_positive_rate = _divide(
            false_positives,
            false_positives + true_negatives,
        )
        generated_at = self.clock()
        if generated_at.tzinfo is None:
            raise ValueError("evaluation clock must be timezone-aware")
        label_counts = Counter(case.label_source.value for case in records)
        positive_labels = sum(case.is_fraud for case in records)
        return EvaluationReport(
            policy_version=self.policy.version,
            generated_at=generated_at,
            evaluated_transactions=len(records),
            positive_labels=positive_labels,
            negative_labels=len(records) - positive_labels,
            label_source_counts=dict(label_counts),
            true_positives=true_positives,
            true_negatives=true_negatives,
            false_positives=false_positives,
            false_negatives=false_negatives,
            precision=precision,
            recall=recall,
            f1_score=f1_score,
            precision_recall_auc=_average_precision(records),
            false_positive_rate=false_positive_rate,
            fraud_amount_detected_minor=fraud_amount_detected,
            legitimate_amount_incorrectly_held_minor=legitimate_amount_held,
        )


def _average_precision(cases: tuple[EvaluationCase, ...]) -> float:
    """Average precision, the step-wise area under the precision-recall curve."""
    positive_count = sum(case.is_fraud for case in cases)
    if positive_count == 0:
        return 0.0
    ordered = sorted(
        enumerate(cases),
        key=lambda item: (-item[1].final_score, item[0]),
    )
    positives_seen = 0
    precision_sum = 0.0
    for rank, (_, case) in enumerate(ordered, start=1):
        if case.is_fraud:
            positives_seen += 1
            precision_sum += positives_seen / rank
    return precision_sum / positive_count


def _divide(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator else 0.0


def _validate_unique_transactions(
    cases: tuple[EvaluationCase, ...],
) -> None:
    seen: set[str] = set()
    for case in cases:
        if case.transaction_id in seen:
            raise ValueError(
                f"duplicate evaluation transaction {case.transaction_id}"
            )
        seen.add(case.transaction_id)
