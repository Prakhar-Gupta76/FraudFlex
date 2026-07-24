from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from fraudflux_evaluation import (
    EvaluationCase,
    EvaluationPolicy,
    FraudDecisionEvaluator,
    LabelSource,
    build_analyst_cases,
    build_simulator_cases,
    merge_cases,
)
from fraudflux_evaluation.cli import main
from fraudflux_worker import RiskCategory
from tests.test_decision_events import make_outputs


NOW = datetime(2026, 7, 24, tzinfo=timezone.utc)


def case(
    transaction_id: str,
    *,
    is_fraud: bool,
    score: int,
    category: str,
    amount_minor: int,
    source: LabelSource = LabelSource.SIMULATOR,
) -> EvaluationCase:
    action = {
        "low": "approve",
        "medium": "additional_verification",
        "high": "hold_for_review",
    }[category]
    return EvaluationCase(
        transaction_id=transaction_id,
        amount_minor=amount_minor,
        is_fraud=is_fraud,
        label_source=source,
        final_score=score,
        category=category,
        recommended_action=action,
    )


class FraudDecisionEvaluatorTests(unittest.TestCase):
    def test_calculates_confusion_quality_and_amount_metrics(self) -> None:
        records = (
            case("TXN-1", is_fraud=True, score=95, category="high", amount_minor=10_000),
            case("TXN-2", is_fraud=False, score=80, category="high", amount_minor=4_000),
            case("TXN-3", is_fraud=True, score=70, category="medium", amount_minor=5_000),
            case("TXN-4", is_fraud=False, score=50, category="medium", amount_minor=3_000),
            case("TXN-5", is_fraud=True, score=20, category="low", amount_minor=7_000),
            case("TXN-6", is_fraud=False, score=10, category="low", amount_minor=2_000),
        )

        report = FraudDecisionEvaluator(clock=lambda: NOW).evaluate(records)

        self.assertEqual(report.true_positives, 2)
        self.assertEqual(report.true_negatives, 1)
        self.assertEqual(report.false_positives, 2)
        self.assertEqual(report.false_negatives, 1)
        self.assertAlmostEqual(report.precision, 0.5)
        self.assertAlmostEqual(report.recall, 2 / 3)
        self.assertAlmostEqual(report.f1_score, 4 / 7)
        self.assertAlmostEqual(report.false_positive_rate, 2 / 3)
        self.assertAlmostEqual(
            report.precision_recall_auc,
            (1 + 2 / 3 + 3 / 5) / 3,
        )
        self.assertEqual(report.fraud_amount_detected_minor, 15_000)
        self.assertEqual(
            report.legitimate_amount_incorrectly_held_minor,
            4_000,
        )

    def test_no_positive_labels_uses_safe_zero_metrics(self) -> None:
        report = FraudDecisionEvaluator(clock=lambda: NOW).evaluate(
            (
                case(
                    "TXN-1",
                    is_fraud=False,
                    score=10,
                    category="low",
                    amount_minor=100,
                ),
            )
        )

        self.assertEqual(report.precision, 0)
        self.assertEqual(report.recall, 0)
        self.assertEqual(report.f1_score, 0)
        self.assertEqual(report.precision_recall_auc, 0)

    def test_positive_categories_are_a_versioned_policy(self) -> None:
        evaluator = FraudDecisionEvaluator(
            EvaluationPolicy(
                version="high-only-v1",
                positive_categories=("high",),
            ),
            clock=lambda: NOW,
        )

        report = evaluator.evaluate(
            (
                case(
                    "TXN-1",
                    is_fraud=True,
                    score=50,
                    category="medium",
                    amount_minor=100,
                ),
            )
        )

        self.assertEqual(report.policy_version, "high-only-v1")
        self.assertEqual(report.false_negatives, 1)

    def test_duplicate_transactions_are_rejected(self) -> None:
        duplicate = case(
            "TXN-1",
            is_fraud=True,
            score=80,
            category="high",
            amount_minor=100,
        )

        with self.assertRaisesRegex(ValueError, "duplicate"):
            FraudDecisionEvaluator(clock=lambda: NOW).evaluate(
                (duplicate, duplicate)
            )


class EvaluationLoaderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.scored = make_outputs(category=RiskCategory.HIGH)[0].payload
        self.transaction_id = self.scored["transaction_id"]

    def test_joins_private_simulator_ground_truth_to_scored_event(self) -> None:
        cases = build_simulator_cases(
            (
                {
                    "transaction": {
                        "transaction_id": self.transaction_id,
                        "amount_minor": 12_500,
                    },
                    "ground_truth": {
                        "is_fraud": True,
                        "scenario": "account_takeover",
                    },
                },
            ),
            (self.scored,),
        )

        self.assertEqual(cases[0].label_source, LabelSource.SIMULATOR)
        self.assertTrue(cases[0].is_fraud)
        self.assertEqual(cases[0].final_score, 32)
        self.assertEqual(cases[0].category, "high")

    def test_final_analyst_label_overrides_simulator_without_duplication(
        self,
    ) -> None:
        simulator = build_simulator_cases(
            (
                {
                    "transaction": {
                        "transaction_id": self.transaction_id,
                        "amount_minor": 12_500,
                    },
                    "ground_truth": {
                        "is_fraud": True,
                        "scenario": "account_takeover",
                    },
                },
            ),
            (self.scored,),
        )
        analyst = build_analyst_cases(
            (
                {
                    "transaction_id": self.transaction_id,
                    "amount_minor": 12_500,
                    "review_id": "REVIEW-1",
                    "outcome": "legitimate",
                },
                {
                    "transaction_id": self.transaction_id,
                    "amount_minor": 12_500,
                    "review_id": "REVIEW-interim",
                    "outcome": "needs_further_investigation",
                },
            ),
            (self.scored,),
        )

        merged = merge_cases(simulator, analyst)

        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0].label_source, LabelSource.ANALYST)
        self.assertFalse(merged[0].is_fraud)

    def test_missing_scored_decision_is_not_silently_ignored(self) -> None:
        with self.assertRaisesRegex(ValueError, "no scored decision"):
            build_simulator_cases(
                (
                    {
                        "transaction": {
                            "transaction_id": "TXN-missing",
                            "amount_minor": 100,
                        },
                        "ground_truth": {
                            "is_fraud": False,
                            "scenario": "normal",
                        },
                    },
                ),
                (self.scored,),
            )

    def test_duplicate_labels_are_not_silently_collapsed(self) -> None:
        duplicate = case(
            self.transaction_id,
            is_fraud=True,
            score=80,
            category="high",
            amount_minor=100,
        )

        with self.assertRaisesRegex(ValueError, "duplicate simulator label"):
            merge_cases((duplicate, duplicate), ())


class EvaluationCliTests(unittest.TestCase):
    def test_cli_writes_a_machine_readable_report(self) -> None:
        scored = make_outputs()[0].payload
        truth = {
            "transaction": {
                "transaction_id": scored["transaction_id"],
                "amount_minor": 5_000,
            },
            "ground_truth": {"is_fraud": False, "scenario": "normal"},
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            decisions = root / "decisions.jsonl"
            labels = root / "labels.jsonl"
            output = root / "report.json"
            decisions.write_text(json.dumps(scored) + "\n", encoding="utf-8")
            labels.write_text(json.dumps(truth) + "\n", encoding="utf-8")

            exit_code = main(
                (
                    "--decisions",
                    str(decisions),
                    "--ground-truth",
                    str(labels),
                    "--output",
                    str(output),
                )
            )
            report = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 0)
        self.assertEqual(report["evaluated_transactions"], 1)
        self.assertEqual(report["true_negatives"], 1)


if __name__ == "__main__":
    unittest.main()
