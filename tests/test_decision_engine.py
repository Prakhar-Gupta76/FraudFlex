from __future__ import annotations

import math
import unittest

from fraudflux_decision import InitialDecisionEngine
from fraudflux_worker import (
    AnomalyEvaluation,
    CombinedRiskScore,
    RecommendedAction,
    RiskCategory,
    RiskDecision,
    RuleEvaluation,
    RuleHit,
)


def score(
    final_score: int,
    *,
    override: RecommendedAction | None = None,
) -> CombinedRiskScore:
    rules_points = min(70, final_score)
    anomaly_points = final_score - rules_points
    return CombinedRiskScore(
        rules_contribution=rules_points,
        anomaly_contribution=anomaly_points,
        uncapped_score=final_score,
        final_score=final_score,
        policy_version="risk-combiner-test",
        override_action=override,
    )


def rules(contribution: int) -> RuleEvaluation:
    return RuleEvaluation(
        contribution=contribution,
        hits=(
            RuleHit(
                "AMOUNT_5X_NORMAL",
                contribution,
                "Amount is at least five times the normal amount",
            ),
        )
        if contribution
        else (),
        ruleset_version="rules-test-1",
    )


def anomaly(contribution: int) -> AnomalyEvaluation:
    return AnomalyEvaluation(
        contribution=contribution,
        raw_score=0.12,
        deviations=(
            "amount_to_normal_ratio: above normal (z=5.20)",
        )
        if contribution
        else (),
        model_version="model-test-1",
        inference_time_ms=1.5,
    )


def decide(
    final_score: int,
    *,
    override: RecommendedAction | None = None,
    engine: InitialDecisionEngine | None = None,
) -> RiskDecision:
    combined = score(final_score, override=override)
    return (engine or InitialDecisionEngine()).decide(
        combined,
        rules(combined.rules_contribution),
        anomaly(combined.anomaly_contribution),
    )


class DecisionThresholdTests(unittest.TestCase):
    def test_low_boundaries_approve(self) -> None:
        for value in (0, 39):
            with self.subTest(score=value):
                result = decide(value)
                self.assertEqual(RiskCategory.LOW, result.score_category)
                self.assertEqual(RiskCategory.LOW, result.category)
                self.assertEqual(RecommendedAction.APPROVE, result.action)

    def test_medium_boundaries_request_verification(self) -> None:
        for value in (40, 69):
            with self.subTest(score=value):
                result = decide(value)
                self.assertEqual(RiskCategory.MEDIUM, result.score_category)
                self.assertEqual(RiskCategory.MEDIUM, result.category)
                self.assertEqual(RecommendedAction.VERIFY, result.action)

    def test_high_boundaries_hold_for_review(self) -> None:
        for value in (70, 100):
            with self.subTest(score=value):
                result = decide(value)
                self.assertEqual(RiskCategory.HIGH, result.score_category)
                self.assertEqual(RiskCategory.HIGH, result.category)
                self.assertEqual(RecommendedAction.HOLD, result.action)


class DecisionOverrideTests(unittest.TestCase):
    def test_verification_override_elevates_a_low_score(self) -> None:
        result = decide(12, override=RecommendedAction.VERIFY)

        self.assertEqual(RiskCategory.LOW, result.score_category)
        self.assertEqual(RiskCategory.MEDIUM, result.category)
        self.assertEqual(RecommendedAction.VERIFY, result.action)
        self.assertTrue(result.override_applied)
        self.assertTrue(
            any("raised the effective category" in line for line in result.explanation)
        )

    def test_hold_override_elevates_a_low_score(self) -> None:
        result = decide(12, override=RecommendedAction.HOLD)

        self.assertEqual(RiskCategory.LOW, result.score_category)
        self.assertEqual(RiskCategory.HIGH, result.category)
        self.assertEqual(RecommendedAction.HOLD, result.action)
        self.assertTrue(result.override_applied)

    def test_weaker_override_does_not_reduce_high_score_action(self) -> None:
        result = decide(80, override=RecommendedAction.VERIFY)

        self.assertEqual(RiskCategory.HIGH, result.score_category)
        self.assertEqual(RiskCategory.HIGH, result.category)
        self.assertEqual(RecommendedAction.HOLD, result.action)
        self.assertFalse(result.override_applied)
        self.assertTrue(
            any("already satisfied" in line for line in result.explanation)
        )


class DecisionExplanationTests(unittest.TestCase):
    def test_explanation_records_factual_score_inputs_and_deviations(
        self,
    ) -> None:
        combined = score(82)
        result = InitialDecisionEngine().decide(
            combined,
            rules(70),
            anomaly(12),
        )
        joined = " ".join(result.explanation)

        self.assertIn("Final score 82/100", joined)
        self.assertIn("70 rules points", joined)
        self.assertIn("12 anomaly points", joined)
        self.assertIn("AMOUNT_5X_NORMAL", joined)
        self.assertIn("contributed 70 points", joined)
        self.assertIn("moderately unusual", joined)
        self.assertIn("amount_to_normal_ratio", joined)
        self.assertIn("hold_for_review", joined)
        self.assertEqual(
            "decision-policy-1.0.0",
            result.decision_policy_version,
        )

    def test_explanation_does_not_assert_unobserved_misconduct(self) -> None:
        result = decide(90)
        explanation = " ".join(result.explanation).casefold()

        for unsupported_claim in (
            "customer is fraudulent",
            "customer committed fraud",
            "stolen card",
            "criminal",
            "guilty",
        ):
            self.assertNotIn(unsupported_claim, explanation)

    def test_processing_latency_includes_upstream_and_decision_time(self) -> None:
        times = iter((1_000_000_000, 1_002_000_000))
        engine = InitialDecisionEngine(timer_ns=lambda: next(times))
        combined = score(20)

        result = engine.decide(
            combined,
            rules(20),
            anomaly(0),
            upstream_processing_latency_ms=3.25,
        )

        self.assertEqual(5.25, result.processing_latency_ms)

    def test_invalid_upstream_latency_is_rejected(self) -> None:
        combined = score(20)
        for invalid in (-1.0, math.inf, math.nan):
            with self.subTest(latency=invalid):
                with self.assertRaisesRegex(ValueError, "latency"):
                    InitialDecisionEngine().decide(
                        combined,
                        rules(20),
                        anomaly(0),
                        upstream_processing_latency_ms=invalid,
                    )


class RiskDecisionContractTests(unittest.TestCase):
    def test_score_category_must_match_numeric_score(self) -> None:
        with self.assertRaisesRegex(ValueError, "score category"):
            RiskDecision(
                final_score=20,
                score_category=RiskCategory.HIGH,
                category=RiskCategory.HIGH,
                action=RecommendedAction.HOLD,
                explanation=("Invalid category",),
                override_applied=True,
            )

    def test_action_must_match_effective_category(self) -> None:
        with self.assertRaisesRegex(ValueError, "action"):
            RiskDecision(
                final_score=50,
                category=RiskCategory.MEDIUM,
                action=RecommendedAction.APPROVE,
                explanation=("Invalid action",),
            )

    def test_category_elevation_requires_override(self) -> None:
        with self.assertRaisesRegex(ValueError, "override"):
            RiskDecision(
                final_score=20,
                category=RiskCategory.HIGH,
                action=RecommendedAction.HOLD,
                explanation=("Unexplained elevation",),
            )


if __name__ == "__main__":
    unittest.main()
