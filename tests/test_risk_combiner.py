from __future__ import annotations

import unittest

from fraudflux_risk import InitialRiskScoreCombiner
from fraudflux_worker import (
    AnomalyEvaluation,
    CombinedRiskScore,
    RecommendedAction,
    RuleEvaluation,
    RuleHit,
)


def rules(
    contribution: int,
    *,
    override: RecommendedAction | None = None,
) -> RuleEvaluation:
    return RuleEvaluation(
        contribution=contribution,
        hits=(
            RuleHit(
                "TEST_RULE",
                contribution,
                "Test rule matched",
            ),
        )
        if contribution
        else (),
        ruleset_version="rules-test-1",
        override_action=override,
    )


def anomaly(contribution: int) -> AnomalyEvaluation:
    return AnomalyEvaluation(
        contribution=contribution,
        raw_score=0.01,
        deviations=(),
        model_version="model-test-1",
        inference_time_ms=1.0,
    )


class InitialRiskScoreCombinerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.combiner = InitialRiskScoreCombiner()

    def test_zero_inputs_produce_zero_score(self) -> None:
        result = self.combiner.combine(rules(0), anomaly(0))

        self.assertEqual(0, result.rules_contribution)
        self.assertEqual(0, result.anomaly_contribution)
        self.assertEqual(0, result.uncapped_score)
        self.assertEqual(0, result.final_score)
        self.assertFalse(result.requires_review)

    def test_rules_and_anomaly_contributions_are_added(self) -> None:
        result = self.combiner.combine(rules(37), anomaly(18))

        self.assertEqual(55, result.uncapped_score)
        self.assertEqual(55, result.final_score)
        self.assertEqual(
            "risk-combiner-1.0.0",
            result.policy_version,
        )

    def test_maximum_70_30_split_produces_one_hundred(self) -> None:
        result = self.combiner.combine(rules(70), anomaly(30))

        self.assertEqual(70, result.rules_contribution)
        self.assertEqual(30, result.anomaly_contribution)
        self.assertEqual(100, result.uncapped_score)
        self.assertEqual(100, result.final_score)

    def test_rule_hit_totals_do_not_inflate_capped_rule_contribution(
        self,
    ) -> None:
        evaluation = RuleEvaluation(
            contribution=70,
            hits=(
                RuleHit("RULE_ONE", 50, "First signal"),
                RuleHit("RULE_TWO", 40, "Second signal"),
            ),
            ruleset_version="rules-test-1",
        )

        result = self.combiner.combine(evaluation, anomaly(5))

        self.assertEqual(75, result.final_score)
        self.assertEqual(70, result.rules_contribution)

    def test_safe_override_is_carried_without_changing_numeric_score(
        self,
    ) -> None:
        result = self.combiner.combine(
            rules(10, override=RecommendedAction.HOLD),
            anomaly(2),
        )

        self.assertEqual(12, result.final_score)
        self.assertEqual(RecommendedAction.HOLD, result.override_action)
        self.assertTrue(result.requires_review)

    def test_approval_override_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "cannot force approval"):
            self.combiner.combine(
                rules(5, override=RecommendedAction.APPROVE),
                anomaly(0),
            )

    def test_boolean_contributions_are_rejected_as_non_integer_scores(
        self,
    ) -> None:
        invalid_rules = RuleEvaluation(
            contribution=True,
            hits=(),
            ruleset_version="rules-test-1",
        )

        with self.assertRaisesRegex(ValueError, "must be an integer"):
            self.combiner.combine(invalid_rules, anomaly(0))


class CombinedRiskScoreContractTests(unittest.TestCase):
    def test_inconsistent_uncapped_score_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "rules plus anomaly"):
            CombinedRiskScore(
                rules_contribution=20,
                anomaly_contribution=10,
                uncapped_score=31,
                final_score=31,
                policy_version="test",
            )

    def test_inconsistent_final_cap_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "100-point cap"):
            CombinedRiskScore(
                rules_contribution=20,
                anomaly_contribution=10,
                uncapped_score=30,
                final_score=29,
                policy_version="test",
            )

    def test_blank_policy_version_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "version"):
            CombinedRiskScore(
                rules_contribution=0,
                anomaly_contribution=0,
                uncapped_score=0,
                final_score=0,
                policy_version=" ",
            )


if __name__ == "__main__":
    unittest.main()
