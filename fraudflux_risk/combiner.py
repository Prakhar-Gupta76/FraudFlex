"""Initial explainable 70/30 FraudFlux score combination policy."""

from __future__ import annotations

from fraudflux_worker import (
    AnomalyEvaluation,
    CombinedRiskScore,
    RecommendedAction,
    RuleEvaluation,
)


class InitialRiskScoreCombiner:
    POLICY_VERSION = "risk-combiner-1.0.0"
    RULES_CAP = 70
    ANOMALY_CAP = 30
    FINAL_CAP = 100

    def combine(
        self,
        rules: RuleEvaluation,
        anomaly: AnomalyEvaluation,
    ) -> CombinedRiskScore:
        if type(rules.contribution) is not int:
            raise ValueError("rules contribution must be an integer")
        if type(anomaly.contribution) is not int:
            raise ValueError("anomaly contribution must be an integer")
        if not 0 <= rules.contribution <= self.RULES_CAP:
            raise ValueError("rules contribution exceeds the 70-point cap")
        if not 0 <= anomaly.contribution <= self.ANOMALY_CAP:
            raise ValueError("anomaly contribution exceeds the 30-point cap")
        if rules.override_action == RecommendedAction.APPROVE:
            raise ValueError("a high-confidence override cannot force approval")

        uncapped = rules.contribution + anomaly.contribution
        return CombinedRiskScore(
            rules_contribution=rules.contribution,
            anomaly_contribution=anomaly.contribution,
            uncapped_score=uncapped,
            final_score=min(self.FINAL_CAP, uncapped),
            policy_version=self.POLICY_VERSION,
            override_action=rules.override_action,
        )
