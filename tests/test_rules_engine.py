from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any

from fraudflux_features import CustomerFeatureCalculator
from fraudflux_rules import (
    RuleEvaluationError,
    Ruleset,
    YamlRulesEngine,
    load_default_ruleset,
    load_ruleset,
)
from fraudflux_simulator import TransactionSimulator
from fraudflux_validation import validate_transaction_event
from fraudflux_worker import (
    CustomerHistory,
    FeatureSet,
    RecommendedAction,
)


def event_and_features(**overrides: Any) -> tuple[Any, Any, Any]:
    raw = next(
        TransactionSimulator(seed=901).generate(
            count=1,
            scenario="normal",
            rate=1,
        )
    ).public_event()
    event = validate_transaction_event(raw)
    history = CustomerHistory(
        event.transaction.customer_id,
        {"transactions": []},
    )
    calculated = CustomerFeatureCalculator().calculate(event, history)
    values = dict(calculated.values)
    values["device_is_new"] = False
    values["device_is_known"] = True
    values.update(overrides)
    return event, history, FeatureSet(values)


class DefaultRulesEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = YamlRulesEngine.default()

    def test_default_ruleset_is_versioned_and_loadable(self) -> None:
        ruleset = load_default_ruleset()

        self.assertEqual("mvp-1.0.0", ruleset.version)
        self.assertEqual(70, ruleset.max_contribution)
        self.assertGreaterEqual(len(ruleset.rules), 16)

    def test_only_strongest_amount_rule_is_counted(self) -> None:
        event, history, features = event_and_features(
            amount_to_normal_ratio=11.0
        )

        result = self.engine.evaluate(event, history, features)

        self.assertEqual(30, result.contribution)
        self.assertEqual(("AMOUNT_10X_NORMAL",), _hit_ids(result))
        self.assertEqual(30, result.hits[0].points)
        self.assertIn("ten times", result.hits[0].reason)

    def test_strongest_rule_per_group_and_independent_groups_combine(
        self,
    ) -> None:
        event, history, features = event_and_features(
            amount_to_normal_ratio=6.0,
            transactions_previous_2m=6,
            device_is_new=True,
            device_account_count=4,
        )

        result = self.engine.evaluate(event, history, features)

        self.assertEqual(60, result.contribution)
        self.assertEqual(
            ("AMOUNT_5X_NORMAL", "VELOCITY_6_IN_2M", "SHARED_DEVICE"),
            _hit_ids(result),
        )

    def test_contribution_is_capped_at_seventy(self) -> None:
        event, history, features = event_and_features(
            amount_to_normal_ratio=12.0,
            transactions_previous_2m=8,
            recent_merchant_count_1h=6,
            device_is_new=True,
            device_account_count=5,
            device_deny_listed=True,
            unusual_region=True,
            unusual_country=True,
            impossible_travel=True,
            recent_authentication_failures_10m=6,
            authentication_failures_then_success=True,
            amount_history_count=10,
            merchant_category_rarity=0.95,
            merchant_fraud_rate=0.4,
        )

        result = self.engine.evaluate(event, history, features)

        self.assertEqual(70, result.contribution)
        self.assertGreater(sum(hit.points for hit in result.hits), 70)
        self.assertEqual("mvp-1.0.0", result.ruleset_version)

    def test_deny_list_rule_sets_hold_override(self) -> None:
        event, history, features = event_and_features(
            device_deny_listed=True
        )

        result = self.engine.evaluate(event, history, features)

        self.assertEqual(RecommendedAction.HOLD, result.override_action)
        self.assertIn("DENY_LISTED_DEVICE", _hit_ids(result))

    def test_threshold_boundaries_are_inclusive(self) -> None:
        event, history, features = event_and_features(
            amount_to_normal_ratio=5,
            transactions_previous_2m=3,
            recent_authentication_failures_10m=3,
        )

        result = self.engine.evaluate(event, history, features)

        self.assertEqual(
            (
                "AMOUNT_5X_NORMAL",
                "VELOCITY_3_IN_2M",
                "AUTH_FAILURES_3",
            ),
            _hit_ids(result),
        )

    def test_cold_start_does_not_trigger_category_rarity(self) -> None:
        event, history, features = event_and_features(
            amount_history_count=0,
            merchant_category_rarity=1.0,
        )

        result = self.engine.evaluate(event, history, features)

        self.assertNotIn("RARE_MERCHANT_CATEGORY", _hit_ids(result))


class ConfigurableRulesEngineTests(unittest.TestCase):
    def test_all_and_any_conditions_must_both_match(self) -> None:
        engine = YamlRulesEngine(
            Ruleset.model_validate(
                {
                    "version": "test-1",
                    "rules": [
                        {
                            "id": "COMPOUND_RULE",
                            "group": "compound",
                            "points": 12,
                            "reason": "Compound signal matched",
                            "when": {
                                "all": [
                                    {
                                        "field": "device_is_new",
                                        "operator": "truthy",
                                    }
                                ],
                                "any": [
                                    {
                                        "field": "unusual_country",
                                        "operator": "truthy",
                                    },
                                    {
                                        "field": "unusual_region",
                                        "operator": "truthy",
                                    },
                                ],
                            },
                        }
                    ],
                }
            )
        )
        event, history, features = event_and_features(
            device_is_new=True,
            unusual_region=True,
        )

        result = engine.evaluate(event, history, features)

        self.assertEqual(12, result.contribution)
        self.assertEqual(("COMPOUND_RULE",), _hit_ids(result))

    def test_equal_point_tie_keeps_first_declared_rule(self) -> None:
        engine = YamlRulesEngine(
            Ruleset.model_validate(
                {
                    "version": "test-1",
                    "rules": [
                        _simple_rule("FIRST_RULE", "same_group", 10),
                        _simple_rule("SECOND_RULE", "same_group", 10),
                    ],
                }
            )
        )
        event, history, features = event_and_features(device_is_new=True)

        result = engine.evaluate(event, history, features)

        self.assertEqual(("FIRST_RULE",), _hit_ids(result))

    def test_disabled_rule_is_not_evaluated(self) -> None:
        rule = _simple_rule("DISABLED_RULE", "disabled", 10)
        rule["enabled"] = False
        rule["when"]["all"][0]["field"] = "missing_feature"
        engine = YamlRulesEngine(
            Ruleset.model_validate(
                {"version": "test-1", "rules": [rule]}
            )
        )
        event, history, features = event_and_features()

        result = engine.evaluate(event, history, features)

        self.assertEqual(0, result.contribution)
        self.assertEqual((), result.hits)

    def test_missing_configured_field_fails_loudly(self) -> None:
        rule = _simple_rule("BROKEN_RULE", "broken", 10)
        rule["when"]["all"][0]["field"] = "misspelled_feature"
        engine = YamlRulesEngine(
            Ruleset.model_validate(
                {"version": "test-1", "rules": [rule]}
            )
        )
        event, history, features = event_and_features()

        with self.assertRaisesRegex(
            RuleEvaluationError, "misspelled_feature"
        ):
            engine.evaluate(event, history, features)

    def test_missing_field_is_detected_even_after_false_condition(self) -> None:
        rule = _simple_rule("BROKEN_RULE", "broken", 10)
        rule["when"]["all"] = [
            {
                "field": "device_is_new",
                "operator": "truthy",
            },
            {
                "field": "misspelled_feature",
                "operator": "truthy",
            },
        ]
        engine = YamlRulesEngine(
            Ruleset.model_validate(
                {"version": "test-1", "rules": [rule]}
            )
        )
        event, history, features = event_and_features(
            device_is_new=False
        )

        with self.assertRaisesRegex(
            RuleEvaluationError, "misspelled_feature"
        ):
            engine.evaluate(event, history, features)

    def test_transaction_fields_can_be_referenced(self) -> None:
        rule = {
            "id": "LARGE_RAW_AMOUNT",
            "group": "raw_amount",
            "points": 7,
            "reason": "Raw amount threshold matched",
            "when": {
                "all": [
                    {
                        "field": "transaction.amount_minor",
                        "operator": "gt",
                        "value": 0,
                    }
                ]
            },
        }
        engine = YamlRulesEngine(
            Ruleset.model_validate(
                {"version": "test-1", "rules": [rule]}
            )
        )
        event, history, features = event_and_features()

        result = engine.evaluate(event, history, features)

        self.assertEqual(7, result.contribution)


class RulesetValidationTests(unittest.TestCase):
    def test_duplicate_rule_ids_are_rejected(self) -> None:
        rule = _simple_rule("SAME_RULE", "one", 10)

        with self.assertRaisesRegex(ValueError, "unique"):
            Ruleset.model_validate(
                {
                    "version": "test-1",
                    "rules": [
                        rule,
                        _simple_rule("SAME_RULE", "two", 20),
                    ],
                }
            )

    def test_truthy_operator_rejects_a_value(self) -> None:
        rule = _simple_rule("BAD_RULE", "bad", 10)
        rule["when"]["all"][0]["value"] = True

        with self.assertRaisesRegex(ValueError, "must not define"):
            Ruleset.model_validate(
                {"version": "test-1", "rules": [rule]}
            )

    def test_unknown_configuration_fields_are_rejected(self) -> None:
        rule = _simple_rule("BAD_RULE", "bad", 10)
        rule["surprise"] = "unsafe"

        with self.assertRaisesRegex(ValueError, "Extra inputs"):
            Ruleset.model_validate(
                {"version": "test-1", "rules": [rule]}
            )

    def test_rule_cannot_override_a_decision_to_approve(self) -> None:
        rule = _simple_rule("UNSAFE_RULE", "unsafe", 10)
        rule["override_action"] = "approve"

        with self.assertRaisesRegex(ValueError, "cannot override"):
            Ruleset.model_validate(
                {"version": "test-1", "rules": [rule]}
            )

    def test_yaml_file_loader_reports_invalid_configuration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "invalid.yaml"
            path.write_text(
                "version: test-1\nrules: []\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "invalid"):
                load_ruleset(path)


def _simple_rule(
    identifier: str,
    group: str,
    points: int,
) -> dict[str, Any]:
    return {
        "id": identifier,
        "group": group,
        "points": points,
        "reason": f"{identifier} matched",
        "when": {
            "all": [
                {
                    "field": "device_is_new",
                    "operator": "truthy",
                }
            ]
        },
    }


def _hit_ids(result: Any) -> tuple[str, ...]:
    return tuple(hit.rule_id for hit in result.hits)


if __name__ == "__main__":
    unittest.main()
