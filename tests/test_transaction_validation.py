from __future__ import annotations

import json
import unittest
from datetime import datetime, timezone

from fraudflux_simulator import TransactionSimulator
from fraudflux_validation import (
    TransactionValidationError,
    validate_transaction_event,
)


def valid_event() -> dict:
    simulator = TransactionSimulator(seed=101)
    generated = next(
        simulator.generate(count=1, scenario="normal", rate=1)
    )
    return generated.public_event()


def assert_invalid(
    test_case: unittest.TestCase,
    payload: object,
    expected_path: str,
) -> TransactionValidationError:
    with test_case.assertRaises(TransactionValidationError) as context:
        validate_transaction_event(payload)
    test_case.assertIn(
        expected_path,
        [issue.path for issue in context.exception.issues],
    )
    return context.exception


class TransactionValidationTests(unittest.TestCase):
    def test_generated_event_is_valid(self) -> None:
        event = valid_event()

        validated = validate_transaction_event(event)

        self.assertEqual(event["event_id"], validated.event_id)
        self.assertEqual(
            event["transaction"]["transaction_id"],
            validated.transaction.transaction_id,
        )

    def test_json_payload_is_supported(self) -> None:
        event = valid_event()

        validated = validate_transaction_event(json.dumps(event))

        self.assertEqual(event["event_id"], validated.event_id)

    def test_required_identifiers_must_exist_and_not_be_blank(self) -> None:
        missing = valid_event()
        del missing["transaction"]["customer_id"]
        assert_invalid(self, missing, "transaction.customer_id")

        blank = valid_event()
        blank["transaction"]["transaction_id"] = "   "
        assert_invalid(self, blank, "transaction.transaction_id")

        malformed = valid_event()
        malformed["event_id"] = "invalid id with spaces"
        assert_invalid(self, malformed, "event_id")

    def test_amount_must_be_a_positive_integer_in_minor_units(self) -> None:
        for invalid_amount in (0, -1, 12.5, "1000"):
            with self.subTest(amount=invalid_amount):
                event = valid_event()
                event["transaction"]["amount_minor"] = invalid_amount
                assert_invalid(self, event, "transaction.amount_minor")

    def test_currency_must_be_supported(self) -> None:
        for invalid_currency in ("ABC", "inr", "", None):
            with self.subTest(currency=invalid_currency):
                event = valid_event()
                event["transaction"]["currency"] = invalid_currency
                assert_invalid(self, event, "transaction.currency")

    def test_supported_currencies_are_accepted(self) -> None:
        for currency in ("INR", "USD", "EUR", "GBP", "SGD"):
            with self.subTest(currency=currency):
                event = valid_event()
                event["transaction"]["currency"] = currency
                validate_transaction_event(event)

    def test_timestamps_must_be_valid_and_timezone_aware(self) -> None:
        invalid_event_time = valid_event()
        invalid_event_time["event_time"] = "not-a-time"
        assert_invalid(self, invalid_event_time, "event_time")

        naive_transaction_time = valid_event()
        naive_transaction_time["transaction"]["transaction_time"] = (
            "2026-01-01T09:00:00"
        )
        assert_invalid(
            self,
            naive_transaction_time,
            "transaction.transaction_time",
        )

    def test_transaction_time_cannot_be_far_after_event_time(self) -> None:
        event = valid_event()
        event["transaction"]["transaction_time"] = "2026-01-01T10:00:00+00:00"

        error = assert_invalid(self, event, "$")

        self.assertIn(
            "transaction_time cannot be more than 5 minutes",
            error.issues[0].message,
        )

    def test_coordinates_must_be_finite_and_in_range(self) -> None:
        cases = (
            ("latitude", 90.1),
            ("latitude", -90.1),
            ("longitude", 180.1),
            ("longitude", -180.1),
            ("latitude", float("inf")),
        )
        for field, value in cases:
            with self.subTest(field=field, value=value):
                event = valid_event()
                event["transaction"]["location"][field] = value
                assert_invalid(self, event, f"transaction.location.{field}")

    def test_coordinate_boundaries_are_valid(self) -> None:
        event = valid_event()
        event["transaction"]["location"]["latitude"] = 90.0
        event["transaction"]["location"]["longitude"] = -180.0

        validate_transaction_event(event)

    def test_nested_contract_formats_are_enforced(self) -> None:
        cases = (
            ("merchant", "category", "Invalid Category"),
            ("device", "trust_status", "trusted"),
            ("authentication", "method", "magic_link"),
            ("authentication", "result", "approved"),
            ("authentication", "failed_attempts_last_10m", -1),
            ("location", "city", ""),
        )
        for section, field, value in cases:
            with self.subTest(section=section, field=field):
                event = valid_event()
                event["transaction"][section][field] = value
                assert_invalid(
                    self,
                    event,
                    f"transaction.{section}.{field}",
                )

        invalid_channel = valid_event()
        invalid_channel["transaction"]["payment_channel"] = "cash"
        assert_invalid(
            self,
            invalid_channel,
            "transaction.payment_channel",
        )

        invalid_ip = valid_event()
        invalid_ip["transaction"]["ip_address"] = "999.999.1.1"
        assert_invalid(self, invalid_ip, "transaction.ip_address")

    def test_schema_version_and_event_type_are_fixed(self) -> None:
        unsupported_version = valid_event()
        unsupported_version["schema_version"] = "2.0"
        assert_invalid(self, unsupported_version, "schema_version")

        unsupported_type = valid_event()
        unsupported_type["event_type"] = "transaction.updated"
        assert_invalid(self, unsupported_type, "event_type")

    def test_unknown_fields_and_ground_truth_are_rejected(self) -> None:
        unknown_nested = valid_event()
        unknown_nested["transaction"]["merchant"]["secret_score"] = 99
        assert_invalid(
            self,
            unknown_nested,
            "transaction.merchant.secret_score",
        )

        evaluation_event = valid_event()
        evaluation_event["ground_truth"] = {
            "is_fraud": True,
            "scenario": "test",
            "expected_signals": [],
        }
        assert_invalid(self, evaluation_event, "ground_truth")

    def test_invalid_json_returns_a_structured_safe_error(self) -> None:
        error = assert_invalid(self, '{"event_id":', "$")

        detail = error.api_detail()

        self.assertEqual("transaction_validation_failed", detail["code"])
        self.assertEqual("json_invalid", detail["errors"][0]["code"])
        self.assertNotIn('{"event_id":', json.dumps(detail))

    def test_every_simulator_scenario_matches_the_contract(self) -> None:
        simulator = TransactionSimulator(seed=103)
        events = simulator.generate(
            count=100,
            scenario="mixed",
            fraud_rate=0.35,
            rate=10,
        )

        for generated in events:
            validate_transaction_event(generated.public_event())


if __name__ == "__main__":
    unittest.main()
