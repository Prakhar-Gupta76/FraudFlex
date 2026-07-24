from __future__ import annotations

import copy
import unittest
from datetime import datetime, timezone

from fraudflux_simulator import TransactionSimulator
from fraudflux_validation import (
    DeadLetterSource,
    TransactionValidationError,
    build_dead_letter_event,
    validate_transaction_event,
)


def invalid_event_and_error() -> tuple[dict, TransactionValidationError]:
    simulator = TransactionSimulator(seed=201)
    payload = next(
        simulator.generate(count=1, scenario="normal", rate=1)
    ).public_event()
    payload["transaction"]["amount_minor"] = 0
    try:
        validate_transaction_event(payload)
    except TransactionValidationError as error:
        return payload, error
    raise AssertionError("fixture unexpectedly passed validation")


class DeadLetterTests(unittest.TestCase):
    def test_dead_letter_preserves_payload_and_validation_reasons(self) -> None:
        payload, error = invalid_event_and_error()
        failed_at = datetime(2026, 1, 2, tzinfo=timezone.utc)
        source = DeadLetterSource(
            topic="transactions.raw",
            partition=2,
            offset=42,
            consumer_group="fraud-scoring-worker",
        )

        dead_letter = build_dead_letter_event(
            payload,
            error,
            source=source,
            failed_at=failed_at,
        )

        self.assertTrue(dead_letter.dead_letter_id.startswith("DLQ-"))
        self.assertEqual(
            "transaction.validation_failed", dead_letter.event_type
        )
        self.assertEqual(payload["event_id"], dead_letter.original_event_id)
        self.assertEqual(payload, dead_letter.original_payload)
        self.assertEqual(
            "structured", dead_letter.original_payload_encoding
        )
        self.assertEqual(2, dead_letter.source.partition)
        self.assertEqual(42, dead_letter.source.offset)
        self.assertEqual(
            "transaction.amount_minor", dead_letter.errors[0]["path"]
        )

    def test_invalid_json_can_still_be_dead_lettered(self) -> None:
        payload = '{"event_id":'
        try:
            validate_transaction_event(payload)
        except TransactionValidationError as error:
            dead_letter = build_dead_letter_event(payload, error)
        else:
            self.fail("invalid JSON unexpectedly passed validation")

        self.assertIsNone(dead_letter.original_event_id)
        self.assertEqual(payload, dead_letter.original_payload)
        self.assertEqual("text", dead_letter.original_payload_encoding)
        self.assertEqual("json_invalid", dead_letter.errors[0]["code"])

    def test_non_utf8_bytes_are_preserved_as_base64(self) -> None:
        payload = b"\xff\x00\xfe"
        try:
            validate_transaction_event(payload)
        except TransactionValidationError as error:
            dead_letter = build_dead_letter_event(payload, error)
        else:
            self.fail("invalid bytes unexpectedly passed validation")

        self.assertEqual("/wD+", dead_letter.original_payload)
        self.assertEqual("base64", dead_letter.original_payload_encoding)
        dead_letter.model_dump_json()

    def test_dead_letter_does_not_mutate_original_payload(self) -> None:
        payload, error = invalid_event_and_error()
        original = copy.deepcopy(payload)

        build_dead_letter_event(payload, error)

        self.assertEqual(original, payload)


if __name__ == "__main__":
    unittest.main()
