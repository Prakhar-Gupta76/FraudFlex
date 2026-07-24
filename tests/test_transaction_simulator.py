from __future__ import annotations

import json
import unittest
from datetime import datetime, timezone

from fraudflux_simulator import SCENARIOS, TransactionSimulator


class TransactionSimulatorTests(unittest.TestCase):
    def test_same_seed_produces_identical_evaluation_records(self) -> None:
        first = TransactionSimulator(seed=7)
        second = TransactionSimulator(seed=7)

        first_records = [
            event.evaluation_record()
            for event in first.generate(
                count=25, scenario="mixed", rate=5, fraud_rate=0.25
            )
        ]
        second_records = [
            event.evaluation_record()
            for event in second.generate(
                count=25, scenario="mixed", rate=5, fraud_rate=0.25
            )
        ]

        self.assertEqual(first_records, second_records)

    def test_public_event_does_not_leak_ground_truth(self) -> None:
        simulator = TransactionSimulator(seed=9)
        event = next(
            simulator.generate(count=1, scenario="account_takeover", rate=1)
        )

        public_event = event.public_event()

        self.assertNotIn("ground_truth", public_event)
        self.assertNotIn("is_fraud", json.dumps(public_event))
        self.assertTrue(event.ground_truth.is_fraud)

    def test_normal_scenario_uses_known_devices_and_legitimate_labels(self) -> None:
        simulator = TransactionSimulator(seed=11)
        events = list(simulator.generate(count=20, scenario="normal", rate=5))

        self.assertTrue(all(not event.ground_truth.is_fraud for event in events))
        self.assertTrue(
            all(
                event.transaction["device"]["trust_status"] == "known"
                for event in events
            )
        )
        self.assertTrue(
            all(event.transaction["amount_minor"] > 0 for event in events)
        )

    def test_account_takeover_contains_expected_attack_signals(self) -> None:
        simulator = TransactionSimulator(seed=13)
        event = next(
            simulator.generate(count=1, scenario="account_takeover", rate=1)
        )

        self.assertTrue(event.ground_truth.is_fraud)
        self.assertEqual("account_takeover", event.ground_truth.scenario)
        self.assertEqual("new", event.transaction["device"]["trust_status"])
        self.assertGreaterEqual(
            event.transaction["authentication"]["failed_attempts_last_10m"], 5
        )
        self.assertIn("new_device", event.ground_truth.expected_signals)

    def test_card_testing_uses_small_payments_across_merchants(self) -> None:
        simulator = TransactionSimulator(seed=17)
        events = list(
            simulator.generate(count=10, scenario="card_testing", rate=10)
        )

        customer_ids = {
            event.transaction["customer_id"] for event in events
        }
        merchant_ids = {
            event.transaction["merchant"]["merchant_id"] for event in events
        }
        self.assertEqual(1, len(customer_ids))
        self.assertGreaterEqual(len(merchant_ids), 5)
        self.assertTrue(
            all(event.transaction["amount_minor"] < 1000 for event in events)
        )

    def test_high_velocity_uses_one_customer_and_rate_timestamps(self) -> None:
        simulator = TransactionSimulator(
            seed=19,
            start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        events = list(
            simulator.generate(count=4, scenario="high_velocity", rate=4)
        )

        self.assertEqual(
            1, len({event.transaction["customer_id"] for event in events})
        )
        intervals = [
            (events[index].event_time - events[index - 1].event_time).total_seconds()
            for index in range(1, len(events))
        ]
        self.assertEqual([0.25, 0.25, 0.25], intervals)

    def test_impossible_travel_records_distance_signal(self) -> None:
        simulator = TransactionSimulator(seed=23)
        event = next(
            simulator.generate(count=1, scenario="impossible_travel", rate=1)
        )

        distance_signals = [
            signal
            for signal in event.ground_truth.expected_signals
            if signal.startswith("distance_from_previous_km:")
        ]
        self.assertEqual(1, len(distance_signals))
        self.assertGreater(float(distance_signals[0].split(":")[1]), 1000)

    def test_all_supported_scenarios_generate_serializable_events(self) -> None:
        for scenario in SCENARIOS:
            with self.subTest(scenario=scenario):
                simulator = TransactionSimulator(seed=29)
                event = next(
                    simulator.generate(
                        count=1,
                        scenario=scenario,
                        rate=1,
                        fraud_rate=1,
                    )
                )
                json.dumps(event.evaluation_record())

    def test_invalid_generation_options_are_rejected(self) -> None:
        simulator = TransactionSimulator(seed=31)
        with self.assertRaises(ValueError):
            list(simulator.generate(count=0))
        with self.assertRaises(ValueError):
            list(simulator.generate(count=1, rate=0))
        with self.assertRaises(ValueError):
            list(simulator.generate(count=1, fraud_rate=1.1))
        with self.assertRaises(ValueError):
            list(simulator.generate(count=1, scenario="unknown"))


if __name__ == "__main__":
    unittest.main()

