from __future__ import annotations

import copy
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fraudflux_features import (
    CachedHistoryProvider,
    CustomerFeatureCalculator,
    PostgresHistoryProvider,
)
from fraudflux_simulator import TransactionSimulator
from fraudflux_validation import validate_transaction_event
from fraudflux_worker import CustomerHistory


NOW = datetime(2026, 2, 1, 12, 0, tzinfo=timezone.utc)


def current_event(**changes: Any) -> Any:
    raw = next(
        TransactionSimulator(seed=801, start_time=NOW).generate(
            count=1,
            scenario="normal",
            rate=1,
        )
    ).public_event()
    raw["event_time"] = NOW.isoformat()
    raw["transaction"]["transaction_time"] = NOW.isoformat()
    for path, value in changes.items():
        target = raw
        parts = path.split("__")
        for part in parts[:-1]:
            target = target[part]
        target[parts[-1]] = value
    return validate_transaction_event(raw)


def history_record(
    *,
    minutes_ago: float,
    amount_minor: int = 10_000,
    currency: str = "INR",
    merchant_id: str = "MERCHANT-1",
    merchant_category: str = "groceries",
    device_id: str = "DEVICE-1",
    region: str = "Delhi",
    country: str = "India",
    latitude: float = 28.6139,
    longitude: float = 77.2090,
    authentication_result: str = "success",
) -> dict[str, Any]:
    return {
        "amount_minor": amount_minor,
        "currency": currency,
        "merchant_id": merchant_id,
        "merchant_category": merchant_category,
        "device_id": device_id,
        "region": region,
        "country": country,
        "latitude": latitude,
        "longitude": longitude,
        "authentication_result": authentication_result,
        "failed_attempts_last_10m": 0,
        "transaction_time": NOW - timedelta(minutes=minutes_ago),
    }


def history(event: Any, records: list[dict[str, Any]], **values: Any) -> Any:
    return CustomerHistory(
        event.transaction.customer_id,
        {"transactions": records, **values},
    )


class CustomerFeatureCalculatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.calculator = CustomerFeatureCalculator()

    def test_amount_features_use_same_currency_and_robust_median(self) -> None:
        event = current_event(transaction__amount_minor=40_000)
        records = [
            history_record(minutes_ago=5, amount_minor=10_000),
            history_record(minutes_ago=10, amount_minor=20_000),
            history_record(minutes_ago=15, amount_minor=30_000),
            history_record(
                minutes_ago=20,
                amount_minor=9_000_000,
                currency="USD",
            ),
        ]

        values = self.calculator.calculate(
            event, history(event, records)
        ).values

        self.assertEqual(3, values["amount_history_count"])
        self.assertEqual(20_000.0, values["customer_average_amount_minor"])
        self.assertEqual(20_000.0, values["customer_median_amount_minor"])
        self.assertEqual(30_000, values["recent_max_amount_minor"])
        self.assertEqual(2.0, values["amount_to_normal_ratio"])
        self.assertAlmostEqual(
            2.44949, values["amount_deviation_from_normal"], places=5
        )

    def test_velocity_windows_and_distinct_merchants(self) -> None:
        event = current_event()
        records = [
            history_record(minutes_ago=1, amount_minor=100, merchant_id="M-1"),
            history_record(
                minutes_ago=2, amount_minor=200, merchant_id="M-2"
            ),
            history_record(
                minutes_ago=30, amount_minor=300, merchant_id="M-1"
            ),
            history_record(
                minutes_ago=61, amount_minor=400, merchant_id="M-3"
            ),
        ]

        values = self.calculator.calculate(
            event, history(event, records)
        ).values

        self.assertEqual(2, values["transactions_previous_2m"])
        self.assertEqual(3, values["transactions_previous_1h"])
        self.assertEqual(300, values["amount_spent_previous_2m_minor"])
        self.assertEqual(600, values["amount_spent_previous_1h_minor"])
        self.assertEqual(2, values["recent_merchant_count_1h"])

    def test_device_intelligence_features(self) -> None:
        event = current_event(
            transaction__device__device_id="SHARED-DEVICE",
            transaction__device__trust_status="known",
        )
        records = [
            history_record(
                minutes_ago=60 * 24,
                device_id="SHARED-DEVICE",
            )
        ]

        values = self.calculator.calculate(
            event,
            history(
                event,
                records,
                device={
                    "first_seen_at": NOW - timedelta(days=10),
                    "account_count": 4,
                    "deny_listed": True,
                },
            ),
        ).values

        self.assertFalse(values["device_is_new"])
        self.assertFalse(values["device_is_known"])
        self.assertEqual(4, values["device_account_count"])
        self.assertEqual(864_000.0, values["device_age_seconds"])
        self.assertTrue(values["device_deny_listed"])

    def test_location_detects_impossible_travel_and_unusual_country(self) -> None:
        event = current_event(
            transaction__location__city="London",
            transaction__location__country="United Kingdom",
            transaction__location__latitude=51.5072,
            transaction__location__longitude=-0.1276,
        )
        records = [history_record(minutes_ago=10)]

        values = self.calculator.calculate(
            event,
            history(
                event,
                records,
                usual_countries=("India",),
                usual_regions=("Delhi",),
            ),
        ).values

        self.assertGreater(values["distance_from_previous_km"], 6000)
        self.assertEqual(600.0, values["seconds_since_previous_transaction"])
        self.assertTrue(values["impossible_travel"])
        self.assertTrue(values["unusual_country"])
        self.assertTrue(values["unusual_region"])

    def test_merchant_and_authentication_features(self) -> None:
        event = current_event(
            transaction__merchant__merchant_id="NEW-MERCHANT",
            transaction__merchant__category="crypto",
            transaction__authentication__result="success",
            transaction__authentication__failed_attempts_last_10m=3,
        )
        records = [
            history_record(
                minutes_ago=5,
                authentication_result="failure",
            ),
            history_record(minutes_ago=20),
        ]

        values = self.calculator.calculate(
            event,
            history(event, records, merchant={"fraud_rate": 0.27}),
        ).values

        self.assertEqual(1.0, values["merchant_category_rarity"])
        self.assertTrue(values["merchant_is_new"])
        self.assertEqual(0.27, values["merchant_fraud_rate"])
        self.assertEqual(3, values["recent_authentication_failures_10m"])
        self.assertTrue(values["authentication_failures_then_success"])

    def test_cold_start_produces_finite_defaults(self) -> None:
        event = current_event()

        values = self.calculator.calculate(
            event, history(event, [])
        ).values

        self.assertEqual(0, values["amount_history_count"])
        self.assertEqual(1.0, values["amount_to_normal_ratio"])
        self.assertEqual(0.0, values["amount_deviation_from_normal"])
        self.assertFalse(values["impossible_travel"])
        self.assertFalse(values["unusual_country"])

    def test_future_records_are_excluded(self) -> None:
        event = current_event()
        future = history_record(minutes_ago=-1, amount_minor=999_999)

        values = self.calculator.calculate(
            event, history(event, [future])
        ).values

        self.assertEqual(0, values["amount_history_count"])
        self.assertEqual(0, values["transactions_previous_1h"])

    def test_history_customer_must_match_event(self) -> None:
        event = current_event()

        with self.assertRaisesRegex(ValueError, "different customer"):
            self.calculator.calculate(
                event, CustomerHistory("OTHER", {"transactions": []})
            )


class RecordingHistoryProvider:
    def __init__(self) -> None:
        self.calls = 0

    def load(self, event: Any) -> CustomerHistory:
        self.calls += 1
        return history(event, [])


class CachedHistoryProviderTests(unittest.TestCase):
    def test_exact_event_is_cached_until_ttl_and_can_be_invalidated(self) -> None:
        provider = RecordingHistoryProvider()
        now = [100.0]
        cached = CachedHistoryProvider(
            provider,
            max_entries=2,
            ttl_seconds=5,
            clock=lambda: now[0],
        )
        event = current_event()

        cached.load(event)
        cached.load(event)
        self.assertEqual(1, provider.calls)

        cached.invalidate(event.transaction.customer_id)
        cached.load(event)
        self.assertEqual(2, provider.calls)

        now[0] += 6
        cached.load(event)
        self.assertEqual(3, provider.calls)

    def test_cache_is_bounded_by_lru_eviction(self) -> None:
        provider = RecordingHistoryProvider()
        cached = CachedHistoryProvider(provider, max_entries=1)
        first = current_event()
        second_raw = copy.deepcopy(first.model_dump(mode="json"))
        second_raw["event_id"] = "EVENT-SECOND"
        second = validate_transaction_event(second_raw)

        cached.load(first)
        cached.load(second)

        self.assertEqual(1, cached.size)


class FakeCursor:
    def __init__(self, responses: list[Any]) -> None:
        self.responses = responses
        self.executions: list[tuple[str, Any]] = []
        self.active: Any = None

    def execute(self, query: str, parameters: Any) -> None:
        self.executions.append((query, parameters))
        self.active = self.responses[len(self.executions) - 1]

    def fetchall(self) -> Any:
        return self.active

    def fetchone(self) -> Any:
        return self.active

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *args: Any) -> None:
        pass


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self.fake_cursor = cursor

    def cursor(self) -> FakeCursor:
        return self.fake_cursor

    def __enter__(self) -> "FakeConnection":
        return self

    def __exit__(self, *args: Any) -> None:
        pass


class PostgresHistoryProviderTests(unittest.TestCase):
    def test_loads_point_in_time_history_and_current_entity_signals(self) -> None:
        event = current_event()
        row = history_record(minutes_ago=1)
        cursor = FakeCursor(
            [
                [row],
                {
                    "first_seen_at": NOW - timedelta(days=30),
                    "account_count": 2,
                    "deny_listed": False,
                },
                {"fraud_rate": 0.15},
                {
                    "home_country": "India",
                    "home_region": "Delhi",
                    "usual_countries": ["India"],
                    "usual_regions": ["Delhi", "Noida"],
                },
            ]
        )
        provider = PostgresHistoryProvider(
            lambda: FakeConnection(cursor)
        )

        result = provider.load(event)

        self.assertEqual(event.transaction.customer_id, result.customer_id)
        self.assertEqual([row], result.values["transactions"])
        self.assertEqual(2, result.values["device"]["account_count"])
        self.assertEqual(0.15, result.values["merchant"]["fraud_rate"])
        self.assertEqual(("India",), result.values["usual_countries"])
        history_parameters = cursor.executions[0][1]
        self.assertEqual(NOW, history_parameters[2])
        device_parameters = cursor.executions[1][1]
        self.assertEqual(4, len(device_parameters))
        self.assertEqual(
            event.transaction.device.device_id,
            device_parameters[0],
        )


class PostgresInfrastructureTests(unittest.TestCase):
    def test_compose_uses_pinned_bounded_postgres(self) -> None:
        root = Path(__file__).resolve().parents[1]
        compose = (root / "compose.yaml").read_text(encoding="utf-8")

        self.assertIn("image: postgres:16.4-alpine", compose)
        self.assertIn("mem_limit: 384m", compose)
        self.assertIn("001_feature_history.sql", compose)
        self.assertNotIn("image: postgres:latest", compose)

    def test_history_schema_contains_required_tables_and_indexes(self) -> None:
        root = Path(__file__).resolve().parents[1]
        schema = (
            root / "infra" / "postgres" / "001_feature_history.sql"
        ).read_text(encoding="utf-8")

        for table in (
            "customer_profiles",
            "transaction_history",
            "device_deny_list",
            "merchant_risk_profiles",
        ):
            self.assertIn(f"CREATE TABLE IF NOT EXISTS {table}", schema)
        self.assertIn("transaction_history_customer_time_idx", schema)
        self.assertIn("transaction_history_device_time_idx", schema)


if __name__ == "__main__":
    unittest.main()
