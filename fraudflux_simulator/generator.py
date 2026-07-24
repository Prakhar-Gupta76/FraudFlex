"""Deterministic synthetic transaction generator."""

from __future__ import annotations

import math
import random
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

from .models import (
    CustomerProfile,
    GeneratedTransaction,
    GroundTruth,
    Location,
    Merchant,
)

SCENARIOS: Tuple[str, ...] = (
    "normal",
    "mixed",
    "account_takeover",
    "card_testing",
    "impossible_travel",
    "dormant_account_reactivation",
    "merchant_fraud_spike",
    "high_velocity",
)

FRAUD_SCENARIOS: Tuple[str, ...] = tuple(
    scenario for scenario in SCENARIOS if scenario not in {"normal", "mixed"}
)

LOCATIONS: Tuple[Location, ...] = (
    Location("Delhi", "India", 28.6139, 77.2090),
    Location("Noida", "India", 28.5355, 77.3910),
    Location("Mumbai", "India", 19.0760, 72.8777),
    Location("Bengaluru", "India", 12.9716, 77.5946),
    Location("London", "United Kingdom", 51.5072, -0.1276),
    Location("Singapore", "Singapore", 1.3521, 103.8198),
)

MERCHANT_BLUEPRINTS: Tuple[Tuple[str, str], ...] = (
    ("Daily Basket", "groceries"),
    ("Metro Fuel", "fuel"),
    ("City Pharmacy", "pharmacy"),
    ("Quick Bites", "restaurants"),
    ("Northstar Electronics", "electronics"),
    ("FlySmart Travel", "travel"),
    ("Stream World", "digital_services"),
    ("Urban Fashion", "apparel"),
    ("Game Vault", "gaming"),
    ("Coin Harbor", "crypto"),
)

PAYMENT_CHANNELS: Tuple[str, ...] = ("card", "upi", "wallet")
AUTH_METHODS: Tuple[str, ...] = ("pin", "otp", "biometric")


def _distance_km(first: Location, second: Location) -> float:
    """Calculate great-circle distance for scenario verification."""
    earth_radius_km = 6371.0
    lat1 = math.radians(first.latitude)
    lat2 = math.radians(second.latitude)
    delta_lat = math.radians(second.latitude - first.latitude)
    delta_lon = math.radians(second.longitude - first.longitude)
    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(delta_lon / 2) ** 2
    )
    return earth_radius_km * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


class TransactionSimulator:
    """Generate repeatable normal and fraud-scenario payment events."""

    def __init__(
        self,
        *,
        seed: int = 42,
        start_time: Optional[datetime] = None,
        customer_count: int = 25,
    ) -> None:
        if customer_count < 2:
            raise ValueError("customer_count must be at least 2")

        self.seed = seed
        self.random = random.Random(seed)
        self.start_time = start_time or datetime(
            2026, 1, 1, 9, 0, tzinfo=timezone.utc
        )
        if self.start_time.tzinfo is None:
            raise ValueError("start_time must include timezone information")

        self._event_counter = 0
        self.merchants = self._build_merchants()
        self.customers = self._build_customers(customer_count)
        self._attack_customer = self.customers[0]
        self._attack_merchant = self.merchants[-1]

    def generate(
        self,
        *,
        count: int,
        scenario: str = "mixed",
        rate: float = 5.0,
        fraud_rate: float = 0.10,
    ) -> Iterator[GeneratedTransaction]:
        if count < 1:
            raise ValueError("count must be at least 1")
        if scenario not in SCENARIOS:
            raise ValueError(
                f"unknown scenario {scenario!r}; expected one of {SCENARIOS}"
            )
        if rate <= 0:
            raise ValueError("rate must be greater than 0")
        if not 0 <= fraud_rate <= 1:
            raise ValueError("fraud_rate must be between 0 and 1")

        interval = timedelta(seconds=1 / rate)
        for index in range(count):
            event_time = self.start_time + interval * self._event_counter
            selected = self._select_scenario(scenario, fraud_rate)
            yield self._generate_one(selected, event_time, index)
            self._event_counter += 1

    def _select_scenario(self, requested: str, fraud_rate: float) -> str:
        if requested != "mixed":
            return requested
        if self.random.random() >= fraud_rate:
            return "normal"
        return self.random.choice(FRAUD_SCENARIOS)

    def _generate_one(
        self, scenario: str, event_time: datetime, sequence_index: int
    ) -> GeneratedTransaction:
        if scenario == "normal":
            return self._normal(event_time)
        if scenario == "account_takeover":
            return self._account_takeover(event_time)
        if scenario == "card_testing":
            return self._card_testing(event_time, sequence_index)
        if scenario == "impossible_travel":
            return self._impossible_travel(event_time)
        if scenario == "dormant_account_reactivation":
            return self._dormant_reactivation(event_time)
        if scenario == "merchant_fraud_spike":
            return self._merchant_spike(event_time)
        if scenario == "high_velocity":
            return self._high_velocity(event_time)
        raise AssertionError(f"scenario handler missing for {scenario}")

    def _normal(self, event_time: datetime) -> GeneratedTransaction:
        customer = self.random.choice(self.customers[1:])
        merchant = self._preferred_merchant(customer)
        amount = max(
            1000,
            int(
                self.random.gauss(
                    customer.normal_amount_minor,
                    customer.normal_amount_minor * 0.28,
                )
            ),
        )
        return self._create_event(
            customer=customer,
            merchant=merchant,
            event_time=event_time,
            amount_minor=amount,
            device_id=self.random.choice(customer.known_devices),
            device_trust_status="known",
            location=self._near_home(customer),
            failed_attempts=0,
            auth_result="success",
            scenario="normal",
            is_fraud=False,
            expected_signals=[],
        )

    def _account_takeover(self, event_time: datetime) -> GeneratedTransaction:
        customer = self._attack_customer
        location = self._far_location(customer.home_location)
        return self._create_event(
            customer=customer,
            merchant=self._merchant_for_category("electronics"),
            event_time=event_time,
            amount_minor=customer.normal_amount_minor * 8,
            device_id=self._new_device_id(),
            device_trust_status="new",
            location=location,
            failed_attempts=5,
            auth_result="success",
            scenario="account_takeover",
            is_fraud=True,
            expected_signals=[
                "new_device",
                "high_amount_deviation",
                "unusual_location",
                "failed_authentication_then_success",
            ],
        )

    def _card_testing(
        self, event_time: datetime, sequence_index: int
    ) -> GeneratedTransaction:
        customer = self._attack_customer
        merchant = self.merchants[sequence_index % len(self.merchants)]
        return self._create_event(
            customer=customer,
            merchant=merchant,
            event_time=event_time,
            amount_minor=self.random.randint(100, 900),
            device_id=self._new_device_id(prefix="TEST"),
            device_trust_status="new",
            location=customer.home_location,
            failed_attempts=sequence_index % 3,
            auth_result="success",
            scenario="card_testing",
            is_fraud=True,
            expected_signals=[
                "repeated_small_payments",
                "many_merchants",
                "high_transaction_velocity",
            ],
        )

    def _impossible_travel(self, event_time: datetime) -> GeneratedTransaction:
        customer = self._attack_customer
        previous_location = customer.home_location
        far_location = self._far_location(previous_location)
        customer.last_transaction_at = event_time - timedelta(minutes=12)
        customer.last_transaction_location = previous_location
        distance = round(_distance_km(previous_location, far_location), 1)
        return self._create_event(
            customer=customer,
            merchant=self._merchant_for_category("travel"),
            event_time=event_time,
            amount_minor=customer.normal_amount_minor * 4,
            device_id=self.random.choice(customer.known_devices),
            device_trust_status="known",
            location=far_location,
            failed_attempts=0,
            auth_result="success",
            scenario="impossible_travel",
            is_fraud=True,
            expected_signals=[
                "impossible_travel",
                f"distance_from_previous_km:{distance}",
                "unusual_location",
            ],
        )

    def _dormant_reactivation(
        self, event_time: datetime
    ) -> GeneratedTransaction:
        customer = self._attack_customer
        customer.last_customer_activity = event_time - timedelta(days=180)
        return self._create_event(
            customer=customer,
            merchant=self._merchant_for_category("electronics"),
            event_time=event_time,
            amount_minor=customer.normal_amount_minor * 10,
            device_id=self._new_device_id(),
            device_trust_status="new",
            location=self._far_location(customer.home_location),
            failed_attempts=2,
            auth_result="success",
            scenario="dormant_account_reactivation",
            is_fraud=True,
            expected_signals=[
                "dormant_account",
                "new_device",
                "high_amount_deviation",
                "unusual_location",
            ],
        )

    def _merchant_spike(self, event_time: datetime) -> GeneratedTransaction:
        customer = self.random.choice(self.customers)
        return self._create_event(
            customer=customer,
            merchant=self._attack_merchant,
            event_time=event_time,
            amount_minor=customer.normal_amount_minor * 3,
            device_id=self.random.choice(customer.known_devices),
            device_trust_status="known",
            location=customer.home_location,
            failed_attempts=0,
            auth_result="success",
            scenario="merchant_fraud_spike",
            is_fraud=True,
            expected_signals=[
                "merchant_fraud_spike",
                "unusual_merchant_category",
            ],
        )

    def _high_velocity(self, event_time: datetime) -> GeneratedTransaction:
        customer = self._attack_customer
        return self._create_event(
            customer=customer,
            merchant=self.random.choice(self.merchants),
            event_time=event_time,
            amount_minor=customer.normal_amount_minor * 2,
            device_id=self.random.choice(customer.known_devices),
            device_trust_status="known",
            location=customer.home_location,
            failed_attempts=0,
            auth_result="success",
            scenario="high_velocity",
            is_fraud=True,
            expected_signals=["high_transaction_velocity"],
        )

    def _create_event(
        self,
        *,
        customer: CustomerProfile,
        merchant: Merchant,
        event_time: datetime,
        amount_minor: int,
        device_id: str,
        device_trust_status: str,
        location: Location,
        failed_attempts: int,
        auth_result: str,
        scenario: str,
        is_fraud: bool,
        expected_signals: List[str],
    ) -> GeneratedTransaction:
        sequence = self._event_counter + 1
        transaction_id = f"TXN-{self.seed:04d}-{sequence:08d}"
        event_id = f"EVT-{self.seed:04d}-{sequence:08d}"
        transaction = {
            "transaction_id": transaction_id,
            "customer_id": customer.customer_id,
            "account_id": customer.account_id,
            "amount_minor": int(amount_minor),
            "currency": "INR",
            "merchant": {
                "merchant_id": merchant.merchant_id,
                "name": merchant.name,
                "category": merchant.category,
            },
            "payment_channel": self.random.choice(PAYMENT_CHANNELS),
            "device": {
                "device_id": device_id,
                "trust_status": device_trust_status,
            },
            "ip_address": self._ip_address(),
            "location": {
                "city": location.city,
                "country": location.country,
                "latitude": location.latitude,
                "longitude": location.longitude,
            },
            "authentication": {
                "method": self.random.choice(AUTH_METHODS),
                "result": auth_result,
                "failed_attempts_last_10m": failed_attempts,
            },
            "transaction_time": event_time.isoformat(),
        }

        customer.last_transaction_at = event_time
        customer.last_transaction_location = location
        if auth_result == "success":
            customer.last_customer_activity = event_time

        return GeneratedTransaction(
            event_id=event_id,
            event_time=event_time,
            transaction=transaction,
            ground_truth=GroundTruth(
                is_fraud=is_fraud,
                scenario=scenario,
                expected_signals=expected_signals,
            ),
        )

    def _build_merchants(self) -> List[Merchant]:
        merchants: List[Merchant] = []
        for index, (name, category) in enumerate(MERCHANT_BLUEPRINTS, start=1):
            merchants.append(
                Merchant(
                    merchant_id=f"MER-{index:04d}",
                    name=name,
                    category=category,
                    location=LOCATIONS[(index - 1) % 4],
                )
            )
        return merchants

    def _build_customers(self, count: int) -> List[CustomerProfile]:
        customers: List[CustomerProfile] = []
        categories = [category for _, category in MERCHANT_BLUEPRINTS]
        for index in range(1, count + 1):
            home = LOCATIONS[(index - 1) % 4]
            last_activity = self.start_time - timedelta(
                days=self.random.randint(0, 30)
            )
            customers.append(
                CustomerProfile(
                    customer_id=f"CUST-{index:04d}",
                    account_id=f"ACC-{index:04d}",
                    home_location=home,
                    normal_amount_minor=self.random.randint(50_000, 500_000),
                    known_devices=[
                        f"DEV-{index:04d}-01",
                        f"DEV-{index:04d}-02",
                    ],
                    preferred_categories=self.random.sample(categories, 3),
                    last_customer_activity=last_activity,
                    last_transaction_at=self.start_time - timedelta(hours=12),
                    last_transaction_location=home,
                )
            )
        return customers

    def _preferred_merchant(self, customer: CustomerProfile) -> Merchant:
        candidates = [
            merchant
            for merchant in self.merchants
            if merchant.category in customer.preferred_categories
        ]
        return self.random.choice(candidates)

    def _merchant_for_category(self, category: str) -> Merchant:
        return next(
            merchant
            for merchant in self.merchants
            if merchant.category == category
        )

    def _near_home(self, customer: CustomerProfile) -> Location:
        if customer.home_location.city == "Delhi" and self.random.random() < 0.2:
            return LOCATIONS[1]
        return customer.home_location

    def _far_location(self, home: Location) -> Location:
        international = [location for location in LOCATIONS[4:] if location != home]
        return self.random.choice(international)

    def _new_device_id(self, prefix: str = "NEW") -> str:
        return f"{prefix}-{self.random.randint(100000, 999999)}"

    def _ip_address(self) -> str:
        return "198.51.100." + str(self.random.randint(1, 254))

