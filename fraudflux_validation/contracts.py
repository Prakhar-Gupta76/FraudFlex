"""Pydantic contracts for transaction-created events."""

from __future__ import annotations

from datetime import timedelta
from typing import Annotated, Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    FiniteFloat,
    IPvAnyAddress,
    StrictInt,
    StringConstraints,
    model_validator,
)

SUPPORTED_SCHEMA_VERSIONS = frozenset({"1.0"})
SUPPORTED_CURRENCIES = frozenset({"INR", "USD", "EUR", "GBP", "SGD"})

Identifier = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        max_length=64,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$",
    ),
]
DisplayText = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=120),
]
Category = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        max_length=64,
        pattern=r"^[a-z][a-z0-9_]*$",
    ),
]
PositiveAmountMinor = Annotated[
    StrictInt,
    Field(gt=0, le=1_000_000_000_000_000),
]
AttemptCount = Annotated[StrictInt, Field(ge=0, le=1000)]
Latitude = Annotated[FiniteFloat, Field(ge=-90, le=90)]
Longitude = Annotated[FiniteFloat, Field(ge=-180, le=180)]


class ContractModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )


class MerchantContract(ContractModel):
    merchant_id: Identifier
    name: DisplayText
    category: Category


class DeviceContract(ContractModel):
    device_id: Identifier
    trust_status: Literal["known", "new", "deny_listed"]


class LocationContract(ContractModel):
    city: DisplayText
    country: DisplayText
    latitude: Latitude
    longitude: Longitude


class AuthenticationContract(ContractModel):
    method: Literal["pin", "otp", "biometric", "3ds", "password"]
    result: Literal["success", "failure", "challenged"]
    failed_attempts_last_10m: AttemptCount


class TransactionContract(ContractModel):
    transaction_id: Identifier
    customer_id: Identifier
    account_id: Identifier
    amount_minor: PositiveAmountMinor
    currency: Literal["INR", "USD", "EUR", "GBP", "SGD"]
    merchant: MerchantContract
    payment_channel: Literal["card", "upi", "wallet", "bank_transfer"]
    device: DeviceContract
    ip_address: IPvAnyAddress
    location: LocationContract
    authentication: AuthenticationContract
    transaction_time: AwareDatetime


class TransactionEvent(ContractModel):
    event_id: Identifier
    event_type: Literal["transaction.created"]
    schema_version: Literal["1.0"]
    event_time: AwareDatetime
    transaction: TransactionContract

    @model_validator(mode="after")
    def transaction_time_is_not_far_ahead(self) -> "TransactionEvent":
        maximum_clock_skew = timedelta(minutes=5)
        if self.transaction.transaction_time > self.event_time + maximum_clock_skew:
            raise ValueError(
                "transaction_time cannot be more than 5 minutes after event_time"
            )
        return self

