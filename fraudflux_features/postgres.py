"""PostgreSQL point-in-time customer history provider."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Callable, Mapping, Optional, Protocol, Sequence

from fraudflux_validation import TransactionEvent
from fraudflux_worker import CustomerHistory


class Cursor(Protocol):
    def execute(self, query: str, parameters: Sequence[Any]) -> Any: ...

    def fetchall(self) -> Sequence[Mapping[str, Any]]: ...

    def fetchone(self) -> Optional[Mapping[str, Any]]: ...

    def __enter__(self) -> "Cursor": ...

    def __exit__(self, *args: Any) -> None: ...


class Connection(Protocol):
    def cursor(self) -> Cursor: ...

    def __enter__(self) -> "Connection": ...

    def __exit__(self, *args: Any) -> None: ...


@dataclass(frozen=True)
class PostgresHistorySettings:
    dsn: str = (
        "postgresql://fraudflux:fraudflux@localhost:5432/fraudflux"
    )
    lookback_days: int = 90
    transaction_limit: int = 2000

    def __post_init__(self) -> None:
        if not self.dsn.strip():
            raise ValueError("dsn cannot be blank")
        if self.lookback_days < 1:
            raise ValueError("lookback_days must be positive")
        if self.transaction_limit < 1:
            raise ValueError("transaction_limit must be positive")


class PostgresHistoryProvider:
    """Load only records that existed before the transaction being scored."""

    def __init__(
        self,
        connection_factory: Callable[[], Connection],
        *,
        settings: Optional[PostgresHistorySettings] = None,
    ) -> None:
        self.connection_factory = connection_factory
        self.settings = settings or PostgresHistorySettings()

    def load(self, event: TransactionEvent) -> CustomerHistory:
        transaction = event.transaction
        as_of = transaction.transaction_time
        since = as_of - timedelta(days=self.settings.lookback_days)

        with self.connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    _TRANSACTION_HISTORY_QUERY,
                    (
                        transaction.customer_id,
                        since,
                        as_of,
                        self.settings.transaction_limit,
                    ),
                )
                transactions = [dict(row) for row in cursor.fetchall()]

                cursor.execute(
                    _DEVICE_INTELLIGENCE_QUERY,
                    (
                        transaction.device.device_id,
                        as_of,
                        transaction.device.device_id,
                        as_of,
                    ),
                )
                device = dict(cursor.fetchone() or {})

                cursor.execute(
                    _MERCHANT_INTELLIGENCE_QUERY,
                    (transaction.merchant.merchant_id,),
                )
                merchant = dict(cursor.fetchone() or {})

                cursor.execute(
                    _CUSTOMER_PROFILE_QUERY,
                    (transaction.customer_id,),
                )
                profile = dict(cursor.fetchone() or {})

        usual_countries = _profile_values(
            profile.get("usual_countries"),
            profile.get("home_country"),
        )
        usual_regions = _profile_values(
            profile.get("usual_regions"),
            profile.get("home_region"),
        )
        return CustomerHistory(
            customer_id=transaction.customer_id,
            values={
                "as_of": as_of.isoformat(),
                "transactions": transactions,
                "device": device,
                "merchant": merchant,
                "usual_countries": usual_countries,
                "usual_regions": usual_regions,
            },
        )


def create_postgres_history_provider(
    settings: Optional[PostgresHistorySettings] = None,
) -> PostgresHistoryProvider:
    resolved = settings or PostgresHistorySettings()
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:
        raise RuntimeError(
            "psycopg is required for PostgreSQL customer history"
        ) from exc

    def connect() -> Connection:
        return psycopg.connect(resolved.dsn, row_factory=dict_row)

    return PostgresHistoryProvider(connect, settings=resolved)


def _profile_values(raw: Any, fallback: Any) -> tuple[str, ...]:
    if raw:
        if isinstance(raw, str):
            return (raw,)
        return tuple(str(value) for value in raw)
    return (str(fallback),) if fallback else ()


_TRANSACTION_HISTORY_QUERY = """
SELECT
    amount_minor,
    currency,
    merchant_id,
    merchant_category,
    device_id,
    region,
    country,
    latitude,
    longitude,
    authentication_result,
    failed_attempts_last_10m,
    transaction_time
FROM transaction_history
WHERE customer_id = %s
  AND transaction_time >= %s
  AND transaction_time < %s
ORDER BY transaction_time DESC
LIMIT %s
"""

_DEVICE_INTELLIGENCE_QUERY = """
SELECT
    MIN(history.transaction_time) AS first_seen_at,
    COUNT(DISTINCT history.account_id)::integer AS account_count,
    EXISTS (
        SELECT 1
        FROM device_deny_list denied
        WHERE denied.device_id = %s
          AND (denied.expires_at IS NULL OR denied.expires_at > %s)
    ) AS deny_listed
FROM transaction_history history
WHERE history.device_id = %s
  AND history.transaction_time < %s
"""

_MERCHANT_INTELLIGENCE_QUERY = """
SELECT fraud_rate
FROM merchant_risk_profiles
WHERE merchant_id = %s
"""

_CUSTOMER_PROFILE_QUERY = """
SELECT home_country, home_region, usual_countries, usual_regions
FROM customer_profiles
WHERE customer_id = %s
"""
