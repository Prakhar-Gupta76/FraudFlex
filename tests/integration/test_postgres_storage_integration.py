from __future__ import annotations

import os
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
RUN_INTEGRATION = (
    os.getenv("FRAUDFLUX_RUN_POSTGRES_INTEGRATION") == "1"
)


@unittest.skipUnless(
    RUN_INTEGRATION,
    "set FRAUDFLUX_RUN_POSTGRES_INTEGRATION=1 with PostgreSQL running",
)
class PostgresStorageIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        try:
            import psycopg
        except ImportError as exc:
            raise unittest.SkipTest("psycopg is not installed") from exc
        dsn = os.getenv(
            "FRAUDFLUX_POSTGRES_DSN",
            "postgresql://fraudflux:fraudflux@localhost:5432/fraudflux",
        )
        cls.connection = psycopg.connect(dsn, autocommit=True)
        with cls.connection.cursor() as cursor:
            for filename in (
                "001_feature_history.sql",
                "002_operational_storage.sql",
            ):
                cursor.execute(
                    (
                        ROOT / "infra" / "postgres" / filename
                    ).read_text(encoding="utf-8")
                )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.connection.close()

    def test_migrations_are_repeatable_and_tables_are_available(self) -> None:
        migration = (
            ROOT / "infra" / "postgres" / "002_operational_storage.sql"
        ).read_text(encoding="utf-8")
        with self.connection.cursor() as cursor:
            cursor.execute(migration)
            cursor.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                """
            )
            tables = {row[0] for row in cursor.fetchall()}

        self.assertTrue(
            {
                "customer_profiles",
                "transaction_history",
                "risk_decisions",
                "fraud_alerts",
                "analyst_reviews",
                "ruleset_versions",
                "model_versions",
                "outbox_events",
                "audit_history",
            }.issubset(tables)
        )


if __name__ == "__main__":
    unittest.main()
