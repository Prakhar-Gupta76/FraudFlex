from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fraudflux_config import load_environment
from fraudflux_kafka import KafkaProducerSettings, KafkaSecuritySettings
from fraudflux_storage import PostgresStorageSettings
from fraudflux_worker import KafkaConsumerSettings


class EnvironmentLoadingTests(unittest.TestCase):
    def test_explicit_env_file_configures_all_service_settings(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env"
            path.write_text(
                "\n".join(
                    (
                        "FRAUDFLUX_POSTGRES_DSN=postgresql://demo",
                        "FRAUDFLUX_KAFKA_BOOTSTRAP_SERVERS=broker:19092",
                        "FRAUDFLUX_KAFKA_CONSUMER_GROUP=demo-worker",
                    )
                ),
                encoding="utf-8",
            )
            with patch.dict(os.environ, {}, clear=True):
                loaded = load_environment(path, override=True)
                producer = KafkaProducerSettings()
                consumer = KafkaConsumerSettings()
                postgres = PostgresStorageSettings()

        self.assertEqual(loaded, path.resolve())
        self.assertEqual(producer.bootstrap_servers, "broker:19092")
        self.assertEqual(consumer.bootstrap_servers, "broker:19092")
        self.assertEqual(consumer.group_id, "demo-worker")
        self.assertEqual(postgres.dsn, "postgresql://demo")

    def test_existing_process_environment_wins_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env"
            path.write_text("SETTING=from-file\n", encoding="utf-8")
            with patch.dict(
                os.environ,
                {"SETTING": "from-process"},
                clear=True,
            ):
                load_environment(path)
                value = os.environ["SETTING"]

        self.assertEqual(value, "from-process")

    def test_cloud_sasl_values_are_passed_to_kafka_clients(self) -> None:
        with patch.dict(
            os.environ,
            {
                "FRAUDFLUX_KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
                "FRAUDFLUX_KAFKA_SASL_MECHANISM": "PLAIN",
                "FRAUDFLUX_KAFKA_USERNAME": "api-key",
                "FRAUDFLUX_KAFKA_PASSWORD": "api-secret",
            },
            clear=False,
        ):
            security = KafkaSecuritySettings()
            producer = KafkaProducerSettings(security=security)
            consumer = KafkaConsumerSettings(security=security)

        for config in (
            producer.confluent_config(),
            consumer.confluent_config(),
        ):
            self.assertEqual(config["security.protocol"], "SASL_SSL")
            self.assertEqual(config["sasl.mechanism"], "PLAIN")
            self.assertEqual(config["sasl.username"], "api-key")
            self.assertEqual(config["sasl.password"], "api-secret")

    def test_incomplete_sasl_credentials_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires"):
            KafkaSecuritySettings(
                security_protocol="SASL_SSL",
                sasl_mechanism="PLAIN",
                username="only-key",
            )

    def test_explicit_missing_env_file_is_reported(self) -> None:
        with self.assertRaises(FileNotFoundError):
            load_environment("definitely-missing.env")


if __name__ == "__main__":
    unittest.main()
