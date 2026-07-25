"""Environment-driven FastAPI runtime composition."""

from __future__ import annotations

from fraudflux_anomaly import IsolationForestAnomalyModel
from fraudflux_config import environment_value, load_environment
from fraudflux_decision import InitialDecisionEngine
from fraudflux_features import CustomerFeatureCalculator, PostgresHistoryProvider
from fraudflux_kafka import KafkaProducerSettings
from fraudflux_monitoring import OperationalMonitor
from fraudflux_risk import InitialRiskScoreCombiner
from fraudflux_rules import YamlRulesEngine
from fraudflux_storage import (
    PostgresAlertRepository,
    PostgresProcessingStore,
    PostgresQueryRepository,
    PostgresStorageSettings,
    create_connection_factory,
)
from fraudflux_worker import (
    DecisionProcessor,
    KafkaOutputPublisher,
    SharedScoringPipeline,
)

from .application import create_app


def create_runtime_app():
    """Build the real API from environment configuration."""
    load_environment()
    model_path = environment_value("FRAUDFLUX_MODEL_ARTIFACT").strip()
    if not model_path:
        raise RuntimeError(
            "FRAUDFLUX_MODEL_ARTIFACT must point to a trained model artifact"
        )
    dsn = environment_value(
        "FRAUDFLUX_POSTGRES_DSN",
        PostgresStorageSettings().dsn,
    )
    bootstrap_servers = environment_value(
        "FRAUDFLUX_KAFKA_BOOTSTRAP_SERVERS",
        "127.0.0.1:9092",
    )
    cors_origins = tuple(
        origin.strip()
        for origin in environment_value(
            "FRAUDFLUX_CORS_ORIGINS",
            "http://localhost:5173,http://127.0.0.1:5173",
        ).split(",")
        if origin.strip()
    )
    connection_factory = create_connection_factory(
        PostgresStorageSettings(dsn=dsn)
    )
    monitor = OperationalMonitor()
    pipeline = SharedScoringPipeline(
        history_provider=PostgresHistoryProvider(connection_factory),
        feature_calculator=CustomerFeatureCalculator(),
        rules_engine=YamlRulesEngine.default(),
        anomaly_model=IsolationForestAnomalyModel.from_path(model_path),
        risk_combiner=InitialRiskScoreCombiner(),
        decision_engine=InitialDecisionEngine(),
        monitor=monitor,
    )
    processor = DecisionProcessor(
        pipeline=pipeline,
        store=PostgresProcessingStore(connection_factory),
        publisher=KafkaOutputPublisher.from_settings(
            KafkaProducerSettings(
                bootstrap_servers=bootstrap_servers,
                client_id="fraudflux-api-output",
            )
        ),
        monitor=monitor,
    )
    return create_app(
        processor=processor,
        queries=PostgresQueryRepository(connection_factory),
        alerts=PostgresAlertRepository(connection_factory),
        cors_origins=cors_origins,
        monitor=monitor,
    )
