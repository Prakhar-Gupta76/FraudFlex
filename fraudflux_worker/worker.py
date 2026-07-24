"""Authoritative asynchronous transaction-processing orchestration."""

from __future__ import annotations

from datetime import datetime, timezone
from threading import Event
from typing import Callable, Optional

from fraudflux_validation import (
    DeadLetterSource,
    TransactionValidationError,
    build_dead_letter_event,
    validate_transaction_event,
)

from .domain import OutboxMessage, ProcessingOutcome
from .outputs import DecisionOutputFactory
from .ports import (
    AnomalyModel,
    ConsumedMessage,
    Consumer,
    DecisionEngine,
    FeatureCalculator,
    HistoryProvider,
    OutputPublisher,
    ProcessingStore,
    RiskCombiner,
    RulesEngine,
)
from .scoring import DecisionProcessor, SharedScoringPipeline


class KafkaConsumptionError(RuntimeError):
    pass


class FraudScoringWorker:
    def __init__(
        self,
        *,
        consumer: Consumer,
        history_provider: HistoryProvider,
        feature_calculator: FeatureCalculator,
        rules_engine: RulesEngine,
        anomaly_model: AnomalyModel,
        risk_combiner: RiskCombiner,
        decision_engine: DecisionEngine,
        store: ProcessingStore,
        publisher: OutputPublisher,
        output_factory: Optional[DecisionOutputFactory] = None,
        consumer_group: str = "fraudflux-scoring-worker",
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        timer_ns: Optional[Callable[[], int]] = None,
    ) -> None:
        self.consumer = consumer
        self.history_provider = history_provider
        self.feature_calculator = feature_calculator
        self.rules_engine = rules_engine
        self.anomaly_model = anomaly_model
        self.risk_combiner = risk_combiner
        self.decision_engine = decision_engine
        self.store = store
        self.publisher = publisher
        self.output_factory = output_factory or DecisionOutputFactory()
        self.consumer_group = consumer_group
        self.clock = clock
        pipeline_arguments = {}
        if timer_ns is not None:
            pipeline_arguments["timer_ns"] = timer_ns
        self.pipeline = SharedScoringPipeline(
            history_provider=history_provider,
            feature_calculator=feature_calculator,
            rules_engine=rules_engine,
            anomaly_model=anomaly_model,
            risk_combiner=risk_combiner,
            decision_engine=decision_engine,
            **pipeline_arguments,
        )
        self.processor = DecisionProcessor(
            pipeline=self.pipeline,
            store=store,
            publisher=publisher,
            output_factory=self.output_factory,
            clock=clock,
        )

    def process_message(
        self,
        message: ConsumedMessage,
    ) -> ProcessingOutcome:
        if message.error() is not None:
            raise KafkaConsumptionError(str(message.error()))
        try:
            event = validate_transaction_event(message.value())
        except TransactionValidationError as error:
            return self._reject_invalid(message, error)

        processed = self.processor.process(event)
        self._commit(message)
        return (
            ProcessingOutcome.PROCESSED
            if processed.created
            else ProcessingOutcome.DUPLICATE
        )

    def run_once(self, timeout_seconds: float = 1.0) -> ProcessingOutcome:
        message = self.consumer.poll(timeout_seconds)
        if message is None:
            return ProcessingOutcome.NO_MESSAGE
        return self.process_message(message)

    def run_forever(
        self,
        *,
        stop_event: Optional[Event] = None,
        poll_timeout_seconds: float = 1.0,
    ) -> None:
        stopper = stop_event or Event()
        try:
            while not stopper.is_set():
                self.run_once(poll_timeout_seconds)
        finally:
            self.consumer.close()

    def _reject_invalid(
        self,
        message: ConsumedMessage,
        error: TransactionValidationError,
    ) -> ProcessingOutcome:
        record_id = (
            f"source:{message.topic()}:{message.partition()}:{message.offset()}"
        )
        dead_letter = build_dead_letter_event(
            message.value(),
            error,
            source=DeadLetterSource(
                topic=message.topic(),
                partition=message.partition(),
                offset=message.offset(),
                consumer_group=self.consumer_group,
            ),
            failed_at=self.clock(),
        )
        outbox = [
            OutboxMessage(
                outbox_id=f"OUTBOX-{record_id}",
                record_id=record_id,
                topic=self.output_factory.dead_letter_topic,
                key=dead_letter.original_event_id or record_id,
                payload=dead_letter.model_dump(mode="json"),
            )
        ]
        created = self.store.save_rejection_if_absent(record_id, outbox)
        self._publish_pending(record_id)
        self._commit(message)
        return (
            ProcessingOutcome.REJECTED
            if created
            else ProcessingOutcome.DUPLICATE
        )

    def _publish_pending(self, record_id: str) -> None:
        self.processor.publish_pending(record_id)

    def _commit(self, message: ConsumedMessage) -> None:
        self.consumer.commit(message=message, asynchronous=False)
