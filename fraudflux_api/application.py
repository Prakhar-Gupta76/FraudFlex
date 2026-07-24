"""FastAPI application factory for FraudFlux."""

from __future__ import annotations

import asyncio
from typing import Any, Mapping, Optional, Protocol, Sequence

from fastapi import FastAPI, HTTPException, Query, Request, status
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from fraudflux_storage import ReviewOutcome
from fraudflux_validation import TransactionEvent
from fraudflux_worker import DecisionProcessor, ProcessedDecision

from .models import (
    AlertDetail,
    AlertSummary,
    AnalystReviewRequest,
    AnalystReviewResponse,
    DashboardSummary,
    HealthResponse,
    ScoreResponse,
    TransactionDetail,
    TransactionSummary,
)


class QueryRepository(Protocol):
    def list_transactions(
        self,
        *,
        limit: int,
        offset: int,
        category: Optional[str] = None,
        search: Optional[str] = None,
        customer_id: Optional[str] = None,
    ) -> Sequence[Mapping[str, Any]]: ...

    def get_transaction(
        self,
        transaction_id: str,
    ) -> Optional[Mapping[str, Any]]: ...

    def list_alerts(
        self,
        *,
        limit: int,
        offset: int,
        status: Optional[str] = None,
    ) -> Sequence[Mapping[str, Any]]: ...

    def get_alert(
        self,
        alert_id: str,
    ) -> Optional[Mapping[str, Any]]: ...

    def dashboard_summary(self) -> Mapping[str, Any]: ...

    def health(self) -> bool: ...


class AlertReviewRepository(Protocol):
    def review(
        self,
        alert_id: str,
        *,
        review_id: str,
        analyst_id: str,
        outcome: ReviewOutcome,
        notes: Optional[str] = None,
    ) -> bool: ...


def create_app(
    *,
    processor: DecisionProcessor,
    queries: QueryRepository,
    alerts: AlertReviewRepository,
    cors_origins: Sequence[str] = (
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ),
    live_interval_seconds: float = 2.0,
) -> FastAPI:
    if live_interval_seconds <= 0:
        raise ValueError("live_interval_seconds must be positive")
    app = FastAPI(
        title="FraudFlux Risk API",
        version="0.14.0",
        description=(
            "Synchronous scoring and analyst-query API for simulated "
            "FraudFlux transactions."
        ),
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(cors_origins),
        allow_credentials=True,
        allow_methods=["GET", "POST"],
        allow_headers=["Content-Type"],
    )

    @app.post(
        "/transactions/score",
        response_model=ScoreResponse,
        status_code=status.HTTP_200_OK,
        tags=["transactions"],
    )
    def score_transaction(event: TransactionEvent) -> ScoreResponse:
        try:
            processed = processor.process(event)
        except RuntimeError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Scoring or output publication is temporarily unavailable",
            ) from exc
        return _score_response(processed)

    @app.get(
        "/transactions",
        response_model=list[TransactionSummary],
        tags=["transactions"],
    )
    def list_transactions(
        limit: int = Query(default=50, ge=1, le=200),
        offset: int = Query(default=0, ge=0),
        category: Optional[str] = Query(default=None, pattern="^(low|medium|high)$"),
        search: Optional[str] = Query(default=None, max_length=120),
        customer_id: Optional[str] = Query(default=None, max_length=64),
    ) -> Sequence[Mapping[str, Any]]:
        return queries.list_transactions(
            limit=limit,
            offset=offset,
            category=category,
            search=search.strip() if search and search.strip() else None,
            customer_id=customer_id,
        )

    @app.get(
        "/transactions/{transaction_id}",
        response_model=TransactionDetail,
        tags=["transactions"],
    )
    def get_transaction(transaction_id: str) -> Mapping[str, Any]:
        transaction = queries.get_transaction(transaction_id)
        if transaction is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Transaction not found",
            )
        return transaction

    @app.get(
        "/alerts",
        response_model=list[AlertSummary],
        tags=["alerts"],
    )
    def list_alerts(
        limit: int = Query(default=50, ge=1, le=200),
        offset: int = Query(default=0, ge=0),
        alert_status: Optional[str] = Query(
            default=None,
            alias="status",
            pattern="^(open|assigned|resolved)$",
        ),
    ) -> Sequence[Mapping[str, Any]]:
        return queries.list_alerts(
            limit=limit,
            offset=offset,
            status=alert_status,
        )

    @app.get(
        "/alerts/{alert_id}",
        response_model=AlertDetail,
        tags=["alerts"],
    )
    def get_alert(alert_id: str) -> Mapping[str, Any]:
        alert = queries.get_alert(alert_id)
        if alert is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Alert not found",
            )
        return alert

    @app.post(
        "/alerts/{alert_id}/review",
        response_model=AnalystReviewResponse,
        tags=["alerts"],
    )
    def review_alert(
        alert_id: str,
        request: AnalystReviewRequest,
    ) -> AnalystReviewResponse:
        try:
            created = alerts.review(
                alert_id,
                review_id=request.review_id,
                analyst_id=request.analyst_id,
                outcome=request.outcome,
                notes=request.notes,
            )
        except KeyError as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Alert not found",
            ) from exc
        if not created:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Alert already has a final analyst review",
            )
        return AnalystReviewResponse(
            alert_id=alert_id,
            review_id=request.review_id,
            outcome=request.outcome,
            status="resolved",
        )

    @app.get(
        "/dashboard/summary",
        response_model=DashboardSummary,
        tags=["dashboard"],
    )
    def dashboard_summary() -> Mapping[str, Any]:
        return queries.dashboard_summary()

    @app.get("/events/stream", tags=["operations"])
    async def event_stream(request: Request) -> StreamingResponse:
        async def events():
            while not await request.is_disconnected():
                try:
                    summary = await run_in_threadpool(
                        queries.dashboard_summary
                    )
                    payload = DashboardSummary.model_validate(
                        summary
                    ).model_dump_json()
                    yield f"event: dashboard\ndata: {payload}\n\n"
                except Exception:
                    yield (
                        "event: service.degraded\n"
                        'data: {"status":"degraded"}\n\n'
                    )
                await asyncio.sleep(live_interval_seconds)

        return StreamingResponse(
            events(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )

    @app.get(
        "/health",
        response_model=HealthResponse,
        tags=["operations"],
    )
    def health() -> HealthResponse:
        try:
            database_healthy = queries.health()
        except Exception:
            database_healthy = False
        state = "healthy" if database_healthy else "unhealthy"
        return HealthResponse(
            status="healthy" if database_healthy else "degraded",
            service="fraudflux-api",
            version="0.14.0",
            checks={"database": state},
        )

    return app


def _score_response(processed: ProcessedDecision) -> ScoreResponse:
    stored = processed.stored
    return ScoreResponse(
        event_id=stored.input_event_id,
        transaction_id=stored.transaction_id,
        customer_id=stored.customer_id,
        created=processed.created,
        final_score=stored.combined_score.final_score,
        score_category=stored.decision.score_category.value,
        category=stored.decision.category.value,
        recommended_action=stored.decision.action.value,
        override_applied=stored.decision.override_applied,
        explanation=stored.decision.explanation,
        rules_contribution=stored.rules.contribution,
        triggered_rules=tuple(
            {
                "rule_id": hit.rule_id,
                "points": hit.points,
                "reason": hit.reason,
            }
            for hit in stored.rules.hits
        ),
        anomaly_contribution=stored.anomaly.contribution,
        anomaly_level=stored.anomaly.level,
        anomaly_deviations=stored.anomaly.deviations,
        ruleset_version=stored.rules.ruleset_version,
        model_version=stored.anomaly.model_version,
        score_policy_version=stored.combined_score.policy_version,
        decision_policy_version=stored.decision.decision_policy_version,
        processing_latency_ms=stored.decision.processing_latency_ms,
        processed_at=stored.processed_at,
    )
