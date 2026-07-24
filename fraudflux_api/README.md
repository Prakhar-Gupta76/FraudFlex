# FraudFlux FastAPI service

The API and Kafka worker both call `SharedScoringPipeline` through
`DecisionProcessor`. Manual and streamed copies of the same event therefore
use identical features, rules, anomaly inference, combination, explanation,
storage, outbox, and idempotency logic.

## Run locally

```powershell
docker compose up -d
pip install -e .
$env:FRAUDFLUX_MODEL_ARTIFACT = "artifacts/isolation-forest.joblib"
uvicorn fraudflux_api.runtime:create_runtime_app --factory --reload
```

Optional environment variables:

- `FRAUDFLUX_POSTGRES_DSN`
- `FRAUDFLUX_KAFKA_BOOTSTRAP_SERVERS`

OpenAPI documentation is available at `http://127.0.0.1:8000/docs`.
List limits are capped at 200 rows.
