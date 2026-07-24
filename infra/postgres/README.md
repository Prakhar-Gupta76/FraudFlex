# FraudFlux PostgreSQL history store

The local PostgreSQL container is initialized with three ordered schema files:

- `001_feature_history.sql` creates the point-in-time history used by the
  customer feature calculator.
- `002_operational_storage.sql` adds durable scoring decisions, alerts,
  analyst reviews, model/ruleset versions, audit history, rejected-event
  idempotency, and the Kafka outbox.
- `003_analyst_review_history.sql` makes reviews append-only, records status
  transitions, and permits interim reviews before a final resolution.

The history schema contains:

- `customer_profiles`
- `transaction_history`
- `device_deny_list`
- `merchant_risk_profiles`

Start Kafka and PostgreSQL:

```powershell
docker compose up -d
```

The development connection string is:

```text
postgresql://fraudflux:fraudflux@localhost:5432/fraudflux
```

These credentials are only for local development. Use secret-managed
credentials outside the developer machine.

PostgreSQL initialization scripts run only when the Docker volume is first
created. For an existing local volume, apply the new migration explicitly:

```powershell
Get-Content infra/postgres/002_operational_storage.sql |
    docker compose exec -T postgres psql -U fraudflux -d fraudflux
Get-Content infra/postgres/003_analyst_review_history.sql |
    docker compose exec -T postgres psql -U fraudflux -d fraudflux
```

Production environments should apply these versioned files with a migration
runner and secret-managed credentials.

Run the opt-in live migration test after PostgreSQL is healthy:

```powershell
$env:FRAUDFLUX_RUN_POSTGRES_INTEGRATION = "1"
python -m unittest tests.integration.test_postgres_storage_integration -v
```

Ordinary unit tests use database fakes and remain available without Docker or
Psycopg.
