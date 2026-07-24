# FraudFlux PostgreSQL history store

The local PostgreSQL container is initialized with
`001_feature_history.sql`. It contains the point-in-time history required by
the customer feature calculator:

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

The initialization script runs only when the Docker volume is first created.
Later schema changes should use versioned migrations instead of editing an
already-applied file.
