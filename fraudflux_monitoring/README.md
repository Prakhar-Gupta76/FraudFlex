# FraudFlux monitoring and audit

The first MVP uses a thread-safe, in-process metrics registry. It adds no
external service and is appropriate for development or a single service
process. `GET /metrics` exposes Prometheus-compatible text so Prometheus and
Grafana can be added later without changing the business pipeline.

Recorded metrics cover:

- Events produced and consumed by topic and outcome
- Dead-letter and publication failures
- Kafka consumer lag by group, topic, and partition
- Scoring and HTTP request latency, including p50 and p95
- Database and anomaly-model failures
- Process throughput and uptime
- Rule-trigger frequency

Labels are deliberately low-cardinality. Customer, transaction, account, and
device identifiers must never be metric labels.

PostgreSQL remains the durable audit authority. `risk_decisions` retains the
input event, event and processing times, score, action, explanations, ruleset
and model versions. `audit_history` retains append-only decision and analyst
changes. `DecisionAuditSnapshot` validates and exposes the required decision
evidence without mutating it.
