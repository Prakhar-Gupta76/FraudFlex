# FraudFlux Architecture

## 1. Purpose

FraudFlux is a real-time payment risk intelligence platform for learning and
system-design practice. It processes synthetic transaction events, calculates
behavioural features, evaluates rules and a machine-learning anomaly model,
assigns a risk score, and presents explainable alerts to an analyst.

The MVP does not connect to real payment networks and must not make decisions
about real financial transactions.

## 2. MVP Architecture

```text
Transaction Simulator
        |
        v
Transaction Validation
        |
        v
Kafka Producer
        |
        v
Kafka: transactions.raw
        |
        v
Fraud-Scoring Worker
        |
        v
Customer Feature Calculator
        |
        +--------------------+
        |                    |
        v                    v
Rules Engine          Anomaly Model
        |                    |
        +---------+----------+
                  |
                  v
          Risk-Score Combiner
                  |
                  v
     Decision and Explanation Engine
                  |
        +---------+----------+
        |         |          |
        v         v          v
   PostgreSQL   Kafka    FastAPI
                  |          |
                  v          v
             Alert Events  Dashboard
                              |
                              v
                       Analyst Review
                              |
                              v
                    Feedback and Metrics
```

## 3. Current Implementation Status

| Component | MVP status |
| --- | --- |
| Transaction simulator | Implemented first |
| Transaction validation | Implemented |
| Kafka producer | Implemented |
| Kafka broker and topics | Implemented |
| Fraud-scoring worker | Planned |
| Feature calculator | Planned |
| Rules engine | Planned |
| Anomaly model | Planned |
| Risk and decision engines | Planned |
| PostgreSQL persistence | Planned |
| FastAPI service | Planned |
| Analyst dashboard | Planned |
| Evaluation and monitoring | Planned |

## 4. Component Flow

### 4.1 Transaction Simulator

The simulator behaves like a payment application producing transactions. It
creates synthetic customers, accounts, merchants, devices, locations, and
transaction histories.

Supported traffic scenarios are:

- Normal customer activity
- Mixed normal and fraudulent activity
- Account takeover
- Card testing
- Impossible travel
- Dormant-account reactivation
- Merchant fraud spike
- High transaction velocity

The simulator supports deterministic random seeds, configurable event counts,
configurable transaction rates, and optional real-time pacing. A public event
does not contain the ground-truth fraud label. Evaluation output may include a
separate ground-truth section.

Example public event:

```json
{
  "event_id": "EVT-00000001",
  "event_type": "transaction.created",
  "schema_version": "1.0",
  "event_time": "2026-01-01T09:00:00+00:00",
  "transaction": {
    "transaction_id": "TXN-00000001",
    "customer_id": "CUST-0001",
    "amount_minor": 4500000,
    "currency": "INR"
  }
}
```

The simulator retains the hidden expected label for later measurement:

```text
Actual label: Fraud
Scenario: Account takeover
```

### 4.2 Transaction Validation

Validation checks the transaction contract before scoring:

- Required IDs are present.
- Amount is positive.
- Currency is supported.
- Timestamps and coordinates are valid.
- Device, merchant, authentication, and location fields use valid formats.
- The schema version is supported.

API requests with invalid data are rejected. Invalid Kafka events are recorded
and published to `transactions.dead-letter` so one malformed event cannot stop
the consumer.

The implemented `fraudflux_validation` package provides:

- Strict Pydantic contracts for the event and every nested transaction object
- One reusable `validate_transaction_event` entry point
- Structured, input-safe error details suitable for an HTTP `422` response
- A dead-letter event builder for future Kafka consumer integration
- Rejection of unknown fields, including accidental simulator ground truth
- Timezone-aware timestamps and bounded geographic coordinates

The validator supports schema version `1.0`, currencies `INR`, `USD`, `EUR`,
`GBP`, and `SGD`, and the documented payment, device, and authentication
enumerations. The future API and Kafka consumer must call this shared validator
instead of defining their own versions of the transaction contract.

Kafka publication is not part of this component. When the Kafka consumer is
implemented, it will publish the dead-letter record produced here to
`transactions.dead-letter`.

### 4.3 Kafka Producer

The producer serializes a validated transaction, adds event metadata, and
publishes it to Kafka. The customer ID is used as the message key so one
customer's events stay ordered within a partition.

The producer is responsible for:

- JSON serialization
- Event and transaction identifiers
- Event timestamps and schema versions
- Safe retries for temporary publishing failures
- Clear reporting of permanent failures

The implemented `fraudflux_kafka` package provides:

- An event factory that adds missing event IDs, transaction IDs, timestamps,
  event type, and schema version
- Validation through the shared Component 2 transaction contract
- Deterministic UTF-8 JSON serialization
- Customer ID message keys and traceable Kafka headers
- Delivery receipts containing topic, partition, and offset
- Bounded retries when the local producer queue is temporarily full
- Idempotent Kafka configuration with `acks=all`
- Delivery-timeout-controlled broker retries
- Explicit enqueue, delivery, and timeout errors
- A client protocol that permits unit testing without a running broker

The wrapper does not manually resend a message after an uncertain delivery
timeout, because doing so could create a duplicate. Librdkafka performs safe
broker retries within the configured delivery timeout while idempotence is
enabled. A final timeout is reported to the caller as an unknown delivery
state.

The production adapter uses `confluent-kafka`. Component 4 will provide the
local Kafka broker and integration tests.

### 4.4 Kafka Event Broker

The first MVP uses one Kafka broker in KRaft mode.

| Topic | Function |
| --- | --- |
| `transactions.raw` | Newly created transaction events |
| `transactions.scored` | Completed risk decisions |
| `fraud.alerts` | Medium- and high-risk alerts |
| `transactions.dead-letter` | Invalid or unprocessable events |

Kafka separates event generation from scoring. When the scoring worker is
temporarily unavailable, retained events can be processed after it restarts.

The implemented local broker uses:

- Official `apache/kafka:4.3.1` JVM image
- One combined broker/controller node in KRaft mode
- Separate internal, host, and controller listeners
- A persistent Docker named volume
- A 512 MB JVM heap and 1 GB container memory limit
- Automatic topic creation disabled
- Broker health checks before topic initialization
- Idempotent topic creation with explicit partitions, replication, and
  retention

Topic settings for the 8 GB development machine are:

| Topic | Partitions | Replication | Time retention | Per-partition size |
| --- | ---: | ---: | ---: | ---: |
| `transactions.raw` | 3 | 1 | 24 hours | 64 MiB |
| `transactions.scored` | 3 | 1 | 24 hours | 64 MiB |
| `fraud.alerts` | 1 | 1 | 7 days | 128 MiB |
| `transactions.dead-letter` | 1 | 1 | 7 days | 128 MiB |

Using the customer ID as the key keeps one customer's records in the same
partition and therefore ordered within that partition. Kafka retains
acknowledged records independently of whether the future scoring worker is
running. A new or restarted consumer can resume using its committed offset.

This single-broker configuration provides persistence across ordinary container
restarts but no broker-level high availability. Replication factor 1 is an MVP
constraint and must not be used as a production durability design.

The local listener is plaintext and bound to `127.0.0.1`; it is for development
only. Production deployments require authentication, encryption, access
control, multiple brokers, and an appropriate replication factor.

### 4.5 Fraud-Scoring Worker

The worker is the authoritative asynchronous transaction processor. It:

1. Consumes from `transactions.raw`.
2. Validates the event again.
3. checks whether the event was already processed.
4. Loads customer history.
5. Calculates behavioural features.
6. Runs the rules engine.
7. Runs the anomaly model.
8. Combines both results.
9. Produces a category, decision, and explanation.
10. Saves the result.
11. Publishes scored and alert events.
12. Commits the Kafka offset.

The worker is idempotent: redelivery of an event must not create duplicate
decisions or alerts.

### 4.6 Customer Feature Calculator

The feature calculator turns raw transaction and historical data into values
used by rules and models.

Amount features:

- Customer average and median amount
- Recent maximum amount
- Current amount divided by normal amount
- Deviation from the customer's normal range

Velocity features:

- Transactions in the previous two minutes
- Transactions in the previous hour
- Amount spent during recent windows
- Recently used merchant count

Device features:

- New or known device
- Number of accounts using the device
- Time since the device was first seen
- Deny-list status

Location features:

- Distance from the previous location
- Time since the previous transaction
- Impossible-travel indicator
- Unusual region or country

Merchant and authentication features:

- Merchant-category rarity
- New merchant indicator
- Merchant fraud-rate signal
- Recent authentication failures
- Failures followed by a successful payment

The first MVP uses PostgreSQL and a small in-memory cache. Redis and Feast are
Phase 2 improvements.

### 4.7 Rules Engine

The rules engine evaluates explicit, configurable fraud policies. Rules are
stored in versioned YAML configuration.

Example:

```text
Amount at least five times normal: +20
New device: +15
More than five payments in two minutes: +20
```

It returns:

- Rules contribution, capped at 70
- Triggered rule identifiers
- Points contributed by each rule
- Human-readable reasons
- Ruleset version
- Decision overrides

Only the strongest matching rule in the same group is counted.

### 4.8 Anomaly Model

The first anomaly model will use Isolation Forest. It learns patterns of normal
behaviour and estimates how unusual a new combination of features is.

The model output is normalized to a contribution from 0 to 30:

| Anomaly level | Contribution |
| --- | ---: |
| Normal | 0-5 |
| Slightly unusual | 6-10 |
| Moderately unusual | 11-20 |
| Highly unusual | 21-30 |

An anomaly is not proof of fraud. It is supporting evidence alongside explicit
rules. Model output includes the raw score, normalized contribution, model
version, important deviations, and inference time.

### 4.9 Risk-Score Combiner

The initial scoring policy is:

```text
Final Risk Score = min(100, Rules Contribution + Anomaly Contribution)
```

- Rules contribute at most 70 points.
- The anomaly model contributes at most 30 points.
- High-confidence overrides may force review.

The 70/30 split is an initial explainability and safety policy. It will be
calibrated using test results rather than treated as permanent.

### 4.10 Decision and Explanation Engine

The decision engine maps scores to actions:

| Score | Category | MVP action |
| ---: | --- | --- |
| 0-39 | Low | Approve |
| 40-69 | Medium | Request additional verification |
| 70-100 | High | Temporarily hold and alert an analyst |

Every result records:

- Final score and category
- Recommended action
- Triggered rules and their points
- Anomaly contribution
- Important behavioural deviations
- Ruleset and model versions
- Processing latency

Explanations state observed facts and do not make unsupported claims about the
customer.

### 4.11 PostgreSQL Storage

PostgreSQL stores:

- Customer profiles and normal behaviour
- Transactions and processing status
- Risk decisions and explanations
- Alerts and assignments
- Analyst decisions and notes
- Model and ruleset versions
- Audit history

Unique constraints prevent duplicate transaction decisions.

### 4.12 Scored and Alert Events

All completed decisions are published to `transactions.scored`. Medium- and
high-risk results also create events in `fraud.alerts`.

This separation allows downstream services to consume all decisions or only
actionable alerts.

### 4.13 FastAPI Service

FastAPI provides:

- Manual synchronous transaction scoring
- Transaction and alert queries
- Alert details and explanations
- Dashboard summaries
- Analyst-review operations
- Health information

The synchronous API and Kafka worker reuse one scoring implementation so they
cannot silently produce different decisions.

Conceptual routes:

```text
POST /transactions/score
GET  /transactions
GET  /transactions/{transaction_id}
GET  /alerts
GET  /alerts/{alert_id}
POST /alerts/{alert_id}/review
GET  /dashboard/summary
GET  /health
```

### 4.14 Analyst Dashboard

The React and TypeScript dashboard provides:

- Live transaction and alert counts
- Approval, verification, and hold totals
- Risk-score distribution
- Median and p95 latency
- Searchable transaction stream
- Filterable alert queue
- Customer and transaction history
- Rules and anomaly explanations
- Analyst review forms

Server-Sent Events or WebSockets will deliver live updates.

### 4.15 Analyst Review

An analyst can classify an alert as:

- Confirmed fraud
- Legitimate transaction
- Needs further investigation

The audit record retains analyst identity, notes, old and new status, and
review time. Existing history is not overwritten.

### 4.16 Feedback and Evaluation

Simulator ground truth and validated analyst labels are compared with system
decisions to calculate:

- True positives and true negatives
- False positives and false negatives
- Precision
- Recall
- F1 score
- Precision-recall AUC
- False-positive rate
- Fraud amount detected
- Legitimate amount incorrectly held

Analyst feedback does not trigger automatic retraining in the first MVP.

### 4.17 Monitoring and Audit

Operational monitoring covers:

- Events produced and consumed
- Kafka backlog and dead-letter events
- Scoring and API latency
- Database errors
- Model failures
- Throughput
- Rule-trigger frequency

Every decision retains the event ID, event and processing times, ruleset and
model versions, triggered reasons, score, decision, and analyst changes.

## 5. Complete Example

```text
1. The simulator creates a INR 45,000 transaction.
2. Schema validation accepts the transaction.
3. The producer publishes it to transactions.raw.
4. The scoring worker consumes the event.
5. The feature calculator loads customer history.
6. It discovers a new device, unusual amount, and high velocity.
7. Rules contribute 55 points.
8. The anomaly model contributes 24 points.
9. The final score is 79.
10. The decision engine classifies it as high risk.
11. The decision and explanation are stored in PostgreSQL.
12. A scored event and fraud alert are published.
13. The dashboard displays the alert.
14. An analyst investigates and confirms fraud.
15. Evaluation metrics are updated.
16. The complete audit history remains available.
```

## 6. Testing Strategy

The project uses a test-pyramid approach.

### Unit tests

- Simulator determinism and scenario behaviour
- Schema boundaries
- Rules and score caps
- Category thresholds
- Explanation generation
- Model-output normalization

### Integration tests

- Kafka production and consumption
- Dead-letter routing
- Database persistence and uniqueness
- API and scoring integration
- Consumer restart and event redelivery

### End-to-end tests

- Generate a scenario
- Process it through Kafka
- Score and store the transaction
- Display the alert
- Submit an analyst review
- Verify updated metrics and audit history

### ML tests

- Feature schema and range validation
- Time-based and customer-separated evaluation
- Leakage prevention
- Precision, recall, F1, PR-AUC, and false-positive reporting
- Deterministic inference
- Model-version recording

### Performance targets for the 8 GB development machine

| Measurement | Initial target |
| --- | ---: |
| Sustained input | 10 transactions/second |
| Direct scoring API p95 | Below 150 ms |
| Kafka-to-dashboard p95 | Below 1 second |
| Duplicate decisions | 0 |
| Lost acknowledged events | 0 |

These are development targets using synthetic traffic, not production banking
performance claims.

## 7. Phase 2

After the first MVP is stable, the architecture may add:

- Apache Flink for stateful stream features
- Feast for feature definitions and training-serving consistency
- Redis for low-latency online feature retrieval
- XGBoost with labelled data
- SHAP explanations
- Prometheus and Grafana
- Model-drift monitoring
- Multi-broker Kafka
- Authentication and role-based access control
- Kubernetes only when deployment scale justifies it
