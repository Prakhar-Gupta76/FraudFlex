# FraudFlux

**Real-Time Payment Risk Intelligence Platform**

## About the Project

FraudFlux is a real-time payment fraud detection system designed to evaluate
each simulated transaction before a payment decision is made. It combines
configurable fraud rules with machine-learning-based anomaly detection to
produce a risk score, risk category, recommended action, and human-readable
explanation.

The project demonstrates how banks and payment applications can detect
suspicious behaviour at transaction-stream speed while reducing unnecessary
blocks on legitimate customers.

FraudFlux is an educational and portfolio project. Its first version processes
synthetic transactions only and must not be treated as a production banking or
payment-security system.

## Project Aim

FraudFlux aims to:

- Simulate a continuous stream of normal and fraudulent payment transactions.
- Evaluate transactions using customer, device, location, merchant, amount,
  authentication, and transaction-frequency signals.
- Combine deterministic fraud rules with an anomaly-detection model.
- Assign every transaction a risk score from `0` to `100`.
- Categorise transactions as low, medium, or high risk.
- Recommend approval, additional verification, or a temporary hold.
- Explain the signals that contributed to suspicious decisions.
- Provide analysts with a dashboard for monitoring and reviewing alerts.
- Measure detection quality, false positives, throughput, and scoring latency.

## First MVP Scope

The first MVP will contain:

- A synthetic transaction-stream simulator.
- A single Kafka broker for transaction events.
- A Python fraud-scoring worker.
- A configurable rules engine.
- A machine-learning anomaly detector.
- A risk-score API.
- PostgreSQL storage for transactions, alerts, and analyst decisions.
- A web dashboard for live monitoring and alert investigation.

Apache Flink, Feast, Redis, and advanced infrastructure monitoring are planned
as later upgrades and are not required for the first working version.

## Local Configuration

FraudFlux uses one repository-level `.env` file for Python services, command
line tools, Docker Compose, and the React dashboard. Create it once:

```powershell
Copy-Item .env.example .env
```

The checked-in example contains safe local-development defaults. Local Kafka
uses `PLAINTEXT` and requires no username, password, or API key. PostgreSQL uses
the local Docker credentials defined in the same file. The only value that
must point to a generated file before starting the API is:

```env
FRAUDFLUX_MODEL_ARTIFACT=models/isolation-forest-v1.joblib
```

Existing process environment variables take precedence over `.env`. An
alternative file can be selected with `FRAUDFLUX_ENV_FILE`. `.env` is ignored
by Git; `.env.example` is the safe template that should be committed.

For a managed Kafka service, replace the broker and security values supplied
by that provider:

```env
FRAUDFLUX_KAFKA_BOOTSTRAP_SERVERS=provider-host:9092
FRAUDFLUX_KAFKA_SECURITY_PROTOCOL=SASL_SSL
FRAUDFLUX_KAFKA_SASL_MECHANISM=PLAIN
FRAUDFLUX_KAFKA_USERNAME=provider-api-key
FRAUDFLUX_KAFKA_PASSWORD=provider-api-secret
```

Incomplete SASL configuration is rejected before a Kafka client starts.

## Local Startup

Install the Python project and start the local infrastructure:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e .
docker compose up -d
fraudflux-kafka-check
```

After training the model artifact configured in `.env`, start the API without
setting terminal environment variables:

```powershell
uvicorn fraudflux_api.runtime:create_runtime_app --factory --reload
```

Start the dashboard in a second terminal:

```powershell
cd dashboard
npm install
npm run dev
```

The dashboard reads the repository-level `.env` through Vite.

## Transaction Information Used

Each simulated transaction may include:

- Transaction ID and timestamp
- Customer and account ID
- Amount and currency
- Merchant and merchant category
- Payment channel
- Device ID and device trust status
- IP address and approximate location
- Authentication result
- Recent failed attempts
- Recent transaction frequency
- Customer's normal spending range
- Customer's known devices and usual locations

Sensitive or protected personal characteristics must not be used to determine
risk.

## Risk-Scoring Method

FraudFlux calculates the final score from two components:

```text
Final Risk Score = Rules Score + ML Anomaly Contribution
```

- The rules engine can contribute up to `70` points.
- The anomaly model can contribute up to `30` points.
- The final score is capped at `100`.
- Only the strongest rule in the same rule group is counted. For example, an
  amount cannot receive both the `3x` and `5x` amount penalties.

These are initial MVP settings. They must later be calibrated using evaluation
results and the business cost of missed fraud versus false positives.

## Initial Fraud Rules

### Amount Rules

The transaction amount is compared with the customer's normal spending
behaviour.

| Condition | Score |
| --- | ---: |
| Amount is at least 3 times the customer's normal amount | +10 |
| Amount is at least 5 times the customer's normal amount | +20 |
| Amount is at least 10 times the customer's normal amount | +30 |

Only the highest matching amount rule is applied.

### Transaction-Frequency Rules

| Condition | Score |
| --- | ---: |
| 3 to 5 transactions within 2 minutes | +10 |
| More than 5 transactions within 2 minutes | +20 |
| Repeated small payments across several merchants | +15 |

Only the highest matching general frequency rule is applied. A separate
card-testing rule may also apply when the transaction pattern supports it.

### Device Rules

| Condition | Score |
| --- | ---: |
| Transaction uses a new device | +15 |
| Device is shared by several unrelated customer accounts | +20 |
| Device is already present on an internal deny list | +40 |

Only the highest matching device rule is applied.

### Location Rules

| Condition | Score |
| --- | ---: |
| Transaction originates outside the customer's usual region | +10 |
| Transaction originates from an unusual country | +15 |
| Impossible travel from the previous transaction is detected | +30 |

Impossible travel means that the time between two distant transactions is too
short for realistic physical travel. Only the highest matching location rule
is applied.

### Authentication Rules

| Condition | Score |
| --- | ---: |
| 3 or more recent failed authentication attempts | +10 |
| 5 or more failed attempts followed by a successful payment | +20 |
| Authentication or device credentials are known to be compromised | +40 |

Only the highest matching authentication rule is applied.

### Merchant and Behaviour Rules

| Condition | Score |
| --- | ---: |
| Merchant category is unusual for the customer | +10 |
| Payment occurs at an unusual hour for the customer | +5 |
| Dormant account suddenly makes a high-value payment | +15 |
| Merchant has an abnormal recent fraud or dispute rate | +15 |

These rules may be combined when they represent different signals.

## ML Anomaly Contribution

The anomaly detector evaluates whether the complete transaction differs from
the customer's normal behaviour. It may consider:

- Amount deviation from the customer's average.
- Transaction count in recent time windows.
- Distance from usual locations.
- Time since the previous transaction.
- Whether the device is new.
- Merchant-category rarity.
- Time-of-day deviation.
- Recent authentication failures.

The model output is normalised to a contribution between `0` and `30` points.
An anomaly score does not prove fraud; it is supporting evidence used alongside
the rules.

## Risk Categories and Decisions

| Final Score | Risk Category | Recommended MVP Decision |
| ---: | --- | --- |
| 0-39 | Low | Approve |
| 40-69 | Medium | Request additional verification |
| 70-100 | High | Temporarily hold and create an analyst alert |

The high-risk category does not represent a final legal determination of
fraud. In the MVP, "hold" and "block" are simulated statuses only.

## Decision Overrides

Some high-confidence signals may override the ordinary threshold:

- A device or credential on a confirmed deny list produces a high-risk alert.
- A confirmed compromised account produces a high-risk alert.
- Missing or invalid essential transaction data prevents automatic approval
  and sends the transaction for review.
- A service or model failure must not silently classify a transaction as safe;
  it produces an explicit system-review status.

Overrides must be recorded in the explanation and audit history.

## Explainable Alerts

Every medium- or high-risk result should contain:

- Final risk score and risk category.
- Recommended decision.
- Rules that were triggered.
- Points contributed by each rule.
- ML anomaly contribution.
- Important behavioural deviations.
- Model and ruleset versions.
- Transaction-processing time.

Example:

```text
Risk score: 82/100
Category: High
Recommended action: Hold for review

Reasons:
- New device: +15
- Amount is 6.4 times the customer's normal amount: +20
- Impossible travel detected: +30
- ML anomaly contribution: +17
```

## Analyst Review

An analyst can label an alert as:

- Confirmed fraud
- Legitimate transaction
- Needs further investigation

The analyst's label, notes, identity, and review time should be retained as an
audit record. Analyst feedback may later become labelled training data, but it
must be validated before model retraining.

## Evaluation Rules

The project must not use accuracy as its only success metric because fraudulent
transactions are rare. The MVP should report:

- Precision
- Recall
- F1 score
- Precision-recall AUC
- False-positive rate
- Fraud amount detected
- Legitimate amount incorrectly held
- Transactions processed per second
- Median and p95 scoring latency

Results produced using synthetic data must be clearly labelled as simulation
results and must not be presented as real-world banking performance.

The implemented offline command accepts scored-event JSONL plus simulator
ground-truth and/or final analyst-label JSONL:

```powershell
fraudflux-evaluate `
  --decisions scored.jsonl `
  --ground-truth simulator-evaluation.jsonl `
  --analyst-labels analyst-labels.jsonl `
  --output evaluation-report.json
```

Medium- and high-risk decisions count as positive signals by default. Final
analyst labels take precedence over simulator labels, while
`needs_further_investigation` is excluded. Amount metrics use minor currency
units; one evaluation report should contain transactions in one currency.
This workflow calculates metrics only and never triggers model retraining.

## Operational Monitoring and Audit

The final MVP component records event production and consumption, dead-letter
volume, consumer lag, scoring/API latency, database and model failures,
throughput, and rule-trigger frequency. The API exposes these process metrics
at `GET /metrics` in Prometheus-compatible text and includes monitoring in
`GET /health`.

PostgreSQL is the durable audit authority. Decisions retain their original
event, event and processing times, rule/model/policy versions, triggered
reasons, score, action, and explanation. Assignments and analyst reviews are
append-only, preserving the actor, notes, status transition, and review time.
Operational metrics never include customer or transaction IDs as labels.

## Project Principles

- Every risk decision must be explainable and auditable.
- Rules and thresholds must be configurable and versioned.
- The same event must not create duplicate payment decisions.
- Event timestamps must be distinguished from processing timestamps.
- Missing data and system failures must be visible, not silently ignored.
- Sensitive data must not be written to ordinary application logs.
- Protected personal attributes must never be used as fraud signals.
- Analyst feedback must not be used for training without validation.
- Model performance and false positives must be monitored over time.
- Human review remains available for ambiguous and high-impact decisions.

## Disclaimer

FraudFlux is intended for learning, demonstration, and system-design practice.
Its simulated decisions must not be used to approve, decline, or block real
financial transactions without professional security review, legal review,
proper data governance, extensive validation, and regulatory compliance.
