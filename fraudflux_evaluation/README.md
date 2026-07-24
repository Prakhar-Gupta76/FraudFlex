# FraudFlux feedback and evaluation

This package evaluates completed decisions offline. It does not import the
training pipeline, update a model artifact, or trigger automatic retraining.

By default, medium- and high-risk decisions count as positive fraud signals.
This policy is versioned and configurable. Monetary values remain in minor
currency units so evaluation does not introduce floating-point rounding. One
report should contain transactions in one currency; generate separate reports
when comparing multiple currencies.

## Inputs

The decisions file contains `transaction.scored` JSON events from
`transactions.scored`.

Simulator labels come from the simulator's evaluation records, which retain
the private `ground_truth` object:

```json
{"transaction":{"transaction_id":"TXN-1","amount_minor":5000},"ground_truth":{"is_fraud":true,"scenario":"account_takeover"}}
```

Final analyst-label records use:

```json
{"transaction_id":"TXN-1","amount_minor":5000,"review_id":"REVIEW-1","outcome":"confirmed_fraud"}
```

`needs_further_investigation` is deliberately excluded because it is not a
validated final label. When both sources label one transaction, the validated
analyst outcome takes precedence without double-counting the transaction.

## Run

```powershell
fraudflux-evaluate `
  --decisions scored.jsonl `
  --ground-truth simulator-evaluation.jsonl `
  --analyst-labels analyst-labels.jsonl `
  --output evaluation-report.json
```

The report includes the confusion matrix, precision, recall, F1, step-wise
precision-recall area (average precision), false-positive rate, fraud amount
detected, legitimate amount incorrectly held, and label-source counts.
