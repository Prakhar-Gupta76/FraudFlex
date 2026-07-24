# FraudFlux anomaly model

This package trains and serves the MVP Isolation Forest model.

Training input is JSON Lines containing either a feature object:

```json
{"amount_history_count": 42, "...": "..."}
```

or a nested object:

```json
{"features": {"amount_history_count": 42, "...": "..."}}
```

Train an artifact:

```powershell
fraudflux-train-anomaly `
  --input data/normal-features.jsonl `
  --output models/isolation-forest-v1.joblib `
  --model-version iforest-1.0.0
```

The input should contain reviewed normal transactions created with the same
point-in-time feature schema used for online scoring. Ground-truth labels and
simulator-only fields must not be included as model features.

Artifacts include the estimator, calibration, feature schema, training
statistics, model version and scikit-learn version. Only load artifacts from
the trusted FraudFlux training pipeline: joblib is not a safe format for
untrusted files.
