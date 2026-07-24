# FraudFlux decision and explanation engine

The initial decision policy maps numeric scores as follows:

| Score | Score category | Action |
| ---: | --- | --- |
| 0-39 | Low | Approve |
| 40-69 | Medium | Additional verification |
| 70-100 | High | Temporary hold and analyst alert |

Use it after Component 9:

```python
from fraudflux_decision import InitialDecisionEngine

decision = InitialDecisionEngine().decide(
    combined_score,
    rule_evaluation,
    anomaly_evaluation,
    upstream_processing_latency_ms=latency_so_far,
)
```

The result distinguishes the numeric `score_category` from the effective
`category`. A safe override may increase the effective category and action,
but it cannot reduce a score-required action.

Explanations describe scores, triggered conditions, model deviations and
configured actions. They do not claim that fraud or other misconduct occurred.
All holds and approvals are simulated MVP decisions.
