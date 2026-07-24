# FraudFlux risk-score combiner

The initial policy is intentionally small and auditable:

```text
rules contribution:  0-70
anomaly contribution: 0-30
final score = min(100, rules + anomaly)
```

Use it through:

```python
from fraudflux_risk import InitialRiskScoreCombiner

combined = InitialRiskScoreCombiner().combine(rules, anomaly)
```

`combined` retains each contribution, the uncapped sum, final score,
`risk-combiner-1.0.0` policy version and any safe review override.

Overrides never force approval and do not modify the numeric score. The
decision engine must interpret them as a minimum action: hold is stronger than
additional verification.

Changing weights, caps or override behavior requires a new policy version and
replay evaluation against labelled transactions. Historical decisions must
retain the policy version used at scoring time.
