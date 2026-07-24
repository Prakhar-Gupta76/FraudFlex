# FraudFlux rulesets

`mvp-v1.yaml` is the default versioned fraud policy. Every rule defines:

- A unique uppercase `id`
- A lowercase `group`
- Points from 0 to 70
- A human-readable `reason`
- An `all`, `any`, or combined condition set
- An optional safe decision override

Rules in the same group are alternatives. Only the matching rule with the
highest points is included. Rules in separate groups can combine until the
rules contribution reaches its configured cap.

Supported operators:

```text
eq ne gt gte lt lte in not_in truthy falsy
```

Bare field names refer to calculated features. Prefixes can explicitly select
another source:

```yaml
- field: amount_to_normal_ratio
  operator: gte
  value: 5

- field: transaction.payment_channel
  operator: in
  value: [card, wallet]
```

Missing fields and invalid comparisons fail the event instead of silently
turning a fraud policy off. Ruleset changes should use a new version and be
evaluated against labelled replay data before deployment.
