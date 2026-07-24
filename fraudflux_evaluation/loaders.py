"""Join simulator ground truth and analyst labels with scored events."""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence

from fraudflux_events import ScoredTransactionEvent, parse_decision_event

from .models import EvaluationCase, LabelSource


FINAL_ANALYST_OUTCOMES = {
    "confirmed_fraud": True,
    "legitimate": False,
}


def build_simulator_cases(
    ground_truth_records: Iterable[Mapping[str, Any]],
    scored_payloads: Iterable[Mapping[str, Any]],
) -> tuple[EvaluationCase, ...]:
    decisions = _scored_by_transaction(scored_payloads)
    cases: list[EvaluationCase] = []
    for record in ground_truth_records:
        transaction = _mapping(record, "transaction")
        truth = _mapping(record, "ground_truth")
        transaction_id = _required_string(
            transaction,
            "transaction_id",
        )
        decision = _required_decision(decisions, transaction_id)
        is_fraud = truth.get("is_fraud")
        if type(is_fraud) is not bool:
            raise ValueError("ground_truth.is_fraud must be a boolean")
        cases.append(
            _case(
                decision,
                amount_minor=_positive_amount(transaction),
                is_fraud=is_fraud,
                source=LabelSource.SIMULATOR,
                context=str(truth.get("scenario") or "simulator"),
            )
        )
    return tuple(cases)


def build_analyst_cases(
    label_records: Iterable[Mapping[str, Any]],
    scored_payloads: Iterable[Mapping[str, Any]],
) -> tuple[EvaluationCase, ...]:
    decisions = _scored_by_transaction(scored_payloads)
    cases: list[EvaluationCase] = []
    for record in label_records:
        outcome = record.get("outcome")
        if outcome == "needs_further_investigation":
            continue
        if outcome not in FINAL_ANALYST_OUTCOMES:
            raise ValueError(f"unsupported analyst outcome {outcome!r}")
        transaction_id = _required_string(record, "transaction_id")
        cases.append(
            _case(
                _required_decision(decisions, transaction_id),
                amount_minor=_positive_amount(record),
                is_fraud=FINAL_ANALYST_OUTCOMES[outcome],
                source=LabelSource.ANALYST,
                context=str(record.get("review_id") or "analyst-review"),
            )
        )
    return tuple(cases)


def merge_cases(
    simulator_cases: Sequence[EvaluationCase],
    analyst_cases: Sequence[EvaluationCase],
) -> tuple[EvaluationCase, ...]:
    """Merge labels by transaction, preferring validated analyst outcomes."""
    _validate_unique_cases(simulator_cases, "simulator")
    _validate_unique_cases(analyst_cases, "analyst")
    merged = {case.transaction_id: case for case in simulator_cases}
    merged.update({case.transaction_id: case for case in analyst_cases})
    return tuple(merged[key] for key in sorted(merged))


def _validate_unique_cases(
    cases: Sequence[EvaluationCase],
    source: str,
) -> None:
    seen: set[str] = set()
    for case in cases:
        if case.transaction_id in seen:
            raise ValueError(
                f"duplicate {source} label for transaction "
                f"{case.transaction_id}"
            )
        seen.add(case.transaction_id)


def _scored_by_transaction(
    payloads: Iterable[Mapping[str, Any]],
) -> Mapping[str, ScoredTransactionEvent]:
    decisions: dict[str, ScoredTransactionEvent] = {}
    for payload in payloads:
        event = parse_decision_event(payload)
        if not isinstance(event, ScoredTransactionEvent):
            raise ValueError("evaluation decisions must be transaction.scored")
        if event.transaction_id in decisions:
            raise ValueError(
                f"duplicate scored transaction {event.transaction_id}"
            )
        decisions[event.transaction_id] = event
    return decisions


def _required_decision(
    decisions: Mapping[str, ScoredTransactionEvent],
    transaction_id: str,
) -> ScoredTransactionEvent:
    try:
        return decisions[transaction_id]
    except KeyError as exc:
        raise ValueError(
            f"no scored decision for transaction {transaction_id}"
        ) from exc


def _case(
    decision: ScoredTransactionEvent,
    *,
    amount_minor: int,
    is_fraud: bool,
    source: LabelSource,
    context: str,
) -> EvaluationCase:
    return EvaluationCase(
        transaction_id=decision.transaction_id,
        amount_minor=amount_minor,
        is_fraud=is_fraud,
        label_source=source,
        final_score=decision.risk.final_score,
        category=decision.risk.category,
        recommended_action=decision.risk.recommended_action,
        label_context=context,
    )


def _mapping(
    value: Mapping[str, Any],
    key: str,
) -> Mapping[str, Any]:
    candidate = value.get(key)
    if not isinstance(candidate, Mapping):
        raise ValueError(f"{key} must be an object")
    return candidate


def _required_string(value: Mapping[str, Any], key: str) -> str:
    candidate = value.get(key)
    if not isinstance(candidate, str) or not candidate.strip():
        raise ValueError(f"{key} must be a non-blank string")
    return candidate


def _positive_amount(value: Mapping[str, Any]) -> int:
    amount = value.get("amount_minor")
    if type(amount) is not int or amount <= 0:
        raise ValueError("amount_minor must be a positive integer")
    return amount
