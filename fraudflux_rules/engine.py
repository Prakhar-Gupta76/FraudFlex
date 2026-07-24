"""Ruleset loading and deterministic fraud-rule evaluation."""

from __future__ import annotations

import operator
from importlib.resources import files
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Optional

import yaml
from pydantic import ValidationError

from fraudflux_validation import TransactionEvent
from fraudflux_worker import (
    CustomerHistory,
    FeatureSet,
    RecommendedAction,
    RuleEvaluation,
    RuleHit,
)

from .schema import Condition, RulePolicy, Ruleset


class RuleEvaluationError(RuntimeError):
    """A configured rule could not be evaluated safely."""


def load_ruleset(path: str | Path) -> Ruleset:
    source = Path(path)
    try:
        payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ValueError(f"cannot read ruleset {source}: {exc}") from exc
    except yaml.YAMLError as exc:
        raise ValueError(f"ruleset {source} is not valid YAML: {exc}") from exc
    return _validate_payload(payload, source=str(source))


def load_default_ruleset() -> Ruleset:
    resource = (
        files("fraudflux_rules")
        .joinpath("rulesets")
        .joinpath("mvp-v1.yaml")
    )
    try:
        payload = yaml.safe_load(resource.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ValueError(f"default ruleset is not valid YAML: {exc}") from exc
    return _validate_payload(payload, source="built-in mvp-v1.yaml")


def _validate_payload(payload: Any, *, source: str) -> Ruleset:
    if not isinstance(payload, Mapping):
        raise ValueError(f"ruleset {source} must contain a YAML mapping")
    try:
        return Ruleset.model_validate(payload)
    except ValidationError as exc:
        raise ValueError(f"ruleset {source} is invalid: {exc}") from exc


class YamlRulesEngine:
    def __init__(self, ruleset: Ruleset) -> None:
        self.ruleset = ruleset

    @classmethod
    def from_path(cls, path: str | Path) -> "YamlRulesEngine":
        return cls(load_ruleset(path))

    @classmethod
    def default(cls) -> "YamlRulesEngine":
        return cls(load_default_ruleset())

    def evaluate(
        self,
        event: TransactionEvent,
        history: CustomerHistory,
        features: FeatureSet,
    ) -> RuleEvaluation:
        context = {
            "event": event,
            "transaction": event.transaction,
            "history": history.values,
            "features": features.values,
        }
        winners: dict[str, tuple[int, RulePolicy]] = {}
        for index, rule in enumerate(self.ruleset.rules):
            if not rule.enabled or not _matches(rule, context):
                continue
            current = winners.get(rule.group)
            if current is None or rule.points > current[1].points:
                winners[rule.group] = (index, rule)

        selected = [
            rule
            for _, rule in sorted(
                winners.values(),
                key=lambda selected_rule: selected_rule[0],
            )
        ]
        hits = tuple(
            RuleHit(rule.id, rule.points, rule.reason)
            for rule in selected
        )
        uncapped = sum(hit.points for hit in hits)
        override = _strongest_override(
            rule.override_action for rule in selected
        )
        return RuleEvaluation(
            contribution=min(uncapped, self.ruleset.max_contribution, 70),
            hits=hits,
            ruleset_version=self.ruleset.version,
            override_action=override,
        )


def _matches(
    rule: RulePolicy,
    context: Mapping[str, Any],
) -> bool:
    all_results = tuple(
        _evaluate_condition(condition, context)
        for condition in rule.when.all
    )
    any_results = tuple(
        _evaluate_condition(condition, context)
        for condition in rule.when.any
    )
    return all(all_results) and (
        any(any_results) if any_results else True
    )


def _evaluate_condition(
    condition: Condition,
    context: Mapping[str, Any],
) -> bool:
    actual = _resolve_field(condition.field, context)
    if condition.operator == "truthy":
        return bool(actual)
    if condition.operator == "falsy":
        return not bool(actual)
    if condition.operator == "in":
        return actual in condition.value
    if condition.operator == "not_in":
        return actual not in condition.value

    comparisons: Mapping[str, Callable[[Any, Any], bool]] = {
        "eq": operator.eq,
        "ne": operator.ne,
        "gt": operator.gt,
        "gte": operator.ge,
        "lt": operator.lt,
        "lte": operator.le,
    }
    try:
        return comparisons[condition.operator](actual, condition.value)
    except (TypeError, ValueError) as exc:
        raise RuleEvaluationError(
            f"rule condition {condition.field} {condition.operator} "
            f"{condition.value!r} cannot compare value {actual!r}"
        ) from exc


def _resolve_field(path: str, context: Mapping[str, Any]) -> Any:
    parts = path.split(".")
    if parts[0] in context:
        current: Any = context[parts[0]]
        parts = parts[1:]
    else:
        current = context["features"]

    for part in parts:
        if isinstance(current, Mapping):
            if part not in current:
                raise RuleEvaluationError(
                    f"configured rule field {path!r} is missing"
                )
            current = current[part]
        else:
            if not hasattr(current, part):
                raise RuleEvaluationError(
                    f"configured rule field {path!r} is missing"
                )
            current = getattr(current, part)
    return current


def _strongest_override(
    actions: Iterable[Optional[RecommendedAction]],
) -> Optional[RecommendedAction]:
    severity = {
        RecommendedAction.APPROVE: 0,
        RecommendedAction.VERIFY: 1,
        RecommendedAction.HOLD: 2,
    }
    present = [action for action in actions if action is not None]
    return max(present, key=severity.__getitem__) if present else None
