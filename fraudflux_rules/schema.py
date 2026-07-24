"""Strict configuration contract for fraud rulesets."""

from __future__ import annotations

from typing import Annotated, Any, Literal, Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)
from fraudflux_worker import RecommendedAction


RuleIdentifier = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        max_length=80,
        pattern=r"^[A-Z][A-Z0-9_]*$",
    ),
]
GroupIdentifier = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        max_length=80,
        pattern=r"^[a-z][a-z0-9_]*$",
    ),
]
Version = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        max_length=40,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$",
    ),
]
FieldPath = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        max_length=160,
        pattern=r"^[a-z][a-z0-9_.]*$",
    ),
]

ComparisonOperator = Literal[
    "eq",
    "ne",
    "gt",
    "gte",
    "lt",
    "lte",
    "in",
    "not_in",
    "truthy",
    "falsy",
]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Condition(StrictModel):
    field: FieldPath
    operator: ComparisonOperator
    value: Any = None

    @model_validator(mode="after")
    def validate_operator_value(self) -> "Condition":
        supplied = "value" in self.model_fields_set
        if self.operator in {"truthy", "falsy"} and supplied:
            raise ValueError(
                f"operator {self.operator!r} must not define a value"
            )
        if self.operator not in {"truthy", "falsy"} and not supplied:
            raise ValueError(
                f"operator {self.operator!r} requires a value"
            )
        if self.operator in {"in", "not_in"} and (
            not isinstance(self.value, (list, tuple, set, frozenset))
            or isinstance(self.value, (str, bytes))
        ):
            raise ValueError(
                f"operator {self.operator!r} requires a list-like value"
            )
        return self


class ConditionSet(StrictModel):
    all: tuple[Condition, ...] = ()
    any: tuple[Condition, ...] = ()

    @model_validator(mode="after")
    def contains_a_condition(self) -> "ConditionSet":
        if not self.all and not self.any:
            raise ValueError("a rule must contain at least one condition")
        return self


class RulePolicy(StrictModel):
    id: RuleIdentifier
    group: GroupIdentifier
    points: int = Field(ge=0, le=70)
    reason: str = Field(min_length=1, max_length=240)
    when: ConditionSet
    override_action: Optional[RecommendedAction] = None
    enabled: bool = True

    @field_validator("reason")
    @classmethod
    def normalize_reason(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("reason cannot be blank")
        return normalized

    @field_validator("override_action")
    @classmethod
    def override_cannot_force_approval(
        cls,
        value: Optional[RecommendedAction],
    ) -> Optional[RecommendedAction]:
        if value == RecommendedAction.APPROVE:
            raise ValueError("a fraud rule cannot override a decision to approve")
        return value


class Ruleset(StrictModel):
    version: Version
    max_contribution: int = Field(default=70, ge=1, le=70)
    rules: tuple[RulePolicy, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def rule_ids_are_unique(self) -> "Ruleset":
        identifiers = [rule.id for rule in self.rules]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("rule IDs must be unique")
        return self
