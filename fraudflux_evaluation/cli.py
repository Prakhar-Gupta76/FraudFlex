"""Command-line evaluation of scored FraudFlux decisions."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from fraudflux_config import load_environment

from .evaluator import FraudDecisionEvaluator
from .loaders import (
    build_analyst_cases,
    build_simulator_cases,
    merge_cases,
)
from .models import EvaluationPolicy


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Evaluate FraudFlux scored decisions against labels."
    )
    parser.add_argument(
        "--decisions",
        required=True,
        help="JSONL transaction.scored events",
    )
    parser.add_argument(
        "--ground-truth",
        help="JSONL simulator evaluation records",
    )
    parser.add_argument(
        "--analyst-labels",
        help="JSONL final analyst labels with transaction amount",
    )
    parser.add_argument("--output", help="Optional JSON report destination")
    parser.add_argument(
        "--positive-category",
        action="append",
        choices=("low", "medium", "high"),
        dest="positive_categories",
        help="Category treated as a positive prediction; repeat as needed",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    load_environment()
    arguments = build_parser().parse_args(argv)
    if not arguments.ground_truth and not arguments.analyst_labels:
        raise SystemExit(
            "provide --ground-truth, --analyst-labels, or both"
        )
    decisions = _read_json_lines(Path(arguments.decisions))
    simulator_cases = (
        build_simulator_cases(
            _read_json_lines(Path(arguments.ground_truth)),
            decisions,
        )
        if arguments.ground_truth
        else ()
    )
    analyst_cases = (
        build_analyst_cases(
            _read_json_lines(Path(arguments.analyst_labels)),
            decisions,
        )
        if arguments.analyst_labels
        else ()
    )
    policy = EvaluationPolicy(
        positive_categories=tuple(
            arguments.positive_categories or ("medium", "high")
        )
    )
    report = FraudDecisionEvaluator(policy).evaluate(
        merge_cases(simulator_cases, analyst_cases)
    )
    serialized = report.model_dump_json(indent=2)
    if arguments.output:
        Path(arguments.output).write_text(
            serialized + "\n",
            encoding="utf-8",
        )
    else:
        print(serialized)
    return 0


def _read_json_lines(path: Path) -> tuple[Mapping[str, Any], ...]:
    records: list[Mapping[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise ValueError(f"cannot read {path}: {exc}") from exc
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"{path}:{line_number} is not valid JSON"
            ) from exc
        if not isinstance(value, Mapping):
            raise ValueError(
                f"{path}:{line_number} must contain a JSON object"
            )
        records.append(value)
    return tuple(records)


if __name__ == "__main__":
    raise SystemExit(main())
