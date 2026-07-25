"""Train a versioned Isolation Forest artifact from JSON Lines features."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from fraudflux_config import load_environment

from .artifact import save_artifact
from .training import IsolationForestTrainer, TrainerConfig


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Train the FraudFlux Isolation Forest anomaly model"
    )
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--model-version", required=True)
    parser.add_argument("--estimators", type=int, default=100)
    parser.add_argument("--min-samples", type=int, default=50)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    load_environment()
    args = build_parser().parse_args(argv)
    records = _read_json_lines(args.input)
    trainer = IsolationForestTrainer(
        TrainerConfig(
            n_estimators=args.estimators,
            min_training_samples=args.min_samples,
        )
    )
    artifact = trainer.train(records, model_version=args.model_version)
    save_artifact(artifact, args.output)
    print(
        json.dumps(
            {
                "artifact": str(args.output),
                "model_version": artifact.model_version,
                "feature_schema_version": artifact.feature_schema_version,
                "training_samples": artifact.training_samples,
            },
            sort_keys=True,
        )
    )
    return 0


def _read_json_lines(path: Path) -> list[Mapping[str, Any]]:
    records = []
    with path.open("r", encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"{path}:{line_number} is not valid JSON"
                ) from exc
            if not isinstance(payload, Mapping):
                raise ValueError(
                    f"{path}:{line_number} must contain a JSON object"
                )
            feature_values = payload.get("features", payload)
            if not isinstance(feature_values, Mapping):
                raise ValueError(
                    f"{path}:{line_number} features must be an object"
                )
            records.append(feature_values)
    return records


if __name__ == "__main__":
    raise SystemExit(main())
