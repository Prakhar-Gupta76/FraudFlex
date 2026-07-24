"""Command-line interface for the FraudFlux transaction simulator."""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Sequence, TextIO

from .generator import SCENARIOS, TransactionSimulator


def _timezone_aware_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "start time must be a valid ISO-8601 datetime"
        ) from exc
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError(
            "start time must include a timezone, such as +00:00 or Z"
        )
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate synthetic payment transaction events as JSON Lines."
    )
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--rate", type=float, default=5.0)
    parser.add_argument("--scenario", choices=SCENARIOS, default="mixed")
    parser.add_argument("--fraud-rate", type=float, default=0.10)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--start-time", type=_timezone_aware_datetime)
    parser.add_argument(
        "--include-ground-truth",
        action="store_true",
        help="Include simulator-only labels for evaluation output.",
    )
    parser.add_argument(
        "--realtime",
        action="store_true",
        help="Pace output using --rate instead of generating immediately.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write JSON Lines to this file instead of standard output.",
    )
    return parser


def run(args: argparse.Namespace, stream: TextIO) -> int:
    simulator = TransactionSimulator(seed=args.seed, start_time=args.start_time)
    events = simulator.generate(
        count=args.count,
        scenario=args.scenario,
        rate=args.rate,
        fraud_rate=args.fraud_rate,
    )

    delay = 1 / args.rate
    for index, generated in enumerate(events):
        record = (
            generated.evaluation_record()
            if args.include_ground_truth
            else generated.public_event()
        )
        stream.write(json.dumps(record, separators=(",", ":")) + "\n")
        stream.flush()
        if args.realtime and index + 1 < args.count:
            time.sleep(delay)
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.count < 1:
        parser.error("--count must be at least 1")
    if args.rate <= 0:
        parser.error("--rate must be greater than 0")
    if not 0 <= args.fraud_rate <= 1:
        parser.error("--fraud-rate must be between 0 and 1")

    if args.output is None:
        return run(args, sys.stdout)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="\n") as stream:
        return run(args, stream)

