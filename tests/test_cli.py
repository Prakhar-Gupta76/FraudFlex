from __future__ import annotations

import io
import json
import unittest

from fraudflux_simulator.cli import build_parser, run


class SimulatorCliTests(unittest.TestCase):
    def test_cli_writes_requested_number_of_json_lines(self) -> None:
        args = build_parser().parse_args(
            [
                "--count",
                "3",
                "--scenario",
                "normal",
                "--seed",
                "5",
            ]
        )
        output = io.StringIO()

        exit_code = run(args, output)
        records = [json.loads(line) for line in output.getvalue().splitlines()]

        self.assertEqual(0, exit_code)
        self.assertEqual(3, len(records))
        self.assertTrue(all("ground_truth" not in record for record in records))

    def test_cli_can_include_ground_truth_for_evaluation(self) -> None:
        args = build_parser().parse_args(
            [
                "--count",
                "1",
                "--scenario",
                "account_takeover",
                "--include-ground-truth",
            ]
        )
        output = io.StringIO()

        run(args, output)
        record = json.loads(output.getvalue())

        self.assertTrue(record["ground_truth"]["is_fraud"])
        self.assertEqual(
            "account_takeover", record["ground_truth"]["scenario"]
        )


if __name__ == "__main__":
    unittest.main()

