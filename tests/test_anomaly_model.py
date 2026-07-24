from __future__ import annotations

import io
import json
import math
import random
import tempfile
import unittest
from contextlib import redirect_stdout
from dataclasses import replace
from pathlib import Path
from typing import Any

from fraudflux_anomaly import (
    FeatureVectorizer,
    IsolationForestAnomalyModel,
    IsolationForestTrainer,
    ScoreCalibration,
    TrainerConfig,
    load_artifact,
    save_artifact,
)
from fraudflux_anomaly.cli import main
from fraudflux_anomaly.features import FeatureVectorError
from fraudflux_worker import AnomalyEvaluation, FeatureSet


def normal_records(count: int = 80) -> list[dict[str, Any]]:
    randomizer = random.Random(1201)
    records = []
    for _ in range(count):
        transactions_hour = randomizer.randint(0, 5)
        records.append(
            {
                "amount_history_count": randomizer.randint(10, 200),
                "amount_to_normal_ratio": max(
                    0.2, randomizer.gauss(1.0, 0.15)
                ),
                "amount_deviation_from_normal": abs(
                    randomizer.gauss(0.7, 0.35)
                ),
                "transactions_previous_2m": randomizer.choice(
                    [0, 0, 0, 1]
                ),
                "transactions_previous_1h": transactions_hour,
                "recent_merchant_count_1h": min(
                    transactions_hour, randomizer.randint(0, 3)
                ),
                "device_is_new": randomizer.random() < 0.03,
                "device_account_count": 1,
                "device_first_seen_known": True,
                "device_age_seconds": randomizer.randint(
                    86_400, 31_536_000
                ),
                "device_deny_listed": False,
                "previous_location_known": True,
                "distance_from_previous_km": abs(
                    randomizer.gauss(5, 4)
                ),
                "seconds_since_previous_transaction": randomizer.randint(
                    600, 172_800
                ),
                "travel_speed_kmh": abs(randomizer.gauss(25, 20)),
                "impossible_travel": False,
                "unusual_country": False,
                "unusual_region": randomizer.random() < 0.04,
                "merchant_category_rarity": randomizer.uniform(0, 0.55),
                "merchant_is_new": randomizer.random() < 0.08,
                "merchant_fraud_rate": randomizer.uniform(0, 0.03),
                "recent_authentication_failures_10m": randomizer.choice(
                    [0, 0, 0, 0, 1]
                ),
                "authentication_failures_then_success": False,
            }
        )
    return records


def obvious_outlier() -> dict[str, Any]:
    record = dict(normal_records(1)[0])
    record.update(
        {
            "amount_to_normal_ratio": 100,
            "amount_deviation_from_normal": 80,
            "transactions_previous_2m": 50,
            "transactions_previous_1h": 100,
            "recent_merchant_count_1h": 40,
            "device_is_new": True,
            "device_account_count": 25,
            "device_deny_listed": True,
            "distance_from_previous_km": 15_000,
            "seconds_since_previous_transaction": 60,
            "travel_speed_kmh": 900_000,
            "impossible_travel": True,
            "unusual_country": True,
            "unusual_region": True,
            "merchant_category_rarity": 1.0,
            "merchant_is_new": True,
            "merchant_fraud_rate": 0.9,
            "recent_authentication_failures_10m": 20,
            "authentication_failures_then_success": True,
        }
    )
    return record


def trainer() -> IsolationForestTrainer:
    return IsolationForestTrainer(
        TrainerConfig(
            n_estimators=40,
            min_training_samples=20,
            random_state=77,
        )
    )


class FeatureVectorTests(unittest.TestCase):
    def test_vectorizer_rejects_missing_nonfinite_and_negative_values(
        self,
    ) -> None:
        vectorizer = FeatureVectorizer()
        complete = normal_records(1)[0]

        missing = dict(complete)
        del missing["amount_to_normal_ratio"]
        with self.assertRaisesRegex(FeatureVectorError, "missing"):
            vectorizer.vectorize(missing)

        nonfinite = dict(complete)
        nonfinite["amount_to_normal_ratio"] = math.inf
        with self.assertRaisesRegex(FeatureVectorError, "finite"):
            vectorizer.vectorize(nonfinite)

        negative = dict(complete)
        negative["transactions_previous_1h"] = -1
        with self.assertRaisesRegex(FeatureVectorError, "negative"):
            vectorizer.vectorize(negative)

    def test_log_transforms_are_deterministic(self) -> None:
        vectorizer = FeatureVectorizer()
        record = normal_records(1)[0]

        first = vectorizer.vectorize(record)
        second = vectorizer.vectorize(record)

        self.assertEqual(first.tolist(), second.tolist())
        ratio_index = vectorizer.names.index("amount_to_normal_ratio")
        self.assertAlmostEqual(
            math.log1p(record["amount_to_normal_ratio"]),
            first[ratio_index],
        )


class ScoreCalibrationTests(unittest.TestCase):
    def test_score_ranges_map_to_documented_contribution_bands(self) -> None:
        calibration = ScoreCalibration(0, 1, 2, 3, 4)

        self.assertEqual(0, calibration.contribution(0))
        self.assertIn(calibration.contribution(0.5), range(1, 6))
        self.assertIn(calibration.contribution(1.5), range(6, 11))
        self.assertIn(calibration.contribution(2.5), range(11, 21))
        self.assertIn(calibration.contribution(3.5), range(21, 31))
        self.assertEqual(30, calibration.contribution(5))

    def test_calibration_requires_ordered_finite_anchors(self) -> None:
        with self.assertRaisesRegex(ValueError, "ordered"):
            ScoreCalibration(0, 2, 1, 3, 4)
        with self.assertRaisesRegex(ValueError, "finite"):
            ScoreCalibration(0, 1, 2, 3, math.inf)


class IsolationForestModelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.records = normal_records()
        cls.artifact = trainer().train(
            cls.records,
            model_version="iforest-test-1",
        )

    def test_training_records_are_mostly_normal_or_slight(self) -> None:
        model = IsolationForestAnomalyModel(self.artifact)
        contributions = sorted(
            model.evaluate(FeatureSet(record)).contribution
            for record in self.records
        )

        self.assertLessEqual(
            contributions[len(contributions) // 2],
            5,
        )
        self.assertLessEqual(sum(score > 10 for score in contributions), 3)

    def test_obvious_outlier_is_highly_unusual_and_explainable(self) -> None:
        times = iter((1_000_000_000, 1_003_500_000))
        model = IsolationForestAnomalyModel(
            self.artifact,
            timer_ns=lambda: next(times),
        )

        result = model.evaluate(FeatureSet(obvious_outlier()))

        self.assertGreaterEqual(result.contribution, 21)
        self.assertLessEqual(result.contribution, 30)
        self.assertEqual("highly_unusual", result.level)
        self.assertEqual("iforest-test-1", result.model_version)
        self.assertTrue(math.isfinite(result.raw_score))
        self.assertEqual(3.5, result.inference_time_ms)
        self.assertGreaterEqual(len(result.deviations), 1)
        self.assertLessEqual(len(result.deviations), 3)
        self.assertTrue(
            any("normal (z=" in item for item in result.deviations)
        )

    def test_same_seed_produces_same_scores(self) -> None:
        second_artifact = trainer().train(
            self.records,
            model_version="iforest-test-2",
        )
        first = IsolationForestAnomalyModel(self.artifact).evaluate(
            FeatureSet(obvious_outlier())
        )
        second = IsolationForestAnomalyModel(second_artifact).evaluate(
            FeatureSet(obvious_outlier())
        )

        self.assertEqual(first.raw_score, second.raw_score)
        self.assertEqual(first.contribution, second.contribution)
        self.assertEqual(first.deviations, second.deviations)

    def test_artifact_round_trip_preserves_inference(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "model.joblib"
            save_artifact(self.artifact, path)
            loaded = load_artifact(path)

            before = IsolationForestAnomalyModel(self.artifact).evaluate(
                FeatureSet(obvious_outlier())
            )
            after = IsolationForestAnomalyModel(loaded).evaluate(
                FeatureSet(obvious_outlier())
            )

        self.assertEqual(before.raw_score, after.raw_score)
        self.assertEqual(before.contribution, after.contribution)
        self.assertEqual(
            self.artifact.feature_transforms,
            loaded.feature_transforms,
        )

    def test_training_rejects_too_few_records(self) -> None:
        with self.assertRaisesRegex(ValueError, "insufficient"):
            trainer().train(
                normal_records(5),
                model_version="too-small",
            )

    def test_incompatible_library_or_feature_schema_is_rejected(self) -> None:
        wrong_library = replace(
            self.artifact,
            sklearn_version="0.0",
        )
        with self.assertRaisesRegex(ValueError, "scikit-learn"):
            IsolationForestAnomalyModel(wrong_library)

        wrong_features = replace(
            self.artifact,
            feature_names=("wrong_feature",)
            + self.artifact.feature_names[1:],
        )
        with self.assertRaisesRegex(ValueError, "feature definitions"):
            IsolationForestAnomalyModel(wrong_features)


class AnomalyEvaluationContractTests(unittest.TestCase):
    def test_levels_follow_contribution_bands(self) -> None:
        expected = {
            0: "normal",
            5: "normal",
            6: "slightly_unusual",
            10: "slightly_unusual",
            11: "moderately_unusual",
            20: "moderately_unusual",
            21: "highly_unusual",
            30: "highly_unusual",
        }
        for contribution, level in expected.items():
            with self.subTest(contribution=contribution):
                result = AnomalyEvaluation(
                    contribution,
                    0.0,
                    (),
                    "test-model",
                    1.0,
                )
                self.assertEqual(level, result.level)

    def test_invalid_timing_and_raw_score_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "raw score"):
            AnomalyEvaluation(1, math.nan, (), "model")
        with self.assertRaisesRegex(ValueError, "inference time"):
            AnomalyEvaluation(1, 0.1, (), "model", -1)


class TrainingCliTests(unittest.TestCase):
    def test_cli_trains_a_versioned_artifact_from_json_lines(self) -> None:
        records = normal_records(20)
        with tempfile.TemporaryDirectory() as directory:
            input_path = Path(directory) / "features.jsonl"
            output_path = Path(directory) / "model.joblib"
            input_path.write_text(
                "".join(
                    json.dumps({"features": record}) + "\n"
                    for record in records
                ),
                encoding="utf-8",
            )
            stdout = io.StringIO()

            with redirect_stdout(stdout):
                exit_code = main(
                    [
                        "--input",
                        str(input_path),
                        "--output",
                        str(output_path),
                        "--model-version",
                        "cli-model-1",
                        "--estimators",
                        "20",
                        "--min-samples",
                        "20",
                    ]
                )

            artifact = load_artifact(output_path)

        self.assertEqual(0, exit_code)
        self.assertEqual("cli-model-1", artifact.model_version)
        self.assertEqual(20, artifact.training_samples)
        self.assertIn('"training_samples": 20', stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
