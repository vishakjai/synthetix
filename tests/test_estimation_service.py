from pathlib import Path
import shutil
import tempfile
import unittest

from estimations import EstimationStore, load_artifact_json
from estimations.service import DEFAULT_TEAM_MODEL_LIBRARY_PATH, build_brownfield_estimate


FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "estimation" / "v1"


class EstimationServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path(tempfile.mkdtemp(prefix="synthetix-estimation-service-"))
        self.store = EstimationStore(self.temp_dir)

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_build_brownfield_estimate_persists_schema_valid_artifacts(self):
        result = build_brownfield_estimate(
            chunk_manifest=load_artifact_json(FIXTURE_ROOT / "input" / "chunk_manifest.json"),
            risk_register=load_artifact_json(FIXTURE_ROOT / "input" / "risk_register.json"),
            traceability_scores=load_artifact_json(FIXTURE_ROOT / "input" / "traceability_scores.json"),
            business_need="Modernize the brownfield application while preserving required business capability.",
            store=self.store,
            run_id="run_123",
            estimate_id="estimate_test",
            team_model_library_path=DEFAULT_TEAM_MODEL_LIBRARY_PATH,
        )
        self.assertEqual(result.estimate_id, "estimate_test")
        self.assertTrue(result.paths.estimation_input_path.exists())
        self.assertTrue(result.paths.wbs_path.exists())
        self.assertTrue(result.paths.assumption_ledger_path.exists())
        self.assertTrue(result.paths.estimate_summary_path.exists())
        self.assertEqual(
            result.estimate_summary["estimate"]["team_model_selected"],
            "HUMAN_ONLY",
        )
        self.assertEqual(
            result.estimate_summary["estimate"]["effort"]["total_hours"]["p50"],
            1423.2,
        )


if __name__ == "__main__":
    unittest.main()
