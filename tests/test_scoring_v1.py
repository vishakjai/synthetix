from pathlib import Path
import unittest

from estimations import (
    apply_team_model_to_wbs,
    build_brownfield_wbs_from_files,
    load_artifact_json,
    load_team_model_library,
)


FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "estimation" / "v1"


class EstimationScoringTest(unittest.TestCase):
    def test_human_only_estimate_matches_expected_output(self):
        wbs = build_brownfield_wbs_from_files(
            str(FIXTURE_ROOT / "input" / "chunk_manifest.json"),
            str(FIXTURE_ROOT / "input" / "risk_register.json"),
            str(FIXTURE_ROOT / "input" / "traceability_scores.json"),
        )
        library = load_team_model_library(FIXTURE_ROOT / "calibration" / "team_models.yml")
        generated = apply_team_model_to_wbs(wbs, library, "HUMAN_ONLY")
        expected = load_artifact_json(FIXTURE_ROOT / "expected" / "estimate_summary_human_only.json")
        self.assertEqual(generated, expected)

    def test_human_led_agent_assisted_estimate_matches_expected_output(self):
        wbs = build_brownfield_wbs_from_files(
            str(FIXTURE_ROOT / "input" / "chunk_manifest.json"),
            str(FIXTURE_ROOT / "input" / "risk_register.json"),
            str(FIXTURE_ROOT / "input" / "traceability_scores.json"),
        )
        library = load_team_model_library(FIXTURE_ROOT / "calibration" / "team_models.yml")
        generated = apply_team_model_to_wbs(wbs, library, "HUMAN_LED_AGENT_ASSISTED")
        expected = load_artifact_json(FIXTURE_ROOT / "expected" / "estimate_summary_human_led_agent_assisted.json")
        self.assertEqual(generated, expected)


if __name__ == "__main__":
    unittest.main()
