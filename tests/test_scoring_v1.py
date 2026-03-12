from pathlib import Path
import unittest

from estimations import (
    apply_team_model_to_wbs,
    build_brownfield_wbs_from_files,
    load_team_model_library,
)


FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "estimation" / "v1"


class EstimationScoringTest(unittest.TestCase):
    def _build(self, model_key: str):
        wbs = build_brownfield_wbs_from_files(
            str(FIXTURE_ROOT / "input" / "chunk_manifest.json"),
            str(FIXTURE_ROOT / "input" / "risk_register.json"),
            str(FIXTURE_ROOT / "input" / "traceability_scores.json"),
        )
        library = load_team_model_library(FIXTURE_ROOT / "calibration" / "team_models.yml")
        return apply_team_model_to_wbs(wbs, library, model_key)

    def test_human_only_estimate_is_consulting_weighted(self):
        generated = self._build("HUMAN_ONLY")
        self.assertGreater(generated["total_hours_likely"], 500)
        self.assertGreater(generated["timeline_weeks_likely"], 7)
        self.assertTrue(generated["architect_required"])
        self.assertTrue(generated["dba_required"])
        self.assertIn("ARCH", generated["hours_by_role"])
        self.assertIn("DBA", generated["hours_by_role"])
        self.assertIn("QA", generated["hours_by_role"])
        kinds = {item["kind"] for item in generated["items"]}
        self.assertIn("FOUNDATION", kinds)
        self.assertIn("RISK_REMEDIATION", kinds)
        self.assertIn("BROWNFIELD_CHUNK", kinds)
        self.assertGreaterEqual(len(generated["items"]), 8)

    def test_human_led_agent_assisted_is_faster_than_human_only(self):
        human = self._build("HUMAN_ONLY")
        assisted = self._build("HUMAN_LED_AGENT_ASSISTED")
        self.assertLess(assisted["total_hours_likely"], human["total_hours_likely"])
        self.assertLessEqual(assisted["timeline_weeks_likely"], human["timeline_weeks_likely"])
        self.assertTrue(assisted["architect_required"])
        self.assertTrue(assisted["dba_required"])


if __name__ == "__main__":
    unittest.main()
