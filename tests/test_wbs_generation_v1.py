from pathlib import Path
import unittest

from estimations import build_brownfield_wbs_from_files


FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "estimation" / "v1"


class BrownfieldWBSGenerationTest(unittest.TestCase):
    def test_fixture_wbs_matches_expected_structure(self):
        generated = build_brownfield_wbs_from_files(
            str(FIXTURE_ROOT / "input" / "chunk_manifest.json"),
            str(FIXTURE_ROOT / "input" / "risk_register.json"),
            str(FIXTURE_ROOT / "input" / "traceability_scores.json"),
        )
        items = list(generated.get("wbs_items") or [])
        self.assertGreaterEqual(len(items), 8)
        kinds = {str(item.get("kind") or "") for item in items}
        self.assertIn("BROWNFIELD_CHUNK", kinds)
        self.assertIn("FOUNDATION", kinds)
        self.assertIn("RISK_REMEDIATION", kinds)
        self.assertTrue(any(str(item.get("id") or "") == "WBS-FOUNDATION-BASELINE" for item in items))
        self.assertTrue(any(str(item.get("id") or "") == "WBS-SHARED-INFRA" for item in items))
        self.assertTrue(any(float(item.get("estimated_hours_likely") or 0.0) > 0 for item in items))


if __name__ == "__main__":
    unittest.main()
