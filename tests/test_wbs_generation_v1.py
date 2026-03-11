from pathlib import Path
import unittest

from estimations import build_brownfield_wbs_from_files, load_artifact_json


FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "estimation" / "v1"


class BrownfieldWBSGenerationTest(unittest.TestCase):
    def test_fixture_wbs_matches_expected_output(self):
        generated = build_brownfield_wbs_from_files(
            str(FIXTURE_ROOT / "input" / "chunk_manifest.json"),
            str(FIXTURE_ROOT / "input" / "risk_register.json"),
            str(FIXTURE_ROOT / "input" / "traceability_scores.json"),
        )
        expected = load_artifact_json(FIXTURE_ROOT / "expected" / "wbs.json")
        self.assertEqual(generated, expected)


if __name__ == "__main__":
    unittest.main()
