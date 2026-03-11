from pathlib import Path
import unittest

from estimations import BrownfieldIntake, GreenfieldIntake, NaturalLanguageIntake


FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "estimation" / "v1"


class EstimationIntakeTest(unittest.TestCase):
    def test_brownfield_intake_builds_valid_estimation_input(self):
        artifact = BrownfieldIntake(
            chunk_manifest_path=FIXTURE_ROOT / "input" / "chunk_manifest.json",
            risk_register_path=FIXTURE_ROOT / "input" / "risk_register.json",
            traceability_scores_path=FIXTURE_ROOT / "input" / "traceability_scores.json",
        ).build()
        self.assertEqual(artifact.payload["intake"]["mode"], "brownfield")
        self.assertEqual(artifact.payload["intake"]["confidence_tier"], "PLANNING")
        self.assertEqual(len(artifact.payload["inputs"]["source_artifacts"]), 3)
        self.assertEqual(artifact.payload["decisions"]["migration_strategy"], "incremental_modernization")

    def test_greenfield_intake_builds_valid_estimation_input(self):
        artifact = GreenfieldIntake(
            business_need="Launch a new customer onboarding portal.",
            tech_specs=["spec://brd-v1"],
            target_stack=["nextjs", "nestjs", "postgres"],
        ).build()
        self.assertEqual(artifact.payload["intake"]["mode"], "greenfield")
        self.assertEqual(artifact.payload["intake"]["confidence_tier"], "INDICATIVE")
        self.assertEqual(artifact.payload["decisions"]["migration_strategy"], "rewrite")
        self.assertEqual(artifact.payload["inputs"]["tech_specs"], ["spec://brd-v1"])

    def test_natural_language_intake_builds_valid_estimation_input(self):
        artifact = NaturalLanguageIntake(
            business_need="Estimate a modernization of the payments platform.",
            intake_notes=["Assume two-week sprints until staffing is confirmed."],
        ).build()
        self.assertEqual(artifact.payload["intake"]["mode"], "natural_language")
        self.assertEqual(artifact.payload["decisions"]["migration_strategy"], "unknown")
        self.assertEqual(
            artifact.payload["intake"]["intake_notes"],
            ["Assume two-week sprints until staffing is confirmed."],
        )


if __name__ == "__main__":
    unittest.main()
