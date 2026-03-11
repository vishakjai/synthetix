from pathlib import Path
import unittest

from estimations import (
    EstimateSummaryArtifact,
    TeamModelLibraryArtifact,
    WBSArtifact,
    load_artifact_json,
    load_team_model_library,
)


FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "estimation" / "v1"


class EstimationTypesTest(unittest.TestCase):
    @staticmethod
    def _meta(artifact_type: str, artifact_id: str) -> dict:
        return {
            "artifact_type": artifact_type,
            "artifact_version": "1.0",
            "artifact_id": artifact_id,
            "generated_at": "2026-03-10T00:00:00Z",
            "context": {
                "stage": "Estimate",
                "source_mode": "repo",
                "source_ref": "fixture://estimation/v1",
            },
        }

    def test_wbs_fixture_validates_against_schema(self):
        payload = {
            "meta": self._meta("wbs_v1", "art_wbs_fixture"),
            "wbs": {
                "wbs_id": "WBS-FIXTURE",
                "items": [
                    {
                        "wbs_item_id": "WBS-CHUNK_000",
                        "title": "Migrate Admin subsystem",
                        "kind": "MODULE_MIGRATION",
                        "phase": "Build",
                        "size_tier": "L",
                        "effort_hours": {"p10": 403.8, "p50": 475.0, "p90": 593.8},
                        "roles": [
                            {"role": "BA", "hours": {"p10": 60.6, "p50": 71.2, "p90": 89.1}},
                            {"role": "DEV", "hours": {"p10": 222.1, "p50": 261.2, "p90": 326.6}},
                        ],
                        "dependencies": [],
                    }
                ],
            },
        }
        artifact = WBSArtifact(payload)
        artifact.validate()

    def test_estimate_summary_fixture_validates_against_schema(self):
        payload = {
            "meta": self._meta("estimate_summary_v1", "art_estimate_fixture"),
            "estimate": {
                "estimate_id": "EST-1",
                "confidence_tier": "PLANNING",
                "team_model_selected": "HUMAN_ONLY",
                "timeline": {
                    "p10_weeks": 11.3,
                    "p50_weeks": 13.3,
                    "p90_weeks": 16.6,
                    "phase_breakdown": [{"phase": "Build", "p50_weeks": 8.0}],
                },
                "effort": {
                    "total_hours": {"p10": 1172.3, "p50": 1379.2, "p90": 1724.0},
                    "by_role": [{"role": "DEV", "hours": {"p10": 644.8, "p50": 758.6, "p90": 948.3}}],
                },
                "cost": {},
                "key_assumptions": ["Fixture assumption"],
                "blockers": [],
                "artifact_refs": {
                    "estimation_input": {"artifact_type": "estimation_input_v1", "artifact_id": "art_input_1", "artifact_version": "1.0"},
                    "wbs": {"artifact_type": "wbs_v1", "artifact_id": "art_wbs_1", "artifact_version": "1.0"},
                    "assumption_ledger": {"artifact_type": "assumption_ledger_v1", "artifact_id": "art_assumptions_1", "artifact_version": "1.0"},
                    "team_models": {"artifact_type": "team_model_library_v1", "artifact_id": "art_models_1", "artifact_version": "1.0"},
                },
            },
        }
        artifact = EstimateSummaryArtifact(payload)
        artifact.validate()

    def test_team_model_library_loads_fixture(self):
        library = load_team_model_library(FIXTURE_ROOT / "calibration" / "team_models.yml")
        self.assertEqual(library.weekly_capacity_hours, 30.0)
        self.assertIn("HUMAN_ONLY", library.models)
        self.assertIn("HUMAN_LED_AGENT_ASSISTED", library.models)

    def test_team_model_library_fixture_validates_against_schema(self):
        payload = {
            "meta": self._meta("team_model_library_v1", "art_models_fixture"),
            "models": [
                {
                    "model_id": "human-only-fixture",
                    "model_type": "HUMAN_ONLY",
                    "description": "Fully Human",
                    "role_capacity": [
                        {"role": "BA", "headcount": 0.5, "hours_per_week": 30},
                        {"role": "DEV", "headcount": 2, "hours_per_week": 30},
                    ],
                    "task_acceleration": [
                        {"task_type": "ANALYSIS", "acceleration_factor": 1.0},
                        {"task_type": "IMPLEMENTATION", "acceleration_factor": 1.0},
                    ],
                }
            ],
        }
        artifact = TeamModelLibraryArtifact(payload)
        artifact.validate()

    def test_fixture_files_are_present(self):
        self.assertTrue((FIXTURE_ROOT / "input" / "chunk_manifest.json").exists())
        expected = load_artifact_json(FIXTURE_ROOT / "expected" / "estimate_summary_human_only.json")
        self.assertEqual(expected["model_key"], "HUMAN_ONLY")
