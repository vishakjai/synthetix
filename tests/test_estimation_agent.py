import unittest

from estimations import EstimationAgent


class EstimationAgentTest(unittest.TestCase):
    def test_deterministic_intake_extracts_run_and_team_model(self):
        agent = EstimationAgent()
        result = agent.intake(
            message="Estimate modernization of run 20260311_123456_abcd1234 with a human-led agent-assisted team.",
            mode="brownfield",
            current={},
        )
        draft = result["draft"]
        self.assertEqual(draft["run_id"], "20260311_123456_abcd1234")
        self.assertEqual(draft["team_model_key"], "HUMAN_LED_AGENT_ASSISTED")
        self.assertEqual(draft["confidence_tier"], "PLANNING")

    def test_deterministic_explain_returns_grounded_summary(self):
        agent = EstimationAgent()
        bundle = {
            "estimate_summary": {
                "meta": {"artifact_id": "art_estimate_summary_demo"},
                "estimate": {
                    "effort": {"hours": {"p50": 120.0}},
                    "timeline": {"weeks": {"p50": 4.5}},
                    "team_model": {"key": "HUMAN_ONLY"},
                },
            },
            "assumption_ledger": {
                "assumptions": [{"id": "ASSUME-001"}, {"id": "ASSUME-002"}],
            },
            "wbs": {
                "wbs": {
                    "items": [
                        {
                            "wbs_item_id": "WBS-CHUNK_AUTH",
                            "title": "Migrate auth subsystem",
                            "size_tier": "M",
                            "effort_hours": {"p50": 72.0},
                        }
                    ]
                }
            },
        }
        result = agent.explain(
            estimate_bundle=bundle,
            question="Why is this four weeks?",
            wbs_item_id="WBS-CHUNK_AUTH",
        )
        response = result["response"]
        self.assertIn("120.0 hours", response["answer"])
        self.assertEqual(response["wbs_item_id"], "WBS-CHUNK_AUTH")
        self.assertIn("ASSUME-001", response["assumption_refs"])


if __name__ == "__main__":
    unittest.main()
