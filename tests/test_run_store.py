import unittest
from unittest.mock import Mock

from agents.architect import ArchitectAgent
from tests.test_architect_handoff import ArchitectHandoffPackageTest
from utils.run_store import _compact_pipeline_state


class RunStoreCompactionTest(unittest.TestCase):
    def test_compaction_summarizes_architect_output_instead_of_storing_full_hld_payload(self):
        state = ArchitectHandoffPackageTest()._state()
        normalized = ArchitectAgent(Mock())._normalize_output({"legacy_system": {}}, state)
        pipeline_state = {
            "run_id": "run_compact",
            "workflow_state": "ARCHITECTING",
            "integration_context": {"project_state_mode": "brownfield"},
            "architect_output": {
                "architect_package": normalized.get("architect_package", {}),
                "architect_handoff_package": normalized.get("architect_handoff_package", {}),
            },
        }

        compact = _compact_pipeline_state(pipeline_state)

        self.assertEqual(compact.get("run_id"), "run_compact")
        self.assertEqual(compact.get("integration_context"), {"project_state_mode": "brownfield"})
        self.assertIn("architect_output_summary", compact)
        self.assertNotIn("architect_output", compact)
        summary_keys = set(compact.get("architect_output_summary", {}).get("keys", []))
        self.assertIn("architect_package", summary_keys)
        self.assertIn("architect_handoff_package", summary_keys)


if __name__ == "__main__":
    unittest.main()
