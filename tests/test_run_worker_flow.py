import unittest
from unittest.mock import Mock

from web.server import PipelineRunManager


class RunWorkerFlowTest(unittest.TestCase):
    def test_launch_deferred_run_keeps_waiting_approval_runs_stable(self):
        store = Mock()
        store.load_run.return_value = {
            "pipeline_status": "waiting_approval",
            "pipeline_state": {
                "pending_approval": {
                    "type": "developer_plan",
                    "stage": 3,
                    "message": "Review developer plan and approve with selected options.",
                }
            },
            "stage_status": {"1": "success", "2": "success", "3": "waiting_approval"},
            "saved_at": "2026-03-14T00:00:00+00:00",
        }
        store.finalize_run = Mock()
        manager = PipelineRunManager(store)

        result = manager.launch_deferred_run("run_waiting")

        self.assertEqual(result, {"ok": True, "status": "waiting_approval", "run_id": "run_waiting"})
        store.finalize_run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
