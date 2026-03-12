from pathlib import Path
import asyncio
import json
import shutil
import tempfile
import unittest

import web.server as server
from estimations import EstimationStore, load_artifact_json


FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "estimation" / "v1"


class _FakeRequest:
    def __init__(self, *, payload: dict | None = None, path_params: dict | None = None):
        self._payload = payload or {}
        self.path_params = path_params or {}
        self.query_params = {}

    async def body(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")


class EstimationApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path(tempfile.mkdtemp(prefix="synthetix-estimation-api-"))
        self.original_store = server.ESTIMATION_STORE
        self.original_manager = server.MANAGER
        server.ESTIMATION_STORE = EstimationStore(self.temp_dir)

    def tearDown(self) -> None:
        server.ESTIMATION_STORE = self.original_store
        server.MANAGER = self.original_manager
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_create_and_fetch_brownfield_estimate(self):
        payload = {
            "mode": "brownfield",
            "run_id": "run_123",
            "estimate_id": "estimate_api",
            "business_need": "Modernize the brownfield application while preserving required business capability.",
            "team_model_key": "HUMAN_ONLY",
            "chunk_manifest": load_artifact_json(FIXTURE_ROOT / "input" / "chunk_manifest.json"),
            "risk_register": load_artifact_json(FIXTURE_ROOT / "input" / "risk_register.json"),
            "traceability_scores": load_artifact_json(FIXTURE_ROOT / "input" / "traceability_scores.json"),
        }
        create_resp = asyncio.run(server.api_create_estimate(_FakeRequest(payload=payload)))
        self.assertEqual(create_resp.status_code, 200)
        created = json.loads(create_resp.body)
        self.assertTrue(created["ok"])
        self.assertEqual(created["estimate_id"], "estimate_api")
        estimate = created["estimate_summary"]["estimate"]
        self.assertGreater(estimate["effort"]["total_hours"]["p50"], 500)
        self.assertTrue(estimate["proposed_team"])
        self.assertTrue(estimate["workstreams"])

        get_resp = asyncio.run(server.api_get_estimate(_FakeRequest(path_params={"estimate_id": "estimate_api"})))
        self.assertEqual(get_resp.status_code, 200)
        fetched = json.loads(get_resp.body)
        self.assertTrue(fetched["ok"])
        self.assertEqual(fetched["artifacts"]["estimate_summary"]["estimate"]["team_model_selected"], "HUMAN_ONLY")

        list_resp = asyncio.run(server.api_list_run_estimates(_FakeRequest(path_params={"run_id": "run_123"})))
        self.assertEqual(list_resp.status_code, 200)
        rows = json.loads(list_resp.body)["estimates"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["estimate_id"], "estimate_api")

    def test_create_brownfield_estimate_from_run_id_only(self):
        class _FakeManager:
            def get_run(self, run_id):
                if run_id != "run_from_stage1":
                    return None
                return {
                    "agent_results": [
                        {
                            "stage": 1,
                            "output": {
                                "chunk_manifest_v1": load_artifact_json(FIXTURE_ROOT / "input" / "chunk_manifest.json"),
                                "raw_artifacts": {
                                    "risk_register": load_artifact_json(FIXTURE_ROOT / "input" / "risk_register.json"),
                                    "chunk_qa_report_v1": {
                                        "chunks": [
                                            {"chunk_id": "chunk_000", "analyzed": True, "checks": [{"status": "PASS"}]},
                                            {"chunk_id": "chunk_001", "analyzed": True, "checks": [{"status": "WARN"}]},
                                            {"chunk_id": "chunk_002", "analyzed": False, "checks": []},
                                            {"chunk_id": "chunk_003", "analyzed": True, "checks": []},
                                            {"chunk_id": "chunk_004", "analyzed": True, "checks": [{"status": "PASS"}]},
                                            {"chunk_id": "chunk_005", "analyzed": True, "checks": [{"status": "PASS"}]},
                                        ]
                                    },
                                },
                            },
                        }
                    ]
                }

        server.MANAGER = _FakeManager()
        payload = {
            "mode": "brownfield",
            "run_id": "run_from_stage1",
            "estimate_id": "estimate_from_run",
            "business_need": "Modernize the brownfield application while preserving required business capability.",
            "team_model_key": "HUMAN_ONLY",
        }
        create_resp = asyncio.run(server.api_create_estimate(_FakeRequest(payload=payload)))
        self.assertEqual(create_resp.status_code, 200)
        created = json.loads(create_resp.body)
        self.assertTrue(created["ok"])
        self.assertEqual(created["estimate_id"], "estimate_from_run")
        self.assertEqual(created["run_id"], "run_from_stage1")
        self.assertGreater(created["estimate_summary"]["estimate"]["effort"]["total_hours"]["p50"], 0)

    def test_create_brownfield_estimate_from_persisted_stage_snapshot(self):
        class _FakeStore:
            def load_stage_snapshot(self, run_id, stage):
                if run_id != "run_from_blob" or stage != 1:
                    return None
                return {
                    "chunk_manifest_v1": load_artifact_json(FIXTURE_ROOT / "input" / "chunk_manifest.json"),
                    "raw_artifacts": {
                        "risk_register": load_artifact_json(FIXTURE_ROOT / "input" / "risk_register.json"),
                        "traceability_scores_v1": load_artifact_json(FIXTURE_ROOT / "input" / "traceability_scores.json"),
                    },
                }

        class _FakeManager:
            store = _FakeStore()

            def get_run(self, run_id):
                if run_id != "run_from_blob":
                    return None
                return {"run_id": run_id, "status": "completed"}

        server.MANAGER = _FakeManager()
        payload = {
            "mode": "brownfield",
            "run_id": "run_from_blob",
            "estimate_id": "estimate_from_blob",
            "business_need": "Modernize the brownfield application while preserving required business capability.",
            "team_model_key": "HUMAN_ONLY",
        }
        create_resp = asyncio.run(server.api_create_estimate(_FakeRequest(payload=payload)))
        self.assertEqual(create_resp.status_code, 200)
        created = json.loads(create_resp.body)
        self.assertTrue(created["ok"])
        self.assertEqual(created["estimate_id"], "estimate_from_blob")
        self.assertEqual(created["run_id"], "run_from_blob")
        self.assertGreater(created["estimate_summary"]["estimate"]["effort"]["total_hours"]["p50"], 0)

    def test_create_brownfield_estimate_from_run_landscape_chunk_manifest_only(self):
        class _FakeManager:
            def get_run(self, run_id):
                if run_id != "run_from_landscape":
                    return None
                return {
                    "run_id": run_id,
                    "status": "completed",
                    "integration_context": {
                        "discover_cache": {
                            "landscape": {
                                "chunk_manifest_v1": load_artifact_json(FIXTURE_ROOT / "input" / "chunk_manifest.json"),
                            }
                        }
                    },
                    "agent_results": [
                        {
                            "stage": 1,
                            "output": {},
                        }
                    ],
                }

        server.MANAGER = _FakeManager()
        payload = {
            "mode": "brownfield",
            "run_id": "run_from_landscape",
            "estimate_id": "estimate_from_landscape",
            "business_need": "Modernize the brownfield application while preserving required business capability.",
            "team_model_key": "HUMAN_ONLY",
        }
        create_resp = asyncio.run(server.api_create_estimate(_FakeRequest(payload=payload)))
        self.assertEqual(create_resp.status_code, 200)
        created = json.loads(create_resp.body)
        self.assertTrue(created["ok"])
        self.assertEqual(created["estimate_id"], "estimate_from_landscape")
        self.assertEqual(created["run_id"], "run_from_landscape")
        self.assertGreater(created["estimate_summary"]["estimate"]["effort"]["total_hours"]["p50"], 0)

    def test_create_brownfield_estimate_from_queued_request_analyst_summary_inventory_only(self):
        class _FakeManager:
            def get_run(self, run_id):
                if run_id != "run_from_queued_summary":
                    return None
                return {
                    "run_id": run_id,
                    "status": "queued",
                    "pipeline_state": {
                        "queued_request": {
                            "integration_context": {
                                "discover_cache": {
                                    "analyst_summary": {
                                        "legacy_code_inventory": {
                                            "source_files_scanned": 49,
                                            "source_loc_total": 9038,
                                        },
                                        "raw_artifacts": {
                                            "risk_register": {"risks": []},
                                        },
                                    }
                                }
                            }
                        }
                    },
                }

        server.MANAGER = _FakeManager()
        payload = {
            "mode": "brownfield",
            "run_id": "run_from_queued_summary",
            "estimate_id": "estimate_from_queued_summary",
            "business_need": "Modernize the brownfield application while preserving required business capability.",
            "team_model_key": "HUMAN_ONLY",
        }
        create_resp = asyncio.run(server.api_create_estimate(_FakeRequest(payload=payload)))
        self.assertEqual(create_resp.status_code, 200)
        created = json.loads(create_resp.body)
        self.assertTrue(created["ok"])
        self.assertEqual(created["estimate_id"], "estimate_from_queued_summary")
        self.assertEqual(created["run_id"], "run_from_queued_summary")
        self.assertGreater(created["estimate_summary"]["estimate"]["effort"]["total_hours"]["p50"], 0)

    def test_create_brownfield_estimate_prefers_pipeline_integration_discover_cache(self):
        class _FakeManager:
            def get_run(self, run_id):
                if run_id != "run_with_pipeline_discover_cache":
                    return None
                return {
                    "run_id": run_id,
                    "status": "completed",
                    "integration_context": {
                        "brownfield": {"repo_url": "https://example.com/repo.git"},
                    },
                    "pipeline_state": {
                        "integration_context": {
                            "discover_cache": {
                                "landscape": {
                                    "component_inventory_v1": {
                                        "snapshot_id": "snap_pipeline",
                                        "components": [
                                            {
                                                "component_id": "comp::1",
                                                "name": "Component A",
                                                "component_type": "vb6_project",
                                                "paths": ["ComponentA/main.frm"],
                                                "estimated_loc": 1200,
                                                "risk_flags": [],
                                            }
                                        ],
                                    }
                                }
                            }
                        }
                    },
                }

        server.MANAGER = _FakeManager()
        payload = {
            "mode": "brownfield",
            "run_id": "run_with_pipeline_discover_cache",
            "estimate_id": "estimate_pipeline_discover_cache",
            "business_need": "Modernize the brownfield application while preserving required business capability.",
            "team_model_key": "HUMAN_ONLY",
        }
        create_resp = asyncio.run(server.api_create_estimate(_FakeRequest(payload=payload)))
        self.assertEqual(create_resp.status_code, 200)
        created = json.loads(create_resp.body)
        self.assertTrue(created["ok"])
        self.assertEqual(created["estimate_id"], "estimate_pipeline_discover_cache")
        self.assertEqual(created["run_id"], "run_with_pipeline_discover_cache")
        self.assertGreater(created["estimate_summary"]["estimate"]["effort"]["total_hours"]["p50"], 0)

    def test_create_brownfield_estimate_from_run_component_inventory_only(self):
        class _FakeManager:
            def get_run(self, run_id):
                if run_id != "run_from_components":
                    return None
                return {
                    "run_id": run_id,
                    "status": "completed",
                    "integration_context": {
                        "discover_cache": {
                            "landscape": {
                                "component_inventory_v1": {
                                    "snapshot_id": "snap_components",
                                    "components": [
                                        {
                                            "component_id": "vb6::bank",
                                            "name": "BANK",
                                            "component_type": "vb6_project",
                                            "paths": ["BankApp1/BANK.vbp", "BankApp1/frmdeposit.frm", "BankApp1/Module1.bas"],
                                            "estimated_loc": 3200,
                                            "risk_flags": ["shared_module_review"],
                                        }
                                    ],
                                }
                            }
                        }
                    },
                    "agent_results": [
                        {
                            "stage": 1,
                            "output": {},
                        }
                    ],
                }

        server.MANAGER = _FakeManager()
        payload = {
            "mode": "brownfield",
            "run_id": "run_from_components",
            "estimate_id": "estimate_from_components",
            "business_need": "Modernize the brownfield application while preserving required business capability.",
            "team_model_key": "HUMAN_ONLY",
        }
        create_resp = asyncio.run(server.api_create_estimate(_FakeRequest(payload=payload)))
        self.assertEqual(create_resp.status_code, 200)
        created = json.loads(create_resp.body)
        self.assertTrue(created["ok"])
        self.assertEqual(created["estimate_id"], "estimate_from_components")
        self.assertEqual(created["run_id"], "run_from_components")
        self.assertGreater(created["estimate_summary"]["estimate"]["effort"]["total_hours"]["p50"], 0)

    def test_create_brownfield_estimate_from_pipeline_repo_snapshot_and_list_agent_results(self):
        chunk_manifest = load_artifact_json(FIXTURE_ROOT / "input" / "chunk_manifest.json")
        risk_register = load_artifact_json(FIXTURE_ROOT / "input" / "risk_register.json")

        class _FakeManager:
            store = object()

            def get_run(self, run_id):
                if run_id != "run_from_pipeline_snapshot":
                    return None
                return {
                    "run_id": run_id,
                    "status": "completed",
                    "integration_context": {
                        "repo_scan_cache": {
                            "repo_snapshot": {
                                "chunk_manifest_v1": chunk_manifest,
                            }
                        }
                    },
                    "pipeline_state": {
                        "repo_snapshot": {
                            "chunk_manifest_v1": chunk_manifest,
                        },
                        "analyst_output": {
                            "raw_artifacts": {
                                "risk_register": risk_register,
                            }
                        },
                        "agent_results": [
                            {
                                "stage": 1,
                                "output": {},
                            }
                        ],
                    },
                }

        server.MANAGER = _FakeManager()
        payload = {
            "mode": "brownfield",
            "run_id": "run_from_pipeline_snapshot",
            "estimate_id": "estimate_from_pipeline_snapshot",
            "business_need": "Modernize the brownfield application while preserving required business capability.",
            "team_model_key": "HUMAN_ONLY",
        }
        create_resp = asyncio.run(server.api_create_estimate(_FakeRequest(payload=payload)))
        self.assertEqual(create_resp.status_code, 200)
        created = json.loads(create_resp.body)
        self.assertTrue(created["ok"])
        self.assertEqual(created["estimate_id"], "estimate_from_pipeline_snapshot")
        self.assertEqual(created["run_id"], "run_from_pipeline_snapshot")
        self.assertGreater(created["estimate_summary"]["estimate"]["effort"]["total_hours"]["p50"], 0)

    def test_estimate_intake_endpoint(self):
        resp = asyncio.run(
            server.api_estimate_intake(
                _FakeRequest(
                    payload={
                        "mode": "brownfield",
                        "current": {},
                        "message": "Estimate modernization of run 20260311_123456_abcd1234 with an agent-assisted team.",
                    }
                )
            )
        )
        self.assertEqual(resp.status_code, 200)
        body = json.loads(resp.body)
        self.assertTrue(body["ok"])
        self.assertEqual(body["draft"]["run_id"], "20260311_123456_abcd1234")
        self.assertEqual(body["draft"]["team_model_key"], "HUMAN_LED_AGENT_ASSISTED")

    def test_estimate_explain_endpoint(self):
        payload = {
            "mode": "brownfield",
            "run_id": "run_123",
            "estimate_id": "estimate_api",
            "business_need": "Modernize the brownfield application while preserving required business capability.",
            "team_model_key": "HUMAN_ONLY",
            "chunk_manifest": load_artifact_json(FIXTURE_ROOT / "input" / "chunk_manifest.json"),
            "risk_register": load_artifact_json(FIXTURE_ROOT / "input" / "risk_register.json"),
            "traceability_scores": load_artifact_json(FIXTURE_ROOT / "input" / "traceability_scores.json"),
        }
        asyncio.run(server.api_create_estimate(_FakeRequest(payload=payload)))
        resp = asyncio.run(
            server.api_estimate_explain(
                _FakeRequest(
                    payload={"question": "Why is this estimate this large?", "wbs_item_id": "WBS-CHUNK_000"},
                    path_params={"estimate_id": "estimate_api"},
                )
            )
        )
        self.assertEqual(resp.status_code, 200)
        body = json.loads(resp.body)
        self.assertTrue(body["ok"])
        self.assertEqual(body["estimate_id"], "estimate_api")
        self.assertIn("deterministic brownfield kernel", body["response"]["answer"])


if __name__ == "__main__":
    unittest.main()
