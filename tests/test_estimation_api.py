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
        server.ESTIMATION_STORE = EstimationStore(self.temp_dir)

    def tearDown(self) -> None:
        server.ESTIMATION_STORE = self.original_store
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
        self.assertEqual(created["estimate_summary"]["estimate"]["effort"]["total_hours"]["p50"], 1379.2)

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


if __name__ == "__main__":
    unittest.main()
