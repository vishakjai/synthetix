import asyncio
import io
import json
import unittest
from unittest.mock import Mock
from zipfile import ZipFile

from agents.architect import ArchitectAgent
from tests.test_architect_handoff import ArchitectHandoffPackageTest, _FakeRequest
from utils.architect_hld_docx import build_architect_hld_docx_bytes
import web.server as server


class ArchitectHldTest(unittest.TestCase):
    def _normalized(self):
        return ArchitectAgent(Mock())._normalize_output({"legacy_system": {}}, ArchitectHandoffPackageTest()._state())

    def test_architect_package_includes_hld_documents_and_hashes(self):
        normalized = self._normalized()
        package = normalized.get("architect_package", {})
        hld_documents = package.get("hld_documents", {})
        self.assertEqual(hld_documents.get("generation_status"), "generated")
        self.assertTrue(package.get("package_meta", {}).get("artifact_hashes"))
        legacy = hld_documents.get("legacy_hld", {})
        target = hld_documents.get("target_hld", {})
        self.assertTrue(str(legacy.get("docx_path", "")).endswith(".docx"))
        self.assertGreaterEqual(int(legacy.get("known_unknown_count", 0) or 0), 2)
        self.assertEqual(int(target.get("delta_map_row_count", 0) or 0), len(package.get("artifacts", {}).get("traceability_matrix", {}).get("mappings", [])))
        self.assertEqual(int(target.get("adr_references", 0) or 0), len(package.get("artifacts", {}).get("architecture_decision_records", [])))
        principle_section = next(section for section in target.get("render_payload", {}).get("sections", []) if section.get("title") == "2. Architectural Principles")
        principle_rows = principle_section.get("tables", [])[0].get("rows", [])
        self.assertEqual(len(principle_rows), len(package.get("artifacts", {}).get("architecture_decision_records", [])))

    def test_architect_hld_docx_renderer_emits_valid_docx_package(self):
        normalized = self._normalized()
        payload = normalized.get("architect_package", {}).get("hld_documents", {}).get("legacy_hld", {}).get("render_payload", {})
        docx_bytes = build_architect_hld_docx_bytes(payload)
        self.assertTrue(docx_bytes)
        with ZipFile(io.BytesIO(docx_bytes)) as zf:
            names = set(zf.namelist())
        self.assertIn("word/document.xml", names)
        self.assertIn("[Content_Types].xml", names)


class ArchitectHldApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.original_manager = server.MANAGER

    def tearDown(self) -> None:
        server.MANAGER = self.original_manager

    def test_download_architect_hld_docx_from_persisted_stage_snapshot(self):
        normalized = ArchitectAgent(Mock())._normalize_output({"legacy_system": {}}, ArchitectHandoffPackageTest()._state())
        architect_package = normalized.get("architect_package", {})

        class _FakeStore:
            def load_stage_snapshot(self, run_id, stage):
                if run_id != "run_hld" or stage != 2:
                    return None
                return {"output": {"architect_package": architect_package}}

        class _FakeManager:
            store = _FakeStore()

            def _hydrate_record(self, run_id):
                return {"run_id": run_id, "pipeline_state": {}}

            def get_run(self, run_id):
                return {"run_id": run_id, "pipeline_state": {}}

        server.MANAGER = _FakeManager()
        response = asyncio.run(
            server.api_download_architect_hld_docx(_FakeRequest(path_params={"run_id": "run_hld"}, query_params={"type": "legacy"}))
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn("attachment;", response.headers.get("content-disposition", "").lower())


if __name__ == "__main__":
    unittest.main()
